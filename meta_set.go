// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitalosdb

import (
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/manifest"
)

type metaEdit = manifest.MetaEdit

type metaSet struct {
	atomic struct {
		logSeqNum     uint64
		visibleSeqNum uint64
	}

	minUnflushedLogNum FileNum
	nextFileNum        FileNum
	dirname            string
	opts               *Options
	meta               *manifest.Metadata
}

func (ms *metaSet) init(dirname string, opts *Options) error {
	manifestPath := base.MakeFilepath(opts.FS, dirname, fileTypeMeta, 0)
	meta, err := manifest.NewMetadata(manifestPath, opts.FS)
	if err != nil {
		return err
	}

	ms.dirname = dirname
	ms.opts = opts
	ms.meta = meta
	ms.nextFileNum = 1

	if meta.MinUnflushedLogNum != 0 {
		ms.minUnflushedLogNum = meta.MinUnflushedLogNum
	}
	if meta.NextFileNum != 0 {
		ms.nextFileNum = meta.NextFileNum
	}
	if meta.LastSeqNum != 0 {
		ms.atomic.logSeqNum = meta.LastSeqNum + 1
	}

	ms.markFileNumUsed(ms.minUnflushedLogNum)

	return nil
}

func (ms *metaSet) markFileNumUsed(fileNum FileNum) {
	if ms.nextFileNum <= fileNum {
		ms.nextFileNum = fileNum + 1
	}
}

func (ms *metaSet) getNextFileNum() FileNum {
	x := ms.nextFileNum
	ms.nextFileNum++
	return x
}

func (ms *metaSet) apply(me *metaEdit) error {
	if me.MinUnflushedLogNum != 0 {
		if me.MinUnflushedLogNum < ms.minUnflushedLogNum || ms.nextFileNum <= me.MinUnflushedLogNum {
			return errors.Errorf("inconsistent metaEdit minUnflushedLogNum %s", me.MinUnflushedLogNum)
		}
	}

	me.NextFileNum = ms.nextFileNum
	logSeqNum := atomic.LoadUint64(&ms.atomic.logSeqNum)
	me.LastSeqNum = logSeqNum - 1
	if logSeqNum == 0 {
		ms.opts.Logger.Errorf("logSeqNum must be a positive integer: %d", logSeqNum)
	}

	ms.meta.WriteMetaEdit(me)

	if me.MinUnflushedLogNum != 0 {
		ms.minUnflushedLogNum = me.MinUnflushedLogNum
	}

	return nil
}

func (ms *metaSet) close() error {
	err := ms.meta.Close()
	return err
}
