// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
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

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/manifest"
)

type metaEdit = manifest.MetaEditor
type bitowerMetaEditor = manifest.BitowerMetaEditor

type metaSet struct {
	atomic struct {
		logSeqNum     uint64
		visibleSeqNum uint64
	}

	dirname string
	opts    *Options
	meta    *manifest.Metadata
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
	ms.atomic.logSeqNum = 1
	if meta.LastSeqNum != 0 {
		ms.atomic.logSeqNum = meta.LastSeqNum + 1
	}

	return nil
}

func (ms *metaSet) apply(sme *bitowerMetaEditor) error {
	me := &metaEdit{}
	logSeqNum := atomic.LoadUint64(&ms.atomic.logSeqNum)
	me.LastSeqNum = logSeqNum - 1

	ms.meta.Write(me, sme)

	return nil
}

func (ms *metaSet) close() error {
	err := ms.meta.Close()
	return err
}
