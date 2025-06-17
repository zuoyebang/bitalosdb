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
	"bytes"
	"io"
	"os"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/errors"
	"github.com/zuoyebang/bitalosdb/internal/manifest"
	"github.com/zuoyebang/bitalosdb/internal/record"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
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

func (ms *metaSet) updateManifest() error {
	manifestFileNum, exists, err := findCurrentManifest(ms.opts.FS, ms.dirname)
	if !exists {
		ms.opts.Logger.Info("no need to update manifest")
		return err
	}

	manifestPath := base.MakeFilepath(ms.opts.FS, ms.dirname, fileTypeManifest, manifestFileNum)
	manifestFilename := ms.opts.FS.PathBase(manifestPath)
	manifestFile, err := ms.opts.FS.Open(manifestPath)
	if err != nil {
		return err
	}
	defer manifestFile.Close()
	rr := record.NewReader(manifestFile, 0)
	for {
		r, err := rr.Next()
		if err == io.EOF || record.IsInvalidRecord(err) {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "bitalosdb: error when loading manifest file %s", manifestFilename)
		}
		var ve manifest.VersionEdit
		err = ve.Decode(r)
		if err != nil {
			if err == io.EOF || record.IsInvalidRecord(err) {
				break
			}
			return err
		}

		if ve.LastSeqNum != 0 {
			ms.atomic.logSeqNum = ve.LastSeqNum + 1
		}
	}

	currentFilename := base.MakeFilepath(ms.opts.FS, ms.dirname, fileTypeCurrent, 0)
	if err = ms.opts.FS.Remove(currentFilename); err != nil {
		return err
	}

	return nil
}

func findCurrentManifest(
	fs vfs.FS, dirname string,
) (manifestNum FileNum, exists bool, err error) {
	manifestNum, err = readCurrentFile(fs, dirname)
	if os.IsNotExist(err) {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}
	return manifestNum, true, nil
}

func readCurrentFile(fs vfs.FS, dirname string) (FileNum, error) {
	current, err := fs.Open(base.MakeFilepath(fs, dirname, fileTypeCurrent, 0))
	if err != nil {
		return 0, errors.Wrapf(err, "bitalosdb: could not open CURRENT file for DB %q", dirname)
	}
	defer current.Close()
	stat, err := current.Stat()
	if err != nil {
		return 0, err
	}
	n := stat.Size()
	if n == 0 {
		return 0, errors.Errorf("bitalosdb: CURRENT file for DB %q is empty", dirname)
	}
	if n > 4096 {
		return 0, errors.Errorf("bitalosdb: CURRENT file for DB %q is too large", dirname)
	}
	b := make([]byte, n)
	_, err = current.ReadAt(b, 0)
	if err != nil {
		return 0, err
	}
	if b[n-1] != '\n' {
		return 0, base.CorruptionErrorf("bitalosdb: CURRENT file for DB %q is malformed", dirname)
	}
	b = bytes.TrimSpace(b)

	_, manifestFileNum, ok := base.ParseFilename(fs, string(b))
	if !ok {
		return 0, base.CorruptionErrorf("bitalosdb: MANIFEST name %v is malformed", b)
	}
	return manifestFileNum, nil
}
