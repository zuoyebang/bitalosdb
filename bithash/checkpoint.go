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

package bithash

import (
	"os"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

func (b *Bithash) Checkpoint(destDir string) (err error) {
	if _, err = os.Stat(destDir); !oserror.IsNotExist(err) {
		return errors.Errorf("bithash: checkpoint dir exist %s", destDir)
	}

	var dir vfs.File
	err = os.Mkdir(destDir, 0755)
	if err != nil {
		return err
	}
	dir, err = b.fs.OpenDir(destDir)
	if err != nil {
		return err
	}

	if err = b.closeMutableWriters(); err != nil {
		return err
	}

	{
		manifest := MakeFilepath(b.fs, b.dirname, fileTypeManifest, 0)
		err = vfs.Copy(b.fs, manifest, MakeFilepath(b.fs, destDir, fileTypeManifest, 0))
		if err != nil {
			return err
		}

		fileNumMap := MakeFilepath(b.fs, b.dirname, fileTypeFileNumMap, 0)
		err = vfs.Copy(b.fs, fileNumMap, MakeFilepath(b.fs, destDir, fileTypeFileNumMap, 0))
		if err != nil {
			return err
		}

		compactLog := MakeFilepath(b.fs, b.dirname, fileTypeCompactLog, 0)
		err = vfs.Copy(b.fs, compactLog, MakeFilepath(b.fs, destDir, fileTypeCompactLog, 0))
		if err != nil {
			return err
		}
	}

	copyTable := func() error {
		b.meta.mu.RLock()
		defer b.meta.mu.RUnlock()
		for fn := range b.meta.mu.filesMeta {
			filename := MakeFilepath(b.fs, b.dirname, fileTypeTable, fn)
			if _, err = b.fs.Stat(filename); err != nil {
				return err
			}
			err = vfs.LinkOrCopy(b.fs, filename, path.Join(destDir, b.fs.PathBase(filename)))
			if err != nil {
				return err
			}
		}
		return nil
	}
	err = copyTable()
	if err != nil {
		return err
	}

	err = dir.Sync()
	if err != nil {
		return err
	}
	err = dir.Close()
	dir = nil

	return err
}
