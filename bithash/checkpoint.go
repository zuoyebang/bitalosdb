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

package bithash

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

func (b *Bithash) Checkpoint(fs vfs.FS, destDir string) (err error) {
	if _, err = os.Stat(destDir); !oserror.IsNotExist(err) {
		return errors.Errorf("bithash: checkpoint dir exist %s", destDir)
	}

	var dir vfs.File
	err = os.Mkdir(destDir, 0755)
	if err != nil {
		return err
	}
	dir, err = fs.OpenDir(destDir)
	if err != nil {
		return err
	}

	if err = b.closeMutableWriters(); err != nil {
		return err
	}

	manifest := MakeFilepath(fs, b.dirname, fileTypeManifest, 0)
	destManifest := MakeFilepath(fs, destDir, fileTypeManifest, 0)
	err = vfs.Copy(fs, manifest, destManifest)
	if err != nil {
		return err
	}

	fileNumMap := MakeFilepath(fs, b.dirname, fileTypeFileNumMap, 0)
	destFileNumMap := MakeFilepath(fs, destDir, fileTypeFileNumMap, 0)
	if err = vfs.Copy(fs, fileNumMap, destFileNumMap); err != nil {
		return err
	}

	compactLog := MakeFilepath(fs, b.dirname, fileTypeCompactLog, 0)
	destCompactLog := MakeFilepath(fs, destDir, fileTypeCompactLog, 0)
	if err = vfs.Copy(fs, compactLog, destCompactLog); err != nil {
		return err
	}

	copyTable := func() error {
		b.meta.mu.RLock()
		defer b.meta.mu.RUnlock()
		for fn := range b.meta.mu.filesMeta {
			filename := MakeFilepath(fs, b.dirname, fileTypeTable, fn)
			if _, err = fs.Stat(filename); err != nil {
				return err
			}
			err = vfs.LinkOrCopy(fs, filename, filepath.Join(destDir, fs.PathBase(filename)))
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
