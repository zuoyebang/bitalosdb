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

package bitpage

import (
	"os"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

func (b *Bitpage) Checkpoint(dstDir string) (err error) {
	if _, err = os.Stat(dstDir); !oserror.IsNotExist(err) {
		return errors.Errorf("bitpage: checkpoint dir exist %s", dstDir)
	}

	var dir vfs.File
	var files []string

	err = os.Mkdir(dstDir, 0755)
	if err != nil {
		return err
	}
	dir, err = b.opts.FS.OpenDir(dstDir)
	if err != nil {
		return err
	}

	b.pages.Range(func(k, v interface{}) bool {
		pn := k.(PageNum)
		p := v.(*page)
		if b.PageSplitted(pn) {
			return true
		}

		p.mu.Lock()
		defer p.mu.Unlock()

		if p.mu.stMutable != nil {
			if err = p.makeMutableForWrite(true); err != nil {
				return false
			}
		}

		for i := range p.mu.stQueue {
			if !p.mu.stQueue[i].empty() {
				files = append(files, p.mu.stQueue[i].path())
				idxPath := p.mu.stQueue[i].idxFilePath()
				if utils.IsFileExist(idxPath) {
					files = append(files, idxPath)
				}
			}
		}

		if p.mu.arrtable != nil {
			files = append(files, p.mu.arrtable.path())
		}

		return true
	})
	if err != nil {
		return err
	}

	for i := range files {
		newname := path.Join(dstDir, b.opts.FS.PathBase(files[i]))
		if err = vfs.LinkOrCopy(b.opts.FS, files[i], newname); err != nil {
			return err
		}
		b.opts.Logger.Infof("bitpage checkpoint save %s to %s", files[i], newname)
	}

	metaFile := makeFilepath(b.dirname, fileTypeManifest, 0, 0)
	metaNewFile := makeFilepath(dstDir, fileTypeManifest, 0, 0)
	if err = vfs.Copy(b.opts.FS, metaFile, metaNewFile); err != nil {
		return err
	}
	b.opts.Logger.Infof("bitpage checkpoint save %s to %s", metaFile, metaNewFile)

	err = dir.Sync()
	if err != nil {
		return err
	}
	return dir.Close()
}
