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

package bitpage

import (
	"os"
	"path/filepath"

	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

func (b *Bitpage) Checkpoint(fs vfs.FS, dstDir string) (err error) {
	var dir vfs.File
	var files []string

	if err = os.Mkdir(dstDir, 0755); err != nil {
		return err
	}
	dir, err = fs.OpenDir(dstDir)
	if err != nil {
		return err
	}

	b.pages.Range(func(k, v interface{}) bool {
		if b.IsPageFreed(k.(PageNum)) {
			return true
		}

		p := v.(*page)
		if p.stMutable != nil && !p.stMutable.empty() {
			p.stMutable.writeIdxToFile()
			if err = p.makeMutableForWrite(); err != nil {
				return false
			}
		}

		for _, st := range p.stQueue {
			if !st.empty() {
				stFiles := st.getFilePath()
				files = append(files, stFiles...)
			}
		}

		if p.arrtable != nil {
			atFiles := p.arrtable.getFilePath()
			files = append(files, atFiles...)
		}

		return true
	})
	if err != nil {
		return err
	}

	for _, file := range files {
		if !os2.IsExist(file) {
			continue
		}
		dstFile := filepath.Join(dstDir, fs.PathBase(file))
		if err = vfs.LinkOrCopy(fs, file, dstFile); err != nil {
			return err
		}
	}

	metaFile := makeFilepath(b.dirname, fileTypeManifest, 0, 0)
	metaNewFile := makeFilepath(dstDir, fileTypeManifest, 0, 0)
	if err = vfs.Copy(fs, metaFile, metaNewFile); err != nil {
		return err
	}

	err = dir.Sync()
	if err != nil {
		return err
	}
	return dir.Close()
}
