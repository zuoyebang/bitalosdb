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

package base

import (
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type Cleaner interface {
	Clean(fs vfs.FS, fileType FileType, path string) error
}

type NeedsFileContents interface {
	needsFileContents()
}

type DeleteCleaner struct{}

func (DeleteCleaner) Clean(fs vfs.FS, fileType FileType, path string) error {
	return fs.Remove(path)
}

func (DeleteCleaner) String() string {
	return "delete"
}

type ArchiveCleaner struct{}

var _ NeedsFileContents = ArchiveCleaner{}

func (ArchiveCleaner) Clean(fs vfs.FS, fileType FileType, path string) error {
	switch fileType {
	case FileTypeLog:
		destDir := fs.PathJoin(fs.PathDir(path), "archive")

		if err := fs.MkdirAll(destDir, 0755); err != nil {
			return err
		}

		destPath := fs.PathJoin(destDir, fs.PathBase(path))
		return fs.Rename(path, destPath)

	default:
		return fs.Remove(path)
	}
}

func (ArchiveCleaner) String() string {
	return "archive"
}

func (ArchiveCleaner) needsFileContents() {
}
