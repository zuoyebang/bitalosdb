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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type FileNum uint64

func (fn FileNum) String() string { return fmt.Sprintf("%06d", fn) }

type FileType int

const (
	FileTypeLog FileType = iota
	FileTypeLock
	FileTypeManifest
	FileTypeCurrent
	FileTypeMeta
)

const (
	BitreeFilePrefix  = "bitree."
	BitpageFilePrefix = "bitpage."
	BithashPathPrefix = "bithash."
	BitablePathPrefix = "bitable."
	WalPathPrefix     = "wal."
)

func MakeFilename(fileType FileType, fileNum FileNum) string {
	switch fileType {
	case FileTypeLog:
		return fmt.Sprintf("%s.log", fileNum)
	case FileTypeLock:
		return "LOCK"
	case FileTypeManifest:
		return fmt.Sprintf("MANIFEST-%s", fileNum)
	case FileTypeMeta:
		return "BITALOSMETA"
	case FileTypeCurrent:
		return "CURRENT"
	}
	panic("unreachable")
}

func MakeFilepath(fs vfs.FS, dirname string, fileType FileType, fileNum FileNum) string {
	return fs.PathJoin(dirname, MakeFilename(fileType, fileNum))
}

func MakeBitreeFilepath(dir string, i int) string {
	return filepath.Join(dir, BitreeFilePrefix+strconv.Itoa(i))
}

func MakeBitpagepath(dir string, i int) string {
	return filepath.Join(dir, BitpageFilePrefix+strconv.Itoa(i))
}

func MakeBithashpath(dir string, i int) string {
	return filepath.Join(dir, BithashPathPrefix+strconv.Itoa(i))
}

func MakeBitablepath(dir string, i int) string {
	return filepath.Join(dir, BitablePathPrefix+strconv.Itoa(i))
}

func MakeWalpath(dir string, i int) string {
	return filepath.Join(dir, WalPathPrefix+strconv.Itoa(i))
}

func ParseFilename(fs vfs.FS, filename string) (fileType FileType, fileNum FileNum, ok bool) {
	filename = fs.PathBase(filename)
	switch {
	case filename == "CURRENT":
		return FileTypeCurrent, 0, true
	case filename == "LOCK":
		return FileTypeLock, 0, true
	case filename == "BITALOSMETA":
		return FileTypeMeta, 0, true
	case strings.HasPrefix(filename, "MANIFEST-"):
		fileNum, ok = parseFileNum(filename[len("MANIFEST-"):])
		if !ok {
			break
		}
		return FileTypeManifest, fileNum, true
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}
		fileNum, ok = parseFileNum(filename[:i])
		if !ok {
			break
		}
		switch filename[i+1:] {
		case "log":
			return FileTypeLog, fileNum, true
		}
	}
	return 0, fileNum, false
}

func parseFileNum(s string) (fileNum FileNum, ok bool) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fileNum, false
	}
	return FileNum(u), true
}

func GetFilePathBase(path string) string {
	if path == "" {
		return "."
	}

	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[0 : len(path)-1]
	}

	pos := strings.LastIndex(path, "/")
	if pos >= 0 {
		pos = strings.LastIndex(path[:pos], "/")
		if pos >= 0 {
			path = path[pos+1:]
		}
	}

	if path == "" {
		return "/"
	}
	return path
}
