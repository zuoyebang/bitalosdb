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
	"fmt"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type FileNum uint32

func (fn FileNum) String() string { return fmt.Sprintf("%d", fn) }

type FileType int

const (
	fileTypeManifest FileType = iota
	fileTypeFileNumMap
	fileTypeFileNumMapTmp
	fileTypeCompactLog
	fileTypeTable
	fileTypeManifestTemp
)

func MakeFilename(fileType FileType, fileNum FileNum) string {
	switch fileType {
	case fileTypeManifest:
		return "BITHASHMANIFEST"
	case fileTypeManifestTemp:
		return "BITHASHMANIFESTTEMP"
	case fileTypeFileNumMap:
		return "FILENUMMAP"
	case fileTypeFileNumMapTmp:
		return "FILENUMMAPTMP"
	case fileTypeCompactLog:
		return "COMPACTLOG"
	case fileTypeTable:
		return fmt.Sprintf("%s.bht", fileNum)
	default:
		return ""
	}
}

func MakeFilepath(fs vfs.FS, dirname string, fileType FileType, fileNum FileNum) string {
	switch fileType {
	case fileTypeManifest:
		return fs.PathJoin(dirname, "BITHASHMANIFEST")
	case fileTypeManifestTemp:
		return fs.PathJoin(dirname, "BITHASHMANIFESTTEMP")
	case fileTypeFileNumMap:
		return fs.PathJoin(dirname, "FILENUMMAP")
	case fileTypeFileNumMapTmp:
		return fs.PathJoin(dirname, "FILENUMMAPTMP")
	case fileTypeCompactLog:
		return fs.PathJoin(dirname, "COMPACTLOG")
	case fileTypeTable:
		return fs.PathJoin(dirname, MakeFilename(fileType, fileNum))
	default:
		return ""
	}
}

func ParseFilename(fs vfs.FS, filename string) (fileType FileType, fileNum FileNum, ok bool) {
	filename = fs.PathBase(filename)
	switch filename {
	case "BITHASHMANIFEST":
		return fileTypeManifest, 0, true
	case "BITHASHMANIFESTTEMP":
		return fileTypeManifestTemp, 0, true
	case "FILENUMMAP":
		return fileTypeFileNumMap, 0, true
	case "FILENUMMAPTMP":
		return fileTypeFileNumMapTmp, 0, true
	case "COMPACTLOG":
		return fileTypeCompactLog, 0, true
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
		case "bht":
			return fileTypeTable, fileNum, true
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
