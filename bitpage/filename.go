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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalosdb/internal/utils"
)

type FileNum uint32

func (fn FileNum) String() string { return fmt.Sprintf("%d", fn) }

type PageNum uint32

func (fn PageNum) String() string { return fmt.Sprintf("%d", fn) }

func (fn PageNum) ToByte() []byte { return utils.Uint32ToBytes(uint32(fn)) }

type FileType int

const (
	fileTypeManifest FileType = iota
	fileTypeSklTable
	fileTypeSuperTable
	fileTypeSuperTableIndex
	fileTypeArrayTable
)

const (
	manifestFileName = "BITPAGEMANIFEST"
)

func makeFilename(fileType FileType, pageNum PageNum, fileNum FileNum) string {
	switch fileType {
	case fileTypeManifest:
		return manifestFileName
	case fileTypeSklTable:
		return fmt.Sprintf("%s_%s.st", pageNum, fileNum)
	case fileTypeSuperTable:
		return fmt.Sprintf("%s_%s.xt", pageNum, fileNum)
	case fileTypeSuperTableIndex:
		return fmt.Sprintf("%s_%s.xti", pageNum, fileNum)
	case fileTypeArrayTable:
		return fmt.Sprintf("%s_%s.at", pageNum, fileNum)
	default:
		return ""
	}
}

func makeFilepath(dirname string, fileType FileType, pageNum PageNum, fileNum FileNum) string {
	switch fileType {
	case fileTypeManifest:
		return filepath.Join(dirname, manifestFileName)
	case fileTypeSklTable, fileTypeSuperTable, fileTypeSuperTableIndex, fileTypeArrayTable:
		return filepath.Join(dirname, makeFilename(fileType, pageNum, fileNum))
	default:
		return ""
	}
}

func parseFilename(filename string) (fileType FileType, pageNum PageNum, fileNum FileNum, ok bool) {
	filename = filepath.Base(filename)
	switch filename {
	case manifestFileName:
		return fileTypeManifest, 0, 0, true
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}

		nums := strings.Split(filename[:i], "_")
		if len(nums) != 2 {
			break
		}

		pn, err := strconv.ParseUint(nums[0], 10, 64)
		if err != nil {
			break
		}

		fn, err := strconv.ParseUint(nums[1], 10, 64)
		if err != nil {
			break
		}

		pageNum = PageNum(pn)
		fileNum = FileNum(fn)

		switch filename[i+1:] {
		case "st":
			return fileTypeSklTable, pageNum, fileNum, true
		case "xt":
			return fileTypeSuperTable, pageNum, fileNum, true
		case "xti":
			return fileTypeSuperTableIndex, pageNum, fileNum, true
		case "at":
			return fileTypeArrayTable, pageNum, fileNum, true
		}
	}

	return 0, pageNum, fileNum, false
}
