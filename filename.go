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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
)

type FileType = base.FileType

const (
	fileTypeManifest FileType = iota
	fileTypeVectorTable
)

func makeFilename(fileType FileType, tupleNum uint32) string {
	switch fileType {
	case fileTypeVectorTable:
		return fmt.Sprintf("%d", tupleNum)
	default:
		return ""
	}
}

func parseFilename(filename string) (fileType FileType, tupleNum uint32, ok bool) {
	filename = filepath.Base(filename)
	i := strings.IndexByte(filename, '.')
	if i < 0 {
		return 0, 0, false
	}
	tn, err := strconv.ParseUint(filename[:i], 10, 64)
	if err != nil {
		return 0, 0, false
	}

	tupleNum = uint32(tn)
	return fileTypeVectorTable, tupleNum, true
}
