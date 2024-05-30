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

package base

import (
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

func TestParseFilename(t *testing.T) {
	testCases := map[string]bool{
		"000000.log":             true,
		"000000.log.zip":         false,
		"000000..log":            false,
		"a000000.log":            false,
		"abcdef.log":             false,
		"000001ldb":              false,
		"000001.sst":             true,
		"CURRENT":                true,
		"CURRaNT":                false,
		"LOCK":                   true,
		"xLOCK":                  false,
		"x.LOCK":                 false,
		"MANIFEST":               false,
		"MANIFEST123456":         false,
		"MANIFEST-":              false,
		"MANIFEST-123456":        true,
		"MANIFEST-123456.doc":    false,
		"OPTIONS":                false,
		"OPTIONS123456":          false,
		"OPTIONS-":               false,
		"OPTIONS-123456":         true,
		"OPTIONS-123456.doc":     false,
		"CURRENT.123456":         false,
		"CURRENT.dbtmp":          false,
		"CURRENT.123456.dbtmp":   true,
		"temporary.123456.dbtmp": true,
	}
	fs := vfs.NewMem()
	for tc, want := range testCases {
		_, _, got := ParseFilename(fs, fs.PathJoin("foo", tc))
		if got != want {
			t.Errorf("%q: got %v, want %v", tc, got, want)
		}
	}
}

func TestFilenameRoundTrip(t *testing.T) {
	testCases := map[FileType]bool{
		FileTypeCurrent:  false,
		FileTypeLock:     false,
		FileTypeLog:      true,
		FileTypeManifest: true,
	}
	fs := vfs.NewMem()
	for fileType, numbered := range testCases {
		fileNums := []FileNum{0}
		if numbered {
			fileNums = []FileNum{0, 1, 2, 3, 10, 42, 99, 1001}
		}
		for _, fileNum := range fileNums {
			filename := MakeFilepath(fs, "foo", fileType, fileNum)
			gotFT, gotFN, gotOK := ParseFilename(fs, filename)
			if !gotOK {
				t.Errorf("could not parse %q", filename)
				continue
			}
			if gotFT != fileType || gotFN != fileNum {
				t.Errorf("filename=%q: got %v, %v, want %v, %v", filename, gotFT, gotFN, fileType, fileNum)
				continue
			}
		}
	}
}
