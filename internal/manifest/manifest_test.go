// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/mmap"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

const testDir = "./test-data"

var testFS = vfs.Default

func testInitDir() {
	_, err := os.Stat(testDir)
	if os.IsNotExist(err) {
		os.MkdirAll(testDir, 0775)
	}
}

func TestMetaWrite(t *testing.T) {
	defer os.RemoveAll(testDir)
	testInitDir()
	metaFile := filepath.Join(testDir, "manifest")
	meta, err := NewMetadata(metaFile, testFS)
	require.NoError(t, err)

	require.Equal(t, metaVersion2, meta.header.version)

	require.Equal(t, uint8(0), meta.GetFieldFlushedBitable())
	meta.SetFieldFlushedBitable()
	require.Equal(t, uint8(1), meta.GetFieldFlushedBitable())

	num := 10
	seqNum := uint64(0)
	for i := range meta.Bmes {
		for j := 0; j < num; j++ {
			fn := meta.Bmes[i].GetNextFileNum()
			require.Equal(t, FileNum(j), fn)
			seqNum++
		}

		me := &MetaEditor{
			LastSeqNum: seqNum,
		}
		sme := &BitowerMetaEditor{
			Index:              i,
			NextFileNum:        meta.Bmes[i].NextFileNum,
			MinUnflushedLogNum: meta.Bmes[i].NextFileNum - 1,
		}
		meta.Write(me, sme)

		require.Equal(t, FileNum(num), meta.Bmes[i].NextFileNum)
		require.Equal(t, FileNum(num-1), meta.Bmes[i].MinUnflushedLogNum)
	}

	require.NoError(t, meta.Close())
	meta, err = NewMetadata(metaFile, testFS)
	require.NoError(t, err)
	require.Equal(t, metaVersion2, meta.header.version)
	require.Equal(t, seqNum, meta.LastSeqNum)
	require.Equal(t, uint8(1), meta.GetFieldFlushedBitable())
	for i := range meta.Bmes {
		require.Equal(t, FileNum(num), meta.Bmes[i].NextFileNum)
		require.Equal(t, FileNum(num-1), meta.Bmes[i].MinUnflushedLogNum)
	}

	require.NoError(t, meta.Close())
}

func TestMetaUpdateV1toV2(t *testing.T) {
	defer os.RemoveAll(testDir)
	testInitDir()
	filename := filepath.Join(testDir, "manifest")

	m := &Metadata{fs: testFS}
	require.NoError(t, m.create(filename))
	file, err := mmap.Open(filename, 0)
	require.NoError(t, err)
	file.WriteUInt16At(metaVersion1, metaHeaderOffset)
	file.WriteUInt64At(10, fieldOffsetMinUnflushedLogNumV1)
	file.WriteUInt64At(20, fieldOffsetNextFileNumV1)
	file.WriteUInt64At(30, fieldOffsetLastSeqNumV1)
	file.WriteUInt8At(1, fieldOffsetIsBitableFlushedV1)
	m.file = file
	require.NoError(t, m.Close())

	meta, err := NewMetadata(filename, testFS)
	require.NoError(t, err)
	require.Equal(t, metaVersion2, meta.header.version)
	require.Equal(t, uint64(30), meta.LastSeqNum)
	require.Equal(t, uint8(1), meta.GetFieldFlushedBitable())
	for i := range meta.Bmes {
		require.Equal(t, FileNum(20), meta.Bmes[i].NextFileNum)
		require.Equal(t, FileNum(10), meta.Bmes[i].MinUnflushedLogNum)
	}
	lastSeqNum := uint64(100)
	lastFileNum := FileNum(200)
	for i := range meta.Bmes {
		fn := meta.Bmes[i].GetNextFileNum()
		require.Equal(t, FileNum(20), fn)

		me := &MetaEditor{
			LastSeqNum: lastSeqNum,
		}
		sme := &BitowerMetaEditor{
			Index:              i,
			NextFileNum:        lastFileNum,
			MinUnflushedLogNum: lastFileNum,
		}
		meta.Write(me, sme)

		require.Equal(t, lastSeqNum, meta.LastSeqNum)
		require.Equal(t, lastFileNum, meta.Bmes[i].NextFileNum)
		require.Equal(t, lastFileNum, meta.Bmes[i].MinUnflushedLogNum)
	}
	require.NoError(t, meta.Close())

	meta, err = NewMetadata(filename, testFS)
	require.NoError(t, err)
	require.Equal(t, metaVersion2, meta.header.version)
	require.Equal(t, lastSeqNum, meta.LastSeqNum)
	require.Equal(t, uint8(1), meta.GetFieldFlushedBitable())
	for i := range meta.Bmes {
		require.Equal(t, lastFileNum, meta.Bmes[i].NextFileNum)
		require.Equal(t, lastFileNum, meta.Bmes[i].MinUnflushedLogNum)
	}
	require.NoError(t, meta.Close())
}
