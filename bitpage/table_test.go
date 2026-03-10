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
	"os"
	"testing"

	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestTable_ExpandSize(t *testing.T) {
	defer os.Remove(testPath)
	os.Remove(testPath)
	tbl := testNewTable()
	sz, _ := tbl.calcExpandSize(1)
	require.Equal(t, 32768, sz)
	sz, _ = tbl.calcExpandSize(32770)
	require.Equal(t, 65536, sz)
	sz, _ = tbl.calcExpandSize(65540)
	require.Equal(t, 131072, sz)
	sz, _ = tbl.calcExpandSize((1 << 30) + 1)
	require.Equal(t, 1207959552, sz)
	sz, _ = tbl.calcExpandSize(1<<30 + 128<<20 + 1)
	require.Equal(t, 1342177280, sz)
}

func TestTable_WriteRead(t *testing.T) {
	defer os.Remove(testPath)
	os.Remove(testPath)
	tbl := testNewTable()

	vals := make([][]byte, 0, 100)
	offs := make([]uint32, 0, 100)
	for i := 1; i <= 100; i++ {
		vals = append(vals, utils.FuncRandBytes(i))
	}
	for _, val := range vals {
		size := len(val)
		off, e1 := tbl.alloc(uint32(size))
		offs = append(offs, off)
		require.NoError(t, e1)
		wn, e2 := tbl.writeAtOffset(val, off)
		require.NoError(t, e2)
		require.Equal(t, size, wn)
	}

	for i, offset := range offs {
		size := i + 1
		v := tbl.GetBytes(offset, uint32(size))
		require.Equal(t, vals[i], v)
	}

	tblOffset := tbl.Size()

	require.NoError(t, tbl.Close())

	tbl1 := testNewTable()
	require.Equal(t, tblOffset, tbl1.Size())
	fmt.Println(tbl1.Size())
	for i, offset := range offs {
		size := i + 1
		v := tbl1.GetBytes(offset, uint32(size))
		require.Equal(t, vals[i], v)
	}
	require.NoError(t, tbl1.Close())
	_ = os.Remove(testPath)
}

func TestTable_Expand(t *testing.T) {
	defer os.Remove(testPath)
	os.Remove(testPath)
	tbl := testNewTable()
	val := utils.FuncRandBytes(1024)
	for i := 1; i <= 40*1024; i++ {
		off, e1 := tbl.alloc(1024)
		require.NoError(t, e1)
		wn, e2 := tbl.writeAtOffset(val, off)
		require.NoError(t, e2)
		require.Equal(t, 1024, wn)
		require.Equal(t, val, tbl.GetBytes(off, 1024))
	}
	offset := TblDataOffset
	for i := 1; i <= 40*1024; i++ {
		v := tbl.GetBytes(uint32(offset), 1024)
		require.Equal(t, val, v)
		offset += 1024
	}
	tblOffset := tbl.Size()
	require.Equal(t, int64(tbl.filesz), tbl.getFileSize())
	require.Equal(t, 67108864, tbl.filesz)
	require.Equal(t, uint32(41943044), tbl.Size())
	require.Equal(t, 67108864, tbl.datasz)
	require.NoError(t, tbl.Close())

	tbl1 := testNewTable()
	require.Equal(t, tblOffset, tbl1.Size())
	offset = TblDataOffset
	for i := 1; i <= 40*1024; i++ {
		v := tbl1.GetBytes(uint32(offset), 1024)
		require.Equal(t, val, v)
		offset += 1024
	}
	require.Equal(t, int64(tbl1.filesz), tbl1.getFileSize())
	require.Equal(t, 67108864, tbl1.filesz)
	require.Equal(t, uint32(41943044), tbl1.Size())
	require.Equal(t, 67108864, tbl1.datasz)
	require.NoError(t, tbl1.Close())
	_ = os.Remove(testPath)
}

func TestTable_KeySize(t *testing.T) {
	buf := make([]byte, TblItemHeaderSize)
	var valueSize uint16
	for i := uint32(1); i < uint32(10000); i++ {
		if i%2 == 0 {
			valueSize = 0
		} else {
			valueSize = 10
		}
		encodeKeySize(buf, i, valueSize)
		kz, voff := decodeKeySize(buf)
		require.Equal(t, i, kz)
		require.Equal(t, valueSize, voff)
	}
}
