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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

var testRunPerf = false
var testTblPath = filepath.Join(testDir, "tbl")

func testNewPage() *page {
	opts := testInitOpts()
	bpage := &Bitpage{
		dirname: testDir,
		opts:    opts,
	}
	testInitDir()
	return newPage(bpage, PageNum(1))
}

func testNewSuperTable(t *testing.T, exist bool) *superTable {
	st, err := newSuperTable(testNewPage(), testTblPath, FileNum(1), exist)
	require.NoError(t, err)
	return st
}

func testCloseSt(st *superTable) error {
	st.p.bp.freeStArenaBuf()
	return st.close()
}

func TestSuperTable_Open(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false)
	require.Equal(t, true, st.empty())
	require.Equal(t, stVersionDefault, st.version)
	require.Equal(t, uint64(stHeaderSize), st.inuseBytes())
	require.Equal(t, false, st.indexModified)

	val, found, kind, _ := st.get([]byte("a"), 0)
	require.Equal(t, false, found)
	require.Equal(t, internalKeyKindInvalid, kind)
	require.Equal(t, []byte(nil), val)

	num := 100
	kvList := testMakeSortedKV(num, 0, 10)
	for i := 0; i < num; i++ {
		require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, st.mergeIndexes())
	require.Equal(t, true, st.indexModified)
	require.NoError(t, st.close())

	st = testNewSuperTable(t, true)
	require.Equal(t, false, st.empty())
	require.Equal(t, stVersionDefault, st.version)
	require.Equal(t, uint64(4398), st.inuseBytes())

	for i := 0; i < num; i++ {
		val, found, kind, _ = st.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}
	require.NoError(t, st.close())
}

func TestSuperTable_Write(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false)
	require.Equal(t, true, st.empty())

	val, found, kind, _ := st.get([]byte("a"), 0)
	require.Equal(t, false, found)
	require.Equal(t, internalKeyKindInvalid, kind)
	require.Equal(t, []byte(nil), val)

	num := 100
	kvList := testMakeSortedKV(num, 0, 10)
	writeKV := func(pos int) {
		require.NoError(t, st.set(*kvList[pos].Key, kvList[pos].Value))
	}

	for i := 0; i < 50; i++ {
		writeKV(i)
	}
	require.Equal(t, false, st.empty())
	require.Equal(t, 50, len(st.pending))
	require.NoError(t, st.mergeIndexes())
	require.Equal(t, 0, len(st.pending))
	indexes := st.readIndexes()
	require.Equal(t, 50, len(indexes))

	for i := 80; i < num; i++ {
		writeKV(i)
	}
	require.Equal(t, 20, len(st.pending))
	require.NoError(t, st.mergeIndexes())
	require.Equal(t, 0, len(st.pending))
	indexes = st.readIndexes()
	require.Equal(t, 70, len(indexes))

	for i := 50; i < 80; i++ {
		writeKV(i)
	}
	require.Equal(t, 30, len(st.pending))
	require.NoError(t, st.mergeIndexes())
	require.Equal(t, 0, len(st.pending))
	indexes = st.readIndexes()
	require.Equal(t, num, len(indexes))

	for i := 0; i < num; i++ {
		val, found, kind, _ = st.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}

	require.NoError(t, st.close())

	st = testNewSuperTable(t, true)
	for i := 0; i < num; i++ {
		val, found, kind, _ = st.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}

	require.NoError(t, st.close())
}

func TestSuperTable_WriteIncomplete(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false)
	num := 20
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)
	for i := 0; i < 10; i++ {
		require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
	}
	wn, err := st.writer.writer.Write([]byte("panic"))
	require.NoError(t, err)
	require.Equal(t, 5, wn)
	require.NoError(t, st.writer.fdatasync())
	require.Equal(t, int64(451), st.tbl.fileStatSize())
	require.Equal(t, uint32(446), st.tbl.Size())
	require.NoError(t, testCloseSt(st))

	st1 := testNewSuperTable(t, true)
	require.Equal(t, int64(451), st1.tbl.fileStatSize())
	require.Equal(t, uint32(446), st1.tbl.Size())
	for i := 10; i < num; i++ {
		require.NoError(t, st1.set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, st1.mergeIndexes())
	require.Equal(t, int64(878), st1.tbl.fileStatSize())
	require.Equal(t, uint32(878), st1.tbl.Size())
	require.NoError(t, testCloseSt(st1))
	st2 := testNewSuperTable(t, true)
	require.Equal(t, int64(878), st2.tbl.fileStatSize())
	require.Equal(t, uint32(878), st2.tbl.Size())
	for i := 0; i < num; i++ {
		val, found, kind, _ := st2.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}
	require.NoError(t, testCloseSt(st2))
}

func TestSuperTable_MergeIndexes(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false)
	val, found, kind, _ := st.get([]byte("a"), 0)
	require.Equal(t, false, found)
	require.Equal(t, internalKeyKindInvalid, kind)
	require.Equal(t, []byte(nil), val)

	num := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	writeKV := func(pos int) {
		require.NoError(t, st.set(*kvList[pos].Key, kvList[pos].Value))
	}

	seqNum += uint64(num)
	for i := 0; i < 50; i++ {
		require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
	}
	require.Equal(t, 50, len(st.pending))
	require.NoError(t, st.mergeIndexes())
	require.Equal(t, 0, len(st.pending))
	indexes := st.readIndexes()
	require.Equal(t, 50, len(indexes))
	fmt.Println("set 0-49")

	for i := 50; i < 90; i++ {
		writeKV(i)
	}
	require.Equal(t, 40, len(st.pending))
	require.NoError(t, st.mergeIndexes())
	require.Equal(t, 0, len(st.pending))
	indexes = st.readIndexes()
	require.Equal(t, 90, len(indexes))
	fmt.Println("set 50-89")

	for i := 20; i < 90; i++ {
		val, found, kind, _ = st.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}

	wn := 0
	for i := 20; i < num; i++ {
		if i < 90 {
			if i%2 == 0 {
				kvList[i].Value = nil
				kvList[i].Key.SetSeqNum(seqNum)
				kvList[i].Key.SetKind(internalKeyKindDelete)
				seqNum++
				writeKV(i)
				wn++
			} else if i%3 == 0 {
				kvList[i].Value = utils.FuncRandBytes(20)
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				writeKV(i)
				wn++
			}
		} else {
			writeKV(i)
			wn++
		}
	}
	require.Equal(t, wn, len(st.pending))
	require.NoError(t, st.mergeIndexes())
	require.Equal(t, 0, len(st.pending))
	indexes = st.readIndexes()
	require.Equal(t, 100, len(indexes))
	fmt.Println("update 20-99")

	for i := 0; i < num; i++ {
		val, found, kind, _ = st.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		if i >= 20 && i < 90 && i%2 == 0 {
			require.Equal(t, internalKeyKindDelete, kind)
			require.Equal(t, 0, len(val))
		} else {
			require.Equal(t, internalKeyKindSet, kind)
			require.Equal(t, kvList[i].Value, val)
		}
	}

	wn = 0
	for i := 30; i < 50; i++ {
		if i%2 == 0 {
			kvList[i].Value = nil
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Key.SetKind(internalKeyKindDelete)
			seqNum++
			writeKV(i)
			wn++
		} else if i%3 == 0 {
			kvList[i].Value = utils.FuncRandBytes(20)
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			writeKV(i)
			wn++
		}
	}
	require.Equal(t, wn, len(st.pending))
	require.NoError(t, st.mergeIndexes())
	require.Equal(t, 0, len(st.pending))
	indexes = st.readIndexes()
	require.Equal(t, 100, len(indexes))
	fmt.Println("update 30-49")

	read := func(s *superTable) {
		for i := 0; i < num; i++ {
			val, found, kind, _ = s.get(kvList[i].Key.UserKey, 0)
			require.Equal(t, true, found)
			if i >= 20 && i < 90 && i%2 == 0 {
				require.Equal(t, internalKeyKindDelete, kind)
				require.Equal(t, 0, len(val))
			} else {
				require.Equal(t, internalKeyKindSet, kind)
				require.Equal(t, kvList[i].Value, val)
			}
		}
	}

	read(st)
	require.NoError(t, st.close())

	st = testNewSuperTable(t, true)
	read(st)
	require.NoError(t, st.close())
	st = testNewSuperTable(t, true)
	read(st)
	require.NoError(t, st.close())
}

func TestSuperTable_Iter(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false)
	num := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)

	checkKV := func(ik *internalKey, iv []byte, i int) {
		if i == -1 {
			require.Equal(t, nilInternalKey, ik)
			require.Equal(t, []byte(nil), iv)
		} else {
			require.Equal(t, kvList[i].Key.String(), ik.String())
			require.Equal(t, kvList[i].Value, iv)
		}
	}

	iter := st.newIter(nil)
	itKey, itVal := iter.First()
	if itKey != nil || itVal != nil {
		t.Fatal("empty iter First fail")
	}
	itKey, itVal = iter.Last()
	if itKey != nil || itVal != nil {
		t.Fatal("empty iter Last fail")
	}
	require.NoError(t, iter.Close())

	for i := 0; i < num; i++ {
		if i%2 == 0 {
			kvList[i].Key.SetKind(internalKeyKindDelete)
			kvList[i].Value = []byte{}
		}
		require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, st.mergeIndexes())

	seek := func(s *superTable) {
		iter = s.newIter(nil)
		itKey, itVal = iter.First()
		checkKV(itKey, itVal, 0)
		itKey, itVal = iter.Next()
		checkKV(itKey, itVal, 1)
		itKey, itVal = iter.Next()
		checkKV(itKey, itVal, 2)
		itKey, itVal = iter.Prev()
		checkKV(itKey, itVal, 1)
		itKey, itVal = iter.Prev()
		checkKV(itKey, itVal, 0)
		itKey, itVal = iter.Last()
		checkKV(itKey, itVal, 99)
		itKey, itVal = iter.Prev()
		checkKV(itKey, itVal, 98)
		itKey, itVal = iter.Next()
		checkKV(itKey, itVal, 99)

		itKey, itVal = iter.SeekGE(kvList[51].Key.UserKey)
		checkKV(itKey, itVal, 51)
		itKey, itVal = iter.Next()
		checkKV(itKey, itVal, 52)
		itKey, itVal = iter.SeekGE(kvList[99].Key.UserKey)
		checkKV(itKey, itVal, 99)
		itKey, itVal = iter.Next()
		checkKV(itKey, itVal, -1)

		itKey, itVal = iter.SeekLT(kvList[43].Key.UserKey)
		checkKV(itKey, itVal, 42)
		itKey, itVal = iter.Prev()
		checkKV(itKey, itVal, 41)
		itKey, itVal = iter.SeekLT(kvList[1].Key.UserKey)
		checkKV(itKey, itVal, 0)
		itKey, itVal = iter.Prev()
		checkKV(itKey, itVal, -1)
		itKey, itVal = iter.SeekLT(kvList[0].Key.UserKey)
		checkKV(itKey, itVal, -1)

		itKey, itVal = iter.SeekLT(sortedkv.MakeSortedKey(99999))
		checkKV(itKey, itVal, 99)
		itKey, itVal = iter.SeekLT(sortedkv.MakeSortedKey(-1))
		checkKV(itKey, itVal, -1)
		itKey, itVal = iter.SeekGE(sortedkv.MakeSortedKey(99999))
		checkKV(itKey, itVal, -1)
		itKey, itVal = iter.SeekGE(sortedkv.MakeSortedKey(-1))
		checkKV(itKey, itVal, 0)
	}

	seek(st)
	require.NoError(t, st.close())
	st = testNewSuperTable(t, true)
	seek(st)
	require.NoError(t, st.close())
	st = testNewSuperTable(t, true)
	seek(st)
	require.NoError(t, st.close())
}

func TestSuperTableIndex_Empty(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false)
	require.Equal(t, true, st.empty())
	require.NoError(t, st.close())
	st1 := testNewSuperTable(t, true)
	require.Equal(t, true, st1.empty())
	require.NoError(t, st1.close())
	st2 := testNewSuperTable(t, true)
	num := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)
	for i := 0; i < num; i++ {
		require.NoError(t, st2.set(*kvList[i].Key, kvList[i].Value))
	}
	require.Equal(t, false, st2.empty())
	require.NoError(t, st2.mergeIndexes())
	checkKV := func(s *superTable) {
		for i := 0; i < num; i++ {
			val, found, kind, _ := s.get(kvList[i].Key.UserKey, 0)
			require.Equal(t, true, found)
			require.Equal(t, internalKeyKindSet, kind)
			require.Equal(t, kvList[i].Value, val)
		}
	}
	checkKV(st2)
	require.NoError(t, st2.close())
	st3 := testNewSuperTable(t, true)
	require.Equal(t, false, st3.empty())
	checkKV(st3)
	require.NoError(t, st3.close())
}

func TestSuperTableIndex_Rebuild(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	fmt.Println("open st 1")
	st := testNewSuperTable(t, false)
	num := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)

	writeData := func(start, end int) {
		if end > num {
			end = num
		}
		for i := start; i < end; i++ {
			if i%5 == 0 {
				kvList[i].Key.SetKind(internalKeyKindDelete)
				kvList[i].Value = []byte{}
			} else {
				kvList[i].Value = utils.FuncRandBytes(20)
			}
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
	}
	step := 20
	for pos := 0; pos < num; pos += step {
		writeData(pos, pos+step)
		pos -= 10
	}
	for i := 0; i < 50; i++ {
		if i%5 == 0 {
			kvList[i].Key.SetKind(internalKeyKindSet)
			kvList[i].Value = utils.FuncRandBytes(20)
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
	}
	require.NoError(t, st.writer.fdatasync())
	require.NoError(t, st.close())

	checkKV := func(s *superTable) {
		for i := 0; i < num; i++ {
			val, found, kind, _ := s.get(kvList[i].Key.UserKey, 0)
			require.Equal(t, true, found)
			if i >= 50 && i%5 == 0 {
				require.Equal(t, internalKeyKindDelete, kind)
				require.Equal(t, 0, len(val))
			} else {
				require.Equal(t, internalKeyKindSet, kind)
				require.Equal(t, kvList[i].Value, val)
			}
		}
	}

	fmt.Println("open st 2")
	st = testNewSuperTable(t, true)
	checkKV(st)
	require.Equal(t, true, st.indexModified)
	require.NoError(t, st.close())
	fmt.Println("open st 3")
	st = testNewSuperTable(t, true)
	checkKV(st)
	require.Equal(t, false, st.indexModified)
	require.NoError(t, st.close())
}

func TestSuperTableIndex_Rebuild1(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false)
	num := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)

	writeData := func(s *superTable, start, end int) {
		if end > num {
			end = num
		}
		for i := start; i < end; i++ {
			if i%5 == 0 {
				kvList[i].Key.SetKind(internalKeyKindDelete)
				kvList[i].Value = []byte{}
			} else {
				kvList[i].Value = utils.FuncRandBytes(20)
			}
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			require.NoError(t, s.set(*kvList[i].Key, kvList[i].Value))
		}
	}
	writeData(st, 0, 30)
	writeData(st, 70, 100)
	writeData(st, 50, 75)
	writeData(st, 35, 60)
	writeData(st, 12, 50)
	wn, err := st.writer.writer.Write([]byte("panic"))
	require.NoError(t, err)
	require.Equal(t, 5, wn)
	require.NoError(t, st.writer.fdatasync())
	require.Equal(t, int64(7411), st.tbl.fileStatSize())
	require.NoError(t, st.close())

	checkKV := func(s *superTable) {
		for i := 0; i < num; i++ {
			val, found, kind, _ := s.get(kvList[i].Key.UserKey, 0)
			require.Equal(t, true, found)
			if i%5 == 0 {
				require.Equal(t, internalKeyKindDelete, kind)
				require.Equal(t, 0, len(val))
			} else {
				require.Equal(t, internalKeyKindSet, kind)
				require.Equal(t, kvList[i].Value, val)
			}
		}
	}

	st1 := testNewSuperTable(t, true)
	require.Equal(t, uint32(7406), st1.tbl.Size())
	checkKV(st1)
	kvList1 := testMakeSortedKV(num, seqNum, 10)
	for i := 0; i < 10; i++ {
		require.NoError(t, st1.set(*kvList1[i].Key, kvList1[i].Value))
	}
	require.NoError(t, st1.writer.fdatasync())
	require.NoError(t, st1.close())
	st2 := testNewSuperTable(t, true)
	checkKV(st2)
	require.NoError(t, st2.close())
}

func TestSuperTableIndex_Rebuild2(t *testing.T) {
	defer os.RemoveAll(testDir)

	num := 100
	seqNum := uint64(0)
	for loop := 0; loop < 10; loop++ {
		os.RemoveAll(testDir)
		st := testNewSuperTable(t, false)
		kvList := testMakeSortedKV(num, seqNum, 10)
		seqNum += uint64(num)
		writeData := func(s *superTable, start, end int) {
			if end > num {
				end = num
			}
			for i := start; i < end; i++ {
				if i%5 == 0 {
					kvList[i].Key.SetKind(internalKeyKindDelete)
					kvList[i].Value = []byte{}
				} else {
					kvList[i].Value = utils.FuncRandBytes(20)
				}
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				require.NoError(t, s.set(*kvList[i].Key, kvList[i].Value))
			}
		}
		step := 20
		for pos := 0; pos < num; pos += step {
			writeData(st, pos, pos+step)
			pos -= 10
		}
		require.NoError(t, st.writer.fdatasync())
		require.NoError(t, st.close())
		require.Equal(t, false, utils.IsFileExist(st.getIdxFilePath()))

		st = testNewSuperTable(t, true)
		for i := 0; i < num; i++ {
			val, found, kind, _ := st.get(kvList[i].Key.UserKey, 0)
			require.Equal(t, true, found)
			if i%5 == 0 {
				require.Equal(t, internalKeyKindDelete, kind)
				require.Equal(t, 0, len(val))
			} else {
				require.Equal(t, internalKeyKindSet, kind)
				require.Equal(t, kvList[i].Value, val)
			}
		}
		require.NoError(t, st.close())
		require.Equal(t, true, utils.IsFileExist(st.getIdxFilePath()))
	}
}

func TestSuperTableIndex_Rebuild_Perf(t *testing.T) {
	if !testRunPerf {
		return
	}

	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false)
	num := 6000000
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)

	writeData := func(start, end int) {
		if end > num {
			end = num
		}
		for i := start; i < end; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
	}
	step := 500000
	for pos := 0; pos < num; pos += step {
		writeData(pos, pos+step)
	}
	require.NoError(t, st.writer.fdatasync())
	require.NoError(t, st.close())

	checkKV := func(s *superTable) {
		for i := 0; i < num; i++ {
			val, found, kind, _ := s.get(kvList[i].Key.UserKey, 0)
			require.Equal(t, true, found)
			require.Equal(t, internalKeyKindSet, kind)
			require.Equal(t, kvList[i].Value, val)
		}
	}

	st1 := testNewSuperTable(t, true)
	startTime := time.Now()
	checkKV(st1)
	check1Cost := time.Since(startTime).Seconds()
	fmt.Println("checkKV 1", check1Cost)
	require.NoError(t, st1.close())
	st1 = testNewSuperTable(t, true)
	reopenCost := time.Since(startTime).Seconds()
	fmt.Println("reopen", reopenCost-check1Cost)
	checkKV(st1)
	check2Cost := time.Since(startTime).Seconds()
	fmt.Println("checkKV 2", check2Cost-reopenCost)
	require.NoError(t, st1.close())
}
