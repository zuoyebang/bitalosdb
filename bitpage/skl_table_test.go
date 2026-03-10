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
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

func testSklNewPage() *page {
	opts := testInitOpts()
	bpage := &Bitpage{
		dirname: testDir,
		opts:    opts,
	}
	testInitDir()
	return newPage(bpage, PageNum(1))
}

func testNewSklTable(t *testing.T, exist bool) *sklTable {
	st, err := newSklTable(testSklNewPage(), testTblPath, FileNum(1), exist, consts.BitpageStiCompressCountMax)
	require.NoError(t, err)
	return st
}

func testNewSklTableForStiCompress(t *testing.T, exist bool, compressCount uint32) *sklTable {
	st, err := newSklTable(testSklNewPage(), testTblPath, FileNum(1), exist, compressCount)
	require.NoError(t, err)
	return st
}

func testCloseSklTable(st *sklTable) error {
	st.p.bp.freeStArenaBuf()
	return st.close()
}

func TestSklTableOpen(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSklTable(t, false)
	require.Equal(t, true, st.empty())
	require.Equal(t, TblVersionDefault, st.version)
	require.Equal(t, uint64(TblFileHeaderSize), st.inuseBytes())

	val, found, kind := st.get([]byte("a"), 0)
	require.Equal(t, false, found)
	require.Equal(t, internalKeyKindInvalid, kind)
	require.Equal(t, []byte(nil), val)

	num := 100
	kvList := testMakeSortedKV(num, 0, 10)
	for i := 0; i < num; i++ {
		require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, st.flushFinish())
	require.NoError(t, testCloseSklTable(st))

	st = testNewSklTable(t, true)
	require.Equal(t, false, st.empty())
	require.Equal(t, TblVersionDefault, st.version)
	require.Equal(t, uint64(6498), st.inuseBytes())

	for i := 0; i < num; i++ {
		val, found, kind = st.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}
	require.NoError(t, testCloseSklTable(st))
}

func TestSklTableWrite(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSklTable(t, false)
	require.Equal(t, true, st.empty())

	val, found, kind := st.get([]byte("a"), 0)
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
	require.NoError(t, st.flushFinish())
	require.Equal(t, false, st.empty())

	for i := 80; i < num; i++ {
		writeKV(i)
	}
	require.NoError(t, st.flushFinish())
	for i := 50; i < 80; i++ {
		writeKV(i)
	}
	require.NoError(t, st.flushFinish())

	for i := 0; i < num; i++ {
		val, found, kind = st.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}

	require.NoError(t, testCloseSklTable(st))

	st = testNewSklTable(t, true)
	for i := 0; i < num; i++ {
		val, found, kind = st.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}

	require.NoError(t, testCloseSklTable(st))
}

func TestSklTableWriteOneKey(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSklTable(t, false)
	require.Equal(t, true, st.empty())

	key := sortedkv.MakeKKVSubKey(kkv.DataTypeHash, 100, 100)
	seqNum := []uint64{98, 100, 102}
	ik1 := base.MakeInternalKey(key, seqNum[1], internalKeyKindSet)
	require.NoError(t, st.set(ik1, []byte("2")))
	ik2 := base.MakeInternalKey(key, seqNum[0], internalKeyKindSet)
	require.NoError(t, st.set(ik2, []byte("1")))
	require.NoError(t, st.flushFinish())

	val, found, _ := st.get(key, 0)
	require.Equal(t, true, found)
	require.Equal(t, []byte("2"), val)

	iter := st.newIter(nil)
	cnt := 0
	pos := 1
	for itKey, _ := iter.First(); itKey != nil; itKey, _ = iter.Next() {
		cnt++
		require.Equal(t, seqNum[pos], itKey.SeqNum())
		pos--
	}
	require.Equal(t, 2, cnt)
	require.NoError(t, iter.Close())

	ik3 := base.MakeInternalKey(key, seqNum[2], internalKeyKindSet)
	require.NoError(t, st.set(ik3, []byte("3")))
	require.NoError(t, st.flushFinish())

	val, found, _ = st.get(key, 0)
	require.Equal(t, true, found)
	require.Equal(t, []byte("3"), val)

	iter = st.newIter(nil)
	cnt = 0
	pos = 2
	for itKey, _ := iter.First(); itKey != nil; itKey, _ = iter.Next() {
		cnt++
		require.Equal(t, seqNum[pos], itKey.SeqNum())
		pos--
	}
	require.Equal(t, 3, cnt)
	require.NoError(t, iter.Close())

	require.NoError(t, testCloseSklTable(st))
}

func TestSklTableSet(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSklTableForStiCompress(t, false, 64)
	require.Equal(t, true, st.empty())

	num := 40
	seqNum := uint64(0)
	vsize := 10
	kvList := testMakeSortedKV(num, seqNum, vsize)

	writeKV := func(pos int) {
		seqNum++
		kvList[pos].Key.SetSeqNum(seqNum)
		kvList[pos].Key.SetKind(internalKeyKindSet)
		err := st.set(*kvList[pos].Key, kvList[pos].Value)
		require.NoError(t, err)
	}

	writeDeleteKey := func(pos int) {
		seqNum++
		kvList[pos].Key.SetSeqNum(seqNum)
		kvList[pos].Key.SetKind(internalKeyKindDelete)
		err := st.set(*kvList[pos].Key, nil)
		require.NoError(t, err)
	}

	readKey := func(pos int, expExist bool, expKind internalKeyKind) {
		_, exist, kind := st.get(kvList[pos].Key.UserKey, 0)
		require.Equal(t, expExist, exist)
		require.Equal(t, expKind, kind)
	}

	for i := 0; i < 20; i++ {
		writeKV(i)
	}
	require.NoError(t, st.flushFinish())
	require.Equal(t, 20, st.itemCount())

	for i := 0; i < 20; i++ {
		readKey(i, true, internalKeyKindSet)
	}

	for i := 10; i < 15; i++ {
		writeDeleteKey(i)
	}
	require.NoError(t, st.flushFinish())
	for i := 10; i < 15; i++ {
		readKey(i, true, internalKeyKindDelete)
	}

	st.flushIndexes()

	for i := 10; i < 15; i++ {
		readKey(i, true, internalKeyKindDelete)
	}
	for i := 15; i < 20; i++ {
		readKey(i, true, internalKeyKindSet)
	}

	for i := 0; i < 15; i++ {
		writeKV(i)
	}
	require.NoError(t, st.flushFinish())
	for i := 0; i < 20; i++ {
		readKey(i, true, internalKeyKindSet)
	}
	for i := 20; i < 40; i++ {
		writeKV(i)
	}
	require.NoError(t, st.flushFinish())
	for i := 20; i < 40; i++ {
		readKey(i, true, internalKeyKindSet)
	}

	require.NoError(t, testCloseSklTable(st))
}

func TestSklTableSetIncomplete(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSklTable(t, false)
	num := 20
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)
	for i := 0; i < 10; i++ {
		require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, st.flushFinish())
	wn, err := st.dataFile.writeAt([]byte("panic"), int64(st.dataFile.Size()))
	require.NoError(t, err)
	require.Equal(t, 5, wn)
	require.NoError(t, testCloseSklTable(st))

	st1 := testNewSklTable(t, true)
	require.Equal(t, uint64(648), st1.inuseBytes())
	for i := 10; i < num; i++ {
		require.NoError(t, st1.set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, st1.flushFinish())
	require.NoError(t, testCloseSklTable(st1))
	st2 := testNewSklTable(t, true)
	require.Equal(t, uint64(1298), st2.inuseBytes())
	for i := 0; i < num; i++ {
		val, found, kind := st2.get(kvList[i].Key.UserKey, 0)
		require.Equal(t, true, found)
		require.Equal(t, internalKeyKindSet, kind)
		require.Equal(t, kvList[i].Value, val)
	}
	require.NoError(t, testCloseSklTable(st2))
}

func TestSklTableIter(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	checkSt := func(st *sklTable) {
		num := 2000
		seqNum := uint64(0)
		kvList := testMakeSortedKV(num+100, seqNum, 10)
		seqNum += uint64(num)

		checkKV := func(ik *InternalKKVKey, iv []byte, i int) {
			if i == -1 {
				require.Equal(t, nilInternalKKVKey, ik)
				require.Equal(t, []byte(nil), iv)
			} else {
				expKey := kkv.MakeInternalKey(*kvList[i].Key)
				require.Equal(t, true, kkv.InternalKeyEqual(&expKey, ik))
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

		for i := 10; i < num; i++ {
			if i%2 == 0 {
				kvList[i].Key.SetKind(internalKeyKindDelete)
				kvList[i].Value = nil
			}
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, st.flushFinish())

		seek := func(s *sklTable) {
			iter = s.newIter(nil)
			itKey, itVal = iter.First()
			checkKV(itKey, itVal, 10)
			itKey, itVal = iter.Next()
			checkKV(itKey, itVal, 11)
			itKey, itVal = iter.Next()
			checkKV(itKey, itVal, 12)

			itKey, itVal = iter.SeekGE(kvList[51].Key.UserKey)
			checkKV(itKey, itVal, 51)
			itKey, itVal = iter.Next()
			checkKV(itKey, itVal, 52)
			itKey, itVal = iter.SeekGE(kvList[num-1].Key.UserKey)
			checkKV(itKey, itVal, num-1)
			itKey, itVal = iter.Next()
			checkKV(itKey, itVal, -1)

			itKey, itVal = iter.SeekLT(kvList[43].Key.UserKey)
			checkKV(itKey, itVal, 42)
			itKey, itVal = iter.SeekLT(kvList[11].Key.UserKey)
			checkKV(itKey, itVal, 10)
			itKey, itVal = iter.SeekLT(kvList[0].Key.UserKey)
			checkKV(itKey, itVal, -1)

			itKey, itVal = iter.SeekLT(kvList[num+10].Key.UserKey)
			checkKV(itKey, itVal, num-1)
			itKey, itVal = iter.SeekLT(kvList[1].Key.UserKey)
			checkKV(itKey, itVal, -1)
			itKey, itVal = iter.SeekGE(kvList[num+10].Key.UserKey)
			checkKV(itKey, itVal, -1)
			itKey, itVal = iter.SeekGE(kvList[1].Key.UserKey)
			checkKV(itKey, itVal, 10)
			iter.Close()
		}

		seek(st)
		require.NoError(t, testCloseSklTable(st))
		st = testNewSklTable(t, true)
		seek(st)
		require.NoError(t, testCloseSklTable(st))
		st = testNewSklTable(t, true)
		seek(st)
		require.NoError(t, testCloseSklTable(st))
	}

	st1 := testNewSklTableForStiCompress(t, false, consts.BitpageStiCompressCountDefault)
	checkSt(st1)

	st2 := testNewSklTable(t, false)
	checkSt(st2)
}

func TestSklTableEmpty(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSklTable(t, false)
	require.Equal(t, true, st.empty())
	require.NoError(t, testCloseSklTable(st))
	st = testNewSklTable(t, true)
	require.Equal(t, true, st.empty())
	require.NoError(t, testCloseSklTable(st))
	st = testNewSklTable(t, true)
	num := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)
	for i := 0; i < num; i++ {
		require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, st.flushFinish())
	require.Equal(t, false, st.empty())
	checkKV := func(s *sklTable) {
		for i := 0; i < num; i++ {
			val, found, kind := s.get(kvList[i].Key.UserKey, 0)
			require.Equal(t, true, found)
			require.Equal(t, internalKeyKindSet, kind)
			require.Equal(t, kvList[i].Value, val)
		}
	}
	checkKV(st)
	require.NoError(t, testCloseSklTable(st))
	st = testNewSklTable(t, true)
	require.Equal(t, false, st.empty())
	checkKV(st)
	require.NoError(t, testCloseSklTable(st))
}

func TestSklTableRebuild0(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSklTable(t, false)
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
		require.NoError(t, st.flushFinish())
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
	require.NoError(t, st.flushFinish())
	require.NoError(t, testCloseSklTable(st))

	checkKV := func(s *sklTable) {
		for i := 0; i < num; i++ {
			val, found, kind := s.get(kvList[i].Key.UserKey, 0)
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

	st = testNewSklTable(t, true)
	checkKV(st)
	require.NoError(t, testCloseSklTable(st))

	st = testNewSklTable(t, true)
	checkKV(st)
	require.NoError(t, testCloseSklTable(st))
}

func TestSklTableRebuild1(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	cc := uint32(256)
	st := testNewSklTableForStiCompress(t, false, cc)
	num := 1000
	kvList := testMakeSortedKV(num, 0, 10)

	for i := 0; i < num; i++ {
		if i%5 == 0 {
			kvList[i].Key.SetKind(internalKeyKindDelete)
			kvList[i].Value = []byte{}
		} else {
			kvList[i].Value = utils.FuncRandBytes(20)
		}
		require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, st.flushFinish())

	wn, err := st.dataFile.writeAt([]byte("panic"), int64(st.dataFile.Size()))
	require.NoError(t, err)
	require.Equal(t, 5, wn)
	require.NoError(t, testCloseSklTable(st))

	readData := func() {
		for i := 0; i < num; i++ {
			val, found, kind := st.get(kvList[i].Key.UserKey, 0)
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

	st = testNewSklTableForStiCompress(t, true, cc)
	idxPath := st.getIdxFilePath()
	readData()
	require.NoError(t, testCloseSklTable(st))

	require.NoError(t, os.Remove(idxPath))

	st = testNewSklTableForStiCompress(t, true, cc)
	readData()
	require.NoError(t, testCloseSklTable(st))
}

func TestSklTableRebuild2(t *testing.T) {
	for _, isDeleteIdx := range []bool{false, true} {
		t.Run(fmt.Sprintf("isDeleteIdx=%v", isDeleteIdx), func(t *testing.T) {
			defer os.RemoveAll(testDir)

			num := 1000
			seqNum := uint64(0)
			for loop := 0; loop < 10; loop++ {
				os.RemoveAll(testDir)
				st := testNewSklTable(t, false)
				idxPath := st.getIdxFilePath()
				kvList := testMakeSortedKV(num, seqNum, 10)
				for i := 0; i < num; i++ {
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
				require.NoError(t, st.flushFinish())
				require.NoError(t, testCloseSklTable(st))

				if isDeleteIdx {
					require.NoError(t, os.Remove(idxPath))
					require.Equal(t, false, os2.IsExist(idxPath))
				}

				st = testNewSklTableForStiCompress(t, true, 256)
				for i := 0; i < num; i++ {
					val, found, kind := st.get(kvList[i].Key.UserKey, 0)
					require.Equal(t, true, found)
					if i%5 == 0 {
						require.Equal(t, internalKeyKindDelete, kind)
						require.Equal(t, 0, len(val))
					} else {
						require.Equal(t, internalKeyKindSet, kind)
						require.Equal(t, kvList[i].Value, val)
					}
				}
				require.NoError(t, testCloseSklTable(st))
				require.Equal(t, true, os2.IsExist(idxPath))
			}
		})
	}
}

func TestSklTableStiCompress(t *testing.T) {
	keyNums := []int{50, 128, 256, 65535}
	for _, keyNum := range keyNums {
		t.Run(fmt.Sprintf("keyNum=%d", keyNum), func(t *testing.T) {
			defer os.RemoveAll(testDir)
			os.RemoveAll(testDir)

			st := testNewSklTableForStiCompress(t, false, 128)
			vsize := 20
			kvList := testMakeSortedKV(keyNum+10, 1, vsize)
			seqNum := uint64(keyNum + 11)

			for i := 0; i < keyNum; i++ {
				kv := kvList[i]
				kv.Sn = kv.Key.SeqNum()
				require.NoError(t, st.set(*kv.Key, kv.Value))
			}
			require.NoError(t, st.flushFinish())

			for i := 0; i < keyNum; i++ {
				kv := kvList[i]
				val, found, kind := st.get(kv.Key.UserKey, 0)
				require.Equal(t, true, found)
				require.Equal(t, internalKeyKindSet, kind)
				require.Equal(t, kv.Value, val)
			}

			for i := 0; i < keyNum; i++ {
				if i%2 != 0 {
					continue
				}
				kv := kvList[i]
				seqNum++
				kv.Key.SetSeqNum(seqNum)
				kv.Sn = seqNum
				if i%10 == 0 {
					kv.Key.SetKind(internalKeyKindDelete)
					kv.Value = []byte(nil)
				} else {
					kv.Value = utils.FuncRandBytes(vsize)
				}
				require.NoError(t, st.set(*kv.Key, kv.Value))
			}
			require.NoError(t, st.flushFinish())

			for i := 0; i < keyNum+10; i++ {
				kv := kvList[i]
				val, found, kind := st.get(kv.Key.UserKey, 0)
				if i >= keyNum {
					require.Equal(t, false, found)
				} else {
					require.Equal(t, true, found)
					require.Equal(t, kv.Value, val)
					if i%10 == 0 {
						require.Equal(t, internalKeyKindDelete, kind)
					} else {
						require.Equal(t, internalKeyKindSet, kind)
					}
				}
			}

			require.NoError(t, testCloseSklTable(st))
		})
	}
}

func TestSklTableStiCompressDupKey(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	compressCount := uint32(128)
	st := testNewSklTableForStiCompress(t, false, compressCount)
	vsize := 20
	keyNum := 1000
	kvList := testMakeSortedKV(keyNum+1, 1, vsize)
	seqNum := uint64(keyNum + 1)
	oneKey := base.MakeInternalKey([]byte("oneKey"), 1, base.InternalKeyKindSet)

	readData := func(end int) {
		for i := 0; i < end; i++ {
			kv := kvList[i]
			val, found, kind := st.get(kv.Key.UserKey, 0)
			require.Equal(t, true, found)
			require.Equal(t, internalKeyKindSet, kind)
			require.Equal(t, kv.Value, val)
		}
	}

	cnt := 0
	for i := 0; i < keyNum; i++ {
		kv := kvList[i]
		kv.Sn = kv.Key.SeqNum()
		cnt++
		require.NoError(t, st.set(*kv.Key, kv.Value))
		require.NoError(t, st.flushFinish())
		val, found, _ := st.get(oneKey.UserKey, 0)
		if i == 0 {
			require.Equal(t, false, found)
		} else {
			require.Equal(t, true, found)
			n, err := utils.FmtSliceToInt64(val)
			require.NoError(t, err)
			require.Equal(t, int64(i-1), n)
		}

		oneKey.SetSeqNum(seqNum)
		seqNum++
		cnt++
		require.NoError(t, st.set(oneKey, utils.FmtInt64ToSlice(int64(i))))
		require.NoError(t, st.flushFinish())

		readData(i)
	}

	val, found, _ := st.get(oneKey.UserKey, 0)
	require.Equal(t, true, found)
	n, err := utils.FmtSliceToInt64(val)
	require.NoError(t, err)
	require.Equal(t, int64(keyNum-1), n)

	readData(keyNum)

	checkIterData := func(i int, ik *InternalKKVKey, iv []byte) {
		expKey := kkv.MakeInternalKey(*kvList[i].Key)
		require.Equal(t, true, kkv.InternalKeyEqual(&expKey, ik))
		require.Equal(t, kvList[i].Value, iv)
	}

	iter := st.newIter(nil)
	ik, iv := iter.SeekGE(kvList[1].Key.UserKey)
	checkIterData(1, ik, iv)
	ik, iv = iter.Next()
	checkIterData(2, ik, iv)
	ik, iv = iter.Next()
	checkIterData(3, ik, iv)
	ik, iv = iter.SeekGE(kvList[200].Key.UserKey)
	checkIterData(200, ik, iv)
	ik, iv = iter.Next()
	checkIterData(201, ik, iv)
	ik, iv = iter.SeekGE(kvList[500].Key.UserKey)
	checkIterData(500, ik, iv)
	require.NoError(t, iter.Close())

	require.NoError(t, testCloseSklTable(st))
}

func TestSklTableStiCompressIter(t *testing.T) {
	keyNums := []int{50, 128, 256, 65535}
	for _, keyNum := range keyNums {
		t.Run(fmt.Sprintf("keyNum=%d", keyNum), func(t *testing.T) {
			defer os.RemoveAll(testDir)
			os.RemoveAll(testDir)

			st := testNewSklTableForStiCompress(t, false, 128)
			vsize := 20
			kvList := testMakeSortedKV(keyNum+10, 1, vsize)
			seqNum := uint64(keyNum + 11)

			for i := 0; i < keyNum; i++ {
				kv := kvList[i]
				require.NoError(t, st.set(*kv.Key, kv.Value))
			}
			require.NoError(t, st.flushFinish())

			iter := st.newIter(nil)
			pos := 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				kv := kvList[pos]
				require.Equal(t, internalKeyKindSet, ik.Kind())
				require.Equal(t, kv.Value, iv)
				require.Equal(t, kv.Sn, ik.SeqNum())
				pos++
			}
			require.NoError(t, iter.Close())

			kvList1 := testCopySortedKV(kvList)

			for i := 0; i < keyNum; i++ {
				if i%2 != 0 {
					continue
				}

				var kind internalKeyKind
				var value []byte
				if i%10 == 0 {
					kind = internalKeyKindDelete
					value = nil
				} else {
					kind = internalKeyKindSet
					value = utils.FuncRandBytes(vsize)
				}

				seqNum++
				ikey := base.MakeInternalKey(kvList[i].Key.UserKey, seqNum, kind)
				kv := &sortedkv.SortedKVItem{
					Key:   &ikey,
					Value: value,
					Sn:    seqNum,
				}
				require.NoError(t, st.set(*kv.Key, kv.Value))
				kvList1 = append(kvList1, kv)
			}
			require.NoError(t, st.flushFinish())

			sort.Sort(kvList1)

			iter = st.newIter(nil)
			pos = 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				kv := kvList1[pos]
				require.Equal(t, kv.Value, iv)
				expKey := kkv.MakeInternalKey(*kvList1[pos].Key)
				require.Equal(t, true, kkv.InternalKeyEqual(&expKey, ik))
				pos++
			}
			require.NoError(t, iter.Close())

			require.NoError(t, testCloseSklTable(st))
		})
	}
}

func TestSklTableSklIndexRef(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	var seqNum atomic.Uint64
	var wg sync.WaitGroup
	readConcurrency := 5
	wnum := 10000
	rnum := 10000
	value := utils.FuncRandBytes(10)
	st := testNewSklTableForStiCompress(t, false, 4096)

	sleepFunc := func() {
		ms := time.Duration(rand.Intn(20) + 1)
		time.Sleep(ms * time.Microsecond)
	}

	wg.Add(3)
	go func() {
		defer wg.Done()
		for j := 0; j < wnum; j++ {
			sleepFunc()
			sn := seqNum.Add(1)
			key := base.MakeInternalKey([]byte(fmt.Sprintf("key_%d", sn)), sn, internalKeyKindSet)
			if err := st.set(key, value); err != nil {
				t.Errorf("set fail key:%s err:%s", key.String(), err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		var rgwg sync.WaitGroup
		for i := 0; i < readConcurrency; i++ {
			rgwg.Add(1)
			go func() {
				defer rgwg.Done()
				for {
					sn := seqNum.Load()
					if sn <= 100 {
						time.Sleep(time.Millisecond)
					} else {
						break
					}
				}
				for j := 0; j < rnum; j++ {
					id := rand.Intn(int(seqNum.Load()-100)) + 1
					key := []byte(fmt.Sprintf("key_%d", id))
					st.get(key, 0)
					sleepFunc()
				}
			}()
		}
		rgwg.Wait()
	}()

	go func() {
		defer wg.Done()
		var rwg sync.WaitGroup
		for i := 0; i < readConcurrency; i++ {
			rwg.Add(1)
			go func(index int) {
				defer rwg.Done()
				for j := 0; j < rnum; j++ {
					sleepFunc()
					iter := st.newIter(nil)
					iter.First()
					iter.Close()
				}
			}(i)
		}
		rwg.Wait()
	}()

	wg.Wait()

	require.Equal(t, int32(1), st.skli.l.GetRefCnt())

	require.NoError(t, testCloseSklTable(st))
}
