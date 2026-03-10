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
	"os"
	"path/filepath"
	"testing"

	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

var testTblPath = filepath.Join(testDir, "tbl")

func testNewPage(params []bool) *page {
	opts := testNewOpts(params[0], params[1])
	bpage := &Bitpage{
		dirname: testDir,
		opts:    opts,
	}
	testInitDir()
	return newPage(bpage, PageNum(1))
}

func testNewSuperTable(t *testing.T, exist bool, params []bool) *superTable {
	st, err := newSuperTable(testNewPage(params), testTblPath, FileNum(1), exist)
	require.NoError(t, err)
	return st
}

func testCloseSuperTable(st *superTable) error {
	st.p.bp.freeStArenaBuf()
	return st.close()
}

func testCheckGetKV(t *testing.T, st *superTable, k *internalKey, v []byte) {
	val, found, kind := st.get(k.UserKey, hash.Fnv32(k.UserKey))
	require.Equal(t, k.Kind(), kind)
	require.Equal(t, true, found)
	if len(v) == 0 {
		require.Equal(t, 0, len(val))
	} else {
		require.Equal(t, v, val)
	}

	found, kind = st.exist(k.UserKey, hash.Fnv32(k.UserKey))
	require.Equal(t, k.Kind(), kind)
	require.Equal(t, true, found)
}

func TestSuperTableOpen(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)

		st := testNewSuperTable(t, false, params)
		require.Equal(t, true, st.empty())
		require.Equal(t, TblVersionDefault, st.dataFile.getVersion())
		require.Equal(t, uint64(TblFileHeaderSize), st.inuseBytes())
		require.Equal(t, false, st.indexModified)

		val, found, kind := st.get([]byte("a"), 0)
		require.Equal(t, false, found)
		require.Equal(t, internalKeyKindInvalid, kind)
		require.Equal(t, []byte(nil), val)
		kn := 1000
		kkn := 5
		kvList := sortedkv.MakeSortedAllKVList(kn, kkn, 1, 10, 1)
		keyNum := len(kvList)

		read := func(s *superTable, n int) {
			for i := 0; i < n; i++ {
				testCheckGetKV(t, st, kvList[i].Key, kvList[i].Value)
			}
		}

		for i := 0; i < keyNum; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
			if i%1000 == 999 {
				require.NoError(t, st.mergeIndexes())
				read(st, i+1)
			}
		}

		read(st, keyNum)
		require.NoError(t, testCloseSuperTable(st))

		st = testNewSuperTable(t, true, params)
		require.Equal(t, false, st.empty())
		require.Equal(t, TblVersionDefault, st.dataFile.getVersion())
		read(st, keyNum)
		require.NoError(t, testCloseSuperTable(st))
	})
}

func TestSuperTableWriteRead(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)

		st := testNewSuperTable(t, false, params)
		require.Equal(t, true, st.empty())
		seqNum := uint64(1)
		kn := 1000
		kkn := 10
		mn := 5
		kvList := sortedkv.MakeSortedAllKVList(kn, kkn, mn, 10, seqNum)
		keyNum := len(kvList)
		seqNum += uint64(keyNum)

		for i := 0; i < 100; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
		require.Equal(t, false, st.empty())
		require.Equal(t, 100, len(st.offsetPending))
		require.NoError(t, st.mergeIndexes())
		require.Equal(t, 0, len(st.offsetPending))
		indexes := st.readIndexes()
		require.Equal(t, 100, indexes.oiNum)

		for i := keyNum - 20; i < keyNum; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
		require.Equal(t, 20, len(st.offsetPending))
		require.NoError(t, st.mergeIndexes())
		require.Equal(t, 0, len(st.offsetPending))
		indexes = st.readIndexes()
		require.Equal(t, 120, indexes.oiNum)

		for i := 100; i < keyNum-20; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
		require.Equal(t, keyNum-120, len(st.offsetPending))
		require.NoError(t, st.mergeIndexes())
		require.Equal(t, 0, len(st.offsetPending))
		indexes = st.readIndexes()
		require.Equal(t, keyNum, indexes.oiNum)
		for i := 0; i < keyNum; i++ {
			testCheckGetKV(t, st, kvList[i].Key, kvList[i].Value)
		}

		for i := 0; i < keyNum; i++ {
			if i%3 == 0 {
				kvList[i].Value = utils.FuncRandBytes(50)
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
			} else if i%3 == 1 {
				kvList[i].Key.SetKind(internalKeyKindDelete)
				kvList[i].Value = nil
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
			}
		}
		require.NoError(t, st.mergeIndexes())
		require.NoError(t, testCloseSuperTable(st))

		st = testNewSuperTable(t, true, params)
		indexes = st.readIndexes()
		require.Equal(t, keyNum, indexes.oiNum)
		for i := 0; i < keyNum; i++ {
			testCheckGetKV(t, st, kvList[i].Key, kvList[i].Value)
		}
		require.NoError(t, testCloseSuperTable(st))
	})
}

func TestSuperTableReadOutOfRange(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)

		st := testNewSuperTable(t, false, params)
		require.Equal(t, true, st.empty())

		seqNum := uint64(1)
		kn := 1000
		kkn := 10
		kvList := sortedkv.MakeSortedAllKVList(kn, kkn, 1, 10, seqNum)
		keyNum := len(kvList)
		seqNum += uint64(keyNum)

		for i := kkn; i < keyNum-kkn; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, st.mergeIndexes())
		indexes := st.readIndexes()
		viNum := indexes.viNum

		read := func(s *superTable) {
			found, _ := s.exist(kvList[0].Key.UserKey, 0)
			require.Equal(t, false, found)
			for i := kkn; i < keyNum-kkn; i++ {
				testCheckGetKV(t, s, kvList[i].Key, kvList[i].Value)
			}
			found, _ = s.exist(kvList[keyNum-1].Key.UserKey, 0)
			require.Equal(t, false, found)
		}

		read(st)
		require.NoError(t, testCloseSuperTable(st))

		st1 := testNewSuperTable(t, true, params)
		indexes = st1.readIndexes()
		require.Equal(t, viNum, indexes.viNum)
		require.Equal(t, keyNum-kkn*2, indexes.oiNum)
		read(st1)
		require.NoError(t, testCloseSuperTable(st1))
	})
}

func TestSuperTableMergeIndexes(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)

		st := testNewSuperTable(t, false, params)
		seqNum := uint64(1)
		kn := 1000
		kkn := 2
		kvList := sortedkv.MakeSortedAllKVList(kn, kkn, 1, 10, seqNum)
		num := len(kvList)
		seqNum += uint64(num)

		seqNum += uint64(num)
		for i := 0; i < 50; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
		require.Equal(t, 50, len(st.offsetPending))
		require.NoError(t, st.mergeIndexes())
		require.Equal(t, 0, len(st.offsetPending))
		indexes := st.readIndexes()
		require.Equal(t, 50, indexes.oiNum)

		for i := 50; i < 90; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
		require.Equal(t, 40, len(st.offsetPending))
		require.NoError(t, st.mergeIndexes())
		require.Equal(t, 0, len(st.offsetPending))
		indexes = st.readIndexes()
		require.Equal(t, 90, indexes.oiNum)

		for i := 20; i < 90; i++ {
			testCheckGetKV(t, st, kvList[i].Key, kvList[i].Value)
		}

		require.NoError(t, st.mergeIndexes())

		wn := 0
		for i := 20; i < num; i++ {
			if i < 90 {
				if i%2 == 0 {
					kvList[i].Value = nil
					kvList[i].Key.SetSeqNum(seqNum)
					kvList[i].Key.SetKind(internalKeyKindDelete)
					seqNum++
					require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
					wn++
				} else if i%3 == 0 {
					kvList[i].Value = utils.FuncRandBytes(20)
					kvList[i].Key.SetSeqNum(seqNum)
					seqNum++
					require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
					wn++
				}
			} else {
				require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
				wn++
			}
		}
		require.Equal(t, wn, len(st.offsetPending))
		require.NoError(t, st.mergeIndexes())

		read := func(s *superTable) {
			for i := 0; i < num; i++ {
				val, found, kind := s.get(kvList[i].Key.UserKey, hash.Fnv32(kvList[i].Key.UserKey))
				require.Equal(t, true, found)
				if i >= 20 && i < 90 && i%2 == 0 {
					require.Equal(t, internalKeyKindDelete, kind)
					require.Equal(t, 0, len(val))
				} else {
					require.Equal(t, internalKeyKindSet, kind)
					if len(kvList[i].Value) == 0 {
						require.Equal(t, 0, len(val))
					} else {
						require.Equal(t, kvList[i].Value, val)
					}
				}
			}
		}

		read(st)

		wn = 0
		for i := 30; i < 50; i++ {
			if i%2 == 0 {
				kvList[i].Value = nil
				kvList[i].Key.SetSeqNum(seqNum)
				kvList[i].Key.SetKind(internalKeyKindDelete)
				seqNum++
				require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
				wn++
			} else if i%3 == 0 {
				kvList[i].Value = utils.FuncRandBytes(20)
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
				wn++
			}
		}
		require.Equal(t, wn, len(st.offsetPending))
		require.NoError(t, st.mergeIndexes())
		require.Equal(t, 0, len(st.offsetPending))

		read(st)
		require.NoError(t, st.close())

		st = testNewSuperTable(t, true, params)
		read(st)
		require.NoError(t, st.close())
		st = testNewSuperTable(t, true, params)
		read(st)
		require.NoError(t, st.close())
	})
}

func TestSuperTableIter(t *testing.T) {
	testcase(func(params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		st := testNewSuperTable(t, false, params)
		kn := 1000
		kkn := 10
		mn := 5
		seqNum := uint64(1)
		kvList := sortedkv.MakeSortedAllKVList(kn, kkn, mn, 10, seqNum)
		keyNum := len(kvList)
		seqNum += uint64(keyNum)
		for i := 0; i < keyNum; i++ {
			require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, st.mergeIndexes())

		checkKey := func(pos int, key *InternalKKVKey) {
			expKey := kkv.MakeInternalKey(*kvList[pos].Key)
			require.Equal(t, true, kkv.InternalKeyEqual(&expKey, key))
		}

		seekFunc := func(s *superTable) {
			for j := 0; j < keyNum; j++ {
				testCheckGetKV(t, s, kvList[j].Key, kvList[j].Value)
			}

			jj := 0
			step := 0
			for jj < keyNum {
				j := jj
				kv := kvList[j]
				dataType := kkv.GetKeyDataType(kv.Key.UserKey)
				if dataType == kkv.DataTypeZsetIndex || dataType == kkv.DataTypeZset {
					step = kkn * mn
				} else {
					step = kkn
				}
				jj += step
				if kkv.IsUseIterFirst(dataType) {
					continue
				}
				keyVersion := kkv.DecodeKeyVersion(kv.Key.UserKey)
				var lowerBound [kkv.SubKeyHeaderLength]byte
				var upperBound [kkv.SubKeyUpperBoundLength]byte
				kkv.EncodeSubKeyLowerBound(lowerBound[:], dataType, keyVersion)
				kkv.EncodeSubKeyUpperBound(upperBound[:], dataType, keyVersion)
				opts := &iterOptions{
					DataType:   dataType,
					LowerBound: lowerBound[:],
					UpperBound: upperBound[:],
					IsTest:     true,
				}
				iter := s.newIter(opts)
				k := 0
				for key, val := iter.SeekGE(opts.LowerBound); key != nil; key, val = iter.Next() {
					checkKey(j+k, key)
					if len(kvList[j+k].Value) == 0 {
						require.Equal(t, 0, len(val))
					} else {
						require.Equal(t, kvList[j+k].Value, val)
					}
					k++
				}
				require.Equal(t, step, k)
				require.NoError(t, iter.Close())

				iter = s.newIter(opts)
				k = 0
				for key, val := iter.SeekLT(opts.UpperBound); key != nil; key, val = iter.Prev() {
					pos := j + step - k - 1
					checkKey(pos, key)
					if len(kvList[pos].Value) == 0 {
						require.Equal(t, 0, len(val))
					} else {
						require.Equal(t, kvList[pos].Value, val)
					}
					k++
				}
				require.Equal(t, step, k)
				require.NoError(t, iter.Close())

				iter = s.newIter(opts)
				pos := j + step - 3
				key, val := iter.SeekGE(kvList[pos].Key.UserKey)
				if kkv.IsUseIterFirst(dataType) {
					pos = j
				}
				checkKey(pos, key)
				if len(kvList[pos].Value) == 0 {
					require.Equal(t, 0, len(val))
				} else {
					require.Equal(t, kvList[pos].Value, val)
				}
				require.NoError(t, iter.Close())

				iter = s.newIter(opts)
				pos = j + step - 3
				key, val = iter.SeekLT(kvList[pos].Key.UserKey)
				pos -= 1
				checkKey(pos, key)
				if len(kvList[pos].Value) == 0 {
					require.Equal(t, 0, len(val))
				} else {
					require.Equal(t, kvList[pos].Value, val)
				}
				require.NoError(t, iter.Close())
			}

			iter := s.newIter(nil)
			i := 0
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				checkKey(i, key)
				if len(kvList[i].Value) == 0 {
					require.Equal(t, 0, len(val))
				} else {
					require.Equal(t, kvList[i].Value, val)
				}
				i++
			}
			require.NoError(t, iter.Close())
			require.Equal(t, keyNum, i)
		}

		seekFunc(st)
		require.NoError(t, testCloseSuperTable(st))

		st1 := testNewSuperTable(t, true, params)
		seekFunc(st1)
		require.NoError(t, testCloseSuperTable(st1))
	})

}

func TestSuperTableEmpty(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)
		st := testNewSuperTable(t, false, params)
		require.Equal(t, true, st.empty())
		require.NoError(t, st.close())
		st1 := testNewSuperTable(t, true, params)
		require.Equal(t, true, st1.empty())
		require.NoError(t, st1.close())
		st2 := testNewSuperTable(t, true, params)
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
				val, found, kind := s.get(kvList[i].Key.UserKey, hash.Fnv32(kvList[i].Key.UserKey))
				require.Equal(t, true, found)
				require.Equal(t, internalKeyKindSet, kind)
				require.Equal(t, kvList[i].Value, val)
			}
		}
		checkKV(st2)
		require.NoError(t, st2.close())
		st3 := testNewSuperTable(t, true, params)
		require.Equal(t, false, st3.empty())
		checkKV(st3)
		require.NoError(t, st3.close())
	})
}

func TestSuperTableRebuild(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)

		st := testNewSuperTable(t, false, params)
		kn := 1000
		kkn := 10
		seqNum := uint64(1)
		kvList := sortedkv.MakeSortedAllKVList(kn, kkn, 1, 10, seqNum)
		num := len(kvList)
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
			require.NoError(t, st.mergeIndexes())
		}
		step := 1000
		for pos := 0; pos < num; pos += step {
			writeData(pos, pos+step)
			pos -= 500
		}
		for i := 0; i < num/2; i++ {
			if i%5 == 0 {
				kvList[i].Key.SetKind(internalKeyKindSet)
				kvList[i].Value = utils.FuncRandBytes(20)
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				require.NoError(t, st.set(*kvList[i].Key, kvList[i].Value))
			}
		}
		require.NoError(t, st.mergeIndexes())

		checkKV := func(s *superTable) {
			for i := 0; i < num; i++ {
				uk := kvList[i].Key.UserKey
				val, found, kind := s.get(uk, 0)
				require.Equal(t, true, found)
				require.Equal(t, kvList[i].Key.Kind(), kind)
				if i >= num/2 && i%5 == 0 {
					require.Equal(t, 0, len(val))
				} else {
					require.Equal(t, kvList[i].Value, val)
				}
			}
		}

		checkKV(st)

		st = testNewSuperTable(t, true, params)
		checkKV(st)
		require.Equal(t, true, st.indexModified)
		require.NoError(t, testCloseSuperTable(st))

		st = testNewSuperTable(t, true, params)
		checkKV(st)
		require.Equal(t, false, st.indexModified)
		require.NoError(t, testCloseSuperTable(st))
	})
}

func TestSuperTableRebuild1(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)

		st := testNewSuperTable(t, false, params)
		kn := 1000
		kkn := 5
		seqNum := uint64(1)
		kvList := sortedkv.MakeSortedAllKVList(kn, kkn, 1, 100, seqNum)
		num := len(kvList)
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
			require.NoError(t, s.mergeIndexes())
		}

		checkKV := func(s *superTable) {
			for i := 0; i < num; i++ {
				val, found, kind := s.get(kvList[i].Key.UserKey, hash.Fnv32(kvList[i].Key.UserKey))
				require.Equal(t, kvList[i].Key.Kind(), kind)
				require.Equal(t, true, found)
				if len(kvList[i].Value) == 0 {
					require.Equal(t, 0, len(val))
				} else {
					require.Equal(t, kvList[i].Value, val)
				}
			}
		}

		writeData(st, 0, 300)
		writeData(st, 700, num)
		writeData(st, 500, 750)
		writeData(st, 350, 600)
		writeData(st, 120, 500)
		_, err := st.dataFile.Write([]byte("panic"))
		require.NoError(t, err)
		require.NoError(t, st.dataFile.Fdatasync())
		checkKV(st)

		st1 := testNewSuperTable(t, true, params)
		checkKV(st1)
		writeData(st1, 0, 10)
		require.NoError(t, testCloseSuperTable(st1))

		st2 := testNewSuperTable(t, true, params)
		checkKV(st2)
		require.NoError(t, testCloseSuperTable(st2))
	})
}

func TestSuperTableUseMiniViChanged(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	st := testNewSuperTable(t, false, []bool{true, true})
	kn := 1000
	kkn := 5
	seqNum := uint64(1)
	kvList := sortedkv.MakeSortedAllKVList(kn, kkn, 1, 100, seqNum)
	num := len(kvList)
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
		require.NoError(t, s.mergeIndexes())
	}

	checkKV := func(s *superTable) {
		for i := 0; i < num; i++ {
			val, found, kind := s.get(kvList[i].Key.UserKey, hash.Fnv32(kvList[i].Key.UserKey))
			require.Equal(t, kvList[i].Key.Kind(), kind)
			require.Equal(t, true, found)
			if len(kvList[i].Value) == 0 {
				require.Equal(t, 0, len(val))
			} else {
				require.Equal(t, kvList[i].Value, val)
			}
		}
	}

	writeData(st, 0, 300)
	writeData(st, 700, num)
	writeData(st, 500, 750)
	writeData(st, 350, 600)
	writeData(st, 120, 500)
	checkKV(st)
	require.NoError(t, testCloseSuperTable(st))

	st1 := testNewSuperTable(t, true, []bool{true, false})
	checkKV(st1)
	writeData(st1, 0, 10)
	require.NoError(t, testCloseSuperTable(st1))

	st2 := testNewSuperTable(t, true, []bool{true, true})
	checkKV(st2)
	require.NoError(t, testCloseSuperTable(st2))
}

func TestSuperTableWRLoop(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		for loop := 0; loop < 5; loop++ {
			os.RemoveAll(testDir)
			st := testNewSuperTable(t, false, params)
			kn := 500
			kkn := 5
			seqNum := uint64(1)
			kvList := sortedkv.MakeSortedAllKVList(kn, kkn, 1, 10, seqNum)
			num := len(kvList)
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
				require.NoError(t, s.mergeIndexes())
			}
			step := 100
			for pos := 0; pos < num; pos += step {
				writeData(st, pos, pos+step)
				pos -= 50
			}
			require.NoError(t, testCloseSuperTable(st))

			st = testNewSuperTable(t, true, params)
			for i := 0; i < num; i++ {
				val, found, kind := st.get(kvList[i].Key.UserKey, hash.Fnv32(kvList[i].Key.UserKey))
				require.Equal(t, true, found)
				require.Equal(t, kvList[i].Key.Kind(), kind)
				if i%5 == 0 {
					require.Equal(t, 0, len(val))
				} else {
					require.Equal(t, kvList[i].Value, val)
				}
			}
			require.NoError(t, testCloseSuperTable(st))
		}
	})
}

//
//func TestReadSuperTable(t *testing.T) {
//	path := "test/1_2.xt"
//	opts := testNewOpts(true, false)
//	bpage := &Bitpage{
//		dirname: testDir,
//		opts:    opts,
//	}
//	testInitDir()
//	pg := newPage(bpage, PageNum(1))
//	s, err := newSuperTable(pg, path, FileNum(2), true)
//	require.NoError(t, err)
//	defer s.close()
//
//	var n int
//	dataFileSize := int(s.dataFile.Size())
//	dataReadBuf := make([]byte, dataFileSize)
//	n, err = s.dataFile.ReadAt(dataReadBuf, 0)
//	require.NoError(t, err)
//	require.Equal(t, dataFileSize, n)
//
//	var prevKey []byte
//	var keyOffset, offset int
//	var keySize uint32
//	var valueSize uint16
//	var its []internalIterator
//
//	sstIter := &sstIterator{}
//	offset = TblFileHeaderSize
//	for {
//		keyOffset = offset
//		if len(dataReadBuf[offset:]) < TblItemKeySize {
//			break
//		}
//
//		keySize, valueSize = decodeKeySize(dataReadBuf[offset:])
//		if keySize == 0 {
//			break
//		}
//		kz := int(keySize) + int(valueSize)
//		if valueSize == 0 {
//			offset += TblItemKeySize
//		} else {
//			offset += TblItemHeaderSize
//		}
//
//		if offset > dataFileSize || len(dataReadBuf[offset:]) < kz {
//			break
//		}
//
//		ikey := base.DecodeInternalKey(dataReadBuf[offset : offset+int(keySize)])
//		offset += kz
//
//		if prevKey != nil && bytes.Compare(prevKey, ikey.UserKey) >= 0 {
//			its = append(its, sstIter)
//			sstIter = &sstIterator{}
//		}
//		sstIter.data = append(sstIter.data, sstItem{
//			key:    ikey,
//			offset: uint32(keyOffset),
//		})
//		sstIter.num += 1
//		prevKey = ikey.UserKey
//	}
//
//	if len(sstIter.data) > 0 {
//		its = append(its, sstIter)
//	}
//
//	iiter := iterator.NewMergingIter(s.logger, s.p.bp.opts.Cmp, its...)
//	iter := &compactionIter{
//		bp:   s.p.bp,
//		iter: iiter,
//	}
//	defer iter.Close()
//
//	var offsetPending []uint32
//	for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
//		offsetPending = append(offsetPending, binary.BigEndian.Uint32(iv))
//	}
//
//	indexes := s.readIndexes()
//	fmt.Println("indexes", indexes.oiNum, len(offsetPending))
//	for i := range indexes.oi {
//		if indexes.oi[i] != offsetPending[i] {
//			fmt.Println("not eq", i, indexes.oi[i], offsetPending[i])
//		}
//	}
//}
//
//func TestIterSuperTable(t *testing.T) {
//	path := "test/1_2.xt"
//	opts := testNewOpts(true, true)
//	bpage := &Bitpage{
//		dirname: testDir,
//		opts:    opts,
//	}
//	testInitDir()
//	pg := newPage(bpage, PageNum(1))
//	st, err := newSuperTable(pg, path, FileNum(2), true)
//	require.NoError(t, err)
//	defer st.close()
//
//	dataType := kkv.DataTypeZsetIndex
//	keyVersion := uint64(13809639)
//	var lowerBound [kkv.SubKeyHeaderLength]byte
//	var upperBound [kkv.SubKeyUpperBoundLength]byte
//	kkv.EncodeSubKeyLowerBound(lowerBound[:], dataType, keyVersion)
//	kkv.EncodeSubKeyUpperBound(upperBound[:], dataType, keyVersion)
//	iterOpts := &iterOptions{
//		DataType:   dataType,
//		LowerBound: lowerBound[:],
//		UpperBound: upperBound[:],
//		IsTest:     true,
//	}
//	iter := st.newIter(iterOpts)
//	defer iter.Close()
//	for k, v := iter.SeekLT(iterOpts.UpperBound); k != nil; k, v = iter.Next() {
//		fmt.Println("iter", kkv.DecodeKeyVersion(k.UserKey),
//			kkv.GetKeyDataType(k.UserKey), string(k.UserKey[17:]), utils.ByteSortToFloat64(k.UserKey[9:17]), v)
//	}
//}
