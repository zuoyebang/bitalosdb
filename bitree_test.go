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

package bitalosdb

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

var testOptsParams = [][]bool{
	{true, false, false},
	{true, false, true},
	{true, true, false},
	{true, true, true},
}

func testOpenDB(useBitable bool) *DB {
	optspool := options.InitDefaultsOptionsPool()
	testOptsUseBitable = useBitable
	optspool.BaseOptions.BitpageFlushSize = 10 << 20
	optspool.BaseOptions.BitpageSplitSize = 15 << 20
	optspool.BithashOptions.TableMaxSize = 10 << 20
	return openTestDB(testDirname, optspool)
}

func testOpenDB0(useBitable bool) *DB {
	optspool := options.InitDefaultsOptionsPool()
	testOptsUseBitable = useBitable
	optspool.BaseOptions.KvSeparateSize = 2000
	optspool.BaseOptions.BitpageFlushSize = 1 << 20
	optspool.BaseOptions.BitpageSplitSize = 2 << 20
	return openTestDB(testDirname, optspool)
}

func testOpenDB1(params []bool) *DB {
	optspool := options.InitDefaultsOptionsPool()
	testOptsUseBitable = true
	testOptsUseMapIndex = params[0]
	testOptsUsePrefixCompress = params[1]
	testOptsUseBlockCompress = params[2]
	optspool.BaseOptions.KvSeparateSize = 2000
	optspool.BaseOptions.BitpageFlushSize = 1 << 20
	optspool.BaseOptions.BitpageSplitSize = 2 << 20
	return openTestDB(testDirname, optspool)
}

func testOpenDB2(params []bool) *DB {
	optspool := options.InitDefaultsOptionsPool()
	testOptsUseBitable = true
	testOptsUseMapIndex = params[0]
	testOptsUsePrefixCompress = params[1]
	testOptsUseBlockCompress = params[2]
	optspool.BaseOptions.UseBitable = true
	optspool.BaseOptions.BitpageFlushSize = 10 << 20
	optspool.BaseOptions.BitpageSplitSize = 15 << 20
	optspool.BithashOptions.TableMaxSize = 10 << 20
	return openTestDB(testDirname, optspool)
}

func testMakeSortedKey(i int) []byte {
	return sortedkv.MakeSortedKey(i)
}

func testMakeSortedKVList(start, end int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKVList(start, end, seqNum, vsize)
}

func testMakeSortedKV2List(start, end int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKV2List(start, end, seqNum, vsize)
}

func testMakeSlotSortedKVList(start, end int, seqNum uint64, vsize int, slotId uint16) sortedkv.SortedKVList {
	return sortedkv.MakeSlotSortedKVList(start, end, seqNum, vsize, slotId)
}

func TestBitowerWriterFlush(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := testOpenDB(false)
	defer func() {
		require.NoError(t, db.Close())
	}()
	seqNum := uint64(0)

	for i := range db.bitowers {
		largeValue := utils.FuncRandBytes(520)
		smallValue := utils.FuncRandBytes(500)
		keyCount := 100
		kvList := testMakeSlotSortedKVList(0, keyCount, seqNum, 1, uint16(i))
		bw, err := db.bitowers[i].newFlushWriter()
		require.NoError(t, err)
		seqNum += uint64(keyCount)

		for j := 0; j < keyCount; j++ {
			if j%2 == 0 {
				kvList[j].Value = smallValue
			} else {
				kvList[j].Value = largeValue
			}
			require.NoError(t, bw.Set(*kvList[j].Key, kvList[j].Value))
		}
		require.NoError(t, bw.Finish())

		for j := 0; j < keyCount; j++ {
			require.NoError(t, verifyGet(db, kvList[i].Key.UserKey, kvList[i].Value))
		}
	}
}

func TestDbWriterFlush(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := testOpenDB(false)
	w, err := db.newFlushWriter()
	require.NoError(t, err)
	largeValue := utils.FuncRandBytes(520)
	smallValue := utils.FuncRandBytes(500)
	keyCount := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKVList(0, keyCount, seqNum, 1)
	seqNum += uint64(keyCount)

	for i := 0; i < keyCount; i++ {
		if i%2 == 0 {
			kvList[i].Value = smallValue
		} else {
			kvList[i].Value = largeValue
		}
		require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, w.Finish())

	for i := 0; i < keyCount; i++ {
		require.NoError(t, verifyGet(db, kvList[i].Key.UserKey, kvList[i].Value))
	}

	require.NoError(t, db.Close())
}

func testcase(caseFunc func([]bool)) {
	for _, params := range testOptsParams {
		fmt.Printf("testcase params:%v\n", params)
		caseFunc(params)
	}
}

func TestBitreeWrite(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDirname)
		os.RemoveAll(testDirname)
		db := testOpenDB1(params)
		loop := 6
		step := 3000
		count := loop * step
		seqNum := uint64(0)
		kvList := testMakeSlotSortedKVList(0, count, seqNum, 1024, testSlotId)
		index := base.GetBitowerIndex(int(testSlotId))
		bitower := db.bitowers[index]
		seqNum += uint64(count)

		writeData := func(index int) {
			w, err := bitower.newFlushWriter()
			require.NoError(t, err)
			start := index * step
			end := start + step
			for i := start; i < end; i++ {
				require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, w.Finish())

			time.Sleep(2 * time.Second)
			for i := start; i < end; i++ {
				require.NoError(t, verifyGet(db, kvList[i].Key.UserKey, kvList[i].Value))
			}
		}

		for i := 0; i < loop; i++ {
			writeData(i)
		}

		bdbIter := bitower.btree.NewBdbIter()
		i := 0
		for ik, iv := bdbIter.First(); ik != nil; ik, iv = bdbIter.Next() {
			fmt.Println("bdbIter", i, ik.String(), utils.BytesToUint32(iv))
			i++
		}
		require.Equal(t, 13, i)
		require.NoError(t, bdbIter.Close())

		require.Equal(t, uint64(6), db.dbState.GetBitpageFlushCount())
		require.Equal(t, uint64(6), db.dbState.GetBitpageSplitCount())
		require.NoError(t, db.Close())
	})
}

func TestBitreeSetPrefixDeleteKey(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := testOpenDB0(false)

	loop := 20
	step := 2000
	kvNum := loop * step
	seqNum := uint64(1)
	kvList := sortedkv.MakeSortedSamePrefixDeleteKVList2(0, kvNum, seqNum, 1024, testSlotId, 1500)
	seqNum += uint64(kvNum + 1)
	kvList2 := sortedkv.MakeSortedSameVersionKVList(0, 2000, seqNum, 1000000, 1024, testSlotId)
	index := base.GetBitowerIndex(int(testSlotId))
	bitower := db.bitowers[index]

	writeData := func(index int) {
		w, err := bitower.newFlushWriter()
		require.NoError(t, err)
		start := index * step
		end := start + step
		for i := start; i < end; i++ {
			require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, w.Finish())
		time.Sleep(1 * time.Second)
	}

	for i := 0; i < loop; i++ {
		writeData(i)
	}

	pdVersions := []uint64{102, 108, 112, 120, 126, 130}
	isPdVersion := func(n uint64) bool {
		for i := range pdVersions {
			if pdVersions[i] == n {
				return true
			}
		}
		return false
	}

	for _, v := range pdVersions {
		seqNum++
		pdKey := sortedkv.MakeKey2(nil, testSlotId, v)
		pdIkey := base.MakeInternalKey(pdKey, seqNum, base.InternalKeyKindPrefixDelete)
		kvList2 = append(kvList2, sortedkv.SortedKVItem{
			Key:   &pdIkey,
			Value: nil,
		})
	}
	sort.Sort(kvList2)
	w, err := bitower.newFlushWriter()
	require.NoError(t, err)
	for i := range kvList2 {
		require.NoError(t, w.Set(*kvList2[i].Key, kvList2[i].Value))
	}
	require.NoError(t, w.Finish())
	time.Sleep(1 * time.Second)

	bitower.btree.ManualFlushBitpage()
	time.Sleep(3 * time.Second)

	for i := 0; i < kvNum; i++ {
		key := kvList[i].Key.UserKey
		v, vcloser, err := db.Get(key)
		ver := db.opts.KeyPrefixDeleteFunc(key)
		if isPdVersion(ver) {
			require.Equal(t, ErrNotFound, err)
		} else {
			require.Equal(t, nil, err)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}
	}

	require.NoError(t, db.Close())
}

func TestBitreeKeyExpire(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := testOpenDB0(true)
	defer func() {
		require.NoError(t, db.Close())
	}()
	count := 30000
	seqNum := uint64(0)
	kvList := testMakeSortedKV2List(0, count, seqNum, 1)
	seqNum += uint64(count)
	bithashCount := 0
	bithashDelCount1 := 0
	bithashDelCount := 0
	smallValBytes := utils.FuncRandBytes(1900)
	largeValBytes := utils.FuncRandBytes(2100)
	now := uint64(time.Now().UnixMilli())

	makeValue := func(i int, valBytes []byte) []byte {
		var val []byte
		var ttl uint64
		if i%5 == 0 {
			if i%15 == 0 {
				ttl = now + 5000
			} else {
				ttl = now - 1000
			}
			val = make([]byte, len(valBytes)+9)
			val[0] = 1
			binary.BigEndian.PutUint64(val[1:9], ttl)
			copy(val[9:], valBytes)
		} else {
			ttl = now + 100000
			if i%3 == 0 {
				val = make([]byte, len(valBytes)+1)
				val[0] = 2
				copy(val[1:], valBytes)
			} else {
				val = make([]byte, len(valBytes)+9)
				val[0] = 1
				if i%7 == 0 {
					ttl = 0
				}
				binary.BigEndian.PutUint64(val[1:9], ttl)
				copy(val[9:], valBytes)
			}
		}

		return val
	}

	w, err := db.newFlushWriter()
	require.NoError(t, err)

	var value []byte
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			value = makeValue(i, largeValBytes)
			bithashCount++
			if i%5 == 0 {
				bithashDelCount++
				if i%15 != 0 {
					bithashDelCount1++
				}
			}
		} else {
			value = makeValue(i, smallValBytes)
		}
		kvList[i].Value = value
		require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, w.Finish())
	time.Sleep(4 * time.Second)

	for i := 0; i < count; i++ {
		k := kvList[i].Key.UserKey
		v, vcloser, err := db.Get(k)
		if i%5 == 0 && i%15 != 0 {
			require.Equal(t, ErrNotFound, err)
		} else {
			require.Equal(t, nil, err)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}
	}

	stats := db.MetricsInfo()
	require.Equal(t, bithashCount, stats.BithashKeyTotal)
	require.Equal(t, bithashDelCount1, stats.BithashDelKeyTotal)

	time.Sleep(5 * time.Second)
	db.compactToBitable(1)

	stats = db.MetricsInfo()
	require.Equal(t, bithashCount, stats.BithashKeyTotal)
	require.Equal(t, bithashDelCount, stats.BithashDelKeyTotal)

	for i := 0; i < count; i++ {
		k := kvList[i].Key.UserKey
		v, vcloser, err := db.Get(k)
		if i%5 == 0 {
			require.Equal(t, ErrNotFound, err)
			if v != nil || vcloser != nil {
				t.Fatal("find expire key return not nil", string(k), i, v)
			}
		} else {
			require.Equal(t, nil, err)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}
	}
}

func TestBitreeCheckpointLock(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := testOpenDB0(true)
	defer func() {
		require.NoError(t, db.Close())
	}()
	seqNum := uint64(0)
	loop := 6
	step := 3000
	count := (loop + 1) * step
	kvList := testMakeSlotSortedKVList(0, count, seqNum, 1024, testSlotId)
	index := base.GetBitowerIndex(int(testSlotId))
	bitower := db.bitowers[index]
	seqNum += uint64(count)

	writeData := func(index int) {
		w, err := bitower.newFlushWriter()
		require.NoError(t, err)
		start := index * step
		end := start + step
		for i := start; i < end; i++ {
			require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, w.Finish())

		for i := start; i < end; i++ {
			require.NoError(t, verifyGet(db, kvList[i].Key.UserKey, kvList[i].Value))
		}

		time.Sleep(2 * time.Second)

		if index == 2 {
			db.dbState.SetDbHighPriority(true)
		} else if index > 2 && index <= 5 {
			require.Equal(t, uint64(3), db.dbState.GetBitpageFlushCount())
			require.Equal(t, uint64(3), db.dbState.GetBitpageSplitCount())
			if index == 5 {
				db.dbState.SetDbHighPriority(false)
			}
		}
	}

	for i := 0; i < loop; i++ {
		writeData(i)
	}

	time.Sleep(3 * time.Second)
	require.Equal(t, uint64(4), db.dbState.GetBitpageFlushCount())
	require.Equal(t, uint64(4), db.dbState.GetBitpageSplitCount())

	writeData(loop)
	time.Sleep(2 * time.Second)
	require.Equal(t, uint64(5), db.dbState.GetBitpageFlushCount())
	require.Equal(t, uint64(5), db.dbState.GetBitpageSplitCount())

	for i := 0; i < count; i++ {
		require.NoError(t, verifyGet(db, kvList[i].Key.UserKey, kvList[i].Value))
	}
}

func TestBitableDeleteExpire(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	for _, vlen := range []int{200, 2048} {
		fmt.Println("vlen=", vlen)
		db := testOpenDB(true)
		now := uint64(time.Now().UnixMilli())
		makeValue := func(i int, valBytes []byte) []byte {
			var val []byte
			var ttl uint64
			if i%5 == 0 {
				ttl = now + 3000
			} else {
				ttl = now + 30000
			}
			val = make([]byte, len(valBytes)+9)
			val[0] = 1
			binary.BigEndian.PutUint64(val[1:9], ttl)
			copy(val[9:], valBytes)
			return val
		}

		seqNum := uint64(0)
		count := 100
		kvList := testMakeSortedKVList(0, count, seqNum, vlen)
		seqNum += uint64(count)

		bw, err := db.newFlushWriter()
		require.NoError(t, err)
		for i := 0; i < count; i++ {
			kvList[i].Value = makeValue(i, kvList[i].Value)
			require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, bw.Finish())

		readKV := func() {
			for i := 0; i < count; i++ {
				require.NoError(t, verifyGet(db, kvList[i].Key.UserKey, kvList[i].Value))
			}
		}

		readKV()
		db.compactToBitable(1)
		readKV()

		bw, err = db.newFlushWriter()
		require.NoError(t, err)
		for i := 0; i < count; i++ {
			if i%2 == 0 {
				kvList[i].Key.SetKind(base.InternalKeyKindDelete)
				kvList[i].Value = []byte(nil)
			} else {
				kvList[i].Key.SetKind(base.InternalKeyKindSet)
				kvList[i].Value = makeValue(i, kvList[i].Value)
			}
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, bw.Finish())

		readDeleteKV := func(jobId int) {
			fmt.Println("readDeleteKV", jobId)
			for i := 0; i < count; i++ {
				v, vcloser, err := db.Get(kvList[i].Key.UserKey)
				if jobId == 2 {
					if i%2 == 0 || i%5 == 0 {
						if err != ErrNotFound {
							t.Fatalf("find expire key:%s", kvList[i].Key.String())
						}
					} else {
						require.Equal(t, nil, err)
						require.Equal(t, kvList[i].Value, v)
						vcloser()
					}
				} else {
					if i%2 == 0 {
						require.Equal(t, ErrNotFound, err)
					} else {
						require.Equal(t, nil, err)
						require.Equal(t, kvList[i].Value, v)
						vcloser()
					}
				}
			}
		}

		readDeleteKV(1)
		time.Sleep(4 * time.Second)
		db.compactToBitable(2)
		readDeleteKV(2)

		require.NoError(t, db.Close())
	}
}

func TestBitableExpire(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	for _, vlen := range []int{200, 2048} {
		isBithash := vlen > consts.KvSeparateSize
		bithashDelCount := 0
		db := testOpenDB(true)

		seqNum := uint64(0)
		count := 1000
		kvList := testMakeSortedKV2List(0, count, seqNum, vlen)
		seqNum += uint64(count)
		now := uint64(time.Now().UnixMilli())

		makeValue := func(i int, valBytes []byte) []byte {
			var val []byte
			var ttl uint64
			if i%5 == 0 {
				ttl = now + 4000
			} else {
				ttl = now + 30000
			}
			val = make([]byte, len(valBytes)+9)
			val[0] = 1
			binary.BigEndian.PutUint64(val[1:9], ttl)
			copy(val[9:], valBytes)
			return val
		}

		writeData := func(stat bool) {
			w, err := db.newFlushWriter()
			require.NoError(t, err)
			for i := 0; i < count; i++ {
				kvList[i].Key.SetKind(base.InternalKeyKindSet)
				kvList[i].Value = makeValue(i, kvList[i].Value)
				require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))

				if isBithash && i%5 == 0 {
					bithashDelCount++
				}
			}
			require.NoError(t, w.Finish())
		}

		readKV := func(deleted bool) {
			for i := 0; i < count; i++ {
				v, vcloser, err := db.Get(kvList[i].Key.UserKey)
				if deleted && i%5 == 0 {
					if err != ErrNotFound {
						t.Fatalf("find expire key:%s", kvList[i].Key.String())
					}
				} else {
					require.Equal(t, nil, err)
					require.Equal(t, kvList[i].Value, v)
					vcloser()
				}
			}
		}

		writeData(false)
		readKV(false)
		db.compactToBitable(1)
		readKV(false)

		time.Sleep(5 * time.Second)

		w, err := db.newFlushWriter()
		require.NoError(t, err)
		seqNum += uint64(count)
		for i := 0; i < count; i++ {
			if i%5 == 0 {
				continue
			}
			kvList[i].Value = makeValue(i, kvList[i].Value)
			require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, w.Finish())
		db.compactToBitable(2)
		if isBithash {
			require.Equal(t, bithashDelCount, db.MetricsInfo().BithashDelKeyTotal)
		}
		readKV(true)

		require.NoError(t, db.Close())
	}
}

func TestBitpageSplitEmptyPage(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := testOpenDB(false)
	defer func() {
		require.NoError(t, db.Close())
	}()
	seqNum := uint64(1)
	largeValue := utils.FuncRandBytes(600)
	smallValue := utils.FuncRandBytes(500)
	count := uint64(1000000)
	var writeIndex atomic.Uint64
	step := uint64(100000)

	write := func() bool {
		db.dbState.LockDbWrite()
		defer db.dbState.UnlockDbWrite()

		i := writeIndex.Load()
		if i >= count {
			return true
		}
		w, err := db.newFlushWriter()
		require.NoError(t, err)
		start := i
		end := start + step
		kvList := testMakeSortedKVList(int(start), int(end), seqNum, 1)
		seqNum += step
		for j := 0; j < int(step); j++ {
			n := sortedkv.ParseSortedKeyInt(kvList[j].Key.UserKey)
			if n%100 == 0 {
				kvList[j].Key.SetKind(base.InternalKeyKindDelete)
				kvList[j].Value = []byte(nil)
			} else {
				if n%2 == 0 {
					kvList[j].Value = smallValue
				} else {
					kvList[j].Value = largeValue
				}
			}
			require.NoError(t, w.Set(*kvList[j].Key, kvList[j].Value))
		}
		require.NoError(t, w.Finish())
		writeIndex.Store(end)
		return false
	}

	for {
		if write() {
			break
		}
	}

	read := func(ri int) {
		key := testMakeSortedKey(ri)
		v, vcloser, err := db.Get(key)
		if ri%100 == 0 {
			if err == nil {
				t.Fatalf("read del key found key:%s ri:%d", string(key), ri)
			}
			if vcloser != nil {
				t.Fatalf("vcloser is not nil key:%s", string(key))
			}
		} else {
			require.Equal(t, nil, err)
			if ri%2 == 0 {
				require.Equal(t, smallValue, v)
			} else {
				require.Equal(t, largeValue, v)
			}
			vcloser()
		}
	}

	jend := int(writeIndex.Load())
	for j := 0; j < jend; j++ {
		read(j)
	}
}

func TestBitreeWriteConcurrentRead(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDirname)
		os.RemoveAll(testDirname)

		db := testOpenDB2(params)

		wd := func() {
			seqNum := uint64(1)
			largeValue := utils.FuncRandBytes(consts.KvSeparateSize * 2)
			smallValue := utils.FuncRandBytes(consts.KvSeparateSize / 2)
			count := uint64(5000000)
			var writeIndex atomic.Uint64
			closeCh := make(chan struct{})
			step := uint64(100000)

			write := func() bool {
				db.dbState.LockDbWrite()
				defer db.dbState.UnlockDbWrite()

				i := writeIndex.Load()
				if i >= count {
					return true
				}
				w, err := db.newFlushWriter()
				require.NoError(t, err)
				start := i
				end := start + step
				kvList := testMakeSortedKVList(int(start), int(end), seqNum, 1)
				seqNum += step
				for j := 0; j < int(step); j++ {
					n := sortedkv.ParseSortedKeyInt(kvList[j].Key.UserKey)
					if n%100 == 0 {
						kvList[j].Key.SetKind(base.InternalKeyKindDelete)
						kvList[j].Value = []byte(nil)
					} else {
						if n%2 == 0 {
							kvList[j].Value = smallValue
						} else {
							kvList[j].Value = largeValue
						}
					}
					require.NoError(t, w.Set(*kvList[j].Key, kvList[j].Value))
				}
				require.NoError(t, w.Finish())
				writeIndex.Store(end)
				return false
			}

			read := func(ri int) {
				key := testMakeSortedKey(ri)
				v, vcloser, err := db.Get(key)
				if ri%100 == 0 {
					if err == nil {
						t.Fatalf("read del key found key:%s ri:%d", string(key), ri)
					}
					if vcloser != nil {
						t.Fatalf("vcloser is not nil key:%s", string(key))
					}
				} else {
					if err == ErrNotFound {
						t.Fatalf("read exist key not found key:%s ri:%d wi:%d", string(key), ri, writeIndex.Load())
					}
					require.Equal(t, nil, err)
					if ri%2 == 0 {
						require.Equal(t, smallValue, v)
					} else {
						require.Equal(t, largeValue, v)
					}
					vcloser()
				}
			}

			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				defer wg.Done()

				for {
					select {
					case <-closeCh:
						return
					default:
						if write() {
							return
						}
					}
				}
			}()

			go func() {
				defer wg.Done()
				rwg := sync.WaitGroup{}

				for i := 0; i < 3; i++ {
					rwg.Add(1)
					go func(index int) {
						defer rwg.Done()
						rn := 0
						for {
							select {
							case <-closeCh:
								return
							default:
								wi := int(writeIndex.Load())
								if wi < 10 {
									time.Sleep(2 * time.Second)
									continue
								}
								ri := rand.Intn(wi - 2)
								read(ri)
								rn++
								if rn%500000 == 0 {
									fmt.Println("read ok", index, rn)
								}
							}
						}
					}(i)
				}
				rwg.Wait()
			}()

			time.Sleep(60 * time.Second)
			close(closeCh)
			wg.Wait()

			jend := int(writeIndex.Load())
			for j := 0; j < jend; j++ {
				read(j)
			}
		}

		wd()

		require.NoError(t, db.Close())
	})
}
