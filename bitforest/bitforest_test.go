// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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

package bitforest

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/manifest"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

const testDir = "./test-data"
const testSlotId int = 1

var testOptsParams = [][]bool{
	{true, false, false},
	{true, false, true},
	{true, true, false},
	{true, true, true},
}

func testInitDir() {
	_, err := os.Stat(testDir)
	if nil != err && !os.IsExist(err) {
		err = os.MkdirAll(testDir, 0775)
	}
}

func testNewMetadata() *manifest.Metadata {
	path := base.MakeFilepath(vfs.Default, testDir, base.FileTypeMeta, 0)
	meta, err := manifest.NewMetadata(path, vfs.Default)
	if err != nil {
		panic(err)
	}
	return meta
}

func testOpenBf(useBitable bool) *Bitforest {
	testInitDir()
	optspool := base.InitTestDefaultsOptionsPool()
	optspool.BaseOptions.UseBitable = useBitable
	optspool.BaseOptions.BitpageFlushSize = 10 << 20
	optspool.BaseOptions.BitpageSplitSize = 15 << 20
	optspool.BithashOptions.TableMaxSize = 10 << 20
	bf, err := NewBitforest(testDir, testNewMetadata(), optspool)
	if err != nil {
		panic(err)
	}
	return bf
}

func testOpenBf0(useBitable bool) *Bitforest {
	testInitDir()
	optsPool := base.InitTestDefaultsOptionsPool()
	optsPool.BaseOptions.UseBitable = useBitable
	optsPool.BaseOptions.KvSeparateSize = 2000
	optsPool.BaseOptions.BitpageFlushSize = 1 << 20
	optsPool.BaseOptions.BitpageSplitSize = 2 << 20
	bf, err := NewBitforest(testDir, testNewMetadata(), optsPool)
	if err != nil {
		panic(err)
	}

	return bf
}

func testOpenBf1(params []bool) *Bitforest {
	testInitDir()
	optsPool := base.InitTestDefaultsOptionsPool()
	optsPool.BaseOptions.UseBitable = true
	optsPool.BaseOptions.KvSeparateSize = 2000
	optsPool.BaseOptions.BitpageFlushSize = 1 << 20
	optsPool.BaseOptions.BitpageSplitSize = 2 << 20
	optsPool.BaseOptions.UseMapIndex = params[0]
	optsPool.BaseOptions.UsePrefixCompress = params[1]
	optsPool.BaseOptions.UseBlockCompress = params[2]
	bf, err := NewBitforest(testDir, testNewMetadata(), optsPool)
	if err != nil {
		panic(err)
	}

	return bf
}

func testOpenBf2(params []bool) *Bitforest {
	testInitDir()
	optsPool := base.InitTestDefaultsOptionsPool()
	optsPool.BaseOptions.UseBitable = true
	optsPool.BaseOptions.BitpageFlushSize = 10 << 20
	optsPool.BaseOptions.BitpageSplitSize = 15 << 20
	optsPool.BithashOptions.TableMaxSize = 10 << 20
	optsPool.BaseOptions.UseMapIndex = params[0]
	optsPool.BaseOptions.UsePrefixCompress = params[1]
	optsPool.BaseOptions.UseBlockCompress = params[2]
	bf, err := NewBitforest(testDir, testNewMetadata(), optsPool)
	if err != nil {
		panic(err)
	}

	return bf
}

func testCloseBf(bf *Bitforest) error {
	err := bf.Close()
	bf.opts.DeleteFilePacer.Close()
	return err
}

func testRemoveDir() error {
	return os.RemoveAll(testDir)
}

func testMakeKey(i int) []byte {
	return sortedkv.MakeSortedKey(i)
}

func testMakeSortedKV(num int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKVList(0, num, seqNum, vsize)
}

func testMakeSortedKV2(num int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKV2List(0, num, seqNum, vsize)
}

func testMakeSortedKVRange(start, end int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKVList(start, end, seqNum, vsize)
}

func testMakeSortedKV2Range(start, end int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKV2List(start, end, seqNum, vsize)
}

func testMakeSortedSameKV(num int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedSameKVList(0, num, seqNum, vsize, testSlotId)
}

func TestBf_Open(t *testing.T) {
	testRemoveDir()
	bf := testOpenBf(true)
	require.Equal(t, uint8(0), bf.meta.GetFieldFlushedBitable())
	require.Equal(t, false, bf.IsFlushedBitable())
	bf.SetFlushedBitable()
	require.Equal(t, uint8(1), bf.meta.GetFieldFlushedBitable())
	require.Equal(t, true, bf.IsFlushedBitable())
	require.NoError(t, testCloseBf(bf))
	bf = testOpenBf(true)
	require.Equal(t, uint8(1), bf.meta.GetFieldFlushedBitable())
	require.Equal(t, true, bf.IsFlushedBitable())
	require.NoError(t, testCloseBf(bf))
	require.NoError(t, testRemoveDir())
}

func TestBf_WriterFlush(t *testing.T) {
	_ = testRemoveDir()
	bf := testOpenBf(false)

	w := bf.GetWriter()
	require.NoError(t, w.Start(1))
	largeValue := utils.FuncRandBytes(520)
	smallValue := utils.FuncRandBytes(500)
	keyCount := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(keyCount, seqNum, 1)
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
		v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
		require.Equal(t, true, vexist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}

	require.NoError(t, testCloseBf(bf))
	require.NoError(t, testRemoveDir())
}

func TestBf_Bitable_Compact_Delete(t *testing.T) {
	_ = testRemoveDir()
	bf := testOpenBf0(true)

	w := bf.GetWriter()
	require.NoError(t, w.Start(1))
	largeValue := utils.FuncRandBytes(1900)
	smallValue := utils.FuncRandBytes(2100)
	seqNum := uint64(0)
	keyCount := 10000
	kvList := testMakeSortedKV(keyCount, seqNum, 1)
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
		v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
		require.Equal(t, true, vexist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}
	bf.CompactBitree(1)
	for i := 0; i < keyCount; i++ {
		v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
		require.Equal(t, true, vexist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}

	w = bf.GetWriter()
	largeValue = utils.FuncRandBytes(1900)
	smallValue = utils.FuncRandBytes(2100)
	require.NoError(t, w.Start(1))
	for i := 0; i < keyCount; i++ {
		if i%9 == 0 {
			kvList[i].Key.SetKind(base.InternalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = []byte(nil)
		} else {
			if i%2 == 0 {
				kvList[i].Value = smallValue
			} else {
				kvList[i].Value = largeValue
			}
			kvList[i].Key.SetKind(base.InternalKeyKindSet)
			kvList[i].Key.SetSeqNum(seqNum)
		}
		seqNum++
		require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, w.Finish())

	for i := 0; i < keyCount; i++ {
		k := kvList[i].Key.UserKey
		v, vexist, vcloser := bf.Get(k)
		if i%9 == 0 {
			require.Equal(t, false, vexist)
			require.Equal(t, 0, len(v))
		} else {
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}
	}

	require.NoError(t, testCloseBf(bf))
	require.NoError(t, testRemoveDir())
}

func TestBf_NewItersNum(t *testing.T) {
	_ = testRemoveDir()
	bf := testOpenBf(true)
	siters := bf.NewSingleBitreeIter(&base.IterOptions{SlotId: 2})
	require.Equal(t, 1, len(siters))
	for i := range siters {
		require.NoError(t, siters[i].Close())
	}
	iters := bf.NewAllBitreeIter(nil)
	require.Equal(t, 8, len(iters))
	for i := range iters {
		require.NoError(t, iters[i].Close())
	}

	bf.SetFlushedBitable()
	siters = bf.NewSingleBitreeIter(&base.IterOptions{SlotId: 2})
	require.Equal(t, 2, len(siters))
	for i := range siters {
		require.NoError(t, siters[i].Close())
	}
	iters = bf.NewAllBitreeIter(nil)
	require.Equal(t, 16, len(iters))
	for i := range iters {
		require.NoError(t, iters[i].Close())
	}
	require.NoError(t, testCloseBf(bf))

	bf = testOpenBf(false)
	siters = bf.NewSingleBitreeIter(&base.IterOptions{SlotId: 2})
	require.Equal(t, 1, len(siters))
	for i := range siters {
		require.NoError(t, siters[i].Close())
	}
	iters = bf.NewAllBitreeIter(nil)
	require.Equal(t, 8, len(iters))
	for i := range iters {
		require.NoError(t, iters[i].Close())
	}
	require.NoError(t, testCloseBf(bf))

	require.NoError(t, testRemoveDir())
}

func TestBf_Checkpoint(t *testing.T) {
	_ = testRemoveDir()

	for _, vlen := range []int{200, 2048} {
		loop := 4
		step := 3000
		count := loop * step
		seqNum := uint64(0)
		kvList := testMakeSortedKV(count, seqNum, vlen)
		seqNum += uint64(count)

		writeData := func(index int) {
			bf := testOpenBf0(true)
			w := bf.GetWriter()
			require.NoError(t, w.Start(1))
			start := index * step
			end := start + step
			for i := start; i < end; i++ {
				require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, w.Finish())
			fmt.Println("set success", index)

			time.Sleep(2 * time.Second)

			if index == 1 {
				bf.CompactBitree(index)
			}

			destDir := fmt.Sprintf("test_checkpoint_%d", index)
			os.RemoveAll(destDir)

			require.NoError(t, os.Mkdir(destDir, 0755))
			require.NoError(t, bf.Checkpoint(destDir))
			fmt.Println("checkpoint success", index)

			for i := start; i < end; i++ {
				v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
				require.Equal(t, true, vexist)
				require.Equal(t, kvList[i].Value, v)
				vcloser()
			}
			fmt.Println("read success", index)

			require.NoError(t, testCloseBf(bf))

			bf = testOpenBf0(true)
			for i := start; i < end; i++ {
				v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
				require.Equal(t, true, vexist)
				require.Equal(t, kvList[i].Value, v)
				vcloser()
			}
			fmt.Println("read dst success", index)
			require.NoError(t, testCloseBf(bf))

			os.RemoveAll(destDir)
		}

		for i := 0; i < loop; i++ {
			writeData(i)
		}

		require.NoError(t, testRemoveDir())
	}
}

func testcase(caseFunc func([]bool)) {
	for _, params := range testOptsParams {
		fmt.Printf("testcase params:%v\n", params)
		caseFunc(params)
	}
}

func TestBf_Bitpage_Write(t *testing.T) {
	testcase(func(params []bool) {
		_ = testRemoveDir()
		bf := testOpenBf1(params)

		loop := 6
		step := 3000
		count := loop * step
		seqNum := uint64(0)
		kvList := testMakeSortedSameKV(count, seqNum, 1024)
		seqNum += uint64(count)

		writeData := func(index int) {
			w := bf.GetWriter()
			require.NoError(t, w.Start(1))
			start := index * step
			end := start + step
			for i := start; i < end; i++ {
				require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, w.Finish())
			fmt.Println("set success", index, bf.dbState.GetBitpageFlushCount(), bf.dbState.GetBitpageSplitCount())

			time.Sleep(2 * time.Second)
			for i := start; i < end; i++ {
				v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
				require.Equal(t, true, vexist)
				require.Equal(t, kvList[i].Value, v)
				vcloser()
			}
		}

		for i := 0; i < loop; i++ {
			writeData(i)
		}

		btree := bf.getBitree(testSlotId)
		bdbIter := btree.NewBdbIter()
		i := 0
		for ik, iv := bdbIter.First(); ik != nil; ik, iv = bdbIter.Next() {
			fmt.Println("bdbIter", i, ik.String(), utils.BytesToUint32(iv))
			i++
		}
		require.Equal(t, 13, i)
		require.NoError(t, bdbIter.Close())

		require.Equal(t, uint64(6), bf.dbState.GetBitpageFlushCount())
		require.Equal(t, uint64(6), bf.dbState.GetBitpageSplitCount())

		require.NoError(t, testCloseBf(bf))
		require.NoError(t, testRemoveDir())
	})
}

func TestBf_Bitpage_Write_CheckExpireFunc(t *testing.T) {
	_ = testRemoveDir()

	base.DefaultKvTimestampFunc = base.TestKvTimestampFunc
	base.DefaultKvCheckExpireFunc = base.TestKvCheckExpireFunc

	bf := testOpenBf0(true)

	count := 30000
	seqNum := uint64(0)
	kvList := testMakeSortedKV2(count, seqNum, 1)
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
				ttl = now + 4000
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

	w := bf.GetWriter()
	require.NoError(t, w.Start(1))

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
	time.Sleep(2 * time.Second)

	for i := 0; i < count; i++ {
		k := kvList[i].Key.UserKey
		v, vexist, vcloser := bf.Get(k)
		if i%5 == 0 && i%15 != 0 {
			if vexist {
				t.Fatal("find expire key return not nil", kvList[i].Key.String(), i, v)
			}
		} else {
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}
	}

	require.Equal(t, bithashCount, bf.Stats().BithashKeyTotal)
	require.Equal(t, bithashDelCount1, bf.Stats().BithashDelKeyTotal)

	time.Sleep(5 * time.Second)
	bf.CompactBitree(1)

	require.Equal(t, bithashCount, bf.Stats().BithashKeyTotal)
	require.Equal(t, bithashDelCount, bf.Stats().BithashDelKeyTotal)

	for i := 0; i < count; i++ {
		k := kvList[i].Key.UserKey
		v, vexist, vcloser := bf.Get(k)
		if i%5 == 0 {
			require.Equal(t, false, vexist)
			if v != nil || vcloser != nil {
				t.Fatal("find expire key return not nil", string(k), i, v)
			}
		} else {
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}
	}

	require.NoError(t, testCloseBf(bf))
	require.NoError(t, testRemoveDir())
}

func TestBf_Bitpage_Write_CheckpointLock(t *testing.T) {
	_ = testRemoveDir()
	bf := testOpenBf0(true)
	defer func() {
		require.NoError(t, testCloseBf(bf))
		require.NoError(t, testRemoveDir())
	}()
	seqNum := uint64(0)
	loop := 6
	step := 3000
	count := (loop + 1) * step
	kvList := testMakeSortedSameKV(count, seqNum, 1024)
	seqNum += uint64(count)
	writeData := func(index int) {
		w := bf.GetWriter()
		require.NoError(t, w.Start(1))
		start := index * step
		end := start + step
		for i := start; i < end; i++ {
			require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, w.Finish())
		fmt.Println("set success", index, bf.dbState.GetBitpageFlushCount(), bf.dbState.GetBitpageSplitCount())

		for i := start; i < end; i++ {
			v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}

		time.Sleep(2 * time.Second)

		if index == 2 {
			bf.dbState.SetHighPriority(true)
		} else if index > 2 && index <= 5 {
			require.Equal(t, uint64(3), bf.dbState.GetBitpageFlushCount())
			require.Equal(t, uint64(3), bf.dbState.GetBitpageSplitCount())
			if index == 5 {
				bf.dbState.SetHighPriority(false)
			}
		}
	}

	for i := 0; i < loop; i++ {
		writeData(i)
	}

	time.Sleep(3 * time.Second)
	require.Equal(t, uint64(4), bf.dbState.GetBitpageFlushCount())
	require.Equal(t, uint64(4), bf.dbState.GetBitpageSplitCount())

	writeData(loop)
	time.Sleep(2 * time.Second)
	require.Equal(t, uint64(5), bf.dbState.GetBitpageFlushCount())
	require.Equal(t, uint64(5), bf.dbState.GetBitpageSplitCount())

	for i := 0; i < count; i++ {
		v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
		require.Equal(t, true, vexist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}
}

func TestBf_Bitable_Delete(t *testing.T) {
	_ = testRemoveDir()

	for _, vlen := range []int{200, 2048} {
		base.DefaultKvTimestampFunc = base.TestKvTimestampFunc
		base.DefaultKvCheckExpireFunc = base.TestKvCheckExpireFunc
		bf := testOpenBf(true)

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
		kvList := testMakeSortedKV(count, seqNum, vlen)
		seqNum += uint64(count)

		w := bf.GetWriter()
		require.NoError(t, w.Start(1))
		for i := 0; i < count; i++ {
			kvList[i].Value = makeValue(i, kvList[i].Value)
			require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, w.Finish())

		readKV := func() {
			for i := 0; i < count; i++ {
				v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
				require.Equal(t, true, vexist)
				require.Equal(t, kvList[i].Value, v)
				vcloser()
			}
		}

		readKV()
		bf.CompactBitree(1)
		readKV()

		w = bf.GetWriter()
		require.NoError(t, w.Start(1))
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
			require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, w.Finish())

		readDeleteKV := func(jobId int) {
			for i := 0; i < count; i++ {
				v, vexist, vcloser := bf.Get(kvList[i].Key.UserKey)
				if jobId == 2 {
					if i%2 == 0 || i%5 == 0 {
						require.Equal(t, false, vexist)
					} else {
						require.Equal(t, true, vexist)
						require.Equal(t, kvList[i].Value, v)
						vcloser()
					}
				} else {
					if i%2 == 0 {
						require.Equal(t, false, vexist)
					} else {
						require.Equal(t, true, vexist)
						require.Equal(t, kvList[i].Value, v)
						vcloser()
					}
				}
			}
		}

		readDeleteKV(1)
		time.Sleep(3 * time.Second)
		bf.CompactBitree(2)
		readDeleteKV(2)

		require.NoError(t, testCloseBf(bf))
		require.NoError(t, testRemoveDir())
	}
}

func TestBf_Bitable_CheckExpire(t *testing.T) {
	_ = testRemoveDir()

	for _, vlen := range []int{200, 2048} {
		isBithash := vlen > consts.KvSeparateSize
		bithashDelCount := 0
		base.DefaultKvTimestampFunc = base.TestKvTimestampFunc
		base.DefaultKvCheckExpireFunc = base.TestKvCheckExpireFunc
		bf := testOpenBf(true)

		seqNum := uint64(0)
		count := 1000
		kvList := testMakeSortedKV2Range(0, count, seqNum, vlen)
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
			w := bf.GetWriter()
			require.NoError(t, w.Start(1))
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
				k := kvList[i].Key.UserKey
				v, vexist, vcloser := bf.Get(k)
				if deleted && i%5 == 0 {
					require.Equal(t, false, vexist)
				} else {
					require.Equal(t, true, vexist)
					require.Equal(t, kvList[i].Value, v)
					vcloser()
				}
			}
		}

		writeData(false)
		readKV(false)
		bf.CompactBitree(1)
		readKV(false)

		time.Sleep(5 * time.Second)

		w := bf.GetWriter()
		require.NoError(t, w.Start(1))
		seqNum += uint64(count)
		for i := 0; i < count; i++ {
			if i%5 == 0 {
				continue
			}
			kvList[i].Value = makeValue(i, kvList[i].Value)
			require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, w.Finish())
		bf.CompactBitree(2)
		if isBithash {
			require.Equal(t, bithashDelCount, bf.Stats().BithashDelKeyTotal)
		}
		readKV(true)

		require.NoError(t, testCloseBf(bf))
		require.NoError(t, testRemoveDir())
	}
}

func TestBf_Bitpage_SplitEmptyPage(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	bf := testOpenBf(false)

	seqNum := uint64(1)
	largeValue := utils.FuncRandBytes(600)
	smallValue := utils.FuncRandBytes(500)
	count := uint64(1000000)
	var writeIndex atomic.Uint64
	step := uint64(100000)

	write := func() bool {
		bf.dbState.LockDbWrite()
		defer bf.dbState.UnlockDbWrite()
		bf.dbState.LockMemFlushing()
		defer bf.dbState.UnLockMemFlushing()

		w := bf.GetWriter()
		require.NoError(t, w.Start(1))
		i := writeIndex.Load()
		if i >= count {
			return true
		}
		start := i
		end := start + step
		kvList := testMakeSortedKVRange(int(start), int(end), seqNum, 1)
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
		key := testMakeKey(ri)
		v, vexist, vcloser := bf.Get(key)
		if ri%100 == 0 {
			if vexist {
				t.Fatalf("read del key found key:%s ri:%d", string(key), ri)
			}
			if vcloser != nil {
				t.Fatalf("vcloser is not nil key:%s", string(key))
			}
		} else {
			if !vexist {
				t.Fatalf("read exist key not found key:%s ri:%d", string(key), ri)
			}
			require.Equal(t, true, vexist)
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

	require.NoError(t, testCloseBf(bf))
}

func TestBf_Write_ConcurrentRead(t *testing.T) {
	testcase(func(params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)

		bf := testOpenBf2(params)

		wd := func() {
			seqNum := uint64(1)
			largeValue := utils.FuncRandBytes(consts.KvSeparateSize * 2)
			smallValue := utils.FuncRandBytes(consts.KvSeparateSize / 2)
			count := uint64(5000000)
			var writeIndex atomic.Uint64
			closeCh := make(chan struct{})
			step := uint64(100000)

			write := func() bool {
				bf.dbState.LockDbWrite()
				defer bf.dbState.UnlockDbWrite()
				bf.dbState.LockMemFlushing()
				defer bf.dbState.UnLockMemFlushing()

				w := bf.GetWriter()
				require.NoError(t, w.Start(1))
				i := writeIndex.Load()
				if i >= count {
					return true
				}
				start := i
				end := start + step
				kvList := testMakeSortedKVRange(int(start), int(end), seqNum, 1)
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
				key := testMakeKey(ri)
				v, vexist, vcloser := bf.Get(key)
				if ri%100 == 0 {
					if vexist {
						t.Fatalf("read del key found key:%s ri:%d", string(key), ri)
					}
					if vcloser != nil {
						t.Fatalf("vcloser is not nil key:%s", string(key))
					}
				} else {
					if !vexist {
						t.Fatalf("read exist key not found key:%s ri:%d wi:%d", string(key), ri, writeIndex.Load())
					}
					require.Equal(t, true, vexist)
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

		require.NoError(t, testCloseBf(bf))
	})
}
