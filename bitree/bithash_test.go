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

package bitree

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/bithash"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestBithashCompact(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	testBithashSize = 1 << 20
	btree, _ := testOpenKKVBitree()

	num := 2000
	seqNum := uint64(0)
	kvList := sortedkv.MakeSlotSortedKVList(0, num, seqNum, 3000)
	seqNum += uint64(num)

	writeFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for i := 0; i < num; i++ {
			require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(1 * time.Second)
	}

	deleteFunc := func(n int) {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for i := 0; i < num; i++ {
			if n > 0 && i%n == 0 {
				continue
			}
			kvList[i].Key.SetKind(internalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = []byte(nil)
			require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
			seqNum++
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(1 * time.Second)
	}

	readFun := func() int {
		btree, _ = testOpenKKVBitree()
		findNum := 0
		for i := 0; i < num; i++ {
			key := kvList[i].Key.UserKey
			value, vexist, vpool := btree.Get(key, hash.Fnv32(key))
			if !vexist {
				continue
			}
			require.Equal(t, kvList[i].Value, value)
			vpool()
			findNum++
		}
		return findNum
	}

	writeFunc()
	deleteFunc(2)

	btree.CompactBithash(0.05)
	require.NoError(t, testBitreeClose(btree))

	readNum := readFun()
	require.Equal(t, 1000, readNum)
	require.Equal(t, bithash.FileNum(7), btree.bhash.GetFileNumMap(bithash.FileNum(4)))
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree()
	deleteFunc(3)
	btree.CompactBithash(0.05)
	require.NoError(t, testBitreeClose(btree))

	readNum = readFun()
	require.Equal(t, int(334), readNum)
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree()
	deleteFunc(0)
	btree.CompactBithash(0.05)
	require.NoError(t, testBitreeClose(btree))

	readNum = readFun()
	require.Equal(t, int(0), readNum)
	require.NoError(t, testBitreeClose(btree))
}

func TestBithashCompact2(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	testBithashSize = 10 << 20
	btree, _ := testOpenKKVBitree()

	writeNum := 0
	deleteNum := 0
	curNum := 0
	kvList := sortedkv.MakeSortedHashKVList(1000, 10, consts.KvSeparateSize*2, uint64(0))
	num := len(kvList)
	seqNum := uint64(num)

	writeFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for writeNum < num {
			item := kvList[writeNum]
			require.NoError(t, bw.Set(*item.Key, item.Value))
			writeNum++
			if writeNum%10000 == 0 {
				break
			}
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(1 * time.Second)
	}

	deleteFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for curNum < writeNum {
			item := kvList[curNum]
			item.Key.SetKind(internalKeyKindDelete)
			item.Key.SetSeqNum(seqNum)
			item.Value = []byte("")
			require.NoError(t, bw.Set(*item.Key, item.Value))
			seqNum++
			deleteNum++
			curNum++
			if curNum > writeNum-100 {
				break
			}
		}
		curNum = writeNum
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(1 * time.Second)
	}

	readFun := func() int {
		btree, _ = testOpenKKVBitree()
		findNum := 0
		for i := 0; i < num; i++ {
			key := kvList[i].Key.UserKey
			value, vexist, vpool := btree.Get(key, hash.Fnv32(key))
			if !vexist {
				continue
			}
			require.Equal(t, kvList[i].Value, value)
			vpool()
			findNum++
		}
		return findNum
	}

	for writeNum < num {
		writeFunc()
		deleteFunc()
		btree.CompactBithash(0.05)
	}

	require.NoError(t, testBitreeClose(btree))
	readNum := readFun()
	require.Equal(t, num-deleteNum, readNum)
	require.NoError(t, testBitreeClose(btree))
}

func TestBithashCompact3(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	testBithashSize = 1 << 20
	btree, _ := testOpenKKVBitree()

	num := 10000
	seqNum := uint64(0)
	kvList := sortedkv.MakeSlotSortedKVList(0, num, seqNum, consts.KvSeparateSize+1)
	seqNum += uint64(num)
	writeNum := 0
	deleteNum := 0

	writeFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for writeNum < num {
			item := kvList[writeNum]
			require.NoError(t, bw.Set(*item.Key, item.Value))
			writeNum++
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(1 * time.Second)
	}

	deleteFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for i := 0; i < writeNum; i++ {
			item := kvList[i]
			if i > writeNum-100 {
				break
			}
			item.Key.SetKind(internalKeyKindDelete)
			item.Key.SetSeqNum(seqNum)
			item.Value = []byte(nil)
			require.NoError(t, bw.Set(*item.Key, item.Value))
			deleteNum++
			seqNum++
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(1 * time.Second)
	}

	writeFunc()
	deleteFunc()
	btree.CompactBithash(0.05)
	fmt.Println("db.bhash.Stats()", btree.bhash.Stats())
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree()
	readNum := 0
	for i := 0; i < writeNum; i++ {
		item := kvList[i]
		key := item.Key.UserKey
		value, vexist, vpool := btree.Get(key, hash.Fnv32(key))
		if i > writeNum-100 {
			if !vexist {
				t.Fatalf("key not find key=%s", item.Key.String())
			}
			require.Equal(t, item.Value, value)
			vpool()
			readNum++
		} else if vexist {
			t.Fatalf("delete key find key=%s", item.Key.String())
		}
	}
	require.Equal(t, num-deleteNum, readNum)
	require.NoError(t, testBitreeClose(btree))
}

func TestBithashCompact4(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	testBithashSize = 1 << 20
	btree, _ := testOpenKKVBitree()
	nowTime := uint64(time.Now().UnixMilli())
	num := 2000
	seqNum := uint64(0)
	var kvList sortedkv.SortedKVList
	for i := 0; i < num; i++ {
		key := sortedkv.MakeSlotKey([]byte("testkey_"+strconv.Itoa(i)), testBitreeIndex)
		ikey := base.MakeInternalKey(key, seqNum, internalKeyKindSet)
		kvList = append(kvList, &sortedkv.SortedKVItem{
			Key: &ikey,
		})
		seqNum++
	}
	sort.Sort(kvList)
	for i := 0; i < num; i++ {
		rvalue := utils.FuncRandBytes(consts.KvSeparateSize + 1)
		v := make([]byte, len(rvalue)+9)
		v[0] = uint8(1)
		if i%4 == 0 {
			binary.BigEndian.PutUint64(v[1:9], nowTime-2000)
		} else {
			binary.BigEndian.PutUint64(v[1:9], 0)
		}
		copy(v[9:], rvalue)
		kvList[i].Value = v
	}

	bw, err := btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 0; i < num; i++ {
		require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, bw.Finish(true, false))
	time.Sleep(1 * time.Second)

	bw, err = btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 0; i < num; i++ {
		if i%2 == 0 {
			kvList[i].Key.SetKind(internalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = []byte(nil)
			require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
			seqNum++
		}
	}
	require.NoError(t, bw.Finish(true, false))
	time.Sleep(1 * time.Second)

	btree.CompactBithash(0.05)
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree()
	findNum := 0
	for i := 0; i < num; i++ {
		key := kvList[i].Key.UserKey
		value, vexist, vpool := btree.Get(key, hash.Fnv32(key))
		if i%2 == 0 {
			require.Equal(t, false, vexist)
		} else {
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, value)
			vpool()
			findNum++
		}
	}
	require.Equal(t, 1000, findNum)

	require.NoError(t, testBitreeClose(btree))
}

func TestBithashCompact5(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	testBithashSize = 1 << 20
	deletePercent := 0.1
	btree, _ := testOpenKKVBitree()
	num := 10000
	seqNum := uint64(0)
	var kvList sortedkv.SortedKVList
	for i := 0; i < num; i++ {
		key := sortedkv.MakeSlotKey([]byte("testkey_"+strconv.Itoa(i)), testBitreeIndex)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, &sortedkv.SortedKVItem{Key: &ikey})
		seqNum++
	}
	sort.Sort(kvList)
	for i := 0; i < num; i++ {
		rvalue := utils.FuncRandBytes(consts.KvSeparateSize)
		v := make([]byte, len(rvalue)+9)
		v[0] = uint8(1)
		binary.BigEndian.PutUint64(v[1:9], 0)
		copy(v[9:], rvalue)
		kvList[i].Value = v
	}

	bw, err := btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 0; i < num/2; i++ {
		require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, bw.Finish(true, false))
	require.NoError(t, testBitreeClose(btree))

	testBithashSize = 1 << 21
	btree, _ = testOpenKKVBitree()
	bw, err = btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := num / 2; i < num; i++ {
		require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, bw.Finish(true, false))
	time.Sleep(1 * time.Second)

	bw, err = btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 0; i < num; i++ {
		if i%2 == 0 {
			kvList[i].Key.SetKind(base.InternalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = []byte(nil)
			require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
			seqNum++
		}
	}
	require.NoError(t, bw.Finish(true, false))
	time.Sleep(1 * time.Second)

	delFiles := btree.bhash.CheckFilesDelPercent(deletePercent)
	require.Equal(t, 2, len(delFiles))

	btree.bithashCompactLowerSize = 700 << 10
	btree.bithashCompactUpperSize = 1400 << 10
	btree.bithashCompactMiniSize = 2 << 20
	btree.CompactBithash(deletePercent)
	stats := btree.BithashStats()
	require.Equal(t, uint32(2), stats.FileTotal.Load())
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree()
	findNum := 0
	for i := 0; i < num; i++ {
		key := kvList[i].Key.UserKey
		value, vexist, vpool := btree.Get(key, hash.Fnv32(key))
		if i%2 == 0 {
			require.Equal(t, false, vexist)
		} else {
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, value)
			vpool()
			findNum++
		}
	}
	require.Equal(t, num/2, findNum)
	require.NoError(t, testBitreeClose(btree))
}

func TestBithashCompactBigFile(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	btree, _ := testOpenKKVBitree()
	num := 150000
	seqNum := uint64(0)
	kvList := sortedkv.MakeSortedHashKVList(num, 1, 10<<10, seqNum)
	seqNum += uint64(num)

	deleteFunc := func(n int) {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for i := 0; i < num; i++ {
			if n > 0 && i%n == 0 {
				continue
			}
			kvList[i].Key.SetKind(base.InternalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = []byte(nil)
			require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
			seqNum++
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(1 * time.Second)
	}

	readFun := func(bitree *Bitree) int {
		findNum := 0
		for i := 0; i < num; i++ {
			key := kvList[i].Key.UserKey
			value, vexist, vpool := bitree.Get(key, hash.Fnv32(key))
			if !vexist {
				continue
			}
			require.Equal(t, kvList[i].Value, value)
			vpool()
			findNum++
		}
		return findNum
	}

	bw, err := btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 0; i < 50000; i++ {
		require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, bw.Finish(true, false))
	time.Sleep(1 * time.Second)
	require.NoError(t, testBitreeClose(btree))

	testBithashSize = 2 << 30
	btree, _ = testOpenKKVBitree()
	bw, err = btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 50000; i < num; i++ {
		require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, bw.Finish(true, false))
	time.Sleep(3 * time.Second)
	deleteFunc(2)
	btree.CompactBithash(0.3)
	readNum := readFun(btree)
	fmt.Println("readNum1", readNum)
	//require.Equal(t, 100000, readNum)
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree()
	deleteFunc(3)
	btree.CompactBithash(0.05)
	readNum = readFun(btree)
	fmt.Println("readNum2", readNum)
	//require.Equal(t, 33334, readNum)
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree()
	deleteFunc(0)
	btree.CompactBithash(0.05)
	readNum = readFun(btree)
	require.Equal(t, 0, readNum)
	require.NoError(t, testBitreeClose(btree))
}
