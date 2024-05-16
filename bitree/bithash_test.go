// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/bithash"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/hash"
)

func TestBithash_Compact(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	testBithashSize = 1 << 20
	btree, _ := testOpenBitree()

	num := 2000
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 2048)
	seqNum += uint64(num)

	writeFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for i := 0; i < num; i++ {
			pn, sentinel, closer := btree.FindKeyPageNum(kvList[i].Key.UserKey)
			closer()
			require.NotEqual(t, nilPageNum, pn)
			require.NoError(t, bw.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
		}
		require.NoError(t, bw.Finish())
		time.Sleep(1 * time.Second)
	}

	deleteFunc := func(n int) {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for i := 0; i < num; i++ {
			if n > 0 && i%n == 0 {
				continue
			}
			pn, sentinel, closer := btree.FindKeyPageNum(kvList[i].Key.UserKey)
			closer()
			require.NotEqual(t, nilPageNum, pn)
			kvList[i].Key.SetKind(base.InternalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = []byte(nil)
			require.NoError(t, bw.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
			seqNum++
		}
		require.NoError(t, bw.Finish())
		time.Sleep(1 * time.Second)
	}

	readFun := func() int {
		btree, _ = testOpenBitree()
		findNum := 0
		for i := 0; i < num; i++ {
			key := kvList[i].Key.UserKey
			value, vexist, vpool := btree.Get(key, hash.Crc32(key))
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
	require.Equal(t, int(1000), readNum)
	require.Equal(t, bithash.FileNum(4), btree.bhash.GetFileNumMap(bithash.FileNum(4)))
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenBitree()
	deleteFunc(3)
	btree.CompactBithash(0.05)
	require.NoError(t, testBitreeClose(btree))

	readNum = readFun()
	require.Equal(t, int(334), readNum)
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenBitree()
	deleteFunc(0)
	btree.CompactBithash(0.05)
	require.NoError(t, testBitreeClose(btree))

	readNum = readFun()
	require.Equal(t, int(0), readNum)
	require.NoError(t, testBitreeClose(btree))
}

func TestBithash_Compact2(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	testBithashSize = 10 << 20
	btree, _ := testOpenBitree()

	writeNum := 0
	deleteNum := 0
	curNum := 0
	num := 30000
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 2048)
	seqNum += uint64(num)

	writeFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for writeNum < num {
			item := kvList[writeNum]
			pn, sentinel, closer := btree.FindKeyPageNum(item.Key.UserKey)
			closer()
			require.NotEqual(t, nilPageNum, pn)
			require.NoError(t, bw.set(*item.Key, item.Value, pn, sentinel))
			writeNum++
			if writeNum%10000 == 0 {
				break
			}
		}
		require.NoError(t, bw.Finish())
		time.Sleep(1 * time.Second)
	}

	deleteFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for curNum < writeNum {
			item := kvList[curNum]
			pn, sentinel, closer := btree.FindKeyPageNum(item.Key.UserKey)
			closer()
			require.NotEqual(t, nilPageNum, pn)
			item.Key.SetKind(base.InternalKeyKindDelete)
			item.Key.SetSeqNum(seqNum)
			item.Value = []byte(nil)
			require.NoError(t, bw.set(*item.Key, item.Value, pn, sentinel))
			seqNum++
			deleteNum++
			curNum++
			if curNum > writeNum-100 {
				break
			}
		}
		curNum = writeNum
		require.NoError(t, bw.Finish())
		time.Sleep(1 * time.Second)
	}

	readFun := func() int {
		btree, _ = testOpenBitree()
		findNum := 0
		for i := 0; i < num; i++ {
			key := kvList[i].Key.UserKey
			value, vexist, vpool := btree.Get(key, hash.Crc32(key))
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

func TestBithash_Compact3(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	testBithashSize = 1 << 20
	btree, _ := testOpenBitree()

	num := 10000
	seqNum := uint64(0)
	kvList := testMakeSortedKV(num, seqNum, 2048)
	seqNum += uint64(num)
	writeNum := 0
	deleteNum := 0

	writeFunc := func() {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for writeNum < num {
			item := kvList[writeNum]
			pn, sentinel, closer := btree.FindKeyPageNum(item.Key.UserKey)
			closer()
			require.NotEqual(t, nilPageNum, pn)
			require.NoError(t, bw.set(*item.Key, item.Value, pn, sentinel))
			writeNum++
		}
		require.NoError(t, bw.Finish())
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
			pn, sentinel, closer := btree.FindKeyPageNum(item.Key.UserKey)
			closer()
			require.NotEqual(t, nilPageNum, pn)
			item.Key.SetKind(base.InternalKeyKindDelete)
			item.Key.SetSeqNum(seqNum)
			item.Value = []byte(nil)
			require.NoError(t, bw.set(*item.Key, item.Value, pn, sentinel))
			deleteNum++
			seqNum++
		}
		require.NoError(t, bw.Finish())
		time.Sleep(1 * time.Second)
	}

	writeFunc()
	deleteFunc()
	btree.CompactBithash(0.05)
	fmt.Println("db.bhash.Stats()", btree.bhash.Stats())
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenBitree()
	readNum := 0
	for i := 0; i < writeNum; i++ {
		item := kvList[i]
		key := item.Key.UserKey
		value, vexist, vpool := btree.Get(key, hash.Crc32(key))
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
