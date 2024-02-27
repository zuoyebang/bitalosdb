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
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/sortedkv"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/bitable"
	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

const testDir = "test"

func testMakeKey(i int) []byte {
	return []byte(fmt.Sprintf("bitforest_key_%d", i))
}

func testOpenBitree() (*Bitree, *base.BitreeOptions) {
	return testOpenBitreeInternal(128<<20, false)
}

func testOpenBitree1(bithashSize int) (*Bitree, *base.BitreeOptions) {
	return testOpenBitreeInternal(bithashSize, false)
}

func testMakeSortedKV(num int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKVList(0, num, seqNum, vsize)
}

func testMakeSortedKey(n int) []byte {
	return sortedkv.MakeSortedKey(n)
}

func testNewBitreePages(t *Bitree) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bkt := tx.Bucket(consts.BdbBucketName)
		if bkt == nil {
			return bdb.ErrBucketNotFound
		}
		for j := 1; j < 10; j++ {
			key := testMakeKey(j)
			pn, err := t.bpage.NewPage()
			if err != nil {
				return err
			}
			if err = bkt.Put(key, pn.ToByte()); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func testOpenBitreeInternal(bithashSize int, useBitable bool) (*Bitree, *base.BitreeOptions) {
	optspool := base.InitTestDefaultsOptionsPool()
	optspool.BaseOptions.UseBitable = useBitable
	optspool.BithashOptions.TableMaxSize = bithashSize
	bitreeOpts := optspool.Clone(base.BitreeOptionsType).(*base.BitreeOptions)
	btree, err := NewBitree(testDir, bitreeOpts)
	if err != nil {
		panic(err)
	}
	return btree, bitreeOpts
}

func TestBitree_Bdb_Seek(t *testing.T) {
	os.RemoveAll(testDir)

	seekFunc := func(index int) {
		fmt.Println("seekFunc", index)
		btree, _ := testOpenBitree()

		pn, sentinel, closer := btree.FindKeyPageNum(consts.BdbMaxKey)
		require.Equal(t, bitpage.PageNum(1), pn)
		require.Equal(t, consts.BdbMaxKey, sentinel)
		closer()

		if index == 0 {
			for i := 0; i < 10; i++ {
				key := testMakeKey(i)
				pn, sentinel, closer = btree.FindKeyPageNum(key)
				require.Equal(t, bitpage.PageNum(1), pn)
				require.Equal(t, consts.BdbMaxKey, sentinel)
				closer()
			}
			err := btree.bdb.Update(func(tx *bdb.Tx) error {
				bkt := tx.Bucket(consts.BdbBucketName)
				if bkt == nil {
					return bdb.ErrBucketNotFound
				}
				for i := 2; i < 10; i++ {
					if i%3 != 0 {
						continue
					}

					key := testMakeKey(i)
					require.NoError(t, bkt.Put(key, bitpage.PageNum(i).ToByte()))
					fmt.Println("set key", string(key), i)
				}
				return nil
			})
			btree.txPool.Update()
			require.NoError(t, err)
		}

		checkValue := func(i, exp int) {
			key := testMakeKey(i)
			pn, sentinel, closer = btree.FindKeyPageNum(key)
			require.Equal(t, bitpage.PageNum(exp), pn)
			closer()
			if exp == 1 {
				require.Equal(t, consts.BdbMaxKey, sentinel)
			} else {
				require.Equal(t, testMakeKey(exp), sentinel)
			}
		}

		checkValue(0, 3)
		checkValue(3, 3)
		checkValue(12, 3)
		checkValue(4, 6)
		checkValue(5, 6)
		checkValue(51, 6)
		checkValue(6, 6)
		checkValue(60, 9)
		checkValue(899, 9)
		checkValue(9, 9)
		checkValue(90, 1)
		checkValue(91, 1)

		require.NoError(t, testBitreeClose(btree))
	}

	for i := 0; i < 5; i++ {
		seekFunc(i)
	}

	require.NoError(t, os.RemoveAll(testDir))
}

func TestBitree_Bdb_Seek_LargeKey(t *testing.T) {
	os.RemoveAll(testDir)
	btree, _ := testOpenBitree()
	defer os.RemoveAll(testDir)

	keyPrefix := utils.FuncRandBytes(1100)
	makeLargeKey := func(i int) []byte {
		return []byte(fmt.Sprintf("bdb_%s_%d", keyPrefix, i))
	}
	err := btree.bdb.Update(func(tx *bdb.Tx) error {
		bkt := tx.Bucket(consts.BdbBucketName)
		if bkt == nil {
			return bdb.ErrBucketNotFound
		}
		for i := 2; i < 200; i++ {
			if i%3 != 0 {
				continue
			}
			key := makeLargeKey(i)
			require.NoError(t, bkt.Put(key, bitpage.PageNum(i).ToByte()))
		}
		return nil
	})
	btree.txPool.Update()
	require.NoError(t, err)

	expPns := []int{102, 102, 21, 3, 42, 51, 6, 72, 81, 9, 102, 111, 12, 132, 141, 15, 162, 171, 18, 192, 21, 21, 24, 24,
		24, 27, 27, 27, 3, 3, 30, 33, 33, 33, 36, 36, 36, 39, 39, 39, 42, 42, 42, 45, 45, 45, 48, 48, 48, 51, 51, 51, 54, 54, 54,
		57, 57, 57, 6, 6, 60, 63, 63, 63, 66, 66, 66, 69, 69, 69, 72, 72, 72, 75, 75, 75, 78, 78, 78, 81, 81, 81, 84, 84, 84, 87,
		87, 87, 9, 9, 90, 93, 93, 93, 96, 96, 96, 99, 99, 99, 102, 102, 102, 105, 105, 105, 108, 108, 108, 111, 111, 111, 114,
		114, 114, 117, 117, 117, 12, 12, 120, 123, 123, 123, 126, 126, 126, 129, 129, 129, 132, 132, 132, 135, 135, 135,
		138, 138, 138, 141, 141, 141, 144, 144, 144, 147, 147, 147, 15, 15, 150, 153, 153, 153, 156, 156, 156, 159, 159,
		159, 162, 162, 162, 165, 165, 165, 168, 168, 168, 171, 171, 171, 174, 174, 174, 177, 177, 177, 18, 18, 180, 183,
		183, 183, 186, 186, 186, 189, 189, 189, 192, 192, 192, 195, 195, 195, 198, 198, 198, 21}

	for i := 0; i < 200; i++ {
		key := makeLargeKey(i)
		pn, _, closer := btree.FindKeyPageNum(key)
		require.Equal(t, bitpage.PageNum(expPns[i]), pn)
		closer()
	}

	checkValue := func(i, exp int) {
		key := makeLargeKey(i)
		pn, sentinel, closer := btree.FindKeyPageNum(key)
		require.Equal(t, bitpage.PageNum(exp), pn)
		closer()
		if exp == 1 {
			require.Equal(t, consts.BdbMaxKey, sentinel)
		} else {
			require.Equal(t, makeLargeKey(exp), sentinel)
		}
	}

	checkValue(201, 21)
	checkValue(255, 27)
	checkValue(402, 42)
	checkValue(518, 54)
	checkValue(627, 63)
	checkValue(734, 75)
	checkValue(846, 87)
	checkValue(899, 9)
	checkValue(953, 96)
	checkValue(999, 1)

	require.NoError(t, os.RemoveAll(testDir))
}

func TestBitree_CompactToBitable(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	btree, _ := testOpenBitreeInternal(10<<20, true)
	defer func() {
		require.NoError(t, testBitreeClose(btree))
	}()

	require.NotEqual(t, (*bitable.Bitable)(nil), btree.btable)

	err := btree.bdb.Update(func(tx *bdb.Tx) error {
		bkt := tx.Bucket(consts.BdbBucketName)
		if bkt == nil {
			return bdb.ErrBucketNotFound
		}
		for i := 1; i < 10; i++ {
			key := testMakeKey(i)
			pn, err := btree.bpage.NewPage()
			require.NoError(t, err)
			require.NoError(t, bkt.Put(key, pn.ToByte()))
		}
		return nil
	})
	btree.txPool.Update()
	require.NoError(t, err)

	require.NoError(t, btree.MemFlushStart())
	largeValue := utils.FuncRandBytes(520)
	smallValue := utils.FuncRandBytes(500)
	keyCount := 1000
	seqNum := uint64(0)
	kvList := testMakeSortedKV(keyCount, seqNum, 10)
	seqNum += uint64(keyCount)

	for i := 0; i < keyCount; i++ {
		pn, sentinel, closer := btree.FindKeyPageNum(kvList[i].Key.UserKey)
		closer()
		require.NotEqual(t, nilPageNum, pn)

		if i%2 == 0 {
			kvList[i].Value = smallValue
			require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
		} else {
			kvList[i].Value = largeValue
			require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
		}
	}
	require.NoError(t, btree.MemFlushFinish())
	time.Sleep(2 * time.Second)

	readIter := func(it base.InternalIterator) {
		i := 0
		for k, v := it.First(); k != nil; k, v = it.Next() {
			if base.InternalCompare(bytes.Compare, *kvList[i].Key, *k) != 0 {
				t.Fatal("InternalCompare", i, k.String(), kvList[i].Key.String())
			}
			require.Equal(t, kvList[i].Value, v)
			i++
		}
		require.Equal(t, keyCount, i)
	}

	btreeIter := btree.newBitreeIter(nil)
	readIter(btreeIter)
	require.NoError(t, btreeIter.Close())

	require.Equal(t, 10, btree.bpage.GetPageCount())
	fmt.Println("CompactBitreeToBitable before", btree.BitpageStats().String())
	pn := btree.CompactBitreeToBitable(1)
	require.Equal(t, 11, btree.bpage.GetPageCount())

	bdbIter := btree.NewBdbIter()
	bdbKey, bdbValue := bdbIter.First()
	require.Equal(t, consts.BdbMaxKey, bdbKey.UserKey)
	require.Equal(t, uint32(pn), utils.BytesToUint32(bdbValue))
	bdbKey, _ = bdbIter.Next()
	require.Equal(t, (*base.InternalKey)(nil), bdbKey)
	require.NoError(t, bdbIter.Close())

	isFree := btree.bpage.CheckFreePages(pn)
	require.Equal(t, true, isFree)

	_ = btree.bdb.Update(func(tx *bdb.Tx) error { return nil })
	btree.txPool.Update()
	_ = btree.bdb.Update(func(tx *bdb.Tx) error { return nil })

	time.Sleep(2 * time.Second)
	require.Equal(t, 1, btree.bpage.GetPageCount())
	fmt.Println("CompactBitreeToBitable after", btree.BitpageStats().String())

	btableIter := btree.newBitableIter(nil)
	readIter(btableIter)
	require.NoError(t, btableIter.Close())

	btreeIter = btree.newBitreeIter(nil)
	ik, _ := btreeIter.First()
	require.Equal(t, (*base.InternalKey)(nil), ik)
	require.NoError(t, btreeIter.Close())
}

func TestBitree_Checkpoint_Flush(t *testing.T) {
	ckDir := testDir + "_ck"
	os.RemoveAll(testDir)
	os.RemoveAll(ckDir)

	require.NoError(t, os.MkdirAll(ckDir, 0755))
	btree, _ := testOpenBitree()
	defer func() {
		require.NoError(t, testBitreeClose(btree))
		require.NoError(t, os.RemoveAll(testDir))
		require.NoError(t, os.RemoveAll(ckDir))
	}()

	require.NoError(t, testNewBitreePages(btree))

	keyCount := 200
	seqNum := uint64(0)
	kvList := testMakeSortedKV(keyCount, seqNum, 200)
	seqNum += uint64(keyCount)

	writeData := func(start int) {
		require.NoError(t, btree.MemFlushStart())
		for i := start; i < start+100; i++ {
			pn, sentinel, closer := btree.FindKeyPageNum(kvList[i].Key.UserKey)
			closer()
			require.NotEqual(t, nilPageNum, pn)
			require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
		}
		require.NoError(t, btree.MemFlushFinish())
	}

	writeData(0)
	time.Sleep(1 * time.Second)
	require.Equal(t, uint64(0), btree.dbState.GetBitpageFlushCount())
	require.NoError(t, btree.Checkpoint(ckDir, testDir))

	writeData(100)
	time.Sleep(1 * time.Second)
	require.Equal(t, uint64(1), btree.dbState.GetBitpageFlushCount())

	for i := 0; i < keyCount; i++ {
		k := kvList[i].Key.UserKey
		v, exist, vcloser := btree.Get(k, hash.Crc32(k))
		require.Equal(t, true, exist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}
}

func TestBitree_BitpageFlushState(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	btree, _ := testOpenBitree()

	require.NoError(t, testNewBitreePages(btree))

	keyCount := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(keyCount, seqNum, 200)
	seqNum += uint64(keyCount)

	var pageNum bitpage.PageNum
	require.NoError(t, btree.MemFlushStart())
	for i := 0; i < keyCount; i++ {
		pn, sentinel, closer := btree.FindKeyPageNum(kvList[i].Key.UserKey)
		closer()
		require.NotEqual(t, nilPageNum, pn)
		require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
		pageNum = pn
	}
	require.NoError(t, btree.MemFlushFinish())

	task := &base.BitpageTaskData{
		Event:    base.BitpageEventFlush,
		Pn:       uint32(pageNum),
		Sentinel: nil,
	}
	btree.pushTaskData(task)
	err := btree.bitpageFlush(task)
	require.Equal(t, bitpage.ErrPageFlushState, err)

	require.NoError(t, testBitreeClose(btree))
}

func TestBitree_BitpageFlushDelPercent(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	optspool := base.InitDefaultsOptionsPool()
	optspool.BaseOptions.BitpageFlushSize = 40 << 10
	bitreeOpts := optspool.Clone(base.BitreeOptionsType).(*base.BitreeOptions)
	btree, err := NewBitree(testDir, bitreeOpts)
	require.NoError(t, err)
	require.NoError(t, testNewBitreePages(btree))

	keyCount := 1000
	seqNum := uint64(0)
	kvList := testMakeSortedKV(keyCount, seqNum, 500)
	seqNum += uint64(keyCount)

	var pageNum bitpage.PageNum
	require.NoError(t, btree.MemFlushStart())
	for i := 0; i < keyCount; i++ {
		k := testMakeKey(i)
		pn, sentinel, closer := btree.FindKeyPageNum(k)
		closer()
		require.NotEqual(t, nilPageNum, pn)
		if i%2 == 0 || i > 700 {
			kvList[i].Key.SetKind(base.InternalKeyKindDelete)
		}
		require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
		pageNum = pn
	}

	delPercent := btree.bpage.GetPageDelPercent(pageNum)
	if delPercent < 0.5 {
		t.Fatal("delpercent err", delPercent)
	}
	require.NoError(t, btree.MemFlushFinish())
	time.Sleep(2 * time.Second)
	require.Equal(t, uint64(1), btree.dbState.GetBitpageFlushCount())

	for i := 0; i < keyCount; i++ {
		k := kvList[i].Key.UserKey
		v, exist, vcloser := btree.Get(k, hash.Crc32(k))
		if i%2 == 0 || i > 700 {
			require.Equal(t, false, exist)
		} else {
			require.Equal(t, true, exist)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}
	}

	require.NoError(t, testBitreeClose(btree))
}

func TestBitree_BitableFlushBatch(t *testing.T) {
	os.RemoveAll(testDir)
	btree, _ := testOpenBitreeInternal(10<<20, true)
	defer func() {
		require.NoError(t, testBitreeClose(btree))
		require.NoError(t, os.RemoveAll(testDir))
	}()

	batch := btree.btable.NewFlushBatch(1 << 20)
	require.Equal(t, true, batch.Empty())
	batch.Set([]byte("123"), []byte("123"))
	require.Equal(t, false, batch.Empty())
	batch.Delete([]byte("123"))
	require.Equal(t, false, batch.Empty())
	batch.AllocFree()
	require.NoError(t, batch.Close())
}

func TestBitree_IterRange(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	btree, _ := testOpenBitree()
	require.NoError(t, testNewBitreePages(btree))

	largeValue := utils.FuncRandBytes(consts.KvSeparateSize + 200)
	smallValue := utils.FuncRandBytes(consts.KvSeparateSize - 10)
	keyCount := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(keyCount, seqNum, 1)
	seqNum += uint64(keyCount)

	require.NoError(t, btree.MemFlushStart())
	for i := 0; i < keyCount; i++ {
		k := testMakeKey(i)
		pn, sentinel, closer := btree.FindKeyPageNum(k)
		closer()
		require.NotEqual(t, nilPageNum, pn)
		if i%3 == 0 {
			kvList[i].Value = smallValue
		} else if i%3 == 1 {
			kvList[i].Value = largeValue
		} else {
			kvList[i].Key.SetKind(base.InternalKeyKindDelete)
		}
		kvList[i].Key.SetSeqNum(seqNum)
		seqNum++
		require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
	}
	require.NoError(t, btree.MemFlushFinish())
	time.Sleep(2 * time.Second)

	rangeLoop := func(o *base.IterOptions, start int) int {
		iter := btree.newBitreeIter(o)
		i := start
		count := 0
		for ik, val := iter.First(); ik != nil; ik, val = iter.Next() {
			require.Equal(t, 0, base.InternalCompare(bytes.Compare, *kvList[i].Key, *ik))
			kind := ik.Kind()
			if i%3 == 0 || i%3 == 1 {
				require.Equal(t, base.InternalKeyKindSet, kind)
				require.Equal(t, kvList[i].Value, val)
			} else {
				require.Equal(t, base.InternalKeyKindDelete, kind)
				require.Equal(t, 0, len(val))
			}
			i++
			count++
		}
		require.NoError(t, iter.Close())
		return count
	}

	rangeReverseLoop := func(o *base.IterOptions, start int) int {
		iter := btree.newBitreeIter(o)
		i := start
		count := 0
		for ik, val := iter.Last(); ik != nil; ik, val = iter.Prev() {
			require.Equal(t, 0, base.InternalCompare(bytes.Compare, *kvList[i].Key, *ik))
			kind := ik.Kind()
			if i%3 == 0 || i%3 == 1 {
				require.Equal(t, base.InternalKeyKindSet, kind)
				require.Equal(t, kvList[i].Value, val)
			} else {
				require.Equal(t, base.InternalKeyKindDelete, kind)
				require.Equal(t, 0, len(val))
			}
			i--
			count++
		}
		require.NoError(t, iter.Close())
		return count
	}

	require.Equal(t, keyCount, rangeLoop(nil, 0))
	require.Equal(t, keyCount, rangeReverseLoop(nil, keyCount-1))
	ops := &base.IterOptions{
		LowerBound: kvList[20].Key.UserKey,
		UpperBound: kvList[50].Key.UserKey,
	}
	require.Equal(t, 30, rangeLoop(ops, 20))
	require.Equal(t, 30, rangeReverseLoop(ops, 49))
	ops = &base.IterOptions{
		LowerBound: kvList[30].Key.UserKey,
	}
	require.Equal(t, 70, rangeLoop(ops, 30))
	ops = &base.IterOptions{
		UpperBound: kvList[50].Key.UserKey,
	}
	require.Equal(t, 50, rangeReverseLoop(ops, 49))

	require.NoError(t, testBitreeClose(btree))
}

func TestBitree_IterSeek(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	btree, _ := testOpenBitree()
	require.NoError(t, testNewBitreePages(btree))

	keyCount := 100
	seqNum := uint64(0)
	kvList := testMakeSortedKV(keyCount, seqNum, 520)
	seqNum += uint64(keyCount)

	require.NoError(t, btree.MemFlushStart())
	for i := 0; i < keyCount; i++ {
		if i >= 1 && i < 13 {
			continue
		}
		pn, sentinel, closer := btree.FindKeyPageNum(kvList[i].Key.UserKey)
		closer()
		require.NotEqual(t, nilPageNum, pn)
		require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
	}
	require.NoError(t, btree.MemFlushFinish())
	time.Sleep(2 * time.Second)

	makeByte := func(pos, extra int) []byte {
		return []byte(fmt.Sprintf("%s%d", string(kvList[pos].Key.UserKey), extra))
	}

	iterOp := func(it *BitreeIterator, op string, seek []byte, want int) {
		var ik *base.InternalKey
		var v []byte
		switch op {
		case "SeekGE":
			ik, v = it.SeekGE(seek)
		case "SeekLT":
			ik, v = it.SeekLT(seek)
		case "First":
			ik, v = it.First()
		case "Next":
			ik, v = it.Next()
		case "Last":
			ik, v = it.Last()
		case "Prev":
			ik, v = it.Prev()
		}
		if want == -1 {
			require.Equal(t, (*base.InternalKey)(nil), ik)
		} else {
			require.Equal(t, kvList[want].Key, ik)
			require.Equal(t, kvList[want].Value, v)
		}
	}

	iter := btree.newBitreeIter(nil)
	iterOp(iter, "SeekGE", kvList[14].Key.UserKey, 14)
	iterOp(iter, "SeekGE", makeByte(14, 1), 15)
	iterOp(iter, "SeekGE", kvList[29].Key.UserKey, 29)
	iterOp(iter, "SeekGE", makeByte(29, 0), 30)
	iterOp(iter, "SeekGE", makeByte(49, 1), 50)
	iterOp(iter, "SeekGE", kvList[99].Key.UserKey, 99)
	iterOp(iter, "SeekGE", testMakeSortedKey(990), -1)
	require.NoError(t, iter.Close())

	iter = btree.newBitreeIter(nil)
	iterOp(iter, "SeekLT", kvList[14].Key.UserKey, 13)
	iterOp(iter, "SeekLT", makeByte(14, 1), 14)
	iterOp(iter, "SeekLT", kvList[30].Key.UserKey, 29)
	iterOp(iter, "SeekLT", kvList[31].Key.UserKey, 30)
	iterOp(iter, "SeekLT", testMakeSortedKey(990), 99)
	iterOp(iter, "SeekLT", kvList[0].Key.UserKey, -1)
	require.NoError(t, iter.Close())

	require.NoError(t, btree.MemFlushStart())
	for i := 0; i < keyCount; i++ {
		if i > 0 && i < 90 {
			continue
		}
		pn, sentinel, closer := btree.FindKeyPageNum(kvList[i].Key.UserKey)
		closer()
		require.NotEqual(t, nilPageNum, pn)
		kvList[i].Key.SetKind(base.InternalKeyKindDelete)
		kvList[i].Key.SetSeqNum(seqNum)
		kvList[i].Value = []byte(nil)
		seqNum++
		require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
	}
	require.NoError(t, btree.MemFlushFinish())
	time.Sleep(1 * time.Second)

	iter = btree.newBitreeIter(nil)
	iterOp(iter, "First", nil, 0)
	iterOp(iter, "Next", nil, 13)
	iterOp(iter, "SeekGE", testMakeSortedKey(2), 13)
	iterOp(iter, "Prev", nil, 0)
	iterOp(iter, "Next", nil, 13)
	iterOp(iter, "Last", nil, 99)
	iterOp(iter, "Prev", nil, 98)
	iterOp(iter, "SeekGE", kvList[89].Key.UserKey, 89)
	iterOp(iter, "Prev", nil, 88)
	iterOp(iter, "SeekLT", kvList[60].Key.UserKey, 59)
	iterOp(iter, "Next", nil, 60)
	iterOp(iter, "Prev", nil, 59)
	require.NoError(t, iter.Close())

	require.NoError(t, testBitreeClose(btree))
}

func TestBitree_IterCache(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	optspool := base.InitDefaultsOptionsPool()
	optspool.BaseOptions.BitpageFlushSize = 1 << 20
	optspool.BaseOptions.UseBlockCompress = true
	bitreeOpts := optspool.Clone(base.BitreeOptionsType).(*base.BitreeOptions)
	btree, err := NewBitree(testDir, bitreeOpts)
	require.NoError(t, err)
	require.NoError(t, testNewBitreePages(btree))

	keyCount := 10000
	seqNum := uint64(0)
	kvList := testMakeSortedKV(keyCount, seqNum, 100)
	seqNum += uint64(keyCount)

	require.NoError(t, btree.MemFlushStart())
	for i := 0; i < keyCount; i++ {
		k := testMakeKey(i)
		pn, sentinel, closer := btree.FindKeyPageNum(k)
		closer()
		require.NotEqual(t, nilPageNum, pn)
		kvList[i].Key.SetSeqNum(seqNum)
		seqNum++
		require.NoError(t, btree.writer.set(*kvList[i].Key, kvList[i].Value, pn, sentinel))
	}
	require.NoError(t, btree.MemFlushFinish())
	time.Sleep(1 * time.Second)

	rangeLoop := func(o *base.IterOptions) int {
		iter := btree.newBitreeIter(o)
		i := 0
		for ik, val := iter.First(); ik != nil; ik, val = iter.Next() {
			require.Equal(t, *kvList[i].Key, *ik)
			require.Equal(t, kvList[i].Value, val)
			i++
		}
		require.NoError(t, iter.Close())
		return i
	}

	iterOpts := &base.IterOptions{DisableCache: true}
	require.Equal(t, keyCount, rangeLoop(iterOpts))
	fmt.Println("start range disable cache", btree.bpage.GetCacheMetrics())

	require.Equal(t, keyCount, rangeLoop(nil))
	fmt.Println("start range use cache", btree.bpage.GetCacheMetrics())

	require.NoError(t, testBitreeClose(btree))
}
