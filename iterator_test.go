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

package bitalosdb

import (
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/unsafe2"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

func testIter(t *testing.T) {
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	o := &IterOptions{}
	iter := new(Iterator)
	cmp := DefaultComparer.Compare

	o = &IterOptions{
		LowerBound: makeTestSlotKey([]byte("key_1")),
		UpperBound: makeTestSlotKey([]byte("key_73")),
		SlotId:     uint32(testSlotId),
	}
	iter = db.NewIter(o)
	for iter.First(); iter.Valid(); iter.Next() {
		if cmp(iter.Key(), o.GetLowerBound()) < 0 {
			t.Fatalf("get %s less than %s", iter.Key(), o.GetLowerBound())
		}
		if cmp(iter.Key(), o.GetUpperBound()) >= 0 {
			t.Fatalf("get %s greater than %s", iter.Key(), o.GetUpperBound())
		}
	}
	require.NoError(t, iter.Close())

	o = &IterOptions{
		LowerBound: makeTestSlotKey([]byte("key_4")),
		UpperBound: makeTestSlotKey([]byte("key_82")),
		SlotId:     uint32(testSlotId),
	}
	iter = db.NewIter(o)
	for iter.SeekGE(makeTestSlotKey([]byte("key_6"))); iter.Valid(); iter.Next() {
		if cmp(iter.Key(), o.GetLowerBound()) < 0 {
			t.Fatalf("get %s less than %s", iter.Key(), o.GetLowerBound())
		}
		if cmp(iter.Key(), o.GetUpperBound()) >= 0 {
			t.Fatalf("get %s greater than %s", iter.Key(), o.GetUpperBound())
		}
	}
	require.NoError(t, iter.Close())

	o = &IterOptions{
		LowerBound: makeTestSlotKey([]byte("key_4")),
		UpperBound: makeTestSlotKey([]byte("key_54")),
		SlotId:     uint32(testSlotId),
	}
	iter = db.NewIter(o)
	for iter.SeekLT(makeTestSlotKey([]byte("key_6"))); iter.Valid(); iter.Prev() {
		if cmp(iter.Key(), o.GetLowerBound()) < 0 {
			t.Fatalf("get %s less than %s", iter.Key(), o.GetLowerBound())
		}
		if cmp(iter.Key(), o.GetUpperBound()) >= 0 {
			t.Fatalf("get %s greater than %s", iter.Key(), o.GetUpperBound())
		}
	}
	require.NoError(t, iter.Close())
}

func TestIter(t *testing.T) {
	defer os.RemoveAll(testDirname)
	testBitalosdbWrite(t, 100, true)
	testIter(t)
}

func testIterSeek(t *testing.T) {
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	iter := new(Iterator)
	cmp := DefaultComparer.Compare
	var want []byte

	o := &IterOptions{
		SlotId: uint32(testSlotId),
	}
	iter = db.NewIter(o)
	defer iter.Close()
	iter.First()
	if iter.Valid() {
		want = makeTestSlotKey([]byte("key_10"))
		iter.Next()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = makeTestSlotKey([]byte("key_71"))
		iter.SeekGE(want)
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = makeTestSlotKey([]byte("key_72"))
		iter.Next()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = makeTestSlotKey([]byte("key_71"))
		iter.Prev()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = makeTestSlotKey([]byte("key_52"))
		iter.SeekLT(makeTestSlotKey([]byte("key_53")))
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = makeTestSlotKey([]byte("key_51"))
		iter.Prev()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = makeTestSlotKey([]byte("key_52"))
		iter.Next()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}
	}
}

func TestNewIter(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	it := db.NewIter(&IterOptions{IsAll: true})
	require.Equal(t, -1, it.index)
	require.Equal(t, 8, len(it.alloc.merging.levels))
	require.NoError(t, it.Close())

	it = db.NewIter(nil)
	require.Equal(t, -1, it.index)
	require.Equal(t, 8, len(it.alloc.merging.levels))
	require.NoError(t, it.Close())

	it = db.NewIter(&IterOptions{IsAll: false})
	require.Equal(t, 0, it.index)
	require.Equal(t, 1, len(it.alloc.merging.levels))
	require.NoError(t, it.Close())

	it = db.NewIter(&IterOptions{SlotId: 6})
	require.Equal(t, 6, it.index)
	require.Equal(t, 1, len(it.alloc.merging.levels))
	require.NoError(t, it.Close())

	it = db.NewIter(&IterOptions{SlotId: 61})
	require.Equal(t, 5, it.index)
	require.Equal(t, 1, len(it.alloc.merging.levels))
	require.NoError(t, it.Close())

	for i := 0; i < 100; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, key, NoSync))
	}

	it = db.NewIter(&IterOptions{IsAll: true})
	require.Equal(t, -1, it.index)
	require.Equal(t, 16, len(it.alloc.merging.levels))
	require.NoError(t, it.Close())

	it = db.NewIter(&IterOptions{SlotId: 6})
	require.Equal(t, 6, it.index)
	require.Equal(t, 2, len(it.alloc.merging.levels))
	require.NoError(t, it.Close())

	it = db.NewIter(&IterOptions{SlotId: 61})
	require.Equal(t, 5, it.index)
	require.Equal(t, 2, len(it.alloc.merging.levels))
	require.NoError(t, it.Close())
}

func TestNewBitreeIter(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	testOptsUseBitable = true
	db := openTestDB(testDirname, nil)
	iterOpts := &options.IterOptions{SlotId: 2}
	index := base.GetBitowerIndex(int(iterOpts.GetSlotId()))
	siters := db.bitowers[index].newBitreeIter(iterOpts)
	require.Equal(t, 1, len(siters))
	for i := range siters {
		require.NoError(t, siters[i].Close())
	}

	newAllBitreeIters := func(o *options.IterOptions) (its []base.InternalIterator) {
		for i := range db.bitowers {
			btreeIters := db.bitowers[i].newBitreeIter(o)
			if len(btreeIters) > 0 {
				its = append(its, btreeIters...)
			}
		}
		return its
	}

	iters := newAllBitreeIters(nil)
	require.Equal(t, 8, len(iters))
	for i := range iters {
		require.NoError(t, iters[i].Close())
	}

	db.setFlushedBitable()
	siters = db.bitowers[index].newBitreeIter(iterOpts)
	require.Equal(t, 2, len(siters))
	for i := range siters {
		require.NoError(t, siters[i].Close())
	}
	iters = newAllBitreeIters(nil)
	require.Equal(t, 16, len(iters))
	for i := range iters {
		require.NoError(t, iters[i].Close())
	}
	require.NoError(t, db.Close())

	db = openTestDB(testDirname, nil)
	siters = db.bitowers[index].newBitreeIter(iterOpts)
	require.Equal(t, 1, len(siters))
	for i := range siters {
		require.NoError(t, siters[i].Close())
	}
	iters = newAllBitreeIters(nil)
	require.Equal(t, 8, len(iters))
	for i := range iters {
		require.NoError(t, iters[i].Close())
	}
	require.NoError(t, db.Close())
}

func TestIterSeek(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	testBitalosdbWrite(t, 100, true)
	testIterSeek(t)
}

func TestIterPrefix(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	keyUpperBound := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] = end[i] + 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil
	}

	keys := []string{"hello", "world", "hello world"}
	for _, key := range keys {
		require.NoError(t, db.Set([]byte(key), []byte(key), Sync))
	}

	rangeIter := func() {
		iterOpts := &IterOptions{
			IsAll: true,
		}
		iter := db.NewIter(iterOpts)
		i := 0
		for iter.First(); iter.Valid(); iter.Next() {
			if i == 0 {
				require.Equal(t, []byte("hello"), iter.Key())
			} else if i == 1 {
				require.Equal(t, []byte("hello world"), iter.Key())
			}
			i++
		}
		require.NoError(t, iter.Close())

		iterOpts = &IterOptions{
			IsAll:      true,
			LowerBound: []byte("hello"),
			UpperBound: keyUpperBound([]byte("hello")),
		}
		iter = db.NewIter(iterOpts)
		i = 0
		for iter.First(); iter.Valid(); iter.Next() {
			if i == 0 {
				require.Equal(t, []byte("hello"), iter.Key())
			} else if i == 1 {
				require.Equal(t, []byte("hello world"), iter.Key())
			}
			i++
		}
		require.NoError(t, iter.Close())
	}

	rangeIter()

	require.NoError(t, db.Flush())
	for _, key := range keys {
		require.NoError(t, verifyGet(db, []byte(key), []byte(key)))
	}

	rangeIter()
}

func TestIterSeekGE(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	keys := []string{"hello", "world", "hello world"}
	for _, key := range keys {
		require.NoError(t, db.Set([]byte(key), []byte(key), Sync))
	}

	rangeIter := func() {
		iterOpts := &IterOptions{
			IsAll: true,
		}
		iter := db.NewIter(iterOpts)

		if iter.SeekGE([]byte("a")); iter.Valid() {
			require.Equal(t, []byte("hello"), iter.Value())
		}
		if iter.SeekGE([]byte("hello w")); iter.Valid() {
			require.Equal(t, []byte("hello world"), iter.Value())
		}
		if iter.SeekGE([]byte("w")); iter.Valid() {
			require.Equal(t, []byte("world"), iter.Value())
		}

		require.NoError(t, iter.Close())
	}

	rangeIter()
	require.NoError(t, db.Flush())
	rangeIter()
}

func TestIterSeekLT(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	batch := db.NewBatchBitower()
	seek := makeTestSlotKey([]byte(fmt.Sprintf("key_%s", utils.Float64ToByteSort(math.MaxFloat64, nil))))
	key0 := makeTestSlotKey([]byte("key_0"))
	val0 := makeTestSlotKey([]byte("val_0"))
	key1 := makeTestSlotKey([]byte("key_1"))
	val1 := makeTestSlotKey([]byte("val_1"))
	key2 := makeTestSlotKey([]byte("key_2"))
	val2 := makeTestSlotKey([]byte("val_2"))
	require.NoError(t, batch.Set(key0, val0, NoSync))
	require.NoError(t, batch.Set(key1, val1, NoSync))
	require.NoError(t, batch.Set(key2, val2, NoSync))
	require.NoError(t, batch.Commit(NoSync))
	require.NoError(t, batch.Close())

	it := db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
	for it.SeekLT(seek); it.Valid(); it.Next() {
		require.Equal(t, key2, it.Key())
		require.Equal(t, val2, it.Value())
	}
	require.NoError(t, it.Close())
	require.NoError(t, db.Close())

	db = openTestDB(testDirname, nil)
	it = db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
	for it.SeekLT(seek); it.Valid(); it.Next() {
		require.Equal(t, key2, it.Key())
		require.Equal(t, val2, it.Value())
	}
	require.NoError(t, it.Close())

	it = db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
	cnt := 2
	for it.Last(); it.Valid(); it.Prev() {
		require.Equal(t, makeTestSlotKey([]byte(fmt.Sprintf("key_%d", cnt))), it.Key())
		require.Equal(t, makeTestSlotKey([]byte(fmt.Sprintf("val_%d", cnt))), it.Value())
		cnt--
	}
	require.NoError(t, it.Close())
}

func TestIterReadAmp(t *testing.T) {
	for _, isNext := range []bool{true, false} {
		t.Run(fmt.Sprintf("isNext=%t", isNext), func(t *testing.T) {
			defer os.RemoveAll(testDirname)
			os.RemoveAll(testDirname)

			db := openTestDB(testDirname, nil)
			defer func() {
				require.NoError(t, db.Close())
			}()

			for index := range db.bitowers {
				it := db.NewIter(&IterOptions{SlotId: uint32(index)})
				for it.First(); it.Valid(); it.Next() {
				}
				require.NoError(t, it.Close())
			}
			for i := range db.bitowers {
				require.Equal(t, uint64(0), db.bitowers[i].iterSlowCount.Load())
			}

			num := (consts.IterSlowCountThreshold + 10) * len(db.bitowers)
			for i := 0; i < num; i++ {
				key := makeTestIntKey(i)
				require.NoError(t, db.Set(key, key, NoSync))
				require.NoError(t, db.Delete(key, NoSync))
			}

			loop := consts.IterReadAmplificationThreshold + 1
			for i := range db.bitowers {
				for j := 0; j < loop; j++ {
					it := db.NewIter(&IterOptions{SlotId: uint32(i)})
					for it.First(); it.Valid(); it.Last() {
					}
					require.NoError(t, it.Close())
					require.Equal(t, uint64(1+j), db.bitowers[i].iterSlowCount.Load())
				}
			}

			db.compactIterReadAmplification(1)
			for i := range db.bitowers {
				require.Equal(t, uint64(0), db.bitowers[i].iterSlowCount.Load())
			}
		})
	}
}

func TestIterPrefixDeleteKey(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	val := testRandBytes(2048)
	pdKey := makeTestSlotKey([]byte("key_prefix_delete"))

	for i := 0; i < 100; i++ {
		newKey := makeTestSlotKey([]byte(fmt.Sprintf("key_%d", i)))
		require.NoError(t, db.Set(newKey, val, NoSync))
	}

	batch := db.NewBatch()
	batch.PrefixDeleteKeySet(pdKey, NoSync)
	require.NoError(t, batch.Commit(NoSync))

	testIter := func() {
		it := db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
		for it.First(); it.Valid(); it.Next() {
			require.NotEqual(t, pdKey, it.Key())
		}
		isFound := it.SeekGE(pdKey)
		require.Equal(t, false, isFound)
		require.NoError(t, it.Close())
		it = db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
		for it.Last(); it.Valid(); it.Prev() {
			require.NotEqual(t, pdKey, it.Key())
		}
		isFound = it.SeekLT(pdKey)
		require.Equal(t, it.Key(), makeTestSlotKey([]byte(fmt.Sprintf("key_%d", 99))))
		require.NoError(t, it.Close())
	}

	testIter()
	require.NoError(t, db.Flush())
	testIter()
}

func TestIterMemShard(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	val := testRandBytes(100)

	batch := db.NewBatch()
	for i := 0; i < 100; i++ {
		key := makeTestSlotKey([]byte(fmt.Sprintf("key_%d", i)))
		require.NoError(t, batch.Set(key, val, NoSync))
	}
	require.NoError(t, batch.Commit(NoSync))
	require.NoError(t, batch.Close())

	iterOpts := &IterOptions{
		LowerBound: makeTestSlotKey([]byte("key_1")),
		SlotId:     uint32(testSlotId),
	}
	it := db.NewIter(iterOpts)
	defer it.Close()
	wg := sync.WaitGroup{}
	for it.SeekGE(iterOpts.LowerBound); it.Valid(); it.Next() {
		key := utils.CloneBytes(it.Key())
		indexStr := strings.Split(string(key), "_")[1]
		index, _ := strconv.Atoi(indexStr)
		if index >= 20 && index <= 40 {
			wg.Add(1)
			go func(k []byte) {
				defer wg.Done()
				b := db.NewBatch()
				require.NoError(t, b.Delete(k, NoSync))
				require.NoError(t, b.Commit(NoSync))
				require.NoError(t, b.Close())
				_, _, err := db.Get(k)
				if err != ErrNotFound {
					t.Fatal("get delete key find ", string(k))
				}
			}(key)
		}
	}
	wg.Wait()
}

func TestIterStat(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	keyList := make([]string, 100)
	for i := 0; i < 100; i++ {
		key := makeTestSlotIntKey(i)
		keyList[i] = unsafe2.String(key)
		for j := 0; j < 100; j++ {
			require.NoError(t, db.Set(key, []byte(fmt.Sprintf("%d", j)), NoSync))
		}
	}
	sort.Strings(keyList)

	iterOpts := &IterOptions{
		SlotId: uint32(testSlotId),
	}
	it := db.NewIter(iterOpts)
	defer it.Close()
	i := 0
	for it.First(); it.Valid(); it.Next() {
		require.Equal(t, unsafe2.ByteSlice(keyList[i]), it.Key())
		require.Equal(t, []byte("99"), it.Value())
		i++
	}
	stats := it.Stats()
	require.Equal(t, 1, stats.ForwardSeekCount[0])
	require.Equal(t, 100, stats.ForwardStepCount[0])
	require.Equal(t, 1, stats.ForwardSeekCount[1])
	require.Equal(t, 200, stats.ForwardStepCount[1])
}

func TestIterAll(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	testOptsDisableWAL = true
	db := openTestDB(testDirname, nil)
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic err:%v stack:%s", r, string(debug.Stack()))
		}
		require.NoError(t, db.Close())
	}()

	keyNum := 10000
	keyList := make([]string, keyNum)
	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		keyList[i] = unsafe2.String(key)
		require.NoError(t, db.Set(key, key, NoSync))
	}
	sort.Strings(keyList)

	iterOpts := &IterOptions{
		LowerBound: nil,
		UpperBound: nil,
		IsAll:      true,
		SlotId:     0,
	}
	it := db.NewIter(iterOpts)
	i := 0
	for it.First(); it.Valid(); it.Next() {
		require.Equal(t, unsafe2.ByteSlice(keyList[i]), it.Key())
		require.Equal(t, it.Key(), it.Value())
		i++
	}
	require.NoError(t, it.Close())
}
