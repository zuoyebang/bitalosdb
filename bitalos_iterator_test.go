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

package bitalosdb

import (
	"crypto/md5"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

func testBitalosdbIter(t *testing.T) {
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	o := &IterOptions{}
	iter := new(Iterator)
	cmp := DefaultComparer.Compare

	o = &IterOptions{
		LowerBound: testMakeSameKey([]byte("key_1")),
		UpperBound: testMakeSameKey([]byte("key_73")),
		SlotId:     testSlotId,
	}
	iter = bitalosDB.db.NewIter(o)
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
		LowerBound: testMakeSameKey([]byte("key_4")),
		UpperBound: testMakeSameKey([]byte("key_82")),
		SlotId:     testSlotId,
	}
	iter = bitalosDB.db.NewIter(o)
	for iter.SeekGE(testMakeSameKey([]byte("key_6"))); iter.Valid(); iter.Next() {
		if cmp(iter.Key(), o.GetLowerBound()) < 0 {
			t.Fatalf("get %s less than %s", iter.Key(), o.GetLowerBound())
		}
		if cmp(iter.Key(), o.GetUpperBound()) >= 0 {
			t.Fatalf("get %s greater than %s", iter.Key(), o.GetUpperBound())
		}
	}
	require.NoError(t, iter.Close())

	o = &IterOptions{
		LowerBound: testMakeSameKey([]byte("key_4")),
		UpperBound: testMakeSameKey([]byte("key_54")),
		SlotId:     testSlotId,
	}
	iter = bitalosDB.db.NewIter(o)
	for iter.SeekLT(testMakeSameKey([]byte("key_6"))); iter.Valid(); iter.Prev() {
		if cmp(iter.Key(), o.GetLowerBound()) < 0 {
			t.Fatalf("get %s less than %s", iter.Key(), o.GetLowerBound())
		}
		if cmp(iter.Key(), o.GetUpperBound()) >= 0 {
			t.Fatalf("get %s greater than %s", iter.Key(), o.GetUpperBound())
		}
	}
	require.NoError(t, iter.Close())
	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbIter(t *testing.T) {
	key := "zset" + strconv.FormatInt(int64(9307), 10)
	val := md5.Sum([]byte(key))
	fmt.Println(fmt.Sprintf("%x", val[0:16]))
	return

	defer os.RemoveAll(testDirname)
	testBitalosdbWrite(t, 100, true)
	testBitalosdbIter(t)
}

func testBitalosdbIterSeek(t *testing.T) {
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	iter := new(Iterator)
	cmp := DefaultComparer.Compare
	var want []byte

	o := &IterOptions{
		SlotId: testSlotId,
	}
	iter = bitalosDB.db.NewIter(o)
	iter.First()
	if iter.Valid() {
		want = testMakeSameKey([]byte("key_10"))
		iter.Next()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = testMakeSameKey([]byte("key_71"))
		iter.SeekGE(want)
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = testMakeSameKey([]byte("key_72"))
		iter.Next()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = testMakeSameKey([]byte("key_71"))
		iter.Prev()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = testMakeSameKey([]byte("key_52"))
		iter.SeekLT(testMakeSameKey([]byte("key_53")))
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = testMakeSameKey([]byte("key_51"))
		iter.Prev()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}

		want = testMakeSameKey([]byte("key_52"))
		iter.Next()
		if cmp(iter.Key(), want) != 0 {
			t.Fatalf("got %s want %s", iter.Key(), want)
		}
	}
	require.NoError(t, iter.Close())
	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbIterSeek(t *testing.T) {
	defer os.RemoveAll(testDirname)
	testBitalosdbWrite(t, 100, true)
	testBitalosdbIterSeek(t)
}

func TestBitalosdbIterPrefix(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

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
		if err = bitalosDB.db.Set([]byte(key), []byte(key), Sync); err != nil {
			t.Fatal(err)
		}
	}

	rangeIter := func() {
		iterOpts := &IterOptions{
			IsAll: true,
		}
		iter := bitalosDB.db.NewIter(iterOpts)
		i := 0
		for iter.First(); iter.Valid(); iter.Next() {
			if i == 0 {
				require.Equal(t, []byte("hello"), iter.Key())
			} else if i == 1 {
				require.Equal(t, []byte("hello world"), iter.Key())
			}
			i++
		}
		if err = iter.Close(); err != nil {
			t.Fatal(err)
		}

		iterOpts = &IterOptions{
			IsAll:      true,
			LowerBound: []byte("hello"),
			UpperBound: keyUpperBound([]byte("hello")),
		}
		iter = bitalosDB.db.NewIter(iterOpts)
		i = 0
		for iter.First(); iter.Valid(); iter.Next() {
			if i == 0 {
				require.Equal(t, []byte("hello"), iter.Key())
			} else if i == 1 {
				require.Equal(t, []byte("hello world"), iter.Key())
			}
			i++
		}
		if err = iter.Close(); err != nil {
			t.Fatal(err)
		}
	}

	rangeIter()

	if err = bitalosDB.db.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		v, vcloser, e := bitalosDB.db.Get([]byte(key))
		if e != nil {
			t.Fatal(e)
		}
		require.Equal(t, []byte(key), v)
		vcloser()
	}

	rangeIter()

	if err = bitalosDB.db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBitalosdbIterSeekGE(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	keys := []string{"hello", "world", "hello world"}
	for _, key := range keys {
		if err = bitalosDB.db.Set([]byte(key), []byte(key), Sync); err != nil {
			t.Fatal(err)
		}
	}

	rangeIter := func() {
		iterOpts := &IterOptions{
			IsAll: true,
		}
		iter := bitalosDB.db.NewIter(iterOpts)

		if iter.SeekGE([]byte("a")); iter.Valid() {
			require.Equal(t, []byte("hello"), iter.Value())
		}
		if iter.SeekGE([]byte("hello w")); iter.Valid() {
			require.Equal(t, []byte("hello world"), iter.Value())
		}
		if iter.SeekGE([]byte("w")); iter.Valid() {
			require.Equal(t, []byte("world"), iter.Value())
		}

		if err = iter.Close(); err != nil {
			t.Fatal(err)
		}
	}

	rangeIter()

	if err = bitalosDB.db.Flush(); err != nil {
		t.Fatal(err)
	}

	rangeIter()

	if err = bitalosDB.db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBitalosdbIterSeekLT(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	batch := bitalosDB.db.NewBatch()
	seek := testMakeSameKey([]byte(fmt.Sprintf("key_%s", utils.Float64ToByteSort(math.MaxFloat64, nil))))
	fmt.Println("seek", string(seek))
	key0 := testMakeSameKey([]byte("key_0"))
	val0 := testMakeSameKey([]byte("val_0"))
	key1 := testMakeSameKey([]byte("key_1"))
	val1 := testMakeSameKey([]byte("val_1"))
	key2 := testMakeSameKey([]byte("key_2"))
	val2 := testMakeSameKey([]byte("val_2"))
	require.NoError(t, batch.Set(key0, val0, bitalosDB.wo))
	require.NoError(t, batch.Set(key1, val1, bitalosDB.wo))
	require.NoError(t, batch.Set(key2, val2, bitalosDB.wo))
	require.NoError(t, batch.Commit(bitalosDB.wo))
	require.NoError(t, batch.Close())

	it := bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	for it.SeekLT(seek); it.Valid(); it.Next() {
		require.Equal(t, key2, it.Key())
		require.Equal(t, val2, it.Value())
	}
	require.NoError(t, it.Close())

	require.NoError(t, bitalosDB.db.Close())

	bitalosDB, err = openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	it = bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	for it.SeekLT(seek); it.Valid(); it.Next() {
		require.Equal(t, key2, it.Key())
		require.Equal(t, val2, it.Value())
	}
	require.NoError(t, it.Close())

	it = bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	cnt := 2
	for it.Last(); it.Valid(); it.Prev() {
		require.Equal(t, testMakeSameKey([]byte(fmt.Sprintf("key_%d", cnt))), it.Key())
		require.Equal(t, testMakeSameKey([]byte(fmt.Sprintf("val_%d", cnt))), it.Value())
		cnt--
	}
	require.NoError(t, it.Close())

	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbIterReadAmp(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	it := bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	cnt := 0
	for it.Last(); it.Valid(); it.Prev() {
		cnt++
	}
	require.NoError(t, it.Close())
	require.Equal(t, uint64(0), bitalosDB.db.iterSlowCount.Load())

	num := consts.IterSlowCountThreshold + 10
	for i := 0; i < num; i++ {
		kv := fmt.Sprintf("kv_%d", i)
		key := testMakeSameKey([]byte(kv))
		val := testMakeSameKey([]byte(kv))
		require.NoError(t, bitalosDB.db.Set(key, val, bitalosDB.wo))
		require.NoError(t, bitalosDB.db.Delete(key, bitalosDB.wo))
	}

	it = bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	cnt = 0
	for it.Last(); it.Valid(); it.Prev() {
		cnt++
	}
	require.NoError(t, it.Close())
	require.Equal(t, uint64(1), bitalosDB.db.iterSlowCount.Load())
	bitalosDB.db.CheckIterReadAmplification(1)

	require.NoError(t, bitalosDB.db.Close())
}
