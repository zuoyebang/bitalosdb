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
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

var defaultLargeVal = testRandBytes(1024)

func testMakeIndexVal(i int, key []byte, vprefix string) (val []byte) {
	if i%2 == 0 {
		val = []byte(fmt.Sprintf("%s_%s%s", key, defaultLargeVal, vprefix))
	} else {
		val = []byte(fmt.Sprintf("%s_%s%s", key, defaultValBytes, vprefix))
	}

	return val
}

func testMakeIndexKey(i int, kprefix string) (key []byte) {
	key = testMakeKey([]byte(fmt.Sprintf("%skey_%d", kprefix, i)))
	return key
}

func testGetKeyIndex(key []byte) int {
	sk := strings.Split(string(key), "_")
	index, _ := strconv.Atoi(sk[len(sk)-1])
	return index
}

func testWriteKVRange(t *testing.T, db *DB, start, end int, kprefix, vprefix string) {
	for i := start; i <= end; i++ {
		newKey := testMakeIndexKey(i, kprefix)
		newValue := testMakeIndexVal(i, newKey, vprefix)
		if err := db.Set(newKey, newValue, NoSync); err != nil {
			t.Fatal("set err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func bdbDeleteKVRange(t *testing.T, db *DB, start, end, gap int, kprefix string) {
	for i := start; i <= end; i++ {
		if i%gap != 0 {
			continue
		}
		newKey := testMakeIndexKey(i, kprefix)
		if err := db.Delete(newKey, NoSync); err != nil {
			t.Fatal("delete err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func bdbWriteKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := testMakeKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultLargeVal, vprefix))
		if err := db.Set(newKey, newValue, nil); err != nil {
			t.Fatal("set err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func bdbReadKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := testMakeKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultLargeVal, vprefix))
		v, closer, err := db.Get(newKey)
		require.NoError(t, err)
		require.Equal(t, newValue, v)
		if closer != nil {
			closer()
		}
	}
}

func bdbWriteKVRange(t *testing.T, db *DB, start, end int, kprefix, vprefix string) {
	for i := start; i <= end; i++ {
		newKey := testMakeKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultValBytes, vprefix))
		if err := db.Set(newKey, newValue, nil); err != nil {
			t.Fatal("set err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func bdbWriteSameKVRange(t *testing.T, db *DB, start, end int, kprefix, vprefix string) {
	for i := start; i <= end; i++ {
		newKey := testMakeSameKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultValBytes, vprefix))
		if err := db.Set(newKey, newValue, nil); err != nil {
			t.Fatal("set err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func bdbReadSameKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := testMakeSameKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultValBytes, vprefix))
		v, closer, err := db.Get(newKey)
		require.NoError(t, err)
		require.Equal(t, newValue, v)
		if closer != nil {
			closer()
		}
	}
}

func bdbDeleteKV(t *testing.T, db *DB, num int, kprefix string) {
	for i := 1; i <= num; i++ {
		if i%2 == 0 {
			continue
		}
		newKey := testMakeKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		if err := db.Delete(newKey, nil); err != nil {
			t.Fatal("delete err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func bdbDeleteSameKV(t *testing.T, db *DB, num int, kprefix string) {
	for i := 1; i <= num; i++ {
		if i%2 == 0 {
			continue
		}
		newKey := testMakeSameKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		if err := db.Delete(newKey, nil); err != nil {
			t.Fatal("delete err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func bdbReadDeleteKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := testMakeKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultValBytes, vprefix))
		v, closer, err := db.Get(newKey)
		if i%2 == 0 {
			require.NoError(t, err)
			require.Equal(t, newValue, v)
		} else {
			require.Equal(t, ErrNotFound, err)
		}

		if closer != nil {
			closer()
		}
	}
}

func bdbReadDeleteSameKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := testMakeSameKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultValBytes, vprefix))
		v, closer, err := db.Get(newKey)
		if i%2 == 0 {
			require.NoError(t, err)
			require.Equal(t, newValue, v)
		} else {
			require.Equal(t, ErrNotFound, err)
		}

		if closer != nil {
			closer()
		}
	}
}

func TestBitalosdbBitableWriteAndRead(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	num := 100
	bdbWriteKV(t, bitalosDB.db, num, "bitableKey1", "bitableValue1")
	bdbReadKV(t, bitalosDB.db, num, "bitableKey1", "bitableValue1")
	require.Equal(t, false, bitalosDB.db.bf.IsFlushedBitable())
	bitalosDB.db.bf.CompactBitree(1)
	bdbReadKV(t, bitalosDB.db, num, "bitableKey1", "bitableValue1")
	bdbWriteKV(t, bitalosDB.db, num, "bdbKey1", "bdbValue1")
	require.Equal(t, true, bitalosDB.db.bf.IsFlushedBitable())
	require.NoError(t, bitalosDB.db.Close())
	bitalosDB, err = openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)
	require.Equal(t, true, bitalosDB.db.bf.IsFlushedBitable())
	bdbReadKV(t, bitalosDB.db, num, "bitableKey1", "bitableValue1")
	bdbReadKV(t, bitalosDB.db, num, "bdbKey1", "bdbValue1")

	fmt.Println("DebugInfo", bitalosDB.db.DebugInfo())
}

func TestBitalosdbBitableDelete(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	num := 1000
	bdbWriteKVRange(t, bitalosDB.db, 1, 500, "", "")
	bitalosDB.db.bf.CompactBitree(1)
	bdbWriteKVRange(t, bitalosDB.db, 501, num, "", "")
	bdbDeleteKV(t, bitalosDB.db, num, "")

	bdbReadDeleteKV(t, bitalosDB.db, num, "", "")
	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbBitableIter(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	num := 100
	bdbWriteSameKVRange(t, bitalosDB.db, 1, 50, "", "")
	bitalosDB.db.bf.CompactBitree(1)
	bdbWriteSameKVRange(t, bitalosDB.db, 51, 100, "", "")
	bdbReadSameKV(t, bitalosDB.db, num, "", "")
	require.NoError(t, bitalosDB.db.Close())
	testBitalosdbIterSeek(t)
	testBitalosdbIter(t)
}

func TestBitalosdbBitableIterSeek(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	makeKey := func(i int) []byte {
		return testMakeSameKey([]byte(fmt.Sprintf("key_%d", i)))
	}

	value := utils.FuncRandBytes(520)
	num := 100
	writeKV := func(filter int) {
		for i := 0; i < num; i++ {
			if i >= 1 && i < 10 {
				continue
			}
			if i%2 == filter {
				continue
			}
			newKey := makeKey(i)
			if err = bitalosDB.db.Set(newKey, value, nil); err != nil {
				t.Fatal("set err", string(newKey), err)
			}
		}
		require.NoError(t, bitalosDB.db.Flush())
	}

	writeKV(0)
	bitalosDB.db.bf.CompactBitree(1)
	writeKV(1)

	seekGE := func(it *Iterator, seek, want int) {
		exist := it.SeekGE(makeKey(seek))
		if want == -1 {
			require.Equal(t, false, exist)
			require.Equal(t, false, it.Valid())
		} else {
			require.Equal(t, true, exist)
			require.Equal(t, true, it.Valid())
			require.Equal(t, makeKey(want), it.Key())
			require.Equal(t, value, it.Value())
		}
	}
	iter := bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	seekGE(iter, 14, 14)
	seekGE(iter, 141, 15)
	seekGE(iter, 29, 29)
	seekGE(iter, 290, 30)
	seekGE(iter, 491, 50)
	seekGE(iter, 990, -1)
	require.NoError(t, iter.Close())

	seekLT := func(it *Iterator, seek, want int) {
		exist := it.SeekLT(makeKey(seek))
		if want == -1 {
			require.Equal(t, false, exist)
			require.Equal(t, false, it.Valid())
		} else {
			require.Equal(t, true, exist)
			require.Equal(t, true, it.Valid())
			require.Equal(t, makeKey(want), it.Key())
			require.Equal(t, value, it.Value())
		}
	}
	iter = bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	seekLT(iter, 14, 13)
	seekLT(iter, 141, 14)
	seekLT(iter, 30, 29)
	seekLT(iter, 31, 30)
	seekLT(iter, 50, 49)
	seekLT(iter, 990, 99)
	seekLT(iter, 0, -1)
	require.NoError(t, iter.Close())

	for i := 0; i < num; i++ {
		if i > 0 && i < 90 {
			continue
		}
		newKey := makeKey(i)
		if err = bitalosDB.db.Delete(newKey, nil); err != nil {
			t.Fatal("delete err", string(newKey), err)
		}
	}
	require.NoError(t, bitalosDB.db.Flush())

	searchKV := func(it *Iterator) {
		defer func() {
			require.NoError(t, iter.Close())
		}()
		require.Equal(t, true, iter.First())
		require.Equal(t, makeKey(10), iter.Key())
		require.Equal(t, value, iter.Value())
		require.Equal(t, true, iter.Next())
		require.Equal(t, makeKey(11), iter.Key())
		require.Equal(t, value, iter.Value())
		seekGE(iter, 2, 20)
		require.Equal(t, true, iter.Prev())
		require.Equal(t, makeKey(19), iter.Key())
		require.Equal(t, value, iter.Value())
		require.Equal(t, true, iter.Next())
		require.Equal(t, makeKey(20), iter.Key())
		require.Equal(t, value, iter.Value())
		require.Equal(t, true, iter.Last())
		require.Equal(t, makeKey(89), iter.Key())
		require.Equal(t, value, iter.Value())
		require.Equal(t, true, iter.Prev())
		require.Equal(t, makeKey(88), iter.Key())
		require.Equal(t, value, iter.Value())
		seekLT(iter, 60, 59)
		require.Equal(t, true, iter.Next())
		require.Equal(t, makeKey(60), iter.Key())
		require.Equal(t, value, iter.Value())
		require.Equal(t, true, iter.Prev())
		require.Equal(t, makeKey(59), iter.Key())
		require.Equal(t, value, iter.Value())
	}

	iter = bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	searchKV(iter)
	iter = bitalosDB.db.NewIter(&IterOptions{IsAll: true})
	searchKV(iter)

	require.NoError(t, bitalosDB.db.Flush())
	iter = bitalosDB.db.NewIter(&IterOptions{SlotId: testSlotId})
	searchKV(iter)
	iter = bitalosDB.db.NewIter(&IterOptions{IsAll: true})
	searchKV(iter)
}

func TestBitalosdbBitableSameDelete(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	num := 1000
	bdbWriteSameKVRange(t, bitalosDB.db, 1, 500, "", "")
	bitalosDB.db.bf.CompactBitree(1)
	bdbWriteSameKVRange(t, bitalosDB.db, 501, num, "", "")
	bdbDeleteSameKV(t, bitalosDB.db, num, "")

	bdbReadDeleteSameKV(t, bitalosDB.db, num, "", "")
	require.NoError(t, bitalosDB.db.Close())
}
