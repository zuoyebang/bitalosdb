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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

var defaultLargeVal = testRandBytes(1024)

func bdbWriteKVRange(t *testing.T, db *DB, start, end int, kprefix, vprefix string) {
	for i := start; i <= end; i++ {
		key := makeTestKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		value := []byte(fmt.Sprintf("%s_%s%s", key, defaultLargeVal, vprefix))
		require.NoError(t, db.Set(key, value, nil))
	}
	require.NoError(t, db.Flush())
}

func bdbReadKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		key := makeTestKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		value := []byte(fmt.Sprintf("%s_%s%s", key, defaultLargeVal, vprefix))
		require.NoError(t, verifyGet(db, key, value))
	}
}

func bdbWriteSameKVRange(t *testing.T, db *DB, start, end int, kprefix, vprefix string) {
	for i := start; i <= end; i++ {
		key := makeTestSlotKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		value := []byte(fmt.Sprintf("%s_%s%s", key, defaultLargeVal, vprefix))
		require.NoError(t, db.Set(key, value, nil))
	}
	require.NoError(t, db.Flush())
}

func bdbReadSameKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		key := makeTestSlotKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		value := []byte(fmt.Sprintf("%s_%s%s", key, defaultLargeVal, vprefix))
		require.NoError(t, verifyGet(db, key, value))
	}
}

func bdbDeleteKV(t *testing.T, db *DB, num int, kprefix string) {
	for i := 1; i <= num; i++ {
		if i%2 == 0 {
			continue
		}
		key := makeTestKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		require.NoError(t, db.Delete(key, nil))
	}
	require.NoError(t, db.Flush())
}

func bdbDeleteSameKV(t *testing.T, db *DB, num int, kprefix string) {
	for i := 1; i <= num; i++ {
		if i%2 == 0 {
			continue
		}
		key := makeTestSlotKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		require.NoError(t, db.Delete(key, nil))
	}
	require.NoError(t, db.Flush())
}

func bdbReadDeleteKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := makeTestKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultLargeVal, vprefix))
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
		newKey := makeTestSlotKey([]byte(fmt.Sprintf("key_%d%s", i, kprefix)))
		newValue := []byte(fmt.Sprintf("%s_%s%s", newKey, defaultLargeVal, vprefix))
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

func TestBitableWriteRead(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	testOptsUseBitable = true
	db := openTestDB(testDirname, nil)
	num := 100
	bdbWriteKVRange(t, db, 1, num, "bitableKey1", "bitableValue1")
	bdbReadKV(t, db, num, "bitableKey1", "bitableValue1")
	require.Equal(t, false, db.isFlushedBitable())
	db.compactToBitable(1)
	bdbReadKV(t, db, num, "bitableKey1", "bitableValue1")
	bdbWriteKVRange(t, db, 1, num, "bdbKey1", "bdbValue1")
	require.Equal(t, true, db.isFlushedBitable())
	require.NoError(t, db.Close())
	testOptsUseBitable = true
	db = openTestDB(testDirname, nil)
	require.Equal(t, true, db.isFlushedBitable())
	bdbReadKV(t, db, num, "bitableKey1", "bitableValue1")
	bdbReadKV(t, db, num, "bdbKey1", "bdbValue1")
	require.NoError(t, db.Close())
}

func TestBitableDelete(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	testOptsUseBitable = true
	db := openTestDB(testDirname, nil)
	num := 1000
	bdbWriteKVRange(t, db, 1, 500, "", "")
	db.compactToBitable(1)
	bdbWriteKVRange(t, db, 501, num, "", "")
	bdbDeleteKV(t, db, num, "")
	bdbReadDeleteKV(t, db, num, "", "")
	require.NoError(t, db.Close())
}

func TestBitableIter(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	testOptsUseBitable = true
	db := openTestDB(testDirname, nil)
	num := 100
	bdbWriteSameKVRange(t, db, 1, 50, "", "")
	db.compactToBitable(1)
	bdbWriteSameKVRange(t, db, 51, 100, "", "")
	bdbReadSameKV(t, db, num, "", "")
	require.NoError(t, db.Close())
	testOptsUseBitable = true
	testIterSeek(t)
	testOptsUseBitable = true
	testIter(t)
}

func TestBitableIterSeek(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	testOptsUseBitable = true
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	makeKey := func(i int) []byte {
		return makeTestSlotKey([]byte(fmt.Sprintf("key_%d", i)))
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
			require.NoError(t, db.Set(newKey, value, nil))
		}
		require.NoError(t, db.Flush())
	}

	writeKV(0)
	db.compactToBitable(1)
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
	iter := db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
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
	iter = db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
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
		if err := db.Delete(newKey, nil); err != nil {
			t.Fatal("delete err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())

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

	iter = db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
	searchKV(iter)
	iter = db.NewIter(&IterOptions{IsAll: true})
	searchKV(iter)

	require.NoError(t, db.Flush())
	iter = db.NewIter(&IterOptions{SlotId: uint32(testSlotId)})
	searchKV(iter)
	iter = db.NewIter(&IterOptions{IsAll: true})
	searchKV(iter)
}

func TestBitableSameDelete(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	testOptsUseBitable = true
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	num := 1000
	bdbWriteSameKVRange(t, db, 1, 500, "", "")
	db.compactToBitable(1)
	bdbWriteSameKVRange(t, db, 501, num, "", "")
	bdbDeleteSameKV(t, db, num, "")
	bdbReadDeleteSameKV(t, db, num, "", "")
}

func TestBitableCompactDelete(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := testOpenDB(false)
	bw, err := db.newFlushWriter()
	require.NoError(t, err)
	largeValue := utils.FuncRandBytes(1900)
	smallValue := utils.FuncRandBytes(2100)
	seqNum := uint64(0)
	keyCount := 10000
	kvList := testMakeSortedKVList(0, keyCount, seqNum, 1)
	seqNum += uint64(keyCount)

	for i := 0; i < keyCount; i++ {
		if i%2 == 0 {
			kvList[i].Value = smallValue
		} else {
			kvList[i].Value = largeValue
		}
		require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, bw.Finish())

	readKV := func(hasDel bool) {
		for i := 0; i < keyCount; i++ {
			v, vcloser, err := db.Get(kvList[i].Key.UserKey)
			if hasDel && i%9 == 0 {
				require.Equal(t, ErrNotFound, err)
				require.Equal(t, 0, len(v))
			} else {
				require.Equal(t, kvList[i].Value, v)
				vcloser()
			}
		}
	}

	readKV(false)
	db.compactToBitable(1)
	readKV(false)

	bw, err = db.newFlushWriter()
	require.NoError(t, err)
	largeValue = utils.FuncRandBytes(1900)
	smallValue = utils.FuncRandBytes(2100)
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
		require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, bw.Finish())

	readKV(true)

	require.NoError(t, db.Close())
}
