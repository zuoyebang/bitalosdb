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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/errors"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/internal/unsafe2"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

const (
	testDirname                     = "./test-data"
	testMaxWriteBufferNumber        = 8
	testMemTableSize                = 64 << 20
	testCacheSize                   = 128 << 20
	testSlotId               uint16 = 1
)

var (
	testOptsCompressType      int    = compress.CompressTypeNo
	testOptsDisableWAL        bool   = false
	testOptsUseMapIndex       bool   = true
	testOptsUsePrefixCompress bool   = false
	testOptsUseBlockCompress  bool   = false
	testOptsCacheType         int    = consts.CacheTypeLru
	testOptsCacheSize         int64  = 0
	testOptsMemtableSize      int    = testMemTableSize
	testOptsUseBitable        bool   = false
	testOptsBitpageFlushSize  uint64 = consts.BitpageDefaultFlushSize
	testOptsBitpageSplitSize  uint64 = consts.BitpageDefaultSplitSize
)

func resetTestOptsVal() {
	testOptsCompressType = compress.CompressTypeNo
	testOptsDisableWAL = false
	testOptsUseMapIndex = true
	testOptsUsePrefixCompress = false
	testOptsUseBlockCompress = false
	testOptsCacheType = consts.CacheTypeLru
	testOptsCacheSize = 0
	testOptsMemtableSize = testMemTableSize
	testOptsUseBitable = false
	testOptsBitpageFlushSize = consts.BitpageDefaultFlushSize
	testOptsBitpageSplitSize = consts.BitpageDefaultSplitSize
}

var defaultLargeValBytes []byte

func openTestDB(dir string, optspool *options.OptionsPool) *DB {
	defer func() {
		resetTestOptsVal()
	}()
	compactInfo := CompactEnv{
		StartHour:     0,
		EndHour:       23,
		DeletePercent: 0.2,
		BitreeMaxSize: 2 << 30,
		Interval:      60,
	}
	opts := &Options{
		BytesPerSync:                consts.DefaultBytesPerSync,
		MemTableSize:                testOptsMemtableSize,
		MemTableStopWritesThreshold: testMaxWriteBufferNumber,
		CacheType:                   testOptsCacheType,
		CacheSize:                   testOptsCacheSize,
		CacheHashSize:               10000,
		Verbose:                     true,
		Comparer:                    DefaultComparer,
		CompactInfo:                 compactInfo,
		LogTag:                      "[bitalosdb/test]",
		Logger:                      DefaultLogger,
		DataType:                    "string",
		CompressionType:             testOptsCompressType,
		UseBithash:                  true,
		UseBitable:                  testOptsUseBitable,
		UseMapIndex:                 testOptsUseMapIndex,
		UsePrefixCompress:           testOptsUsePrefixCompress,
		UseBlockCompress:            testOptsUseBlockCompress,
		DisableWAL:                  testOptsDisableWAL,
		KeyHashFunc:                 options.TestKeyHashFunc,
		KeyPrefixDeleteFunc:         options.TestKeyPrefixDeleteFunc,
		KvCheckExpireFunc:           options.TestKvCheckExpireFunc,
		KvTimestampFunc:             options.TestKvTimestampFunc,
		BitpageFlushSize:            testOptsBitpageFlushSize,
		BitpageSplitSize:            testOptsBitpageSplitSize,
	}
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0775); err != nil {
			panic(err)
		}
	}

	if optspool != nil {
		opts = opts.Clone().EnsureDefaults()
		opts.private.optspool = opts.ensureOptionsPool(optspool)
	}

	db, err := Open(dir, opts)
	if err != nil {
		panic(err)
	}
	return db
}

func testRandBytes(n int) []byte {
	return utils.FuncRandBytes(n)
}

func makeTestKey(key []byte) []byte {
	return sortedkv.MakeKey(key)
}

func makeTestVersionKey(key []byte, slotId uint16, version uint64) []byte {
	return sortedkv.MakeKey2(key, slotId, version)
}

func makeTestIntKey(i int) []byte {
	return sortedkv.MakeSlotKey([]byte(fmt.Sprintf("testkey_%d", i)), uint16(i&consts.DefaultBitowerNumMask))
}

func makeTestSlotIntKey(i int) []byte {
	return sortedkv.MakeSlotKey([]byte(fmt.Sprintf("testkey_%d", i)), testSlotId)
}

func makeTestSlotKey(key []byte) []byte {
	return sortedkv.MakeSlotKey(key, testSlotId)
}

func verifyGet(r Reader, key, val []byte) error {
	v, closer, err := r.Get(key)
	if err != nil {
		return err
	} else if !bytes.Equal(val, v) {
		return errors.Errorf("key %s expected %s, but got %s", string(key), val, v)
	}
	if closer != nil {
		closer()
	}
	return nil
}

func verifyGetNotFound(r Reader, key []byte) error {
	val, _, err := r.Get(key)
	if err != base.ErrNotFound {
		return errors.Errorf("key %s expected nil, but got %s", string(key), val)
	}
	return nil
}

func testBitalosdbWrite(t *testing.T, num int, isSame bool) {
	db := openTestDB(testDirname, nil)
	keyIndex := 0
	defaultLargeValBytes = testRandBytes(2048)
	val := defaultLargeValBytes

	for keyIndex < num {
		keyIndex++
		var key []byte
		if isSame {
			key = makeTestSlotKey([]byte(fmt.Sprintf("key_%d", keyIndex)))
		} else {
			key = makeTestIntKey(keyIndex)
		}
		require.NoError(t, db.Set(key, val, NoSync))
	}

	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())
}

func TestWriteMemtable(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	num := 100
	val := testRandBytes(2048)

	for i := 0; i < num; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, val, NoSync))
	}

	for i := 0; i < num; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, verifyGet(db, key, val))
	}
}

func TestFlushDeletePercent(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	testOptsMemtableSize = 3 << 20
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	val := testRandBytes(2048)
	for i := 0; i < 1000; i++ {
		key := makeTestSlotIntKey(i)
		require.NoError(t, db.Set(key, val, NoSync))
		if i > 400 {
			require.NoError(t, db.Delete(key, NoSync))
			require.NoError(t, db.Delete(key, NoSync))
		}
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < 1000; i++ {
		key := makeTestSlotIntKey(i)
		err := verifyGet(db, key, val)
		if i > 400 {
			require.Equal(t, ErrNotFound, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestWriteBitower(t *testing.T) {
	for _, useCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("useCache=%t", useCache), func(t *testing.T) {
			dirname := testDirname
			defer os.RemoveAll(dirname)
			os.RemoveAll(dirname)

			if useCache {
				testOptsCacheSize = 20 << 20
			} else {
				testOptsCacheSize = 0
			}

			val := testRandBytes(1024)
			stepNum := 1000
			startNum := 0
			endNum := stepNum
			seqNum := uint64(1)
			hasWal := false

			var bitowerMeta [consts.DefaultBitowerNum]bitowerMetaEditor
			for i := 0; i < consts.DefaultBitowerNum; i++ {
				bitowerMeta[i].NextFileNum = FileNum(2)
				bitowerMeta[i].MinUnflushedLogNum = FileNum(1)
			}

			var keyList [][]byte

			writeBitower := func(bdb *DB, index int) {
				if hasWal {
					bitowerMeta[index].MinUnflushedLogNum++
					bitowerMeta[index].NextFileNum++
				}

				writeData := func(start, end int) {
					for i := start; i < end; i++ {
						key := makeTestIntKey(i)
						keyList = append(keyList, key)
						require.NoError(t, bdb.Set(key, val, NoSync))
						seqNum++
					}
					bitower := bdb.bitowers[index]
					require.NoError(t, bitower.Flush())

					bitowerMeta[index].MinUnflushedLogNum++
					bitowerMeta[index].NextFileNum++

					require.Equal(t, seqNum, bdb.meta.atomic.logSeqNum)
					require.Equal(t, bitowerMeta[index].MinUnflushedLogNum, bitower.mu.metaEdit.MinUnflushedLogNum)
					require.Equal(t, bitowerMeta[index].NextFileNum, bitower.mu.metaEdit.NextFileNum)
					require.Equal(t, seqNum, bdb.meta.atomic.logSeqNum)
					require.Equal(t, false, bdb.isFlushedBitable())
				}

				for i := 1; i <= 3; i++ {
					writeData(startNum, endNum)
					startNum = endNum
					endNum += stepNum
				}
			}

			for i := 0; i < 3; i++ {
				db := openTestDB(dirname, nil)
				for i2 := range db.bitowers {
					writeBitower(db, i2)
				}

				require.NoError(t, db.Close())

				if i == 0 {
					hasWal = true
				}
			}

			db := openTestDB(dirname, nil)
			for i := range keyList {
				require.NoError(t, verifyGet(db, keyList[i], val))
			}
			require.NoError(t, db.Close())
		})
	}
}

func TestWriteCloseRead(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	num := 1000
	testBitalosdbWrite(t, num, false)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	keyIndex := 0
	defaultVal := defaultLargeValBytes
	for keyIndex < num {
		keyIndex++
		key := makeTestIntKey(keyIndex)
		require.NoError(t, verifyGet(db, key, defaultVal))
	}
}

func TestWriteDelete(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	val := testRandBytes(1024)

	for i := 0; i < 10; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, val, NoSync))
	}

	for i := 0; i < 10; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, verifyGet(db, key, val))
	}

	for i := 0; i < 5; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Delete(key, NoSync))
	}

	for i := 0; i < 5; i++ {
		key := makeTestIntKey(i)
		_, _, err := db.Get(key)
		require.Equal(t, ErrNotFound, err)
	}

	for i := 5; i < 10; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, verifyGet(db, key, val))
	}
}

func TestLogSeqNum(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	batch := db.NewBatchBitower()
	key0 := makeTestSlotKey([]byte("key_0"))
	key1 := makeTestSlotKey([]byte("key_1"))
	val1 := makeTestSlotKey([]byte("val_1"))
	key2 := makeTestSlotKey([]byte("key_2"))
	val2 := makeTestSlotKey([]byte("val_2"))
	require.NoError(t, batch.Set(key1, val1, NoSync))
	require.NoError(t, batch.Set(key2, val2, NoSync))
	require.NoError(t, batch.Commit(NoSync))
	require.NoError(t, batch.Close())
	require.NoError(t, db.Flush())

	require.NoError(t, verifyGetNotFound(db, key0))
	require.NoError(t, verifyGet(db, key1, val1))

	batch = db.NewBatchBitower()
	val1_1 := makeTestSlotKey([]byte("val_1_1"))
	require.NoError(t, batch.Set(key1, val1_1, NoSync))
	require.NoError(t, batch.Delete(key2, NoSync))
	require.NoError(t, batch.Commit(NoSync))
	require.NoError(t, batch.Close())

	require.NoError(t, verifyGet(db, key1, val1_1))
	require.NoError(t, verifyGetNotFound(db, key2))

	iterOpts := &IterOptions{
		SlotId: uint32(testSlotId),
	}
	it := db.NewIter(iterOpts)
	it.First()
	require.Equal(t, true, it.Valid())
	require.Equal(t, key1, it.Key())
	require.Equal(t, val1_1, it.Value())
	it.Next()
	require.Equal(t, false, it.Valid())
	require.NoError(t, it.Close())
}

func TestSetKeyMultiValue(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	key := makeTestSlotKey([]byte(fmt.Sprintf("key_1")))
	for j := 0; j < 100; j++ {
		v := []byte(fmt.Sprintf("%d", j))
		require.NoError(t, db.Set(key, v, NoSync))
	}

	require.NoError(t, verifyGet(db, key, []byte("99")))
	require.NoError(t, db.Flush())
	require.NoError(t, verifyGet(db, key, []byte("99")))
}

func TestBatchSetMulti(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	num := 100
	kvList := make(map[string][]byte, num)
	for i := 0; i < num; i++ {
		b := db.NewBatchBitower()
		key := makeTestSlotIntKey(i)
		if i%2 == 0 {
			val := testRandBytes(100)
			_ = b.Set(key, val, NoSync)
			kvList[unsafe2.String(key)] = val
		} else {
			val1 := testRandBytes(100)
			val2 := testRandBytes(100)
			_ = b.SetMultiValue(key, val1, val2)
			var val []byte
			val = append(val, val1...)
			val = append(val, val2...)
			kvList[unsafe2.String(key)] = val
		}
		require.NoError(t, b.Commit(NoSync))
		require.NoError(t, b.Close())
	}

	for i := 0; i < 100; i++ {
		key := makeTestSlotIntKey(i)
		require.NoError(t, verifyGet(db, key, kvList[string(key)]))
	}
}

func TestLargeBatch(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	num := 100
	kvList := make(map[string][]byte, num)
	for i := 0; i < 20; i++ {
		b := db.NewBatchBitower()
		key := makeTestSlotIntKey(i)
		val := testRandBytes(100)
		_ = b.Set(key, val, NoSync)
		kvList[unsafe2.String(key)] = val
		require.NoError(t, b.Commit(NoSync))
		require.NoError(t, b.Close())
	}

	b := db.NewBatchBitower()
	for i := 20; i < num; i++ {
		key := makeTestSlotIntKey(i)
		val := testRandBytes(1 << 20)
		_ = b.Set(key, val, NoSync)
		kvList[unsafe2.String(key)] = val
	}
	require.NoError(t, b.Commit(NoSync))
	require.NoError(t, b.Close())

	for i := 0; i < 100; i++ {
		key := makeTestSlotIntKey(i)
		require.NoError(t, verifyGet(db, key, kvList[string(key)]))
	}
}

func TestBithashWriteRead(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	runNum := 2
	type kvPair struct {
		k, v []byte
	}
	kvList := make([][]kvPair, runNum)

	wrFunc := func(i int) {
		num := 50000
		keyIndex := 0
		var valueSize int
		var isExist bool
		kvList[i] = make([]kvPair, num)
		for keyIndex < num {
			key := makeTestIntKey(keyIndex)
			if keyIndex%2 == 0 {
				valueSize = 1200
			} else {
				valueSize = 5
			}
			val := testRandBytes(valueSize)
			require.NoError(t, db.Set(key, val, NoSync))
			kvList[i][keyIndex] = kvPair{
				k: key,
				v: val,
			}
			keyIndex++
		}

		require.NoError(t, db.Flush())

		keyIndex = 0
		for keyIndex < 100 {
			key := makeTestIntKey(keyIndex)
			require.NoError(t, db.Delete(key, NoSync))
			keyIndex++
		}

		require.NoError(t, db.Flush())

		for j, item := range kvList[i] {
			err := verifyGet(db, item.k, item.v)
			if j < 100 {
				require.Equal(t, ErrNotFound, err)
			} else {
				require.NoError(t, err)
			}

			isExist, err = db.Exist(item.k)
			if j < 100 {
				require.Equal(t, ErrNotFound, err)
				require.Equal(t, false, isExist)
			} else {
				require.NoError(t, err)
				require.Equal(t, true, isExist)
			}
		}
	}

	wrFunc(0)
	require.Equal(t, 25000, db.MetricsInfo().BithashKeyTotal)
	require.Equal(t, 50, db.MetricsInfo().BithashDelKeyTotal)
	time.Sleep(2 * time.Second)
	wrFunc(1)
	require.Equal(t, 50000, db.MetricsInfo().BithashKeyTotal)
	require.Equal(t, 25050, db.MetricsInfo().BithashDelKeyTotal)
	db.compactBithash(db.opts.CompactInfo.DeletePercent)
}

func TestBitpageWriteCloseRead(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	val := testRandBytes(2048)

	writeData := func(d *DB) {
		keyIndex := 0
		for keyIndex < 1000 {
			keyIndex++
			newKey := makeTestIntKey(keyIndex)
			require.NoError(t, d.Set(newKey, val, NoSync))
		}
		require.NoError(t, d.Flush())
	}

	readData := func(d *DB) {
		keyIndex := 0
		for keyIndex < 1000 {
			keyIndex++
			key := makeTestIntKey(keyIndex)
			require.NoError(t, verifyGet(d, key, val))

			isExist, err := d.Exist(key)
			require.NoError(t, err)
			require.Equal(t, true, isExist)
		}
	}

	testOptsUsePrefixCompress = false
	testOptsUseBlockCompress = false
	db := openTestDB(testDirname, nil)
	writeData(db)
	time.Sleep(2 * time.Second)
	readData(db)
	require.NoError(t, db.Close())

	testOptsUsePrefixCompress = true
	testOptsUseBlockCompress = true
	db = openTestDB(testDirname, nil)
	writeData(db)
	time.Sleep(2 * time.Second)
	readData(db)
	require.NoError(t, db.Close())
}

func TestCacheSetGet(t *testing.T) {
	for _, cacheType := range []int{consts.CacheTypeLru, consts.CacheTypeLfu} {
		t.Run(fmt.Sprintf("cacheType:%d", cacheType), func(t *testing.T) {
			defer os.RemoveAll(testDirname)
			os.RemoveAll(testDirname)
			testOptsCacheType = cacheType
			testOptsCacheSize = testCacheSize
			db := openTestDB(testDirname, nil)
			num := 1000
			keyIndex := 0
			val := testRandBytes(2048)
			for keyIndex < num {
				key := makeTestIntKey(keyIndex)
				require.NoError(t, db.Set(key, val, NoSync))
				keyIndex++
			}
			require.NoError(t, db.Flush())

			keyIndex = 0
			for keyIndex < 10 {
				key := makeTestIntKey(keyIndex)
				require.NoError(t, db.Delete(key, NoSync))
				keyIndex++
			}
			require.NoError(t, db.Flush())

			keyIndex = 0
			for keyIndex < num {
				key := makeTestIntKey(keyIndex)
				err := verifyGet(db, key, val)
				cacheVal, cacheCloser, cacheFound := db.cache.Get(key, db.cache.GetKeyHash(key))
				if keyIndex < 10 {
					require.Equal(t, ErrNotFound, err)
					require.Equal(t, false, cacheFound)
				} else {
					require.NoError(t, err)
					require.Equal(t, true, cacheFound)
					require.Equal(t, cacheVal, val)
				}
				keyIndex++
				if cacheCloser != nil {
					cacheCloser()
				}
			}
			require.NoError(t, db.Close())
		})
	}
}

func TestMemFlushCheckExpire(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	const randLen = 20
	const valLen = randLen + 9
	valBytes := testRandBytes(valLen)
	now := uint64(time.Now().UnixMilli())
	makeValue := func(i int) []byte {
		val := make([]byte, valLen, valLen)
		ttl := now
		if i%2 == 0 {
			ttl += 20000
		}
		val[0] = 1
		binary.BigEndian.PutUint64(val[1:9], ttl)
		copy(val[9:], valBytes)
		return val[:]
	}
	makeKey := func(i int) []byte {
		keyVersion := uint64(i/100 + 100)
		key := []byte(fmt.Sprintf("key_%d", i))
		return makeTestVersionKey(key, uint16(keyVersion), keyVersion)
	}

	count := 10000

	for i := 0; i < count; i++ {
		key := makeKey(i)
		value := makeValue(i)
		require.NoError(t, db.Set(key, value, NoSync))
	}

	time.Sleep(time.Second)
	require.NoError(t, db.Flush())

	for i := 0; i < count; i++ {
		key := makeKey(i)
		value := makeValue(i)
		err := verifyGet(db, key, value)
		if i%2 == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, ErrNotFound, err)
		}
	}
}

func TestMemFlushPrefixDeleteKey(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	value := testRandBytes(100)
	makeKey := func(i int) []byte {
		keyVersion := uint64(i/100 + 100)
		key := []byte(fmt.Sprintf("key_%d", i))
		return makeTestVersionKey(key, uint16(keyVersion), keyVersion)
	}

	count := 1000
	pdVersions := []uint64{102, 104, 106, 108, 110}
	isPdVersion := func(n uint64) bool {
		for i := range pdVersions {
			if pdVersions[i] == n {
				return true
			}
		}
		return false
	}

	for i := 0; i < count; i++ {
		key := makeKey(i)
		require.NoError(t, db.Set(key, value, NoSync))
	}

	for _, ver := range pdVersions {
		key := makeTestVersionKey(nil, uint16(ver), ver)
		require.NoError(t, db.PrefixDeleteKeySet(key, NoSync))
	}

	require.NoError(t, db.Flush())

	for i := 0; i < count; i++ {
		key := makeKey(i)
		err := verifyGet(db, key, value)
		version := db.opts.KeyPrefixDeleteFunc(key)
		if isPdVersion(version) {
			require.Equal(t, ErrNotFound, err)
		} else {
			require.NoError(t, err)
		}
	}

	for _, ver := range pdVersions {
		key := makeTestVersionKey(nil, uint16(ver), ver)
		_, vcloser, err := db.Get(key)
		require.NoError(t, err)
		if vcloser == nil {
			t.Fatalf("delete version key vcloser is nil version:%d key:%s", ver, string(key))
		}
	}
}

func TestBitableCheckExpire(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	testOptsUseBitable = true
	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	valLen := 20 + 9
	valBytes := testRandBytes(valLen)
	now := uint64(time.Now().UnixMilli())
	count := 10000
	makeKey := func(i int) []byte {
		keyVersion := uint64(i/100 + 100)
		key := []byte(fmt.Sprintf("key_%d", i))
		return makeTestVersionKey(key, uint16(keyVersion), keyVersion)
	}
	makeValue := func(i int) []byte {
		val := make([]byte, valLen)
		ttl := now
		if i%5 == 0 {
			ttl = now + 3000
		} else {
			ttl = now + 30000
		}
		val[0] = 1
		binary.BigEndian.PutUint64(val[1:9], ttl)
		copy(val[9:], valBytes)
		return val[:]
	}

	writeData := func() {
		for i := 0; i < count; i++ {
			key := makeKey(i)
			value := makeValue(i)
			require.NoError(t, db.Set(key, value, NoSync))
		}
		require.NoError(t, db.Flush())
	}

	readData := func() {
		for i := 0; i < count; i++ {
			key := makeKey(i)
			value := makeValue(i)
			require.NoError(t, verifyGet(db, key, value))
		}
	}

	readDeleteKV := func(jobId int) {
		fmt.Println("readDeleteKV", jobId)
		for i := 0; i < count; i++ {
			key := makeKey(i)
			value := makeValue(i)

			isExist := true
			if jobId == 2 && i%5 == 0 {
				isExist = false
			}

			v, vcloser, err := db.Get(key)
			if isExist {
				require.NoError(t, err)
				require.Equal(t, value, v)
				vcloser()
			} else {
				if err != ErrNotFound || len(v) > 0 {
					t.Fatal("find expire key", string(key))
				}
			}
		}
	}

	writeData()
	readData()
	db.compactToBitable(1)
	readData()
	readDeleteKV(1)
	writeData()
	time.Sleep(3 * time.Second)
	db.compactToBitable(1)
	readDeleteKV(2)
}
