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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

const (
	testDirname              = "./test-data"
	testMaxLogFileSize       = 256 << 20
	testMemTableSize         = 256 << 20
	testMaxWriteBufferNumber = 8
	testCacheSize            = 256 << 20
	testSlotId               = uint32(1)

	defaultValBytes = "1qaz2wsx3edc4rfv5tgb6yhn7ujm8ik9ol0p1qaz2wsx3edc4rfv5tgb6yhn7ujm8ik9ol0p1qaz2wsx3edc4rfv5tgb6yhn7ujm8ik9ol0p"
)

var (
	testOptsCompressType      int   = compress.CompressTypeNo
	testOptsDisableWAL        bool  = false
	testOptsUseMapIndex       bool  = false
	testOptsUsePrefixCompress bool  = false
	testOptsUseBlockCompress  bool  = false
	testOptsCacheType         int   = consts.CacheTypeLru
	testOptsCacheSize         int64 = 0
	testOptsMemtableSize      int   = testMemTableSize
	testOptsUseBitable        bool  = false
)

var defaultLargeValBytes []byte

type BitalosDB struct {
	db   *DB
	ro   *IterOptions
	wo   *WriteOptions
	opts *Options
}

func openBitalosDB(dir string, cacheSize int64) (*BitalosDB, error) {
	testOptsMemtableSize = testMemTableSize
	testOptsCacheType = consts.CacheTypeLru
	testOptsCacheSize = cacheSize
	return testOpenBitalosDB(dir)
}

func openBitalosDBByMemsize(dir string, memSize int) (*BitalosDB, error) {
	testOptsMemtableSize = memSize
	testOptsCacheType = consts.CacheTypeLru
	testOptsCacheSize = 0
	return testOpenBitalosDB(dir)
}

func openBitalosDBByCache(dir string, cacheType int, cacheSize int64) (*BitalosDB, error) {
	testOptsMemtableSize = testMemTableSize
	testOptsCacheType = cacheType
	testOptsCacheSize = cacheSize
	return testOpenBitalosDB(dir)
}

func testOpenBitalosDB(dir string) (*BitalosDB, error) {
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
		KeyHashFunc:                 base.DefaultKeyHashFunc,
		DisableWAL:                  testOptsDisableWAL,
	}
	_, err := os.Stat(dir)
	if oserror.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0775); err != nil {
			return nil, err
		}
	}

	pdb, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &BitalosDB{
		db:   pdb,
		ro:   &IterOptions{},
		wo:   NoSync,
		opts: opts,
	}, nil
}

func testRandBytes(n int) []byte {
	return utils.FuncRandBytes(n)
}

func testMakeKey(key []byte) []byte {
	return utils.FuncMakeKey(key)
}

func testMakeSameKey(key []byte) []byte {
	return utils.FuncMakeSameKey(key, uint16(testSlotId))
}

func testMakeKey2(key []byte, slotId uint16, version uint64) []byte {
	return utils.FuncMakeKey2(key, slotId, version)
}

func testBitalosdbWrite(t *testing.T, num int32, isSame bool) {
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	keyIndex := int32(0)
	defaultLargeValBytes = testRandBytes(2048)
	val := defaultLargeValBytes

	for keyIndex < num {
		keyIndex++
		var newKey []byte
		if isSame {
			newKey = testMakeSameKey([]byte(fmt.Sprintf("key_%d", keyIndex)))
		} else {
			newKey = testMakeKey([]byte(fmt.Sprintf("key_%d", keyIndex)))
		}
		if err = bitalosDB.db.Set(newKey, val, bitalosDB.wo); err != nil {
			t.Error("set err:", err)
		}
	}

	require.NoError(t, bitalosDB.db.Flush())
	require.NoError(t, bitalosDB.db.Close())
}

func testBitalosdbRead(t *testing.T, num int32, isSame bool) {
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	keyIndex := int32(0)
	defaultVal := defaultLargeValBytes

	for keyIndex < num {
		keyIndex++
		var newKey []byte
		key := fmt.Sprintf("key_%d", keyIndex)
		if isSame {
			newKey = testMakeSameKey([]byte(key))
		} else {
			newKey = testMakeKey([]byte(key))
		}
		val, closer, err := bitalosDB.db.Get(newKey)
		if err != nil {
			t.Error("get err", err, key)
		} else if len(val) <= 0 {
			t.Error("get val len err")
		} else if !bytes.Equal(defaultVal, val) {
			t.Error("get val err", key)
		}
		if closer != nil {
			closer()
		}

		isExist, err := bitalosDB.db.Exist(newKey)
		if err != nil {
			t.Error("exist err", err, key)
		} else if !isExist {
			t.Error("exist key not found", key)
		}
	}

	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbWriteEncode(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	keyIndex := int32(64710980)
	val := testRandBytes(2048)

	num := 1000

	encodeKVKey := func(key []byte) []byte {
		ek := make([]byte, len(key)+4)
		khash := hash.Fnv32(key)
		binary.BigEndian.PutUint32(ek, khash%1024)
		copy(ek[4:], key)
		return ek
	}

	for i := 0; i < num; i++ {
		keyIndex++
		newKey := []byte(fmt.Sprintf("di_%d", keyIndex))
		key := encodeKVKey(newKey)
		if err = bitalosDB.db.Set(key, val, bitalosDB.wo); err != nil {
			t.Error("set err:", err)
		}
	}

	require.NoError(t, bitalosDB.db.Flush())
	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbWrite(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	writeData := func(bdb *BitalosDB, n int) {
		for i := 0; i < n; i++ {
			key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
			val := testRandBytes(2048)
			require.NoError(t, bdb.db.Set(key, val, bdb.wo))
		}
		require.NoError(t, bdb.db.Flush())
	}

	num := 10000
	writeData(bitalosDB, num)
	require.Equal(t, FileNum(2), bitalosDB.db.mu.meta.minUnflushedLogNum)
	require.Equal(t, FileNum(3), bitalosDB.db.mu.meta.nextFileNum)
	require.Equal(t, uint64(num+1), bitalosDB.db.mu.meta.atomic.logSeqNum)
	require.Equal(t, false, bitalosDB.db.bf.IsFlushedBitable())
	require.NoError(t, bitalosDB.db.Close())

	fn := 3
	for i := 1; i <= 3; i++ {
		bitalosDB, err = openBitalosDB(testDirname, testCacheSize)
		require.NoError(t, err)
		require.Equal(t, FileNum(fn), bitalosDB.db.mu.meta.minUnflushedLogNum)
		require.Equal(t, FileNum(fn+1), bitalosDB.db.mu.meta.nextFileNum)

		add := 50
		writeData(bitalosDB, add)
		fn++
		require.Equal(t, FileNum(fn), bitalosDB.db.mu.meta.minUnflushedLogNum)
		require.Equal(t, FileNum(fn+1), bitalosDB.db.mu.meta.nextFileNum)
		writeData(bitalosDB, add)
		fn++
		require.Equal(t, FileNum(fn), bitalosDB.db.mu.meta.minUnflushedLogNum)
		require.Equal(t, FileNum(fn+1), bitalosDB.db.mu.meta.nextFileNum)
		require.Equal(t, uint64(num+add*2*i+1), bitalosDB.db.mu.meta.atomic.logSeqNum)
		require.Equal(t, false, bitalosDB.db.bf.IsFlushedBitable())
		require.NoError(t, bitalosDB.db.Close())
		fn++
	}
}

func TestBitalosdbWriteRead(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	testBitalosdbWrite(t, 1000, false)
	testBitalosdbRead(t, 1000, false)
}

func TestBitalosdbOpen(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	batch := newBatch(bitalosDB.db)
	require.NoError(t, batch.Set([]byte("key"), []byte("value"), bitalosDB.wo))
	require.NoError(t, batch.Commit(bitalosDB.wo))

	readKey := func() {
		v, closer, err := bitalosDB.db.Get([]byte("key"))
		require.NoError(t, err)
		require.Equal(t, []byte("value"), v)
		if closer != nil {
			closer()
		}
	}

	readKey()
	require.NoError(t, bitalosDB.db.Close())

	bitalosDB, err = openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)
	readKey()
	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbDeleteGet(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	val := []byte(defaultValBytes)

	batch := newBatch(bitalosDB.db)
	for i := 0; i < 10; i++ {
		key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		require.NoError(t, batch.Set(key, val, bitalosDB.wo))
	}
	require.NoError(t, batch.Commit(bitalosDB.wo))
	batch.Reset()

	for i := 0; i < 10; i++ {
		key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		v, closer, err := bitalosDB.db.Get(key)
		if err != nil || !bytes.Equal(v, val) {
			t.Fatal("get exist key fail ", err, string(key))
		}
		if closer != nil {
			closer()
		}
	}

	for i := 0; i < 5; i++ {
		key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		require.NoError(t, batch.Delete(key, bitalosDB.wo))
	}
	require.NoError(t, batch.Commit(bitalosDB.wo))
	require.NoError(t, batch.Close())

	for i := 0; i < 5; i++ {
		key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		_, _, err = bitalosDB.db.Get(key)
		if err != ErrNotFound {
			t.Fatal("get delete key find ", string(key))
		}
	}

	for i := 5; i < 10; i++ {
		key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		v, closer, err := bitalosDB.db.Get(key)
		if err != nil || !bytes.Equal(v, val) {
			t.Fatal("get exist key fail ", err, string(key))
		}
		closer()
	}

	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbDeleteIterator(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()
	val := []byte(defaultValBytes)

	batch := bitalosDB.db.NewBatch()
	for i := 0; i < 100; i++ {
		key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		require.NoError(t, batch.Set(key, val, bitalosDB.wo))
	}
	require.NoError(t, batch.Commit(bitalosDB.wo))
	require.NoError(t, batch.Close())

	iterOpts := &IterOptions{
		LowerBound: testMakeKey([]byte("key_1")),
		IsAll:      true,
	}
	it := bitalosDB.db.NewIter(iterOpts)
	defer it.Close()
	wg := sync.WaitGroup{}
	for it.SeekGE(iterOpts.LowerBound); it.Valid(); it.Next() {
		key := make([]byte, len(it.Key()))
		copy(key, it.Key())
		indexStr := strings.Split(string(key), "_")[1]
		index, _ := strconv.Atoi(indexStr)
		if index >= 20 && index <= 40 {
			wg.Add(1)
			go func(k []byte) {
				defer wg.Done()
				b := bitalosDB.db.NewBatch()
				require.NoError(t, b.Delete(k, bitalosDB.wo))
				require.NoError(t, b.Commit(bitalosDB.wo))
				require.NoError(t, b.Close())
				_, _, err = bitalosDB.db.Get(k)
				if err != ErrNotFound {
					t.Fatal("get delete key find ", string(k))
				}
			}(key)
		}
	}
	wg.Wait()
}

func TestBitalosdbMemIterator(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	for i := 0; i < 100; i++ {
		newKey := testMakeKey([]byte(fmt.Sprintf("total_%d", i)))
		for j := 0; j < 100; j++ {
			err = bitalosDB.db.Set(newKey, []byte(fmt.Sprintf("%d", j)), bitalosDB.wo)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	iterOpts := &IterOptions{
		IsAll: true,
	}
	it := bitalosDB.db.NewIter(iterOpts)
	defer it.Close()
	it.First()
	fmt.Println("it.First() end")
	for ; it.Valid(); it.Next() {
		fmt.Println("iter", string(it.Key()), string(it.Value()))
	}
	stats := it.Stats()
	fmt.Printf("stats: %s\n", stats.String())
}

func TestBitalosdbMemGet(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	newKey := testMakeKey([]byte(fmt.Sprintf("total_1")))
	for j := 0; j < 100; j++ {
		err = bitalosDB.db.Set(newKey, []byte(fmt.Sprintf("%d", j)), bitalosDB.wo)
		if err != nil {
			t.Fatal(err)
		}
	}

	bitalosDB.db.Flush()

	val, vcloser, err := bitalosDB.db.Get(newKey)
	require.NoError(t, err)
	require.Equal(t, []byte(fmt.Sprintf("%d", 99)), val)
	vcloser()
}

func TestBitalosdbSeqNum(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	batch := bitalosDB.db.NewBatch()
	key0 := testMakeKey([]byte("key_0"))
	key1 := testMakeKey([]byte("key_1"))
	val1 := testMakeKey([]byte("val_1"))
	key2 := testMakeKey([]byte("key_2"))
	val2 := testMakeKey([]byte("val_2"))
	require.NoError(t, batch.Set(key1, val1, bitalosDB.wo))
	require.NoError(t, batch.Set(key2, val2, bitalosDB.wo))
	require.NoError(t, batch.Commit(bitalosDB.wo))
	require.NoError(t, batch.Close())
	require.NoError(t, bitalosDB.db.Flush())

	v, closer, err := bitalosDB.db.Get(key0)
	if err != ErrNotFound {
		t.Fatalf("0 get key0 got %s, want nil", string(v))
	}
	if closer != nil {
		t.Fatal("closer is not nil")
	}

	v, closer, err = bitalosDB.db.Get(key1)
	require.Equal(t, val1, v)
	closer()

	batch = bitalosDB.db.NewBatch()
	val1_1 := testMakeKey([]byte("val_1_1"))
	require.NoError(t, batch.Set(key1, val1_1, bitalosDB.wo))
	require.NoError(t, batch.Delete(key2, bitalosDB.wo))
	require.NoError(t, batch.Commit(bitalosDB.wo))
	require.NoError(t, batch.Close())

	v, closer, err = bitalosDB.db.Get(key1)
	require.Equal(t, val1_1, v)
	closer()

	_, _, err = bitalosDB.db.Get(key2)
	if err != ErrNotFound {
		t.Fatalf("3 get key2 got %s, want nil", string(v))
	}

	iterOpts := &IterOptions{
		IsAll: true,
	}
	it := bitalosDB.db.NewIter(iterOpts)
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		fmt.Println("iter", string(it.Key()), string(it.Value()))
	}
}

func TestBitalosdbWriteKvSeparate(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	type kvPair struct {
		k, v []byte
	}

	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	if err != nil {
		require.NoError(t, err)
	}
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	runNum := 2
	kvList := make([][]kvPair, runNum)

	wrFunc := func(i int) {
		num := int32(50000)
		keyIndex := int32(0)
		var valueSize int

		kvList[i] = make([]kvPair, num)

		for keyIndex < num {
			newKey := testMakeKey([]byte(fmt.Sprintf("key_%d", keyIndex)))
			if keyIndex%2 == 0 {
				valueSize = 1200
			} else {
				valueSize = 5
			}
			val := testRandBytes(valueSize)
			if err = bitalosDB.db.Set(newKey, val, bitalosDB.wo); err != nil {
				require.NoError(t, err)
			}
			kvList[i][keyIndex] = kvPair{
				k: newKey,
				v: val,
			}

			keyIndex++
		}

		require.NoError(t, bitalosDB.db.Flush())

		keyIndex = int32(0)
		for keyIndex < 100 {
			newKey := testMakeKey([]byte(fmt.Sprintf("key_%d", keyIndex)))
			if err = bitalosDB.db.Delete(newKey, bitalosDB.wo); err != nil {
				require.NoError(t, err)
			}
			keyIndex++
		}

		require.NoError(t, bitalosDB.db.Flush())

		for j, item := range kvList[i] {
			val, closer, err := bitalosDB.db.Get(item.k)
			if j < 100 {
				require.Equal(t, ErrNotFound, err)
			} else {
				if err != nil {
					t.Errorf("key:%s get err:%v", string(item.k), err)
				} else if len(val) <= 0 {
					t.Errorf("key:%s get val len err len:%d", string(item.k), len(val))
				} else if !bytes.Equal(item.v, val) {
					t.Errorf("key:%s get val err, len:%d check:%v", string(item.k), len(val), bytes.Equal(kvList[0][j].v, val))
				}
				if closer != nil {
					closer()
				}
			}

			isExist, err := bitalosDB.db.Exist(item.k)
			if j < 100 {
				require.Equal(t, false, isExist)
				require.Equal(t, ErrNotFound, err)
			} else {
				if err != nil {
					t.Errorf("key:%s exist err:%v", string(item.k), err)
				} else if !isExist {
					t.Errorf("key:%s exist key not found", string(item.k))
				}
			}
		}
	}

	wrFunc(0)
	if bitalosDB.db.opts.UseBithash {
		require.Equal(t, 25000, bitalosDB.db.ForestInfo().BithashKeyTotal)
		require.Equal(t, 50, bitalosDB.db.ForestInfo().BithashDelKeyTotal)
	}

	time.Sleep(2 * time.Second)
	wrFunc(1)
	if bitalosDB.db.opts.UseBithash {
		require.Equal(t, 50000, bitalosDB.db.ForestInfo().BithashKeyTotal)
		require.Equal(t, 25050, bitalosDB.db.ForestInfo().BithashDelKeyTotal)

		bitalosDB.db.bf.CompactBithash(1, bitalosDB.db.opts.CompactInfo.DeletePercent)
	}

	dinfo := bitalosDB.db.DebugInfo()
	fmt.Println(dinfo)
}

func TestBitalosdbBatchSetMulti(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	kvList := make(map[string][]byte, 0)

	for i := 0; i < 100; i++ {
		b := bitalosDB.db.NewBatch()
		key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		if i%2 == 0 {
			val := testRandBytes(100)
			_ = b.Set(key, val, bitalosDB.wo)
			kvList[string(key)] = val
		} else {
			val1 := testRandBytes(100)
			val2 := testRandBytes(100)
			_ = b.SetMultiValue(key, val1, val2)
			var val []byte
			val = append(val, val1...)
			val = append(val, val2...)
			kvList[string(key)] = val
		}
		require.NoError(t, b.Commit(bitalosDB.wo))
		require.NoError(t, b.Close())
	}

	for i := 0; i < 100; i++ {
		key := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		v, vcloser, err := bitalosDB.db.Get(key)
		require.NoError(t, err)
		require.Equal(t, kvList[string(key)], v)
		vcloser()
	}

	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbWriteByKeyHashPos(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDB(testDirname, testCacheSize)
	require.NoError(t, err)

	keyIndex := int32(0)
	defaultLargeValBytes = testRandBytes(2048)
	val := defaultLargeValBytes

	timestamp := time.Now().Unix()
	for keyIndex < 10000 {
		key := []byte(fmt.Sprintf("key_%d", keyIndex))
		newkey := make([]byte, len(key)+8)
		binary.BigEndian.PutUint64(newkey[0:8], uint64(timestamp))
		copy(newkey[8:], key)
		if err = bitalosDB.db.Set(newkey, val, bitalosDB.wo); err != nil {
			t.Error("set err:", err)
		}
		timestamp++
		keyIndex++
	}

	require.NoError(t, bitalosDB.db.Flush())
	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbFlushByDelPercent(t *testing.T) {
	defer os.RemoveAll(testDirname)
	bitalosDB, err := openBitalosDBByMemsize(testDirname, 3<<20)
	require.NoError(t, err)

	defaultLargeValBytes = testRandBytes(2048)
	val := defaultLargeValBytes

	for i := 0; i < 1000; i++ {
		newKey := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		if err = bitalosDB.db.Set(newKey, val, bitalosDB.wo); err != nil {
			t.Error("set err:", err)
		}
		if i > 400 {
			if err = bitalosDB.db.Delete(newKey, bitalosDB.wo); err != nil {
				t.Error("delete err:", err)
			}
			if err = bitalosDB.db.Delete(newKey, bitalosDB.wo); err != nil {
				t.Error("delete err:", err)
			}
		}
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < 1000; i++ {
		newKey := testMakeKey([]byte(fmt.Sprintf("key_%d", i)))
		v, closer, e := bitalosDB.db.Get(newKey)
		if i > 400 {
			require.Equal(t, ErrNotFound, e)
		} else {
			require.Equal(t, val, v)
			closer()
		}
	}

	require.NoError(t, bitalosDB.db.Close())
}

func TestBitalosdbBitpageWriteRead(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)

	openDb := func(sw bool) *BitalosDB {
		opts := &Options{
			MemTableSize:                testMemTableSize,
			MemTableStopWritesThreshold: testMaxWriteBufferNumber,
			Verbose:                     true,
			LogTag:                      "[bitalosdb/test]",
			Logger:                      DefaultLogger,
			DataType:                    "string",
			UseBithash:                  true,
			UseMapIndex:                 true,
			UsePrefixCompress:           sw,
			UseBlockCompress:            sw,
			KeyHashFunc:                 base.DefaultKeyHashFunc,
			DisableWAL:                  true,
		}
		_, err := os.Stat(dir)
		if nil != err && !os.IsExist(err) {
			err = os.MkdirAll(dir, 0775)
			require.NoError(t, err)
			opts.WALDir = ""
		}
		pdb, err := Open(dir, opts)
		require.NoError(t, err)
		pdb.optspool.BaseOptions.BitpageFlushSize = 1
		return &BitalosDB{
			db:   pdb,
			ro:   &IterOptions{},
			wo:   NoSync,
			opts: opts,
		}
	}

	writeData := func(d *DB) {
		keyIndex := int32(0)
		defaultLargeValBytes = testRandBytes(2048)
		val := defaultLargeValBytes

		for keyIndex < 1000 {
			keyIndex++
			newKey := testMakeKey([]byte(fmt.Sprintf("key_%d", keyIndex)))
			if err := d.Set(newKey, val, NoSync); err != nil {
				t.Error("set err:", err)
			}
		}

		require.NoError(t, d.Flush())
	}

	readData := func(d *DB) {
		keyIndex := int32(0)
		defaultVal := defaultLargeValBytes

		for keyIndex < 1000 {
			keyIndex++
			var newKey []byte
			key := fmt.Sprintf("key_%d", keyIndex)
			newKey = testMakeKey([]byte(key))
			val, closer, err := d.Get(newKey)
			if err != nil {
				t.Error("get err", err, key)
			} else if len(val) <= 0 {
				t.Error("get val len err")
			} else if !bytes.Equal(defaultVal, val) {
				t.Error("get val err", key)
			}
			if closer != nil {
				closer()
			}

			isExist, err := d.Exist(newKey)
			if err != nil {
				t.Error("exist err", err, key)
			} else if !isExist {
				t.Error("exist key not found", key)
			}
		}
	}

	bitalosDB := openDb(false)
	writeData(bitalosDB.db)
	time.Sleep(2 * time.Second)
	readData(bitalosDB.db)
	require.NoError(t, bitalosDB.db.Close())
	bitalosDB = openDb(true)
	writeData(bitalosDB.db)
	time.Sleep(2 * time.Second)
	readData(bitalosDB.db)
	require.NoError(t, bitalosDB.db.Close())
}
