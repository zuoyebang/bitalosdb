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

package looptest

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/errors/oserror"
	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

var (
	testOptsDisableWAL        = false
	testOptsUseMapIndex       = false
	testOptsUsePrefixCompress = false
	testOptsUseBlockCompress  = false
	testOptsUseBitable        = false
	testOptsCacheType         = consts.CacheTypeLru
	testOptsCacheSize         = 0
	testLoopRunTime           = 300
)

var defaultLargeVal = utils.FuncRandBytes(1024)
var defaultSmallVal = utils.FuncRandBytes(128)

var testOptsParams = [][]bool{
	{true, true, false, false, false},
	{true, true, false, true, false},
	{true, true, true, false, false},
	{true, true, true, true, false},
	{false, true, false, false, true},
	{false, true, false, true, true},
	{false, true, true, false, true},
	{false, true, true, true, true},
}

type BitalosDB struct {
	db   *bitalosdb.DB
	ro   *bitalosdb.IterOptions
	wo   *bitalosdb.WriteOptions
	opts *bitalosdb.Options
}

func openTestBitalosDB(dir string) (*BitalosDB, error) {
	compactInfo := bitalosdb.CompactEnv{
		StartHour:     0,
		EndHour:       23,
		DeletePercent: 0.2,
		BitreeMaxSize: 2 << 30,
		Interval:      60,
	}
	opts := &bitalosdb.Options{
		BytesPerSync:                consts.DefaultBytesPerSync,
		MemTableSize:                1 << 30,
		MemTableStopWritesThreshold: 8,
		CacheType:                   testOptsCacheType,
		CacheSize:                   int64(testOptsCacheSize),
		CacheHashSize:               10000,
		Verbose:                     true,
		CompactInfo:                 compactInfo,
		Logger:                      bitalosdb.DefaultLogger,
		UseBithash:                  true,
		UseBitable:                  testOptsUseBitable,
		UseMapIndex:                 testOptsUseMapIndex,
		UsePrefixCompress:           testOptsUsePrefixCompress,
		UseBlockCompress:            testOptsUseBlockCompress,
		DisableWAL:                  testOptsDisableWAL,
		KeyHashFunc:                 options.TestKeyHashFunc,
		KeyPrefixDeleteFunc:         options.TestKeyPrefixDeleteFunc,
	}
	_, err := os.Stat(dir)
	if oserror.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0775); err != nil {
			return nil, err
		}
	}

	pdb, err := bitalosdb.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &BitalosDB{
		db:   pdb,
		ro:   &bitalosdb.IterOptions{},
		wo:   bitalosdb.NoSync,
		opts: opts,
	}, nil
}

func testGetKeyIndex(key []byte) int {
	sk := strings.Split(string(key), "_")
	index, _ := strconv.Atoi(sk[len(sk)-1])
	return index
}

func testMakeIndexKey(i int, kprefix string) (key []byte) {
	key = sortedkv.MakeKey([]byte(fmt.Sprintf("%skey_%d", kprefix, i)))
	return key
}

func testMakeIndexVal(i int, key []byte, vprefix string) (val []byte) {
	if i%2 == 0 {
		val = []byte(fmt.Sprintf("%s_%s%s", key, defaultLargeVal, vprefix))
	} else {
		val = []byte(fmt.Sprintf("%s_%s%s", key, defaultSmallVal, vprefix))
	}

	return val
}

func testWriteKVRange(t *testing.T, db *bitalosdb.DB, start, end int, kprefix, vprefix string) {
	for i := start; i <= end; i++ {
		newKey := testMakeIndexKey(i, kprefix)
		newValue := testMakeIndexVal(i, newKey, vprefix)
		if err := db.Set(newKey, newValue, bitalosdb.NoSync); err != nil {
			t.Fatal("set err", string(newKey), err)
		}
	}
}

func testDeleteKVRange(t *testing.T, db *bitalosdb.DB, start, end, gap int, kprefix string) {
	for i := start; i <= end; i++ {
		if i%gap != 0 {
			continue
		}
		newKey := testMakeIndexKey(i, kprefix)
		if err := db.Delete(newKey, bitalosdb.NoSync); err != nil {
			t.Fatal("delete err", string(newKey), err)
		}
	}
}

func testBitalosdbLoop(t *testing.T, dir string) {
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	bitalosDB, err := openTestBitalosDB(dir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	closeCh := make(chan struct{})
	wKeyIndex := uint32(0)
	rKeyIndex := uint32(0)
	step := uint32(100000)
	deleteGap := 9
	runTime := time.Duration(testLoopRunTime) * time.Second
	readConcurrency := 2
	wdConcurrency := 2

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		defer func() {
			wg.Done()
			fmt.Println("write goroutine exit...")
		}()

		wd := func() {
			wIndex := atomic.AddUint32(&wKeyIndex, step)
			start := wIndex - step + 1
			end := wIndex
			testWriteKVRange(t, bitalosDB.db, int(start), int(end), "", "")
			testDeleteKVRange(t, bitalosDB.db, int(start), int(end), deleteGap, "")
			rIndex := atomic.LoadUint32(&rKeyIndex)
			if rIndex < start {
				atomic.CompareAndSwapUint32(&rKeyIndex, rIndex, start)
			}
		}

		wwg := sync.WaitGroup{}
		for i := 0; i < wdConcurrency; i++ {
			wwg.Add(1)
			go func(index int) {
				defer wwg.Done()
				for {
					select {
					case <-closeCh:
						return
					default:
						wd()
					}
				}
			}(i)
		}
		wwg.Wait()
	}()

	go func() {
		defer func() {
			defer wg.Done()
			fmt.Println("compact goroutine exit...")
		}()
		job := 1
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-closeCh:
				return
			case <-ticker.C:
				bitalosDB.db.CheckAndCompact(job)
				bitalosDB.db.SetCheckpointHighPriority(true)
				time.Sleep(2 * time.Second)
				bitalosDB.db.SetCheckpointHighPriority(false)
				job++
			}
		}
	}()

	go func() {
		var readIterCount atomic.Uint64
		defer func() {
			defer wg.Done()
			fmt.Println("iter read goroutine exit readIterCount=", readIterCount.Load())
		}()
		rwg := sync.WaitGroup{}
		readIter := func() {
			readIterCount.Add(1)
			ms := time.Duration(rand.Intn(200) + 1)
			time.Sleep(ms * time.Millisecond)
			it := bitalosDB.db.NewIter(nil)
			defer it.Close()
			isEmpty := true
			for it.First(); it.Valid(); it.Next() {
				if isEmpty {
					isEmpty = false
				}
				index := testGetKeyIndex(it.Key())
				if index%deleteGap == 0 {
					continue
				}
				newValue := testMakeIndexVal(index, it.Key(), "")
				if !bytes.Equal(newValue, it.Value()) {
					t.Errorf("readIter fail key:%s newValue:%s v:%s", string(it.Key()), string(newValue), string(it.Value()))
				}
			}
			if isEmpty {
				t.Log("readIter isEmpty")
			}
		}
		for i := 0; i < readConcurrency; i++ {
			rwg.Add(1)
			go func(index int) {
				defer rwg.Done()
				time.Sleep(5 * time.Second)
				for {
					select {
					case <-closeCh:
						return
					default:
						readIter()
					}
				}
			}(i)
		}
		rwg.Wait()
	}()

	go func() {
		var readCount atomic.Uint64
		defer func() {
			defer wg.Done()
			fmt.Println("get read goroutine exit readCount=", readCount.Load())
		}()
		rgwg := sync.WaitGroup{}
		readGet := func() {
			ms := time.Duration(rand.Intn(10) + 1)
			time.Sleep(ms * time.Millisecond)
			keyIndex := atomic.LoadUint32(&rKeyIndex)
			if keyIndex <= step+1 {
				time.Sleep(1 * time.Second)
				return
			}
			readCount.Add(1)
			kindex := rand.Intn(int(keyIndex-step-1)) + 1
			newKey := testMakeIndexKey(kindex, "")
			newValue := testMakeIndexVal(kindex, newKey, "")
			v, closer, err := bitalosDB.db.Get(newKey)
			if kindex%deleteGap == 0 {
				if err != bitalosdb.ErrNotFound {
					t.Errorf("get deleteKey fail key:%s keyIndex:%d err:%v\n", string(newKey), keyIndex, err)
				}
			} else {
				if err != nil {
					t.Errorf("readGet existKey fail key:%s keyIndex:%d err:%v\n", string(newKey), keyIndex, err)
				} else if !bytes.Equal(newValue, v) {
					t.Errorf("get existKey fail val not eq key:%s keyIndex:%d newValue:%s v:%s\n", string(newKey), keyIndex, string(newValue), string(v))
				}
			}
			if closer != nil {
				closer()
			}
		}
		for i := 0; i < readConcurrency; i++ {
			rgwg.Add(1)
			go func(index int) {
				defer rgwg.Done()
				time.Sleep(5 * time.Second)
				for {
					select {
					case <-closeCh:
						return
					default:
						readGet()
					}
				}
			}(i)
		}
		rgwg.Wait()
	}()

	time.Sleep(runTime)
	close(closeCh)
	wg.Wait()
}

func TestBitalosdbLoop1(t *testing.T) {
	dir := "data_1_1"
	for _, params := range testOptsParams {
		testOptsDisableWAL = params[0]
		testOptsUseMapIndex = params[1]
		testOptsUsePrefixCompress = params[2]
		testOptsUseBlockCompress = params[3]
		testOptsUseBitable = params[4]
		testOptsCacheType = 0
		testOptsCacheSize = 0
		fmt.Println("TestBitalosdbLoop1 params=", params)
		testBitalosdbLoop(t, dir)
	}
}

func TestBitalosdbLoop2(t *testing.T) {
	dir := "data_1_2"
	for _, params := range testOptsParams {
		testOptsDisableWAL = params[0]
		testOptsUseMapIndex = params[1]
		testOptsUsePrefixCompress = params[2]
		testOptsUseBlockCompress = params[3]
		testOptsUseBitable = params[4]
		testOptsCacheType = consts.CacheTypeLru
		testOptsCacheSize = 1 << 30
		fmt.Println("TestBitalosdbLoop2 params=", params)
		testBitalosdbLoop(t, dir)
	}
}
