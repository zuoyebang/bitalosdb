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
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

var testOptsParams = [][]bool{
	{true, true, false, false},
	{true, true, false, true},
	{true, true, true, false},
	{true, true, true, true},
	{false, true, false, false},
	{false, true, false, true},
	{false, true, true, false},
	{false, true, true, true},
}

func testMakeWdcSameKey(i int, kprefix string) []byte {
	return testMakeSameKey([]byte(fmt.Sprintf("key_%d_%s", i, kprefix)))
}

func testWriteSameKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := testMakeWdcSameKey(i, kprefix)
		newValue := []byte(fmt.Sprintf("%s_%s_%s", newKey, defaultValBytes, vprefix))
		if err := db.Set(newKey, newValue, NoSync); err != nil {
			t.Fatal("set err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func testDeleteSameKV(t *testing.T, db *DB, num int, kprefix string) {
	for i := 1; i <= num; i++ {
		if i%2 == 0 {
			continue
		}
		newKey := testMakeWdcSameKey(i, kprefix)
		if err := db.Delete(newKey, NoSync); err != nil {
			t.Fatal("delete err", string(newKey), err)
		}
	}
	require.NoError(t, db.Flush())
}

func testReadSameKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := testMakeWdcSameKey(i, kprefix)
		newValue := []byte(fmt.Sprintf("%s_%s_%s", newKey, defaultValBytes, vprefix))
		v, closer, err := db.Get(newKey)
		require.NoError(t, err)
		require.Equal(t, newValue, v)
		closer()
		isExist, err1 := db.Exist(newKey)
		require.NoError(t, err1)
		require.Equal(t, true, isExist)
	}
}

func testReadDeleteSameKV(t *testing.T, db *DB, num int, kprefix, vprefix string) {
	for i := 1; i <= num; i++ {
		newKey := testMakeWdcSameKey(i, kprefix)
		v, closer, err := db.Get(newKey)
		if i%2 == 0 {
			require.NoError(t, err)
			newValue := []byte(fmt.Sprintf("%s_%s_%s", newKey, defaultValBytes, vprefix))
			require.Equal(t, newValue, v)
		} else if err != ErrNotFound {
			t.Fatalf("testReadDeleteSameKV get find delete key:%s", newKey)
		}
		if closer != nil {
			closer()
		}

		isExist, err1 := db.Exist(newKey)
		if i%2 == 0 {
			require.NoError(t, err1)
			require.Equal(t, true, isExist)
		} else if err1 != ErrNotFound || isExist {
			t.Fatalf("testReadDeleteSameKV exist find delete key:%s", newKey)
		}
	}
}

func testBitalosdbBitableLoop(t *testing.T, dir string) {
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	require.NoError(t, os.RemoveAll(dir))

	wdc := func() {
		testOptsMemtableSize = testMemTableSize
		testOptsUseBitable = true
		bitalosDB, err := testOpenBitalosDB(dir)
		require.NoError(t, err)

		closeCh := make(chan struct{})
		wKeyIndex := uint32(0)
		rKeyIndex := uint32(0)
		step := uint32(100000)
		deleteGap := 4
		runTime := 600 * time.Second
		readConcurrency := 5
		wdConcurrency := 2

		wg := sync.WaitGroup{}
		wg.Add(4)
		go func() {
			defer wg.Done()

			wd := func() {
				wIndex := atomic.AddUint32(&wKeyIndex, step)
				start := wIndex - step + 1
				end := wIndex
				testWriteKVRange(t, bitalosDB.db, int(start), int(end), "", "")
				bdbDeleteKVRange(t, bitalosDB.db, int(start), int(end), deleteGap, "")
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
					time.Sleep(5 * time.Second)
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
			defer wg.Done()
			job := 1
			for {
				select {
				case <-closeCh:
					return
				default:
					time.Sleep(20 * time.Second)
					bitalosDB.db.CheckAndCompact(job)
					bitalosDB.db.SetCheckpointHighPriority(true)
					time.Sleep(3 * time.Second)
					bitalosDB.db.SetCheckpointHighPriority(false)
					job++
				}
			}
		}()

		go func() {
			defer wg.Done()
			rwg := sync.WaitGroup{}
			readIter := func() {
				ms := time.Duration(rand.Intn(200) + 1)
				time.Sleep(ms * time.Millisecond)
				it := bitalosDB.db.NewIter(nil)
				defer it.Close()
				for it.First(); it.Valid(); it.Next() {
					index := testGetKeyIndex(it.Key())
					if index%deleteGap == 0 {
						continue
					}
					newValue := testMakeIndexVal(index, it.Key(), "")
					if !bytes.Equal(newValue, it.Value()) {
						t.Errorf("readIter fail key:%s newValue:%s v:%s", string(it.Key()), string(newValue), string(it.Value()))
					}
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
			defer wg.Done()
			rgwg := sync.WaitGroup{}
			readGet := func() {
				ms := time.Duration(rand.Intn(10) + 1)
				time.Sleep(ms * time.Millisecond)
				keyIndex := atomic.LoadUint32(&rKeyIndex)
				if keyIndex <= step+1 {
					time.Sleep(1 * time.Second)
					return
				}
				kindex := rand.Intn(int(keyIndex-step-1)) + 1
				newKey := testMakeIndexKey(kindex, "")
				newValue := testMakeIndexVal(kindex, newKey, "")
				v, closer, err := bitalosDB.db.Get(newKey)
				if kindex%deleteGap == 0 {
					if err != ErrNotFound {
						t.Errorf("readGet deleteKey fail key:%s keyIndex:%d err:%v\n", string(newKey), keyIndex, err)
					}
				} else {
					if err != nil {
						t.Errorf("readGet existKey fail key:%s keyIndex:%d err:%v\n", string(newKey), keyIndex, err)
					} else if !bytes.Equal(newValue, v) {
						t.Errorf("readGet existKey fail val not eq key:%s keyIndex:%d newValue:%s v:%s\n", string(newKey), keyIndex, string(newValue), string(v))
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

		require.NoError(t, bitalosDB.db.Close())
	}

	wdc()
}

func TestBitalosdbBitableLoop1(t *testing.T) {
	dir := "data_1_1"
	for _, params := range testOptsParams {
		testOptsDisableWAL = params[0]
		testOptsUseMapIndex = params[1]
		testOptsUsePrefixCompress = params[2]
		testOptsUseBlockCompress = params[3]
		testOptsCacheType = 0
		testOptsCacheSize = 0
		testBitalosdbBitableLoop(t, dir)
	}
}

func TestBitalosdbBitableLoop2(t *testing.T) {
	dir := "data_1_2"
	for _, params := range testOptsParams {
		testOptsDisableWAL = params[0]
		testOptsUseMapIndex = params[1]
		testOptsUsePrefixCompress = params[2]
		testOptsUseBlockCompress = params[3]
		testOptsCacheType = consts.CacheTypeLru
		testOptsCacheSize = 1 << 30
		testBitalosdbBitableLoop(t, dir)
	}
}

func TestBitalosdbBitableLoop3(t *testing.T) {
	dir := "data_1_3"
	for _, params := range testOptsParams {
		testOptsDisableWAL = params[0]
		testOptsUseMapIndex = params[1]
		testOptsUsePrefixCompress = params[2]
		testOptsUseBlockCompress = params[3]
		testOptsCacheType = consts.CacheTypeLfu
		testOptsCacheSize = 1 << 30
		testBitalosdbBitableLoop(t, dir)
	}
}

func testBitalosdbLoop(t *testing.T, dir string) {
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	require.NoError(t, os.RemoveAll(dir))

	wdc := func() {
		testOptsMemtableSize = testMemTableSize
		testOptsUseBitable = false
		bitalosDB, err := testOpenBitalosDB(dir)
		require.NoError(t, err)
		var writeIndex atomic.Uint64
		num := 100000

		closeCh := make(chan struct{})
		wg := sync.WaitGroup{}
		wg.Add(3)
		go func() {
			defer wg.Done()
			n := 1
			for {
				select {
				case <-closeCh:
					return
				default:
					testWriteSameKV(t, bitalosDB.db, num, strconv.Itoa(n), "")

					if n%2 == 0 {
						testDeleteSameKV(t, bitalosDB.db, num, strconv.Itoa(n))
					}
				}
				writeIndex.Store(uint64(n))
				n++
			}
		}()

		go func() {
			defer wg.Done()
			job := 1
			for {
				select {
				case <-closeCh:
					return
				default:
					time.Sleep(10 * time.Second)
					bitalosDB.db.bf.CompactBitree(job)
					job++
					bitalosDB.db.bf.CompactBithash(job, bitalosDB.db.opts.CompactInfo.DeletePercent)
					job++
				}
			}
		}()

		go func() {
			defer wg.Done()
			rwg := sync.WaitGroup{}

			for i := 0; i < 2; i++ {
				rwg.Add(1)
				go func(index int) {
					defer rwg.Done()
					for {
						select {
						case <-closeCh:
							return
						default:
							wi := int(writeIndex.Load())
							if wi < 3 {
								time.Sleep(2 * time.Second)
								continue
							}
							ri := rand.Intn(wi-2) + 1
							if ri%2 == 0 {
								testReadDeleteSameKV(t, bitalosDB.db, num, strconv.Itoa(ri), "")
							} else {
								testReadSameKV(t, bitalosDB.db, num, strconv.Itoa(ri), "")
							}
						}
					}
				}(i)
			}
			rwg.Wait()
		}()

		time.Sleep(600 * time.Second)
		close(closeCh)
		wg.Wait()

		require.NoError(t, bitalosDB.db.Close())
	}

	wdc()
}

func TestBitalosdbLoop1(t *testing.T) {
	dir := "data_1"
	for _, params := range testOptsParams {
		testOptsDisableWAL = params[0]
		testOptsUseMapIndex = params[1]
		testOptsUsePrefixCompress = params[2]
		testOptsUseBlockCompress = params[3]
		testOptsCacheType = 0
		testOptsCacheSize = 0
		fmt.Println("TestBitalosdbLoop1", params)
		testBitalosdbLoop(t, dir)
	}
}

func TestBitalosdbLoop2(t *testing.T) {
	dir := "data_2"
	for _, params := range testOptsParams {
		testOptsDisableWAL = params[0]
		testOptsUseMapIndex = params[1]
		testOptsUsePrefixCompress = params[2]
		testOptsUseBlockCompress = params[3]
		testOptsCacheType = consts.CacheTypeLru
		testOptsCacheSize = 1 << 30
		fmt.Println("TestBitalosdbLoop2", params)
		testBitalosdbLoop(t, dir)
	}
}

func TestBitalosdbLoop3(t *testing.T) {
	dir := "data_3"
	for _, params := range testOptsParams {
		testOptsDisableWAL = params[0]
		testOptsUseMapIndex = params[1]
		testOptsUsePrefixCompress = params[2]
		testOptsUseBlockCompress = params[3]
		testOptsCacheType = consts.CacheTypeLfu
		testOptsCacheSize = 1 << 30
		fmt.Println("TestBitalosdbLoop3", params)
		testBitalosdbLoop(t, dir)
	}
}
