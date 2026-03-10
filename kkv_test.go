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
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/bitree"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/unsafe2"
	"github.com/stretchr/testify/require"
)

func TestKKVWriteReadConcurrency(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	opts := &Options{
		LogTag:              "[test]",
		Logger:              DefaultLogger,
		VectorTableCount:    1,
		VectorTableHashSize: 1 << 20,
		GetNowTimestamp:     options.DefaultGetNowTimestamp,
		MemTableSize:        1 << 30,
		VmTableSize:         1 << 30,
		BitpageFlushSize:    88 << 20,
		BitpageSplitSize:    256 << 20,
		BithashTableSize:    64 << 20,
	}
	require.NoError(t, os.MkdirAll(dir, 0775))
	db, err := Open(dir, opts)
	require.NoError(t, err)

	var wKeyIndex, rKeyIndex, readCount atomic.Uint64
	var wg sync.WaitGroup
	closeCh := make(chan struct{})
	runTime := time.Duration(60) * time.Second
	readConcurrency := 4
	wdConcurrency := 2
	slotNum := uint32(32)
	makeKey := func(i int) []byte {
		return []byte(fmt.Sprintf("key_%d", i))
	}
	makeSubKey := func(i int) []byte {
		return []byte(fmt.Sprintf("key_field_%d", i))
	}
	makeKeySlotId := func(key []byte) uint16 {
		h := hash.Fnv32(key)
		return uint16(h % slotNum)
	}

	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			fmt.Println("write goroutine exit writeCount=", wKeyIndex.Load())
		}()

		var wwg sync.WaitGroup
		for i := 0; i < wdConcurrency; i++ {
			wwg.Add(1)
			go func(index int) {
				defer wwg.Done()
				for {
					select {
					case <-closeCh:
						return
					default:
						var value []byte
						j := int(wKeyIndex.Add(1))
						key := makeKey(j)
						subKey := makeSubKey(j)
						if j%2 == 0 {
							value = []byte(strings.Repeat(unsafe2.String(key), 10))
						} else {
							value = []byte(strings.Repeat(unsafe2.String(key), 100))
						}
						slotId := makeKeySlotId(key)
						n, err := db.HMSet(key, slotId, subKey, value)
						if n != 1 {
							t.Errorf("SetKKV fail n:%d key:%s slotId:%d", n, string(key), slotId)
						}
						require.NoError(t, err)
						ri := rKeyIndex.Add(1)
						if ri%100000 == 0 {
							fmt.Println("store rKeyIndex", rKeyIndex.Load())
						}
						time.Sleep(100 * time.Microsecond)
					}
				}
			}(i)
		}
		wwg.Wait()
	}()

	go func() {
		defer func() {
			defer wg.Done()
			fmt.Println("read goroutine exit readCount=", readCount.Load())
		}()
		var rgwg sync.WaitGroup
		for i := 0; i < readConcurrency; i++ {
			rgwg.Add(1)
			go func(index int) {
				defer rgwg.Done()
				time.Sleep(3 * time.Second)
				for {
					select {
					case <-closeCh:
						return
					default:
						time.Sleep(100 * time.Microsecond)
						keyIndex := rKeyIndex.Load()
						if keyIndex < 100 {
							time.Sleep(1 * time.Second)
							continue
						}
						readCount.Add(1)
						j := rand.Intn(int(keyIndex-100)) + 1
						key := makeKey(j)
						subKey := makeSubKey(j)
						var value []byte
						if j%2 == 0 {
							value = []byte(strings.Repeat(unsafe2.String(key), 10))
						} else {
							value = []byte(strings.Repeat(unsafe2.String(key), 100))
						}
						slotId := makeKeySlotId(key)
						v, closer, err := db.HGet(key, slotId, subKey)
						if err != nil {
							t.Errorf("found key:%d err:%s", j, err)
						} else if !bytes.Equal(value, v) {
							t.Errorf("value not eq key:%d slotId:%d v:%v", j, slotId, v)
						}
						if closer != nil {
							closer()
						}
					}
				}
			}(i)
		}
		rgwg.Wait()
	}()

	time.Sleep(runTime)
	close(closeCh)
	wg.Wait()

	require.NoError(t, db.Close())
}

func TestKKVWriteRead(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	ksize := 10
	vsize := 512
	d := openTestDB1(dir, true)
	keyNum := 10000
	keyList := sortedkv.MakeKKVList(keyNum, 10, DataTypeHash, ksize, vsize)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		kv.Sid = 1
		n, err := d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, len(kv.Kvs)/2, int(n))
		if i%100 == 0 {
			require.NoError(t, d.FlushMemtable())
		}
	}

	time.Sleep(1 * time.Second)
	d.FlushBitpage()
	time.Sleep(3 * time.Second)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, kv.Sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		require.Equal(t, kv.Size, size)

		var subKeys [][]byte
		for j := 0; j < len(kv.Kvs); j += 2 {
			subKeys = append(subKeys, kv.Kvs[j])
			vs, vscloser, vserr := d.HGet(kv.Key, kv.Sid, kv.Kvs[j])
			require.NoError(t, vserr)
			require.Equal(t, kv.Kvs[j+1], vs)
			vscloser()
		}
		subKeys = append(subKeys, nil)
		vs, vscloser, vserr := d.HMGet(kv.Key, kv.Sid, subKeys...)
		require.NoError(t, vserr)
		require.Equal(t, len(subKeys), len(vs))
		require.Equal(t, len(subKeys)-1, len(vscloser))
		for k := 0; k < len(subKeys); k++ {
			if k == len(subKeys)-1 {
				require.Equal(t, []byte(nil), vs[k])
			} else {
				require.Equal(t, kv.Kvs[k*2+1], vs[k])
			}
		}
		for i2 := range vscloser {
			vscloser[i2]()
		}
	}

	require.NoError(t, d.Close())
}

func TestKKVSortedPageSize(t *testing.T) {
	var pages bitree.SortedPageSizeList

	psPnMap := make(map[uint64]uint32)
	psIndexMap := make(map[uint64]int)
	for i := 0; i < 50; i++ {
		ps := bitree.PageSize{
			Index:     int(rand.Int31n(5)),
			Pn:        uint32(rand.Int31n(50)),
			InuseSize: uint64(rand.Int31n(100)*100) + uint64(i),
		}
		psPnMap[ps.InuseSize] = ps.Pn
		psIndexMap[ps.InuseSize] = ps.Index
		pages = append(pages, ps)
	}

	sort.Sort(sort.Reverse(pages))

	for _, page := range pages {
		require.Equal(t, psPnMap[page.InuseSize], page.Pn)
		require.Equal(t, psIndexMap[page.InuseSize], page.Index)
	}
}
