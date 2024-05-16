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

package lfucache

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/cache/lrucache"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

func testNewCache() *LfuCache {
	opts := &options.CacheOptions{
		Size:   1 << 20,
		Shards: 64,
	}
	mc := NewLfuCache(opts)
	return mc
}

func TestFreq(t *testing.T) {
	opts := &options.CacheOptions{
		Size:   1 << 20,
		Shards: 2,
	}
	mc := NewLfuCache(opts)

	value := []byte("mcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_valuemcache_value")

	key := []byte("mcache")
	khash := mc.GetKeyHash(key)
	for i := 0; i < 500; i++ {
		mc.Set(key, value, khash)
		mc.Get(key, khash)
	}

	time.Sleep(5 * time.Second)

	fmt.Printf("----------------------------------------compact----------------------------------------\n")

	for i := 0; i < 500; i++ {
		k := []byte(fmt.Sprintf("xxx_%d", i))
		mc.Set(k, value, mc.GetKeyHash(k))
	}

	time.Sleep(10 * time.Second)

	mc.Get(key, khash)
}

func TestLfucache_Shards(t *testing.T) {
	for i := -1; i < 12; i++ {
		opts := &options.CacheOptions{
			Size:   10 << 20,
			Shards: 2,
			Logger: base.DefaultLogger,
		}
		mc := NewLfuCache(opts)
		fmt.Println(i, mc.shardNum)
		mc.Close()
	}
}

func Test_GetIter_VS_GET(t *testing.T) {
	opts := &options.CacheOptions{
		Size:   512 << 20,
		Shards: 2,
		Logger: base.DefaultLogger,
	}
	lc := NewLfuCache(opts)

	startNum := 0
	totalNum := 1 << 20
	totalNum_total := totalNum

	khashs := make([]uint32, totalNum_total+1, totalNum_total+1)
	value := []byte("v")

	bt := time.Now()
	for i := startNum; i <= totalNum_total; i++ {
		key := []byte(fmt.Sprintf("lfucache_key_%d", i))
		khash := hash.Crc32(key)
		khashs[i] = khash
		if i <= totalNum {
			lc.bucketByHash(khash).set(key, value)
		}
	}
	et := time.Since(bt)
	fmt.Printf("build lfucache time cost = %v\n", et)

	printMemStats()

	bt = time.Now()
	for i := startNum; i <= totalNum_total; i++ {
		key := []byte(fmt.Sprintf("lfucache_key_%d", i))
		khash := khashs[i]
		val, closer, found := lc.bucketByHash(khash).getIter(key)
		if found && bytes.Equal(val, value) {
			closer()
		} else if i <= totalNum {
			fmt.Printf("lfucache get by iter fail, key=%s, val=%s\n", key, val)
		}
	}
	et = time.Since(bt)
	fmt.Printf("get by iter lfucache time cost = %v\n", et)

	bt = time.Now()
	for i := startNum; i <= totalNum_total; i++ {
		key := []byte(fmt.Sprintf("lfucache_key_%d", i))
		khash := khashs[i]
		val, closer, found := lc.bucketByHash(khash).get(key)
		if found && bytes.Equal(val, value) {
			closer()
		} else if i <= totalNum {
			fmt.Printf("lfucache get fail, key=%s, val=%s\n", key, val)
		}
	}
	et = time.Since(bt)
	fmt.Printf("get lfucache time cost = %v\n", et)

	bt = time.Now()
	for i := startNum; i <= totalNum_total; i++ {
		key := []byte(fmt.Sprintf("lfucache_key_%d", i))
		khash := khashs[i]
		found := lc.bucketByHash(khash).exist(key)
		if !found && i <= totalNum {
			fmt.Printf("lfucache exist fail, key=%s\n", key)
		}
	}
	et = time.Since(bt)
	fmt.Printf("exist lfucache time cost = %v\n", et)
}

func Test_LFU_VS_LRU(t *testing.T) {
	startNum := 0
	totalNum := 1 << 20
	totalNum_total := totalNum

	opts := &options.CacheOptions{
		Size:     512 << 20,
		Shards:   64,
		HashSize: totalNum,
		Logger:   base.DefaultLogger,
	}
	lfu := NewLfuCache(opts)
	lru := lrucache.New(opts)

	value := []byte("v")

	bt := time.Now()
	for i := startNum; i <= totalNum_total; i++ {
		key := []byte(fmt.Sprintf("lfucache_key_%d", i))
		khash := hash.Crc32(key)
		if i <= totalNum {
			lfu.bucketByHash(khash).set(key, value)
		}
	}
	et := time.Since(bt)
	fmt.Printf("build lfucache time cost = %v\n", et)

	printMemStats()

	bt = time.Now()
	for i := startNum; i <= totalNum_total; i++ {
		key := []byte(fmt.Sprintf("lfucache_key_%d", i))
		if i <= totalNum {
			lru.Set(key, value, lru.GetKeyHash(key))
		}
	}
	et = time.Since(bt)
	fmt.Printf("build lrucache time cost = %v\n", et)

	printMemStats()

	bt = time.Now()
	for i := startNum; i <= totalNum_total; i++ {
		key := []byte(fmt.Sprintf("lfucache_key_%d", i))
		khash := hash.Crc32(key)
		val, closer, found := lfu.bucketByHash(khash).get(key)
		if found && bytes.Equal(val, value) {
			closer()
		} else if i <= totalNum {
			fmt.Printf("lfucache get fail, key=%s, val=%s\n", key, val)
		}
	}
	et = time.Since(bt)
	fmt.Printf("get lfucache time cost = %v\n", et)

	bt = time.Now()
	for i := startNum; i <= totalNum_total; i++ {
		key := []byte(fmt.Sprintf("lfucache_key_%d", i))
		val, vcloser, vexist := lru.Get(key, lru.GetKeyHash(key))
		if vexist && bytes.Equal(val, value) {
			vcloser()
		} else if i <= totalNum {
			fmt.Printf("lrucache get fail, key=%s\n", key)
		}
	}
	et = time.Since(bt)
	fmt.Printf("get lrucache time cost = %v\n", et)
}

const MB = 1024 * 1024

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Map MEM: Alloc=%vMB; TotalAlloc=%vMB; SYS=%vMB; Mallocs=%v; Frees=%v; HeapAlloc=%vMB; HeapSys=%vMB; HeapIdle=%vMB; HeapReleased=%vMB; GCSys=%vMB; NextGC=%vMB; NumGC=%v; NumForcedGC=%v\n",
		m.Alloc/MB, m.TotalAlloc/MB, m.Sys/MB, m.Mallocs, m.Frees, m.HeapAlloc/MB, m.HeapSys/MB, m.HeapIdle/MB, m.HeapReleased/MB,
		m.GCSys/MB, m.NextGC/MB, m.NumGC, m.NumForcedGC)
}
