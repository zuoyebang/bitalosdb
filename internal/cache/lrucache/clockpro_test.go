// Copyright 2018. All rights reserved. Use of this source code is governed by
// an MIT-style license that can be found in the LICENSE file.

package lrucache

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"golang.org/x/exp/rand"
)

func testNewShards() *LruCache {
	opts := &base.CacheOptions{
		Size:     100,
		Shards:   1,
		HashSize: 1024,
	}
	cache := newShards(opts)
	return cache
}

func TestCache(t *testing.T) {
	// Test data was generated from the python code
	f, err := os.Open("testdata/cache")
	require.NoError(t, err)

	opts := &base.CacheOptions{
		Size:     200,
		Shards:   1,
		HashSize: 1024,
	}
	cache := newShards(opts)
	defer cache.Unref()

	scanner := bufio.NewScanner(f)
	line := 1

	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())

		key, err := strconv.Atoi(string(fields[0]))
		require.NoError(t, err)

		wantHit := fields[1][0] == 'h'

		var hit bool
		h := cache.getValue(1, uint64(key))
		if v := h.Get(); v == nil {
			value := cache.Alloc(1)
			value.Buf()[0] = fields[0][0]
			cache.setValue(1, uint64(key), value).Release()
		} else {
			hit = true
			if !bytes.Equal(v, fields[0][:1]) {
				t.Errorf("%d: cache returned bad data: got %s , want %s\n", line, v, fields[0][:1])
			}
		}
		h.Release()
		if hit != wantHit {
			t.Errorf("%d: cache hit mismatch: got %v, want %v\n", line, hit, wantHit)
		}
		line++
	}
}

func testValue(cache *LruCache, s string, repeat int) *Value {
	b := bytes.Repeat([]byte(s), repeat)
	v := cache.Alloc(len(b))
	copy(v.Buf(), b)
	return v
}

func TestCacheDelete(t *testing.T) {
	cache := testNewShards()
	defer cache.Unref()

	cache.setValue(1, 0, testValue(cache, "a", 5)).Release()
	cache.setValue(1, 1, testValue(cache, "a", 5)).Release()
	cache.setValue(1, 2, testValue(cache, "a", 5)).Release()
	if expected, size := int64(15), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.del(1, 1)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	if h := cache.getValue(1, 0); h.Get() == nil {
		t.Fatalf("expected to find block 0/0")
	} else {
		h.Release()
	}
	if h := cache.getValue(1, 1); h.Get() != nil {
		t.Fatalf("expected to not find block 1/0")
	} else {
		h.Release()
	}
	// Deleting a non-existing block does nothing.
	cache.del(1, 1)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
}

func TestEvictAll(t *testing.T) {
	// Verify that it is okay to evict all of the data from a cache. Previously
	// this would trigger a nil-pointer dereference.
	cache := testNewShards()
	defer cache.Unref()

	cache.setValue(1, 0, testValue(cache, "a", 101)).Release()
	cache.setValue(1, 1, testValue(cache, "a", 101)).Release()
}

func TestMultipleDBs(t *testing.T) {
	cache := testNewShards()
	defer cache.Unref()

	cache.setValue(1, 0, testValue(cache, "a", 5)).Release()
	cache.setValue(2, 0, testValue(cache, "b", 5)).Release()
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	h := cache.getValue(1, 0)
	if v := h.Get(); string(v) != "aaaaa" {
		t.Fatalf("expected aaaaa, but found %s", v)
	}
	h = cache.getValue(2, 0)
	if v := h.Get(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %s", v)
	} else {
		h.Release()
	}
}

func TestZeroSize(t *testing.T) {
	opts := &base.CacheOptions{
		Size:     0,
		Shards:   1,
		HashSize: 1024,
	}
	cache := newShards(opts)
	defer cache.Unref()

	cache.setValue(1, 0, testValue(cache, "a", 5)).Release()
}

func TestReserve(t *testing.T) {
	opts := &base.CacheOptions{
		Size:     4,
		Shards:   2,
		HashSize: 1024,
	}
	cache := newShards(opts)
	defer cache.Unref()

	cache.setValue(1, 1, testValue(cache, "a", 1)).Release()
	cache.setValue(1, 2, testValue(cache, "a", 1)).Release()
	require.EqualValues(t, 2, cache.Size())
	r := cache.Reserve(1)
	require.EqualValues(t, 0, cache.Size())
	cache.setValue(1, 1, testValue(cache, "a", 1)).Release()
	cache.setValue(1, 2, testValue(cache, "a", 1)).Release()
	cache.setValue(1, 3, testValue(cache, "a", 1)).Release()
	cache.setValue(1, 4, testValue(cache, "a", 1)).Release()
	require.EqualValues(t, 2, cache.Size())
	r()
	require.EqualValues(t, 2, cache.Size())
	cache.setValue(1, 1, testValue(cache, "a", 1)).Release()
	cache.setValue(1, 2, testValue(cache, "a", 1)).Release()
	require.EqualValues(t, 4, cache.Size())
}

func TestReserveDoubleRelease(t *testing.T) {
	cache := testNewShards()
	defer cache.Unref()

	r := cache.Reserve(10)
	r()

	result := func() (result string) {
		defer func() {
			if v := recover(); v != nil {
				result = fmt.Sprint(v)
			}
		}()
		r()
		return ""
	}()
	const expected = "cache: cache reservation already released"
	if expected != result {
		t.Fatalf("expected %q, but found %q", expected, result)
	}
}

func TestCacheStressSetExisting(t *testing.T) {
	opts := &base.CacheOptions{
		Size:     1,
		Shards:   1,
		HashSize: 1024,
	}
	cache := newShards(opts)
	defer cache.Unref()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				cache.setValue(1, uint64(i), testValue(cache, "a", 1)).Release()
				runtime.Gosched()
			}
		}(i)
	}
	wg.Wait()
}

func TestCacheSetGet(t *testing.T) {
	const size = 100000

	opts := &base.CacheOptions{
		Size:     size,
		Shards:   1,
		HashSize: 1024,
	}
	cache := newShards(opts)
	defer cache.Unref()

	for i := 0; i < size; i++ {
		v := testValue(cache, "a", 1)
		cache.setValue(1, uint64(i), v).Release()
	}

	for i := 0; i < size; i++ {
		h := cache.getValue(1, uint64(i))
		if h.Get() == nil {
			t.Fatalf("failed to lookup value key=%d", i)
		}
		h.Release()
	}
}

func BenchmarkCacheGet(b *testing.B) {
	const size = 100000

	opts := &base.CacheOptions{
		Size:     size,
		Shards:   1,
		HashSize: 1024,
	}
	cache := newShards(opts)
	defer cache.Unref()

	for i := 0; i < size; i++ {
		v := testValue(cache, "a", 1)
		cache.setValue(1, uint64(i), v).Release()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

		for pb.Next() {
			h := cache.getValue(1, uint64(rng.Intn(size)))
			if h.Get() == nil {
				b.Fatal("failed to lookup value")
			}
			h.Release()
		}
	})
}

func TestReserveColdTarget(t *testing.T) {
	// If coldTarget isn't updated when we call shard.Reserve,
	// then we unnecessarily remove nodes from the
	// cache.
	cache := testNewShards()
	defer cache.Unref()

	for i := 0; i < 50; i++ {
		cache.setValue(uint64(i+1), 0, testValue(cache, "a", 1)).Release()
	}

	if cache.Size() != 50 {
		require.Equal(t, 50, cache.Size(), "nodes were unnecessarily evicted from the cache")
	}

	// There won't be enough space left for 50 nodes in the cache after
	// we call shard.Reserve. This should trigger a call to evict.
	cache.Reserve(51)

	// If we don't update coldTarget in Reserve then the cache gets emptied to
	// size 0. In shard.Evict, we loop until shard.Size() < shard.targetSize().
	// Therefore, 100 - 51 = 49, but we evict one more node.
	if cache.Size() != 48 {
		t.Fatalf("expected positive cache size %d, but found %d", 48, cache.Size())
	}
}
