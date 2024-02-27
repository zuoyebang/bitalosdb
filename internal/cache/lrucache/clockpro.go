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

package lrucache

import (
	"bytes"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/invariants"
)

type key struct {
	id     uint64
	offset uint64
}

func (k key) String() string {
	return fmt.Sprintf("%d/%d", k.id, k.offset)
}

type Handle struct {
	value *Value
}

func (h Handle) Get() []byte {
	if h.value != nil {
		return h.value.buf
	}
	return nil
}

func (h Handle) Release() {
	if h.value != nil {
		h.value.release()
	}
}

type shard struct {
	hits         int64
	misses       int64
	mu           sync.RWMutex
	reservedSize int64
	maxSize      int64
	coldTarget   int64
	blocks       robinHoodMap
	entries      map[*entry]struct{}
	handHot      *entry
	handCold     *entry
	handTest     *entry
	sizeHot      int64
	sizeCold     int64
	sizeTest     int64
	countHot     int64
	countCold    int64
	countTest    int64
}

func (c *shard) Get(id uint64, offset uint64) Handle {
	c.mu.RLock()
	var value *Value
	if e := c.blocks.Get(key{id, offset}); e != nil {
		value = e.acquireValue()
		if value != nil {
			atomic.StoreInt32(&e.referenced, 1)
		}
	}
	c.mu.RUnlock()
	if value == nil {
		atomic.AddInt64(&c.misses, 1)
		return Handle{}
	}
	atomic.AddInt64(&c.hits, 1)
	return Handle{value: value}
}

func (c *shard) Set(id uint64, offset uint64, value *Value) Handle {
	if n := value.refs(); n != 1 {
		panic(fmt.Sprintf("cache: Value has already been added to the cache: refs=%d", n))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{id, offset}
	e := c.blocks.Get(k)

	switch {
	case e == nil:
		e = newEntry(c, k, int64(len(value.buf)))
		e.setValue(value)
		if c.metaAdd(k, e) {
			value.ref.trace("add-cold")
			c.sizeCold += e.size
			c.countCold++
		} else {
			value.ref.trace("skip-cold")
			e.free()
			e = nil
		}

	case e.peekValue() != nil:
		e.setValue(value)
		atomic.StoreInt32(&e.referenced, 1)
		delta := int64(len(value.buf)) - e.size
		e.size = int64(len(value.buf))
		if e.ptype == etHot {
			value.ref.trace("add-hot")
			c.sizeHot += delta
		} else {
			value.ref.trace("add-cold")
			c.sizeCold += delta
		}
		c.evict()

	default:
		c.sizeTest -= e.size
		c.countTest--
		c.metaDel(e)
		c.metaCheck(e)

		e.size = int64(len(value.buf))
		c.coldTarget += e.size
		if c.coldTarget > c.targetSize() {
			c.coldTarget = c.targetSize()
		}

		atomic.StoreInt32(&e.referenced, 0)
		e.setValue(value)
		e.ptype = etHot
		if c.metaAdd(k, e) {
			value.ref.trace("add-hot")
			c.sizeHot += e.size
			c.countHot++
		} else {
			value.ref.trace("skip-hot")
			e.free()
			e = nil
		}
	}

	c.checkConsistency()

	return Handle{value: value}
}

func (c *shard) checkConsistency() {
	switch {
	case c.sizeHot < 0 || c.sizeCold < 0 || c.sizeTest < 0 || c.countHot < 0 || c.countCold < 0 || c.countTest < 0:
		panic(fmt.Sprintf("cache: unexpected negative: %d (%d bytes) hot, %d (%d bytes) cold, %d (%d bytes) test",
			c.countHot, c.sizeHot, c.countCold, c.sizeCold, c.countTest, c.sizeTest))
	case c.sizeHot > 0 && c.countHot == 0:
		panic(fmt.Sprintf("cache: mismatch %d hot size, %d hot count", c.sizeHot, c.countHot))
	case c.sizeCold > 0 && c.countCold == 0:
		panic(fmt.Sprintf("cache: mismatch %d cold size, %d cold count", c.sizeCold, c.countCold))
	case c.sizeTest > 0 && c.countTest == 0:
		panic(fmt.Sprintf("cache: mismatch %d test size, %d test count", c.sizeTest, c.countTest))
	}
}

func (c *shard) Delete(id uint64, offset uint64) {
	k := key{id, offset}
	c.mu.RLock()
	exists := c.blocks.Get(k) != nil
	c.mu.RUnlock()
	if !exists {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.blocks.Get(k)
	if e == nil {
		return
	}
	c.metaEvict(e)

	c.checkConsistency()
}

func (c *shard) Free() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.handHot != nil {
		e := c.handHot
		c.metaDel(c.handHot)
		e.free()
	}

	c.blocks.free()
}

func (c *shard) Reserve(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reservedSize += int64(n)

	targetSize := c.targetSize()
	if c.coldTarget > targetSize {
		c.coldTarget = targetSize
	}

	c.evict()
	c.checkConsistency()
}

func (c *shard) Size() int64 {
	c.mu.RLock()
	size := c.sizeHot + c.sizeCold
	c.mu.RUnlock()
	return size
}

func (c *shard) targetSize() int64 {
	target := c.maxSize - c.reservedSize
	if target < 1 {
		return 1
	}
	return target
}

func (c *shard) metaAdd(key key, e *entry) bool {
	c.evict()
	if e.size > c.targetSize() {
		return false
	}

	c.blocks.Put(key, e)
	if entriesGoAllocated {
		c.entries[e] = struct{}{}
	}

	if c.handHot == nil {
		c.handHot = e
		c.handCold = e
		c.handTest = e
	} else {
		c.handHot.link(e)
	}

	if c.handCold == c.handHot {
		c.handCold = c.handCold.prev()
	}

	return true
}

func (c *shard) metaDel(e *entry) {
	if value := e.peekValue(); value != nil {
		value.ref.trace("metaDel")
	}
	e.setValue(nil)

	c.blocks.Delete(e.key)
	if entriesGoAllocated {
		delete(c.entries, e)
	}

	if e == c.handHot {
		c.handHot = c.handHot.prev()
	}
	if e == c.handCold {
		c.handCold = c.handCold.prev()
	}
	if e == c.handTest {
		c.handTest = c.handTest.prev()
	}

	if e.unlink() == e {
		c.handHot = nil
		c.handCold = nil
		c.handTest = nil
	}
}

func (c *shard) metaCheck(e *entry) {
	if invariants.Enabled {
		if _, ok := c.entries[e]; ok {
			fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in entries map\n%s",
				e, e.key, debug.Stack())
			os.Exit(1)
		}
		if c.blocks.findByValue(e) != nil {
			fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in blocks map\n%s\n%s",
				e, e.key, &c.blocks, debug.Stack())
			os.Exit(1)
		}
		var countHot, countCold, countTest int64
		var sizeHot, sizeCold, sizeTest int64
		for t := c.handHot.next(); t != nil; t = t.next() {
			switch t.ptype {
			case etHot:
				countHot++
				sizeHot += t.size
			case etCold:
				countCold++
				sizeCold += t.size
			case etTest:
				countTest++
				sizeTest += t.size
			}
			if e == t {
				fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in blocks list\n%s",
					e, e.key, debug.Stack())
				os.Exit(1)
			}
			if t == c.handHot {
				break
			}
		}
		if countHot != c.countHot || countCold != c.countCold || countTest != c.countTest ||
			sizeHot != c.sizeHot || sizeCold != c.sizeCold || sizeTest != c.sizeTest {
			fmt.Fprintf(os.Stderr, `divergence of Hot,Cold,Test statistics
				cache's statistics: hot %d, %d, cold %d, %d, test %d, %d
				recalculated statistics: hot %d, %d, cold %d, %d, test %d, %d\n%s`,
				c.countHot, c.sizeHot, c.countCold, c.sizeCold, c.countTest, c.sizeTest,
				countHot, sizeHot, countCold, sizeCold, countTest, sizeTest,
				debug.Stack())
			os.Exit(1)
		}
	}
}

func (c *shard) metaEvict(e *entry) {
	switch e.ptype {
	case etHot:
		c.sizeHot -= e.size
		c.countHot--
	case etCold:
		c.sizeCold -= e.size
		c.countCold--
	case etTest:
		c.sizeTest -= e.size
		c.countTest--
	}
	c.metaDel(e)
	c.metaCheck(e)
	e.free()
}

func (c *shard) evict() {
	for c.targetSize() <= c.sizeHot+c.sizeCold && c.handCold != nil {
		c.runHandCold(c.countCold, c.sizeCold)
	}
}

func (c *shard) runHandCold(countColdDebug, sizeColdDebug int64) {
	if c.countCold != countColdDebug || c.sizeCold != sizeColdDebug {
		panic(fmt.Sprintf("runHandCold: cold count and size are %d, %d, arguments are %d and %d",
			c.countCold, c.sizeCold, countColdDebug, sizeColdDebug))
	}

	e := c.handCold
	if e.ptype == etCold {
		if atomic.LoadInt32(&e.referenced) == 1 {
			atomic.StoreInt32(&e.referenced, 0)
			e.ptype = etHot
			c.sizeCold -= e.size
			c.countCold--
			c.sizeHot += e.size
			c.countHot++
		} else {
			e.setValue(nil)
			e.ptype = etTest
			c.sizeCold -= e.size
			c.countCold--
			c.sizeTest += e.size
			c.countTest++
			for c.targetSize() < c.sizeTest && c.handTest != nil {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.next()

	for c.targetSize()-c.coldTarget <= c.sizeHot && c.handHot != nil {
		c.runHandHot()
	}
}

func (c *shard) runHandHot() {
	if c.handHot == c.handTest && c.handTest != nil {
		c.runHandTest()
		if c.handHot == nil {
			return
		}
	}

	e := c.handHot
	if e.ptype == etHot {
		if atomic.LoadInt32(&e.referenced) == 1 {
			atomic.StoreInt32(&e.referenced, 0)
		} else {
			e.ptype = etCold
			c.sizeHot -= e.size
			c.countHot--
			c.sizeCold += e.size
			c.countCold++
		}
	}

	c.handHot = c.handHot.next()
}

func (c *shard) runHandTest() {
	if c.sizeCold > 0 && c.handTest == c.handCold && c.handCold != nil {
		if c.countCold == 0 {
			panic(fmt.Sprintf("cache: mismatch %d cold size, %d cold count", c.sizeCold, c.countCold))
		}

		c.runHandCold(c.countCold, c.sizeCold)
		if c.handTest == nil {
			return
		}
	}

	e := c.handTest
	if e.ptype == etTest {
		c.sizeTest -= e.size
		c.countTest--
		c.coldTarget -= e.size
		if c.coldTarget < 0 {
			c.coldTarget = 0
		}
		c.metaDel(e)
		c.metaCheck(e)
		e.free()
	}

	c.handTest = c.handTest.next()
}

type LruCache struct {
	refs     int64
	maxSize  int64
	idAlloc  uint64
	shards   []shard
	shardCnt uint64
	logger   base.Logger
	tr       struct {
		sync.Mutex
		msgs []string
	}
}

func newShards(opts *base.CacheOptions) *LruCache {
	shardNum := opts.Shards
	c := &LruCache{
		refs:     1,
		maxSize:  opts.Size,
		idAlloc:  1,
		shards:   make([]shard, shardNum),
		shardCnt: uint64(shardNum),
		logger:   opts.Logger,
	}
	c.trace("alloc", c.refs)
	shardSize := c.maxSize / int64(shardNum)
	shardHashSize := opts.HashSize / shardNum
	for i := range c.shards {
		c.shards[i] = shard{
			maxSize:    shardSize,
			coldTarget: shardSize,
		}
		if entriesGoAllocated {
			c.shards[i].entries = make(map[*entry]struct{})
		}
		c.shards[i].blocks.init(shardHashSize)
	}

	invariants.SetFinalizer(c, func(obj interface{}) {
		c := obj.(*LruCache)
		if v := atomic.LoadInt64(&c.refs); v != 0 {
			c.tr.Lock()
			fmt.Fprintf(os.Stderr,
				"cache: cache (%p) has non-zero reference count: %d\n", c, v)
			if len(c.tr.msgs) > 0 {
				fmt.Fprintf(os.Stderr, "%s\n", strings.Join(c.tr.msgs, "\n"))
			}
			c.tr.Unlock()
			os.Exit(1)
		}
	})
	return c
}

func (lrc *LruCache) getShardByHashId(h uint64) *shard {
	return &lrc.shards[h%lrc.shardCnt]
}

func (lrc *LruCache) Ref() {
	v := atomic.AddInt64(&lrc.refs, 1)
	if v <= 1 {
		panic(fmt.Sprintf("cache: inconsistent reference count: %d", v))
	}
	lrc.trace("ref", v)
}

func (lrc *LruCache) Unref() {
	v := atomic.AddInt64(&lrc.refs, -1)
	lrc.trace("unref", v)
	switch {
	case v < 0:
		panic(fmt.Sprintf("cache: inconsistent reference count: %d", v))
	case v == 0:
		for i := range lrc.shards {
			lrc.shards[i].Free()
		}
	}
}

func (lrc *LruCache) getValue(id uint64, offset uint64) Handle {
	return lrc.getShardByHashId(offset).Get(id, offset)
}

func (lrc *LruCache) setValue(id uint64, offset uint64, value *Value) Handle {
	return lrc.getShardByHashId(offset).Set(id, offset, value)
}

func (lrc *LruCache) del(id uint64, offset uint64) {
	lrc.getShardByHashId(offset).Delete(id, offset)
}

func (lrc *LruCache) MaxSize() int64 {
	return lrc.maxSize
}

func (lrc *LruCache) Size() int64 {
	var size int64
	for i := range lrc.shards {
		size += lrc.shards[i].Size()
	}
	return size
}

func (lrc *LruCache) Alloc(n int) *Value {
	return newValue(n)
}

func (lrc *LruCache) Free(v *Value) {
	if n := v.refs(); n > 1 {
		panic(fmt.Sprintf("cache: Value has been added to the cache: refs=%d", n))
	}
	v.release()
}

func (lrc *LruCache) Reserve(n int) func() {
	shardN := (n + len(lrc.shards) - 1) / len(lrc.shards)
	for i := range lrc.shards {
		lrc.shards[i].Reserve(shardN)
	}
	return func() {
		if shardN == -1 {
			panic("cache: cache reservation already released")
		}
		for i := range lrc.shards {
			lrc.shards[i].Reserve(-shardN)
		}
		shardN = -1
	}
}

type Metrics struct {
	Size   int64
	Count  int64
	Hits   int64
	Misses int64

	ShardsMetrics []ShardMetrics
}

type ShardMetrics struct {
	Size  int64
	Count int64
}

func (m Metrics) String() string {
	var shards bytes.Buffer
	for i := range m.ShardsMetrics {
		shards.WriteString(fmt.Sprintf("[%d:%d:%d]", i, m.ShardsMetrics[i].Size, m.ShardsMetrics[i].Count))
	}

	return fmt.Sprintf("size:%d count:%d hit:%d mis:%d shards:%s", m.Size, m.Count, m.Hits, m.Misses, shards.String())
}

func (lrc *LruCache) Metrics() Metrics {
	var m Metrics
	m.ShardsMetrics = make([]ShardMetrics, lrc.shardCnt)
	for i := range lrc.shards {
		s := &lrc.shards[i]
		s.mu.RLock()
		size := s.sizeHot + s.sizeCold
		count := int64(s.blocks.Count())
		m.ShardsMetrics[i] = ShardMetrics{
			Size:  size,
			Count: count,
		}
		m.Size += size
		m.Count += count
		s.mu.RUnlock()
		m.Hits += atomic.LoadInt64(&s.hits)
		m.Misses += atomic.LoadInt64(&s.misses)
	}
	return m
}

func (lrc *LruCache) MetricsInfo() string {
	return lrc.Metrics().String()
}

func (lrc *LruCache) NewID() uint64 {
	return atomic.AddUint64(&lrc.idAlloc, 1)
}
