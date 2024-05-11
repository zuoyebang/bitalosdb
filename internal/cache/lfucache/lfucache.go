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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/cache"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

const (
	compationWorkerNum int = 4
	runCompactInterval int = 120
)

var _ cache.ICache = (*LfuCache)(nil)

func New(opts *options.CacheOptions) cache.ICache {
	return NewLfuCache(opts)
}

type LfuCache struct {
	maxSize      uint64
	shardNum     uint32
	launchTime   int64
	closed       atomic.Bool
	shards       []*shard
	logger       base.Logger
	workerClosed chan struct{}
	workerNum    int
	workerWg     sync.WaitGroup
	workerParams []chan *workerParam
}

func NewLfuCache(opts *options.CacheOptions) *LfuCache {
	size := uint64(opts.Size)
	shardNum := uint32(opts.Shards)
	maxSize := size / uint64(shardNum)
	memSize := int(maxSize) / 2

	mc := &LfuCache{
		logger:       opts.Logger,
		maxSize:      size,
		shardNum:     shardNum,
		launchTime:   time.Now().Unix(),
		shards:       make([]*shard, shardNum),
		workerNum:    compationWorkerNum,
		workerClosed: make(chan struct{}),
	}

	for i := range mc.shards {
		mc.shards[i] = newCache(mc, i, memSize, maxSize)
	}

	mc.runCompactionTask()
	return mc
}

func (lfc *LfuCache) bucketByHash(khash uint32) *shard {
	if lfc.shardNum == 1 {
		return lfc.shards[0]
	}
	return lfc.shards[khash%lfc.shardNum]
}

func (lfc *LfuCache) ExistAndDelete(key []byte, khash uint32) error {
	sd := lfc.bucketByHash(khash)
	if sd.exist(key) {
		return sd.delete(key)
	}

	return nil
}

func (lfc *LfuCache) Set(key, value []byte, khash uint32) error {
	return lfc.bucketByHash(khash).set(key, value)
}

func (lfc *LfuCache) Get(key []byte, khash uint32) ([]byte, func(), bool) {
	return lfc.bucketByHash(khash).get(key)
}

func (lfc *LfuCache) GetKeyHash(key []byte) uint32 {
	return hash.Crc32(key)
}

func (lfc *LfuCache) Delete(key []byte, khash uint32) error {
	return lfc.bucketByHash(khash).delete(key)
}

func (lfc *LfuCache) Close() {
	lfc.closed.Store(true)
	lfc.closeCompactionTask()

	for i := range lfc.shards {
		lfc.shards[i].close()
	}

	lfc.logger.Infof("lfucache closed")
}

func (lfc *LfuCache) isClosed() bool {
	return lfc.closed.Load()
}

func (lfc *LfuCache) runCompactionTask() {
	lfc.workerParams = make([]chan *workerParam, lfc.workerNum)

	for i := 0; i < lfc.workerNum; i++ {
		lfc.workerParams[i] = make(chan *workerParam)
		go lfc.runCompactionWorker(i)
	}

	lfc.workerWg.Add(1)
	go func() {
		defer lfc.workerWg.Done()

		interval := time.Duration(runCompactInterval)
		jobId := 0
		ticker := time.NewTicker(interval * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-lfc.workerClosed:
				return
			case <-ticker.C:
				if lfc.isClosed() {
					return
				}

				jobId++
				lfc.logger.Infof("lfucache run compact task start scheduleCompact:%d", jobId)
				lfc.runCompaction(compactTypeMemFlush)
				lfc.logger.Infof("lfucache run compact task end scheduleCompact:%d", jobId)
			}
		}
	}()
}

func (lfc *LfuCache) closeCompactionTask() {
	close(lfc.workerClosed)
	lfc.runCompaction(compactTypeClosed)
	for i := 0; i < lfc.workerNum; i++ {
		close(lfc.workerParams[i])
	}
	lfc.workerWg.Wait()
}

type workerParam struct {
	sid   int
	ctype int
	wg    *sync.WaitGroup
}

func (lfc *LfuCache) runCompactionWorker(workId int) {
	lfc.workerWg.Add(1)
	go func(wid int) {
		defer func() {
			lfc.workerWg.Done()
			if r := recover(); r != nil {
				lfc.logger.Errorf("lfucache compaction worker run panic err:%v panic:%s", r, string(debug.Stack()))
				lfc.runCompactionWorker(wid)
			}
		}()

		for {
			p, ok := <-lfc.workerParams[wid]
			if !ok {
				return
			}
			lfc.shards[p.sid].compact(p.ctype)
			p.wg.Done()
		}
	}(workId)
}

func (lfc *LfuCache) runCompaction(ctype int) {
	wg := &sync.WaitGroup{}

	for i := range lfc.shards {
		wid := i % lfc.workerNum
		p := &workerParam{
			sid:   i,
			ctype: ctype,
			wg:    wg,
		}
		wg.Add(1)
		lfc.workerParams[wid] <- p
	}

	wg.Wait()
}
