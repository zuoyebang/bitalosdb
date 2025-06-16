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
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bitask"
	"github.com/zuoyebang/bitalosdb/internal/cache/lfucache"
	"github.com/zuoyebang/bitalosdb/internal/cache/lrucache"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/manual"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

func Open(dirname string, opts *Options) (db *DB, err error) {
	opts = opts.Clone().EnsureDefaults()

	var optsPool *options.OptionsPool
	if opts.private.optspool == nil {
		optsPool = opts.ensureOptionsPool(nil)
	} else {
		optsPool = opts.private.optspool
	}

	d := &DB{
		dirname:             dirname,
		walDirname:          opts.WALDir,
		opts:                opts,
		optspool:            optsPool,
		cmp:                 opts.Comparer.Compare,
		equal:               opts.Comparer.Equal,
		split:               opts.Comparer.Split,
		largeBatchThreshold: (opts.MemTableSize - int(memTableEmptySize)) / 2,
		timeNow:             time.Now,
		compressor:          compress.SetCompressor(opts.CompressionType),
		cache:               nil,
		dbState:             optsPool.DbState,
		taskClosed:          make(chan struct{}),
		meta:                &metaSet{},
	}

	defer func() {
		if r := recover(); db == nil {
			d.closeTask()

			for i := range d.bitowers {
				if d.bitowers[i] != nil {
					for _, mem := range d.bitowers[i].mu.mem.queue {
						switch t := mem.flushable.(type) {
						case *memTable:
							manual.Free(t.arenaBuf)
							t.arenaBuf = nil
						}
					}
				}
			}

			if d.cache != nil {
				d.cache.Close()
			}

			if r != any(nil) {
				panic(r)
			}
		}
	}()

	if err = opts.FS.MkdirAll(dirname, 0755); err != nil {
		return nil, err
	}
	d.dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	if d.walDirname == "" {
		d.walDirname = d.dirname
	}
	fileLock, err := opts.FS.Lock(base.MakeFilepath(opts.FS, dirname, fileTypeLock, 0))
	if err != nil {
		d.dataDir.Close()
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

	if err = d.meta.init(dirname, opts); err != nil {
		return nil, err
	}
	oldLogSeqNum := d.meta.atomic.logSeqNum
	d.initFlushedBitable()

	if opts.CacheSize > 0 {
		cacheOpts := &options.CacheOptions{
			Size:     opts.CacheSize,
			Shards:   opts.CacheShards,
			HashSize: opts.CacheHashSize,
			Logger:   opts.Logger,
		}
		if opts.CacheType == consts.CacheTypeLfu {
			d.cache = lfucache.New(cacheOpts)
		} else {
			d.cache = lrucache.New(cacheOpts)
		}
	}

	d.memFlushTask = bitask.NewMemFlushTask(&bitask.MemFlushTaskOptions{
		Size:   consts.DefaultBitowerNum * 6,
		Logger: d.opts.Logger,
		DoFunc: d.doMemFlushTask,
		TaskWg: &d.taskWg,
	})

	d.bpageTask = bitask.NewBitpageTask(&bitask.BitpageTaskOptions{
		Size:    consts.DefaultBitowerNum * 1000,
		DbState: d.optspool.DbState,
		Logger:  opts.Logger,
		DoFunc:  d.doBitpageTask,
		TaskWg:  &d.taskWg,
	})
	d.optspool.BaseOptions.BitpageTaskPushFunc = d.bpageTask.PushTask

	for i := range d.bitowers {
		if err = openBitower(d, i); err != nil {
			return nil, err
		}
	}

	if d.opts.AutoCompact {
		d.runCompactTask()
	}

	for i := range d.bitowers {
		d.bitowers[i].btree.MaybeScheduleFlush(false)
	}

	d.meta.atomic.visibleSeqNum = d.meta.atomic.logSeqNum
	d.fileLock, fileLock = fileLock, nil

	d.opts.Logger.Infof("open bitalosdb success memSize:%d logSeqNum:%d oldLogSeqNum:%d",
		d.opts.MemTableSize, d.meta.atomic.logSeqNum, oldLogSeqNum)

	return d, nil
}
