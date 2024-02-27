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
	"io"
	"sort"
	"time"

	"github.com/zuoyebang/bitalosdb/bitforest"
	"github.com/zuoyebang/bitalosdb/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/manual"
	record2 "github.com/zuoyebang/bitalosdb/internal/record"
	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/cockroachdb/errors"
)

func Open(dirname string, opts *Options) (db *DB, _ error) {
	opts = opts.Clone().EnsureDefaults()
	optsPool := opts.ensureOptionsPool()

	d := &DB{
		dirname:             dirname,
		walDirname:          opts.WALDir,
		opts:                opts,
		optspool:            optsPool,
		cmp:                 opts.Comparer.Compare,
		equal:               opts.Comparer.Equal,
		split:               opts.Comparer.Split,
		largeBatchThreshold: (opts.MemTableSize - int(memTableEmptySize)) / 2,
		logRecycler:         logRecycler{limit: opts.MemTableStopWritesThreshold + 1},
		compressor:          compress.SetCompressor(opts.CompressionType),
		cache:               nil,
		dbState:             optsPool.DbState,
		bgTasksCh:           make(chan struct{}),
	}

	if opts.CacheSize > 0 {
		cacheOpts := &base.CacheOptions{
			Size:     opts.CacheSize,
			Shards:   opts.CacheShards,
			HashSize: opts.CacheHashSize,
			Logger:   opts.Logger,
		}
		d.cache = NewCache(opts.CacheType, cacheOpts)
		d.optspool.SetExtraOption(func() {
			d.optspool.BitforestOptions.Cache = d.cache
			d.optspool.BitforestOptions.CacheType = opts.CacheType
		})
	}

	d.mu.meta = &metaSet{}

	defer func() {
		if r := recover(); db == nil {
			if d.cache != nil {
				d.cache.Close()
			}

			for _, mem := range d.mu.mem.queue {
				switch t := mem.flushable.(type) {
				case *memTable:
					manual.Free(t.arenaBuf)
					t.arenaBuf = nil
				}
			}
			if r != any(nil) {
				panic(r)
			}
		}
	}()

	if d.equal == nil {
		d.equal = bytes.Equal
	}

	d.commit = newCommitPipeline(commitEnv{
		logSeqNum:     &d.mu.meta.atomic.logSeqNum,
		visibleSeqNum: &d.mu.meta.atomic.visibleSeqNum,
		apply:         d.commitApply,
		write:         d.commitWrite,
		useQueue:      !d.opts.DisableWAL,
	})
	d.mu.nextJobID = 1
	d.mu.mem.nextSize = opts.MemTableSize
	d.mu.mem.cond.L = &d.mu.Mutex
	d.mu.cleaner.cond.L = &d.mu.Mutex
	d.mu.compact.cond.L = &d.mu.Mutex

	d.mu.meta.atomic.logSeqNum = 1

	d.timeNow = time.Now

	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.opts.ReadOnly {
		err := opts.FS.MkdirAll(dirname, 0755)
		if err != nil {
			return nil, err
		}
	}

	var err error
	d.dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	if d.walDirname == "" {
		d.walDirname = d.dirname
	}
	if d.walDirname == d.dirname {
		d.walDir = d.dataDir
	} else {
		if !d.opts.ReadOnly {
			err := opts.FS.MkdirAll(d.walDirname, 0755)
			if err != nil {
				return nil, err
			}
		}
		d.walDir, err = opts.FS.OpenDir(d.walDirname)
		if err != nil {
			return nil, err
		}
	}

	fileLock, err := opts.FS.Lock(base.MakeFilepath(opts.FS, dirname, fileTypeLock, 0))
	if err != nil {
		d.dataDir.Close()
		if d.dataDir != d.walDir {
			d.walDir.Close()
		}
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

	jobID := d.mu.nextJobID
	d.mu.nextJobID++

	if err = d.mu.meta.init(dirname, opts); err != nil {
		return nil, errors.Errorf("init bitalosdb meta fail err:%s", err.Error())
	}

	if !d.opts.ReadOnly {
		var entry *flushableEntry
		d.mu.mem.mutable, entry = d.newMemTable(0, d.mu.meta.atomic.logSeqNum)
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
	}

	d.bf, err = bitforest.NewBitforest(dirname, d.mu.meta.meta, d.optspool)
	if err != nil {
		return nil, err
	}

	if d.opts.AutoCompact {
		d.RunCompactTask()
	}

	ls, err := opts.FS.List(d.walDirname)
	if err != nil {
		return nil, err
	}
	if d.dirname != d.walDirname {
		ls2, err := opts.FS.List(d.dirname)
		if err != nil {
			return nil, err
		}
		ls = append(ls, ls2...)
	}

	type fileNumAndName struct {
		num  FileNum
		name string
	}
	var logFiles []fileNumAndName
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(opts.FS, filename)
		if !ok {
			continue
		}

		if d.mu.meta.nextFileNum <= fn {
			d.mu.meta.nextFileNum = fn + 1
		}

		switch ft {
		case fileTypeLog:
			if fn >= d.mu.meta.minUnflushedLogNum {
				logFiles = append(logFiles, fileNumAndName{fn, filename})
			}
			if d.logRecycler.minRecycleLogNum <= fn {
				d.logRecycler.minRecycleLogNum = fn + 1
			}
		}
	}

	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].num < logFiles[j].num
	})

	for _, lf := range logFiles {
		maxSeqNum, err := d.replayWAL(jobID, opts.FS, opts.FS.PathJoin(d.walDirname, lf.name), lf.num)
		if err != nil {
			return nil, err
		}
		d.mu.meta.markFileNumUsed(lf.num)
		if d.mu.meta.atomic.logSeqNum < maxSeqNum {
			d.mu.meta.atomic.logSeqNum = maxSeqNum
		}
	}

	d.mu.meta.atomic.visibleSeqNum = d.mu.meta.atomic.logSeqNum

	if !d.opts.ReadOnly {
		newLogNum := d.mu.meta.getNextFileNum()

		var me metaEdit
		me.MinUnflushedLogNum = newLogNum
		if err := d.mu.meta.apply(&me); err != nil {
			return nil, err
		}

		newLogName := base.MakeFilepath(opts.FS, d.walDirname, fileTypeLog, newLogNum)
		d.mu.log.queue = append(d.mu.log.queue, fileInfo{fileNum: newLogNum, fileSize: 0})
		logFile, err := opts.FS.Create(newLogName)
		if err != nil {
			return nil, err
		}
		if err := d.walDir.Sync(); err != nil {
			return nil, err
		}
		d.opts.EventListener.WALCreated(WALCreateInfo{
			JobID:   jobID,
			Path:    newLogName,
			FileNum: newLogNum,
		})

		d.mu.mem.queue[len(d.mu.mem.queue)-1].logNum = newLogNum

		logFile = vfs.NewSyncingFile(logFile, vfs.SyncingFileOptions{
			BytesPerSync:    d.opts.WALBytesPerSync,
			PreallocateSize: d.walPreallocateSize(),
		})
		d.mu.log.LogWriter = record2.NewLogWriter(logFile, newLogNum)
		d.mu.log.LogWriter.SetMinSyncInterval(d.opts.WALMinSyncInterval)
	}

	d.updateReadStateLocked()

	if !d.opts.ReadOnly {
		d.scanObsoleteFiles(ls)
		d.deleteObsoleteFiles(jobID, true /* waitForOngoing */)
	}

	d.maybeScheduleFlush(false)

	d.fileLock, fileLock = fileLock, nil

	return d, nil
}

func (d *DB) replayWAL(
	jobID int, fs vfs.FS, filename string, logNum FileNum,
) (maxSeqNum uint64, err error) {
	file, err := fs.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		b               Batch
		buf             bytes.Buffer
		mem             *memTable
		entry           *flushableEntry
		toFlush         flushableList
		rr              = record2.NewReader(file, logNum)
		offset          int64
		lastFlushOffset int64
	)

	if d.opts.ReadOnly {
		mem = d.mu.mem.mutable
		if mem != nil {
			entry = d.mu.mem.queue[len(d.mu.mem.queue)-1]
		}
	}

	flushMem := func() {
		if mem == nil {
			return
		}
		var logSize uint64
		if offset >= lastFlushOffset {
			logSize = uint64(offset - lastFlushOffset)
		}
		lastFlushOffset = offset
		entry.logSize = logSize
		if !d.opts.ReadOnly {
			toFlush = append(toFlush, entry)
		}
		mem, entry = nil, nil
	}

	ensureMem := func(seqNum uint64) {
		if mem != nil {
			return
		}
		mem, entry = d.newMemTable(logNum, seqNum)
		if d.opts.ReadOnly {
			d.mu.mem.mutable = mem
			d.mu.mem.queue = append(d.mu.mem.queue, entry)
		}
	}
	for {
		offset = rr.Offset()
		r, err := rr.Next()
		if err == nil {
			_, err = io.Copy(&buf, r)
		}
		if err != nil {
			if err == io.EOF {
				break
			} else if record2.IsInvalidRecord(err) {
				break
			}
			return 0, errors.Wrap(err, "bitalosdb: error when replaying WAL")
		}

		if buf.Len() < batchHeaderLen {
			return 0, base.CorruptionErrorf("bitalosdb: corrupt log file %q (num %s)",
				filename, errors.Safe(logNum))
		}

		b = Batch{db: d}
		b.SetRepr(buf.Bytes())
		seqNum := b.SeqNum()
		maxSeqNum = seqNum + uint64(b.Count())

		if b.memTableSize >= uint64(d.largeBatchThreshold) {
			flushMem()
			b.data = append([]byte(nil), b.data...)
			b.flushable = newFlushableBatch(&b, d.opts.Comparer)
			entry := d.newFlushableEntry(b.flushable, logNum, b.SeqNum())
			entry.readerRefs.Add(1)
			if d.opts.ReadOnly {
				d.mu.mem.queue = append(d.mu.mem.queue, entry)
			} else {
				toFlush = append(toFlush, entry)
			}
		} else {
			ensureMem(seqNum)
			if err = mem.prepare(&b, false); err != nil && err != arenaskl.ErrArenaFull {
				return 0, err
			}
			for err == arenaskl.ErrArenaFull {
				flushMem()
				ensureMem(seqNum)
				err = mem.prepare(&b, false)
				if err != nil && err != arenaskl.ErrArenaFull {
					return 0, err
				}
			}
			if err = mem.apply(&b, seqNum); err != nil {
				return 0, err
			}
			mem.writerUnref()
		}
		buf.Reset()
	}
	flushMem()

	if !d.opts.ReadOnly {
		c := newFlush(d.opts, toFlush, &d.atomic.bytesFlushed)
		if err = d.runCompaction(jobID, c, -1); err != nil {
			return 0, err
		}
		for i := range toFlush {
			toFlush[i].readerUnref()
		}
	}
	return maxSeqNum, err
}
