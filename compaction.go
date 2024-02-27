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
	"context"
	"errors"
	"io"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

var errFlushInvariant = errors.New("bitalosdb: flush next log number is unset")
var flushLabels = pprof.Labels("bitalosdb", "flush")

type compaction struct {
	cmp                 Compare
	formatKey           base.FormatKey
	logger              Logger
	score               float64
	maxOutputFileSize   uint64
	maxOverlapBytes     uint64
	flushing            flushableList
	bytesIterated       uint64
	bytesWritten        int64
	keyWritten          int64
	atomicBytesIterated *uint64
	closers             []io.Closer
}

func newFlush(opts *Options, flushing flushableList, bytesFlushed *uint64) *compaction {
	return &compaction{
		cmp:                 opts.Comparer.Compare,
		formatKey:           opts.Comparer.FormatKey,
		logger:              opts.Logger,
		flushing:            flushing,
		atomicBytesIterated: bytesFlushed,
	}
}

func (c *compaction) newInputIter() (_ internalIterator, retErr error) {
	if len(c.flushing) == 1 {
		f := c.flushing[0]
		iter := f.newFlushIter(nil, &c.bytesIterated)
		return iter, nil
	}
	iters := make([]internalIterator, 0, len(c.flushing))
	for i := range c.flushing {
		f := c.flushing[i]
		iters = append(iters, f.newFlushIter(nil, &c.bytesIterated))
	}
	return newMergingIter(c.logger, c.cmp, nil, iters...), nil
}

func (c *compaction) String() string {
	return "flush\n"
}

func (d *DB) maybeScheduleFlush(needReport bool) {
	if d.mu.compact.flushing || d.IsClosed() || d.opts.ReadOnly {
		return
	}
	if len(d.mu.mem.queue) <= 1 {
		return
	}

	if !d.passedFlushThreshold() {
		return
	}

	d.mu.compact.flushing = true
	go d.flush(needReport)
}

func (d *DB) passedFlushThreshold() bool {
	var n int
	var size uint64
	for ; n < len(d.mu.mem.queue)-1; n++ {
		if !d.mu.mem.queue[n].readyForFlush() {
			break
		}
		if d.mu.mem.queue[n].flushForced {
			size += uint64(d.opts.MemTableSize)
		} else {
			size += d.mu.mem.queue[n].totalBytes()
		}
	}
	if n == 0 {
		return false
	}

	minFlushSize := uint64(d.opts.MemTableSize) / 2
	return size >= minFlushSize
}

func (d *DB) flush(needReport bool) {
	pprof.Do(context.Background(), flushLabels, func(context.Context) {
		defer func() {
			if r := recover(); r != any(nil) {
				d.opts.Logger.Errorf("bitalosdb: flush panic err:%v stack:%s", r, string(debug.Stack()))
			}
		}()

		d.mu.Lock()
		defer d.mu.Unlock()

		if err := d.flush1(needReport); err != nil {
			d.opts.EventListener.BackgroundError(err)
		}
		d.mu.compact.flushing = false
		d.maybeScheduleFlush(true)

		d.mu.compact.cond.Broadcast()
	})
}

func (d *DB) flush1(needReport bool) (err error) {
	var n int
	for ; n < len(d.mu.mem.queue)-1; n++ {
		if !d.mu.mem.queue[n].readyForFlush() {
			break
		}
	}
	if n == 0 {
		return nil
	}

	minUnflushedLogNum := d.mu.mem.queue[n].logNum
	if !d.opts.DisableWAL {
		for i := 0; i < n; i++ {
			logNum := d.mu.mem.queue[i].logNum
			if logNum >= minUnflushedLogNum {
				return errFlushInvariant
			}
		}
	}

	if needReport && d.opts.FlushReporter != nil {
		d.opts.FlushReporter(d.opts.Id)
	}

	c := newFlush(d.opts, d.mu.mem.queue[:n], &d.atomic.bytesFlushed)

	jobID := d.mu.nextJobID
	d.mu.nextJobID++

	if err = d.runCompaction(jobID, c, n); err == nil {
		var me metaEdit
		me.MinUnflushedLogNum = minUnflushedLogNum
		err = d.mu.meta.apply(&me)
	}

	atomic.StoreUint64(&d.atomic.bytesFlushed, 0)

	var flushed flushableList
	if err == nil {
		flushed = d.mu.mem.queue[:n]
		d.mu.mem.queue = d.mu.mem.queue[n:]
		d.updateReadStateLocked()
	}
	d.deleteObsoleteFiles(jobID, false)

	d.mu.Unlock()
	defer d.mu.Lock()

	for i := range flushed {
		flushed[i].readerUnref()
		close(flushed[i].flushed)
	}

	return err
}

func (d *DB) runCompaction(jobID int, c *compaction, memNum int) (retErr error) {
	d.mu.Unlock()
	defer d.mu.Lock()

	iiter, err := c.newInputIter()
	if err != nil {
		return err
	}

	iter := &compactionIter{
		cmp:  c.cmp,
		iter: iiter,
	}

	defer func() {
		if iter != nil {
			retErr = utils.FirstError(retErr, iter.Close())
		}
		for _, closer := range c.closers {
			retErr = utils.FirstError(retErr, closer.Close())
		}
	}()

	d.dbState.SetHighPriority(true)
	d.dbState.LockDbWrite()
	defer func() {
		d.dbState.SetHighPriority(false)
		d.dbState.UnlockDbWrite()
	}()

	d.dbState.LockMemFlushing()
	defer d.dbState.UnLockMemFlushing()

	d.opts.EventListener.FlushBegin(FlushInfo{
		JobID: jobID,
		Input: memNum,
	})
	startTime := d.timeNow()
	defer func() {
		info := FlushInfo{
			JobID:      jobID,
			Input:      memNum,
			Iterated:   c.bytesIterated,
			Written:    c.bytesWritten,
			keyWritten: c.keyWritten,
			Duration:   d.timeNow().Sub(startTime),
			Done:       true,
			Err:        retErr,
		}
		d.bf.SetFlushMemTime(info.Duration.Milliseconds())
		d.opts.EventListener.FlushEnd(info)
	}()

	bw := d.bf.GetWriter()
	if err = bw.Start(memNum); err != nil {
		return err
	}
	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		atomic.StoreUint64(c.atomicBytesIterated, c.bytesIterated)

		if key.Kind() == InternalKeyKindSet && d.optspool.BaseOptions.KvCheckExpire(key.UserKey, val) {
			key.SetKind(InternalKeyKindDelete)
			val = nil
		}

		if err = bw.Set(*key, val); err != nil {
			return err
		}

		c.bytesWritten += int64(key.Size() + len(val))
		c.keyWritten++
	}

	return bw.Finish()
}

func (d *DB) scanObsoleteFiles(list []string) {
	if d.mu.compact.flushing {
		d.opts.Logger.Error("panic: cannot scan obsolete files concurrently with compaction/flushing")
		return
	}

	minUnflushedLogNum := d.mu.meta.minUnflushedLogNum

	var obsoleteLogs []fileInfo

	for _, filename := range list {
		ft, fn, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		switch ft {
		case fileTypeLog:
			if fn < minUnflushedLogNum {
				fi := fileInfo{fileNum: fn}
				if stat, err := d.opts.FS.Stat(filename); err == nil {
					fi.fileSize = uint64(stat.Size())
				}
				obsoleteLogs = append(obsoleteLogs, fi)
			}
		}
	}

	d.mu.log.queue = merge(d.mu.log.queue, obsoleteLogs)
}

func (d *DB) disableFileDeletions() {
	d.mu.cleaner.disabled++
	for d.mu.cleaner.cleaning {
		d.mu.cleaner.cond.Wait()
	}
	d.mu.cleaner.cond.Broadcast()
}

func (d *DB) enableFileDeletions() {
	if d.mu.cleaner.disabled <= 0 || d.mu.cleaner.cleaning {
		d.opts.Logger.Error("panic: file deletion disablement invariant violated")
		return
	}
	d.mu.cleaner.disabled--
	if d.mu.cleaner.disabled > 0 {
		return
	}
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.deleteObsoleteFiles(jobID, true)
}

func (d *DB) acquireCleaningTurn(waitForOngoing bool) bool {
	for d.mu.cleaner.cleaning && d.mu.cleaner.disabled == 0 && waitForOngoing {
		d.mu.cleaner.cond.Wait()
	}
	if d.mu.cleaner.cleaning {
		return false
	}
	if d.mu.cleaner.disabled > 0 {
		return false
	}
	d.mu.cleaner.cleaning = true
	return true
}

func (d *DB) releaseCleaningTurn() {
	d.mu.cleaner.cleaning = false
	d.mu.cleaner.cond.Broadcast()
}

func (d *DB) deleteObsoleteFiles(jobID int, waitForOngoing bool) {
	if !d.acquireCleaningTurn(waitForOngoing) {
		return
	}
	d.doDeleteObsoleteFiles(jobID)
	d.releaseCleaningTurn()
}

type fileInfo struct {
	fileNum  FileNum
	fileSize uint64
}

func (d *DB) doDeleteObsoleteFiles(jobID int) {
	var obsoleteLogs []fileInfo
	for i := range d.mu.log.queue {
		if d.mu.log.queue[i].fileNum >= d.mu.meta.minUnflushedLogNum {
			obsoleteLogs = d.mu.log.queue[:i]
			d.mu.log.queue = d.mu.log.queue[i:]
			break
		}
	}

	d.mu.Unlock()
	defer d.mu.Lock()

	for _, f := range obsoleteLogs {
		if d.logRecycler.add(f) {
			continue
		}

		path := base.MakeFilepath(d.opts.FS, d.walDirname, fileTypeLog, f.fileNum)
		d.optspool.BaseOptions.DeleteFilePacer.AddFile(path)
		d.opts.EventListener.WALDeleted(WALDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: f.fileNum,
		})
	}
}

func merge(a, b []fileInfo) []fileInfo {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	sort.Slice(a, func(i, j int) bool {
		return a[i].fileNum < a[j].fileNum
	})

	n := 0
	for i := 0; i < len(a); i++ {
		if n == 0 || a[i].fileNum != a[n-1].fileNum {
			a[n] = a[i]
			n++
		}
	}
	return a[:n]
}
