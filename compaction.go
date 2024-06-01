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
	"context"
	"errors"
	"runtime/debug"
	"runtime/pprof"
	"sort"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bitask"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

var errFlushInvariant = errors.New("bitalosdb: flush next log number is unset")
var flushLabels = pprof.Labels("bitalosdb", "flush")

type fileInfo struct {
	fileNum  FileNum
	fileSize uint64
}

type compaction struct {
	cmp                 Compare
	logger              Logger
	flushing            flushableList
	bytesIterated       uint64
	bytesWritten        int64
	keyWritten          int64
	keyPrefixDeleteKind int64
	prefixDeleteNum     int64
}

func newFlush(opts *Options, flushing flushableList) *compaction {
	return &compaction{
		cmp:      opts.Comparer.Compare,
		logger:   opts.Logger,
		flushing: flushing,
	}
}

func (c *compaction) newInputIter() internalIterator {
	if len(c.flushing) == 1 {
		f := c.flushing[0]
		iter := f.newFlushIter(nil, &c.bytesIterated)
		return iter
	}
	iters := make([]internalIterator, 0, len(c.flushing))
	for i := range c.flushing {
		f := c.flushing[i]
		iters = append(iters, f.newFlushIter(nil, &c.bytesIterated))
	}
	return newMergingIter(c.logger, c.cmp, iters...)
}

func (c *compaction) String() string {
	return "memtable flush\n"
}

func (s *Bitower) passedFlushThreshold() bool {
	var n int
	var size uint64
	for ; n < len(s.mu.mem.queue)-1; n++ {
		if !s.mu.mem.queue[n].readyForFlush() {
			break
		}
		if s.mu.mem.queue[n].flushForced {
			size += uint64(s.memTableSize)
		} else {
			size += s.mu.mem.queue[n].totalBytes()
		}
	}
	if n == 0 {
		return false
	}

	minFlushSize := uint64(s.memTableSize) / 2
	return size >= minFlushSize
}

func (s *Bitower) maybeScheduleFlush(needReport bool) {
	if s.mu.compact.flushing || s.db.IsClosed() || len(s.mu.mem.queue) <= 1 {
		return
	}

	if !s.passedFlushThreshold() {
		return
	}

	s.mu.compact.flushing = true

	s.db.memFlushTask.PushTask(&bitask.MemFlushTaskData{
		Index:      s.index,
		NeedReport: needReport,
	})
}

func (s *Bitower) flush(needReport bool) {
	pprof.Do(context.Background(), flushLabels, func(context.Context) {
		defer func() {
			if r := recover(); r != any(nil) {
				s.db.opts.Logger.Errorf("[BITOWER %d] flush panic err:%v stack:%s", s.index, r, string(debug.Stack()))
			}
		}()

		s.mu.Lock()
		defer s.mu.Unlock()

		defer func() {
			s.mu.compact.flushing = false
			s.maybeScheduleFlush(true)
			s.mu.compact.cond.Broadcast()
		}()

		if err := s.flush1(needReport); err != nil {
			s.db.opts.EventListener.BackgroundError(err)
		}
	})
}

func (s *Bitower) flush1(needReport bool) (err error) {
	var n int
	for ; n < len(s.mu.mem.queue)-1; n++ {
		if !s.mu.mem.queue[n].readyForFlush() {
			break
		}
	}
	if n == 0 {
		return nil
	}

	minUnflushedLogNum := s.mu.mem.queue[n].logNum
	if !s.db.opts.DisableWAL {
		for i := 0; i < n; i++ {
			logNum := s.mu.mem.queue[i].logNum
			if logNum >= minUnflushedLogNum {
				return errFlushInvariant
			}
		}
	}

	if needReport && s.db.opts.FlushReporter != nil {
		s.db.opts.FlushReporter(s.db.opts.Id)
	}

	c := newFlush(s.db.opts, s.mu.mem.queue[:n])

	err = s.runCompaction(c, n)
	if err == nil {
		sme := &bitowerMetaEditor{MinUnflushedLogNum: minUnflushedLogNum}
		err = s.metaApply(sme)
	}

	var flushed flushableList
	if err == nil {
		flushed = s.mu.mem.queue[:n]
		s.mu.mem.queue = s.mu.mem.queue[n:]
		s.updateReadState()
	}

	s.doDeleteObsoleteFiles()

	s.mu.Unlock()
	defer s.mu.Lock()

	for i := range flushed {
		flushed[i].readerUnref()
		close(flushed[i].flushed)
	}

	return err
}

func (s *Bitower) runCompaction(c *compaction, memNum int) (err error) {
	s.mu.Unlock()
	defer s.mu.Lock()

	d := s.db

	iter := &compactionIter{
		cmp:  c.cmp,
		iter: c.newInputIter(),
	}

	defer func() {
		err = utils.FirstError(err, iter.Close())
	}()

	d.dbState.SetBitowerHighPriority(s.index, true)
	d.dbState.LockBitowerWrite(s.index)
	defer func() {
		d.dbState.SetBitowerHighPriority(s.index, false)
		d.dbState.UnlockBitowerWrite(s.index)
	}()

	d.opts.EventListener.FlushBegin(FlushInfo{
		Index: s.index,
		Input: memNum,
	})
	startTime := d.timeNow()
	defer func() {
		info := FlushInfo{
			Index:               s.index,
			Input:               memNum,
			Iterated:            c.bytesIterated,
			Written:             c.bytesWritten,
			keyWritten:          c.keyWritten,
			keyPrefixDeleteKind: c.keyPrefixDeleteKind,
			prefixDeleteNum:     c.prefixDeleteNum,
			Duration:            d.timeNow().Sub(startTime),
			Done:                true,
			Err:                 err,
		}
		d.flushMemTime.Store(info.Duration.Milliseconds())
		d.opts.EventListener.FlushEnd(info)
	}()

	var lastPrefixDelete uint64
	var writer *flushBitowerWriter

	checkKeyPrefixDelete := func(ik *InternalKey) bool {
		if lastPrefixDelete == 0 {
			return false
		}

		keyPrefixDelete := d.optspool.BaseOptions.KeyPrefixDeleteFunc(ik.UserKey)
		if lastPrefixDelete == keyPrefixDelete {
			return true
		} else {
			lastPrefixDelete = 0
			return false
		}
	}

	writer, err = s.newFlushWriter()
	if err != nil {
		return err
	}

	defer func() {
		err = writer.Finish()
	}()

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		switch key.Kind() {
		case InternalKeyKindSet:
			if checkKeyPrefixDelete(key) {
				c.prefixDeleteNum++
				continue
			}

			if d.optspool.BaseOptions.KvCheckExpire(key.UserKey, val) {
				key.SetKind(InternalKeyKindDelete)
				val = nil
			}
		case InternalKeyKindDelete:
			if checkKeyPrefixDelete(key) {
				continue
			}
		case InternalKeyKindPrefixDelete:
			lastPrefixDelete = d.optspool.BaseOptions.KeyPrefixDeleteFunc(key.UserKey)
			c.keyPrefixDeleteKind++
		}

		if err = writer.Set(*key, val); err != nil {
			return err
		}

		c.bytesWritten += int64(key.Size() + len(val))
		c.keyWritten++
	}

	return nil
}

func (s *Bitower) doDeleteObsoleteFiles() {
	var obsoleteLogs []fileInfo
	for i := range s.mu.log.queue {
		if s.mu.log.queue[i].fileNum >= s.getMinUnflushedLogNum() {
			obsoleteLogs = s.mu.log.queue[:i]
			s.mu.log.queue = s.mu.log.queue[i:]
			break
		}
	}

	s.mu.Unlock()
	defer s.mu.Lock()

	for _, f := range obsoleteLogs {
		if s.logRecycler.add(f) {
			continue
		}

		filename := s.makeWalFilename(f.fileNum)
		s.db.optspool.BaseOptions.DeleteFilePacer.AddFile(filename)
		s.db.opts.EventListener.WALDeleted(WALDeleteInfo{
			Index:   s.index,
			Path:    filename,
			FileNum: f.fileNum,
		})
	}
}

func (s *Bitower) scanObsoleteFiles(list []string) {
	if s.mu.compact.flushing {
		return
	}

	var obsoleteLogs []fileInfo

	minUnflushedLogNum := s.getMinUnflushedLogNum()
	for _, filename := range list {
		ft, fn, ok := base.ParseFilename(s.db.opts.FS, filename)
		if ok && ft == fileTypeLog && fn < minUnflushedLogNum {
			fi := fileInfo{fileNum: fn}
			if stat, err := s.db.opts.FS.Stat(filename); err == nil {
				fi.fileSize = uint64(stat.Size())
			}
			obsoleteLogs = append(obsoleteLogs, fi)
		}
	}

	s.mu.log.queue = merge(s.mu.log.queue, obsoleteLogs)
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
