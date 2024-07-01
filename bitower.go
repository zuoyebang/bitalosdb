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
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/bitree"
	"github.com/zuoyebang/bitalosdb/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/manual"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/record"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type Bitower struct {
	db               *DB
	btree            *bitree.Bitree
	index            int
	walDirname       string
	walDir           vfs.File
	commit           *commitPipeline
	logRecycler      logRecycler
	memTableSize     int
	memTableReserved atomic.Int64
	iterSlowCount    atomic.Uint64

	readState struct {
		sync.RWMutex
		val *readState
	}

	mu struct {
		sync.Mutex
		metaEdit *bitowerMetaEditor

		log struct {
			queue []fileInfo
			*record.LogWriter
		}

		mem struct {
			cond      sync.Cond
			mutable   *memTable
			queue     flushableList
			switching bool
		}

		compact struct {
			cond     sync.Cond
			flushing bool
		}
	}
}

func openBitower(d *DB, index int) (*Bitower, error) {
	s := &Bitower{
		db:           d,
		index:        index,
		memTableSize: d.opts.MemTableSize,
		logRecycler:  logRecycler{limit: d.opts.MemTableStopWritesThreshold + 1},
	}

	s.walDirname = base.MakeWalpath(d.walDirname, index)
	if err := d.opts.FS.MkdirAll(s.walDirname, 0755); err != nil {
		return nil, err
	}
	walDir, err := d.opts.FS.OpenDir(s.walDirname)
	if err != nil {
		return nil, err
	}
	s.walDir = walDir

	s.commit = newCommitPipeline(commitEnv{
		logSeqNum:     &d.meta.atomic.logSeqNum,
		visibleSeqNum: &d.meta.atomic.visibleSeqNum,
		apply:         s.commitApply,
		write:         s.commitWrite,
		useQueue:      !d.opts.DisableWAL,
	})

	s.mu.mem.cond.L = &s.mu.Mutex
	s.mu.compact.cond.L = &s.mu.Mutex

	s.mu.Lock()
	defer s.mu.Unlock()

	s.initMetaEdit()

	btreeOpts := s.db.optspool.CloneBitreeOptions()
	btreeOpts.IsFlushedBitableCB = s.db.isFlushedBitable
	btreeOpts.Index = s.index
	s.btree, err = bitree.NewBitree(s.db.dirname, btreeOpts)
	if err != nil {
		return nil, err
	}

	var entry *flushableEntry
	s.mu.mem.mutable, entry = s.newMemTable(0, d.meta.atomic.logSeqNum)
	s.mu.mem.queue = append(s.mu.mem.queue, entry)

	ls, err := d.opts.FS.List(s.walDirname)
	if err != nil {
		return nil, err
	}
	type fileNumAndName struct {
		num  FileNum
		name string
	}
	var logFiles []fileNumAndName
	var maxSeqNum uint64
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}

		if s.mu.metaEdit.NextFileNum <= fn {
			s.mu.metaEdit.NextFileNum = fn + 1
		}

		switch ft {
		case fileTypeLog:
			if fn >= s.getMinUnflushedLogNum() {
				logFiles = append(logFiles, fileNumAndName{fn, filename})
			}
			if s.logRecycler.minRecycleLogNum <= fn {
				s.logRecycler.minRecycleLogNum = fn + 1
			}
		}
	}

	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].num < logFiles[j].num
	})

	for _, lf := range logFiles {
		walFilename := d.opts.FS.PathJoin(s.walDirname, lf.name)
		maxSeqNum, err = s.replayWAL(walFilename, lf.num)
		if err != nil {
			return nil, err
		}
		d.opts.Logger.Infof("[BITOWER %d] replayWAL ok wal:%s maxSeqNum:%d", s.index, walFilename, maxSeqNum)
		s.mu.metaEdit.MarkFileNumUsed(lf.num)
		if d.meta.atomic.logSeqNum < maxSeqNum {
			d.meta.atomic.logSeqNum = maxSeqNum
		}
	}

	if !d.opts.DisableWAL {
		newLogNum := s.mu.metaEdit.GetNextFileNum()
		sme := &bitowerMetaEditor{MinUnflushedLogNum: newLogNum}
		if err = s.metaApply(sme); err != nil {
			return nil, err
		}

		newLogName := s.makeWalFilename(newLogNum)
		s.mu.log.queue = append(s.mu.log.queue, fileInfo{fileNum: newLogNum, fileSize: 0})
		logFile, err := d.opts.FS.Create(newLogName)
		if err != nil {
			return nil, err
		}
		if err = s.walDir.Sync(); err != nil {
			return nil, err
		}

		d.opts.EventListener.WALCreated(WALCreateInfo{
			Index:   index,
			Path:    newLogName,
			FileNum: newLogNum,
		})

		s.mu.mem.queue[len(s.mu.mem.queue)-1].logNum = newLogNum

		logFile = vfs.NewSyncingFile(logFile, vfs.SyncingFileOptions{
			BytesPerSync:    d.opts.WALBytesPerSync,
			PreallocateSize: s.walPreallocateSize(),
		})
		s.mu.log.LogWriter = record.NewLogWriter(logFile, newLogNum)
		s.mu.log.LogWriter.SetMinSyncInterval(d.opts.WALMinSyncInterval)
	}

	s.updateReadState()

	s.scanObsoleteFiles(ls)
	s.doDeleteObsoleteFiles()

	return s, nil
}

func (s *Bitower) replayWAL(filename string, logNum FileNum) (maxSeqNum uint64, err error) {
	file, err := s.db.opts.FS.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		b               BatchBitower
		buf             bytes.Buffer
		mem             *memTable
		entry           *flushableEntry
		toFlush         flushableList
		rr              = record.NewReader(file, logNum)
		offset          int64
		lastFlushOffset int64
	)

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
		toFlush = append(toFlush, entry)
		mem, entry = nil, nil
	}

	ensureMem := func(seqNum uint64) {
		if mem != nil {
			return
		}
		mem, entry = s.newMemTable(logNum, seqNum)
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
			} else if record.IsInvalidRecord(err) {
				break
			}
			return 0, errors.Wrap(err, "bitalosdb: error when replaying WAL")
		}

		if buf.Len() < batchHeaderLen {
			return 0, base.CorruptionErrorf("bitalosdb: corrupt log file %q (num %s)",
				filename, errors.Safe(logNum))
		}

		b = BatchBitower{
			db:    s.db,
			index: s.index,
		}
		b.SetRepr(buf.Bytes())
		seqNum := b.SeqNum()
		maxSeqNum = seqNum + uint64(b.Count())

		if b.memTableSize >= uint64(s.db.largeBatchThreshold) {
			flushMem()
			b.data = append([]byte(nil), b.data...)
			b.flushable = newFlushableBatchBitower(&b, s.db.opts.Comparer)
			entry := newFlushableEntry(b.flushable, logNum, b.SeqNum())
			entry.readerRefs.Add(1)
			toFlush = append(toFlush, entry)
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

	if len(toFlush) > 0 {
		c := newFlush(s.db.opts, toFlush)
		if err = s.runCompaction(c, len(toFlush)); err != nil {
			return 0, err
		}
		for i := range toFlush {
			toFlush[i].readerUnref()
		}
	}

	return maxSeqNum, err
}

func (s *Bitower) initMetaEdit() {
	s.mu.metaEdit = &s.db.meta.meta.Bmes[s.index]

	if s.mu.metaEdit.NextFileNum == 0 {
		s.mu.metaEdit.NextFileNum = 1
	}

	s.mu.metaEdit.MarkFileNumUsed(s.mu.metaEdit.MinUnflushedLogNum)
}

func (s *Bitower) getMinUnflushedLogNum() FileNum {
	return s.mu.metaEdit.MinUnflushedLogNum
}

func (s *Bitower) metaApply(sme *bitowerMetaEditor) error {
	if sme.MinUnflushedLogNum != 0 {
		if sme.MinUnflushedLogNum < s.getMinUnflushedLogNum() || s.mu.metaEdit.NextFileNum <= sme.MinUnflushedLogNum {
			return errors.Errorf("inconsistent bitowerMetaEditor minUnflushedLogNum %d", sme.MinUnflushedLogNum)
		}
	}

	sme.Index = s.index
	sme.NextFileNum = s.mu.metaEdit.NextFileNum
	return s.db.meta.apply(sme)
}

func (s *Bitower) walPreallocateSize() int {
	size := s.memTableSize
	size = (size / 10) + size
	return size
}

func (s *Bitower) makeWalFilename(fileNum FileNum) string {
	return base.MakeFilepath(s.db.opts.FS, s.walDirname, fileTypeLog, fileNum)
}

func (s *Bitower) newMemTable(logNum FileNum, logSeqNum uint64) (*memTable, *flushableEntry) {
	size := s.memTableSize
	mem := newMemTable(memTableOptions{
		Options:   s.db.opts,
		arenaBuf:  manual.New(size),
		size:      size,
		logSeqNum: logSeqNum,
	})

	s.memTableReserved.Add(int64(size))

	invariants.SetFinalizer(mem, checkMemTable)

	entry := newFlushableEntry(mem, logNum, logSeqNum)
	entry.releaseMemAccounting = func() {
		manual.Free(mem.arenaBuf)
		mem.arenaBuf = nil
		s.memTableReserved.Add(-int64(size))
	}
	return mem, entry
}

func (s *Bitower) Get(key []byte) ([]byte, func(), error) {
	rs := s.loadReadState()

	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		mValue, mExist, kind := m.get(key)
		if mExist {
			switch kind {
			case InternalKeyKindSet, InternalKeyKindPrefixDelete:
				return mValue, func() { rs.unref() }, nil
			case InternalKeyKindDelete:
				rs.unref()
				return nil, nil, ErrNotFound
			}
		}
	}

	rs.unref()

	useCache := false
	keyHash := hash.Crc32(key)
	if s.db.cache != nil {
		useCache = true
		ivCache, ivCloser, ivExist := s.db.cache.Get(key, keyHash)
		if ivExist {
			return ivCache, ivCloser, nil
		}
	}

	v, exist, closer := s.btree.Get(key, keyHash)
	if exist {
		if useCache && v != nil {
			s.db.cache.Set(key, v, keyHash)
		}

		return v, closer, nil
	}
	return nil, nil, ErrNotFound
}

func (s *Bitower) Exist(key []byte) bool {
	rs := s.loadReadState()
	defer rs.unref()

	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		_, mExist, kind := m.get(key)
		if mExist {
			switch kind {
			case InternalKeyKindSet:
				return true
			case InternalKeyKindDelete, InternalKeyKindPrefixDelete:
				return false
			}
		}
	}

	keyHash := hash.Crc32(key)

	if s.db.cache != nil {
		_, ivCloser, ivExist := s.db.cache.Get(key, keyHash)
		if ivCloser != nil {
			ivCloser()
		}
		return ivExist
	}

	return s.btree.Exist(key, keyHash)
}

func (s *Bitower) Flush() error {
	flushDone, err := s.AsyncFlush()
	if err != nil {
		return err
	}
	if flushDone != nil {
		<-flushDone
	}
	return nil
}

func (s *Bitower) AsyncFlush() (<-chan struct{}, error) {
	if s.db.IsClosed() {
		return nil, ErrClosed
	}

	s.commit.mu.Lock()
	defer s.commit.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	empty := true
	for i := range s.mu.mem.queue {
		if !s.mu.mem.queue[i].empty() {
			empty = false
			break
		}
	}
	if empty {
		return nil, nil
	}
	flushed := s.mu.mem.queue[len(s.mu.mem.queue)-1].flushed
	err := s.makeRoomForWrite(nil, false)
	if err != nil {
		return nil, err
	}
	return flushed, nil
}

func (s *Bitower) newBitreeIter(o *options.IterOptions) (iters []base.InternalIterator) {
	return s.btree.NewIters(o)
}

func (s *Bitower) checkpoint(fs vfs.FS, destDir string, isSync bool) error {
	if !s.db.opts.DisableWAL {
		s.mu.Lock()
		memQueue := s.mu.mem.queue
		s.mu.Unlock()

		if isSync {
			if err := s.db.LogData(nil, s.index, Sync); err != nil {
				return err
			}
		}

		for i := range memQueue {
			logNum := memQueue[i].logNum
			if logNum == 0 {
				continue
			}
			srcPath := base.MakeFilepath(fs, s.walDirname, fileTypeLog, logNum)
			destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
			if err := vfs.Copy(fs, srcPath, destPath); err != nil {
				return err
			}
		}
	}

	return s.checkpointBitree(fs, destDir)
}

func (s *Bitower) checkpointBitree(fs vfs.FS, destDir string) error {
	srcPath := base.MakeBitreeFilepath(s.db.dirname, s.index)
	destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
	if err := vfs.Copy(fs, srcPath, destPath); err != nil {
		return err
	}
	if err := s.btree.Checkpoint(fs, destDir, s.db.dirname); err != nil {
		return err
	}

	return nil
}

func (s *Bitower) Close() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.mu.compact.flushing {
		s.mu.compact.cond.Wait()
	}

	if s.mu.log.LogWriter != nil {
		err = utils.FirstError(err, s.mu.log.Close())
	}

	s.readState.val.unref()

	for _, mem := range s.mu.mem.queue {
		mem.readerUnref()
	}

	if reserved := s.memTableReserved.Load(); reserved != 0 {
		err = utils.FirstError(err, errors.Errorf("leaked memtable reservation:%d", reserved))
	}

	err = utils.FirstError(err, s.walDir.Close())
	err = utils.FirstError(err, s.btree.Close())
	s.db.opts.Logger.Infof("[BITOWER %d] closed...", s.index)
	return err
}

func (s *Bitower) commitApply(b *BatchBitower, mem *memTable) error {
	if b.flushable != nil {
		return nil
	}

	err := mem.apply(b, b.SeqNum())
	if err != nil {
		return err
	}

	if mem.writerUnref() {
		s.mu.Lock()
		s.maybeScheduleFlush(true)
		s.mu.Unlock()
	}
	return nil
}

func (s *Bitower) commitWrite(b *BatchBitower, syncWG *sync.WaitGroup, syncErr *error) (*memTable, error) {
	repr := b.Repr()

	if b.flushable != nil {
		b.flushable.setSeqNum(b.SeqNum())
		if !s.db.opts.DisableWAL {
			if _, err := s.mu.log.SyncRecord(repr, syncWG, syncErr); err != nil {
				return nil, err
			}
		}
	}

	s.mu.Lock()
	err := s.makeRoomForWrite(b, true)
	mem := s.mu.mem.mutable
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if s.db.opts.DisableWAL {
		return mem, nil
	}

	if b.flushable == nil {
		if _, err = s.mu.log.SyncRecord(repr, syncWG, syncErr); err != nil {
			return nil, err
		}
	}

	return mem, err
}

func (s *Bitower) makeRoomForWrite(b *BatchBitower, needReport bool) error {
	force := b == nil || b.flushable != nil
	stalled := false
	for {
		if s.mu.mem.switching {
			s.mu.mem.cond.Wait()
			continue
		}
		if b != nil && b.flushable == nil {
			err := s.mu.mem.mutable.prepare(b, true)
			if err != arenaskl.ErrArenaFull && err != errMemExceedDelPercent {
				if stalled {
					s.db.opts.EventListener.WriteStallEnd()
				}
				return err
			}
		} else if !force {
			if stalled {
				s.db.opts.EventListener.WriteStallEnd()
			}
			return nil
		}

		{
			var size uint64
			for i := range s.mu.mem.queue {
				size += s.mu.mem.queue[i].totalBytes()
			}
			if size >= uint64(s.db.opts.MemTableStopWritesThreshold*s.db.opts.MemTableSize) {
				if !stalled {
					stalled = true
					s.db.opts.EventListener.WriteStallBegin(WriteStallBeginInfo{
						Index:  s.index,
						Reason: "memtable count limit reached",
					})
				}
				s.mu.compact.cond.Wait()
				continue
			}
		}

		var newLogNum FileNum
		var newLogFile vfs.File
		var newLogSize uint64
		var prevLogSize uint64
		var err error

		if !s.db.opts.DisableWAL {
			newLogNum = s.mu.metaEdit.GetNextFileNum()
			s.mu.mem.switching = true
			prevLogSize = uint64(s.mu.log.Size())

			if s.mu.log.queue[len(s.mu.log.queue)-1].fileSize < prevLogSize {
				s.mu.log.queue[len(s.mu.log.queue)-1].fileSize = prevLogSize
			}

			s.mu.Unlock()

			err = s.mu.log.Close()
			newLogName := s.makeWalFilename(newLogNum)

			var recycleLog fileInfo
			var recycleOK bool
			if err == nil {
				recycleLog, recycleOK = s.logRecycler.peek()
				if recycleOK {
					recycleLogName := s.makeWalFilename(recycleLog.fileNum)
					newLogFile, err = s.db.opts.FS.ReuseForWrite(recycleLogName, newLogName)
				} else {
					newLogFile, err = s.db.opts.FS.Create(newLogName)
				}
			}

			if err == nil && recycleOK {
				var finfo os.FileInfo
				finfo, err = newLogFile.Stat()
				if err == nil {
					newLogSize = uint64(finfo.Size())
				}
			}

			if err == nil {
				err = s.walDir.Sync()
			}

			if err != nil && newLogFile != nil {
				newLogFile.Close()
			} else if err == nil {
				newLogFile = vfs.NewSyncingFile(newLogFile, vfs.SyncingFileOptions{
					BytesPerSync:    s.db.opts.WALBytesPerSync,
					PreallocateSize: s.walPreallocateSize(),
				})
			}

			if recycleOK {
				err = utils.FirstError(err, s.logRecycler.pop(recycleLog.fileNum))
			}

			s.db.opts.EventListener.WALCreated(WALCreateInfo{
				Index:           s.index,
				Path:            newLogName,
				FileNum:         newLogNum,
				RecycledFileNum: recycleLog.fileNum,
				Err:             err,
			})

			s.mu.Lock()
			s.mu.mem.switching = false
			s.mu.mem.cond.Broadcast()
		}

		if err != nil {
			s.db.opts.Logger.Errorf("panic: makeRoomForWrite err:%s", err)
			return err
		}

		if !s.db.opts.DisableWAL {
			s.mu.log.queue = append(s.mu.log.queue, fileInfo{fileNum: newLogNum, fileSize: newLogSize})
			s.mu.log.LogWriter = record.NewLogWriter(newLogFile, newLogNum)
			s.mu.log.LogWriter.SetMinSyncInterval(s.db.opts.WALMinSyncInterval)
		}

		immMem := s.mu.mem.mutable
		imm := s.mu.mem.queue[len(s.mu.mem.queue)-1]
		imm.logSize = prevLogSize
		imm.flushForced = imm.flushForced || (b == nil)

		if b != nil && b.flushable != nil {
			entry := newFlushableEntry(b.flushable, imm.logNum, b.SeqNum())
			entry.releaseMemAccounting = func() {}
			s.mu.mem.queue = append(s.mu.mem.queue, entry)
			imm.logNum = 0
		}

		var logSeqNum uint64
		if b != nil {
			logSeqNum = b.SeqNum()
			if b.flushable != nil {
				logSeqNum += uint64(b.Count())
			}
		} else {
			logSeqNum = atomic.LoadUint64(&s.db.meta.atomic.logSeqNum)
		}

		var entry *flushableEntry
		s.mu.mem.mutable, entry = s.newMemTable(newLogNum, logSeqNum)
		s.mu.mem.queue = append(s.mu.mem.queue, entry)
		s.updateReadState()
		if immMem.writerUnref() {
			s.maybeScheduleFlush(needReport)
		}
		force = false
	}
}

func (s *Bitower) newFlushWriter() (*flushBitowerWriter, error) {
	bitreeWriter, err := s.btree.NewBitreeWriter()
	if err != nil {
		return nil, err
	}

	w := &flushBitowerWriter{
		s:      s,
		writer: bitreeWriter,
	}

	return w, nil
}
