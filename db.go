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
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/bitforest"
	"github.com/zuoyebang/bitalosdb/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/cache"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/manual"
	"github.com/zuoyebang/bitalosdb/internal/record"
	"github.com/zuoyebang/bitalosdb/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

var (
	ErrNotFound            = base.ErrNotFound
	ErrClosed              = errors.New("bitalosdb: closed")
	ErrReadOnly            = errors.New("bitalosdb: read-only")
	errMemExceedDelPercent = errors.New("memtable exceed delpercent")
)

type Reader interface {
	Get(key []byte) (value []byte, closer func(), err error)
	NewIter(o *IterOptions) *Iterator
	Close() error
}

type Writer interface {
	Apply(batch *Batch, o *WriteOptions) error
	Delete(key []byte, o *WriteOptions) error
	LogData(data []byte, opts *WriteOptions) error
	Set(key, value []byte, o *WriteOptions) error
}

type DB struct {
	atomic struct {
		memTableCount    int64
		memTableReserved int64
		bytesFlushed     uint64
		bytesCompacted   uint64
		logSize          uint64
	}

	readState struct {
		sync.RWMutex
		val *readState
	}

	dirname             string
	walDirname          string
	opts                *Options
	optspool            *base.OptionsPool
	cmp                 Compare
	equal               Equal
	split               Split
	largeBatchThreshold int
	fileLock            io.Closer
	dataDir             vfs.File
	walDir              vfs.File
	commit              *commitPipeline
	logRecycler         logRecycler
	closed              atomic.Bool
	bf                  *bitforest.Bitforest
	dbState             *statemachine.DbStateMachine
	bgTasks             sync.WaitGroup
	bgTasksCh           chan struct{}
	iterSlowCount       atomic.Uint64
	cache               cache.ICache

	mu struct {
		sync.Mutex
		nextJobID int
		meta      *metaSet
		log       struct {
			queue   []fileInfo
			bytesIn uint64
			*record.LogWriter
		}

		mem struct {
			cond      sync.Cond
			mutable   *memTable
			queue     flushableList
			switching bool
			nextSize  int
		}

		compact struct {
			cond     sync.Cond
			flushing bool
		}

		cleaner struct {
			cond     sync.Cond
			cleaning bool
			disabled int
		}
	}

	timeNow    func() time.Time
	compressor compress.Compressor
}

var _ Reader = (*DB)(nil)
var _ Writer = (*DB)(nil)

func (d *DB) Get(key []byte) ([]byte, func(), error) {
	if d.IsClosed() {
		return nil, nil, ErrClosed
	}

	rs := d.loadReadState()

	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		mValue, mExist, kind := m.get(key)
		if mExist {
			if kind == InternalKeyKindSet {
				return mValue, func() { rs.unref() }, nil
			} else if kind == InternalKeyKindDelete {
				rs.unref()
				return nil, nil, ErrNotFound
			}
		}
	}

	rs.unref()

	bfValue, bfExist, bfCloser := d.bf.Get(key)
	if !bfExist {
		return nil, nil, ErrNotFound
	}

	return bfValue, bfCloser, nil
}

func (d *DB) Exist(key []byte) (bool, error) {
	if d.IsClosed() {
		return false, ErrClosed
	}

	rs := d.loadReadState()
	defer rs.unref()

	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		_, mExist, kind := m.get(key)
		if mExist {
			if kind == InternalKeyKindSet {
				return true, nil
			} else if kind == InternalKeyKindDelete {
				return false, ErrNotFound
			}
		}
	}

	bfExist := d.bf.Exist(key)
	if !bfExist {
		return false, ErrNotFound
	}

	return true, nil
}

func (d *DB) CheckKeySize(key []byte) error {
	if len(key) > consts.MaxKeySize || len(key) == 0 {
		return errors.New("invalid key size")
	}

	return nil
}

func (d *DB) Set(key, value []byte, opts *WriteOptions) error {
	if err := d.CheckKeySize(key); err != nil {
		return err
	}

	b := newBatch(d)
	_ = b.Set(key, value, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) Delete(key []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.Delete(key, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) LogData(data []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.LogData(data, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) Apply(batch *Batch, opts *WriteOptions) error {
	if d.IsClosed() {
		return ErrClosed
	}

	if atomic.LoadUint32(&batch.applied) != 0 {
		return errors.New("bitalosdb: batch already applied")
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	if batch.db != nil && batch.db != d {
		return errors.Errorf("bitalosdb: batch db mismatch: %p != %p", batch.db, d)
	}

	sync := opts.GetSync()
	if sync && d.opts.DisableWAL {
		return errors.New("bitalosdb: WAL disabled")
	}

	if batch.db == nil {
		batch.refreshMemTableSize()
	}
	if int(batch.memTableSize) >= d.largeBatchThreshold {
		batch.flushable = newFlushableBatch(batch, d.opts.Comparer)
	}
	if err := d.commit.Commit(batch, sync); err != nil {
		d.opts.Logger.Errorf("db apply err:%v", err)
		return errors.New("bitalosdb: Apply Commit err")
	}
	if batch.flushable != nil {
		batch.data = nil
	}
	return nil
}

func (d *DB) commitApply(b *Batch, mem *memTable) error {
	if b.flushable != nil {
		return nil
	}

	err := mem.apply(b, b.SeqNum())
	if err != nil {
		return err
	}

	if mem.writerUnref() {
		d.mu.Lock()
		d.maybeScheduleFlush(true)
		d.mu.Unlock()
	}
	return nil
}

func (d *DB) commitWrite(b *Batch, syncWG *sync.WaitGroup, syncErr *error) (*memTable, error) {
	var size int64
	repr := b.Repr()

	if b.flushable != nil {
		b.flushable.setSeqNum(b.SeqNum())
		if !d.opts.DisableWAL {
			var err error
			size, err = d.mu.log.SyncRecord(repr, syncWG, syncErr)
			if err != nil {
				d.opts.Logger.Errorf("panic: commitWrite flushable SyncRecord err:%v", err)
				return nil, err
			}
		}
	}

	d.mu.Lock()

	err := d.makeRoomForWrite(b, true)
	if err == nil && !d.opts.DisableWAL {
		d.mu.log.bytesIn += uint64(len(repr))
	}

	mem := d.mu.mem.mutable

	d.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if d.opts.DisableWAL {
		return mem, nil
	}

	if b.flushable == nil {
		size, err = d.mu.log.SyncRecord(repr, syncWG, syncErr)
		if err != nil {
			d.opts.Logger.Errorf("panic: commitWrite flushable nil SyncRecord err:%v", err)
			return nil, err
		}
	}

	atomic.StoreUint64(&d.atomic.logSize, uint64(size))
	return mem, err
}

type iterAlloc struct {
	dbi                 Iterator
	keyBuf              []byte
	prefixOrFullSeekKey []byte
	merging             mergingIter
	mlevels             [4]mergingIterLevel
}

var iterAllocPool = sync.Pool{
	New: func() interface{} {
		return &iterAlloc{}
	},
}

func (d *DB) newIterInternal(batch *Batch, o *IterOptions) *Iterator {
	if d.IsClosed() {
		return nil
	}

	readState := d.loadReadState()
	seqNum := atomic.LoadUint64(&d.mu.meta.atomic.visibleSeqNum)

	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		db:                  d,
		alloc:               buf,
		cmp:                 d.cmp,
		equal:               d.equal,
		iter:                &buf.merging,
		split:               d.split,
		readState:           readState,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		batch:               batch,
		seqNum:              seqNum,
	}
	if o != nil {
		dbi.opts = *o
	}
	dbi.opts.Logger = d.opts.Logger
	return finishInitializingIter(d, buf)
}

func finishInitializingIter(d *DB, buf *iterAlloc) *Iterator {
	dbi := &buf.dbi
	readState := dbi.readState
	batch := dbi.batch
	seqNum := dbi.seqNum
	memtables := readState.memtables

	mlevels := buf.mlevels[:0]

	numMergingLevels := 0

	if batch != nil {
		numMergingLevels++
	}
	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
			continue
		}
		numMergingLevels++
	}

	if numMergingLevels > cap(mlevels) {
		mlevels = make([]mergingIterLevel, 0, numMergingLevels)
	}

	if batch != nil {
		mlevels = append(mlevels, mergingIterLevel{
			iter: batch.newInternalIter(&dbi.opts),
		})
	}

	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
			continue
		}
		mlevels = append(mlevels, mergingIterLevel{
			iter: mem.newIter(&dbi.opts),
		})
	}

	bfIters := d.bf.NewIters(&dbi.opts)
	for i := len(bfIters) - 1; i >= 0; i-- {
		mlevels = append(mlevels, mergingIterLevel{
			iter: bfIters[i],
		})
	}

	buf.merging.init(&dbi.opts, dbi.cmp, dbi.split, mlevels...)
	buf.merging.snapshot = seqNum
	return dbi
}

func (d *DB) NewBatch() *Batch {
	return newBatch(d)
}

func (d *DB) NewIndexedBatch() *Batch {
	return newIndexedBatch(d, d.opts.Comparer)
}

func (d *DB) NewIter(o *IterOptions) *Iterator {
	return d.newIterInternal(nil, o)
}

func (d *DB) IsClosed() bool {
	return d.closed.Load()
}

func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.IsClosed() {
		return ErrClosed
	}

	d.closed.Store(true)
	d.bf.SetClosed()
	close(d.bgTasksCh)

	defer func() {
		if d.cache != nil {
			d.cache.Close()
		}
		d.optspool.Close()
		d.opts.Logger.Infof("bitalosdb closed")
	}()

	for d.mu.compact.flushing {
		d.mu.compact.cond.Wait()
	}

	var err error
	if !d.opts.ReadOnly {
		err = utils.FirstError(err, d.mu.log.Close())
	} else if d.mu.log.LogWriter != nil {
		err = errors.New("log-writer should be nil in read-only mode")
	}
	err = utils.FirstError(err, d.fileLock.Close())

	err = utils.FirstError(err, d.dataDir.Close())
	if d.dataDir != d.walDir {
		err = utils.FirstError(err, d.walDir.Close())
	}

	d.readState.val.unrefLocked()

	for _, mem := range d.mu.mem.queue {
		mem.readerUnref()
	}
	if reserved := atomic.LoadInt64(&d.atomic.memTableReserved); reserved != 0 {
		err = utils.FirstError(err, errors.Errorf("leaked memtable reservation:%d", reserved))
	}

	for d.mu.cleaner.cleaning {
		d.mu.cleaner.cond.Wait()
	}

	d.mu.Unlock()

	d.bgTasks.Wait()
	err = utils.FirstError(err, d.bf.Close())
	err = utils.FirstError(err, d.mu.meta.close())

	d.mu.Lock()

	return err
}

func (d *DB) Flush() error {
	flushDone, err := d.AsyncFlush()
	if err != nil {
		return err
	}
	if flushDone != nil {
		<-flushDone
	}
	return nil
}

func (d *DB) AsyncFlush() (<-chan struct{}, error) {
	if d.IsClosed() {
		return nil, ErrClosed
	}

	if d.opts.ReadOnly {
		return nil, ErrReadOnly
	}

	d.commit.mu.Lock()
	defer d.commit.mu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	empty := true
	for i := range d.mu.mem.queue {
		if !d.mu.mem.queue[i].empty() {
			empty = false
			break
		}
	}
	if empty {
		return nil, nil
	}
	flushed := d.mu.mem.queue[len(d.mu.mem.queue)-1].flushed
	err := d.makeRoomForWrite(nil, false)
	if err != nil {
		return nil, err
	}
	return flushed, nil
}

func (d *DB) walPreallocateSize() int {
	size := d.opts.MemTableSize
	size = (size / 10) + size
	return size
}

func (d *DB) newMemTable(logNum FileNum, logSeqNum uint64) (*memTable, *flushableEntry) {
	size := d.mu.mem.nextSize
	if d.mu.mem.nextSize < d.opts.MemTableSize {
		d.mu.mem.nextSize *= 2
		if d.mu.mem.nextSize > d.opts.MemTableSize {
			d.mu.mem.nextSize = d.opts.MemTableSize
		}
	}

	atomic.AddInt64(&d.atomic.memTableCount, 1)
	atomic.AddInt64(&d.atomic.memTableReserved, int64(size))

	mem := newMemTable(memTableOptions{
		Options:   d.opts,
		arenaBuf:  manual.New(int(size)),
		logSeqNum: logSeqNum,
	})

	invariants.SetFinalizer(mem, checkMemTable)

	entry := d.newFlushableEntry(mem, logNum, logSeqNum)
	entry.releaseMemAccounting = func() {
		manual.Free(mem.arenaBuf)
		mem.arenaBuf = nil
		atomic.AddInt64(&d.atomic.memTableCount, -1)
		atomic.AddInt64(&d.atomic.memTableReserved, -int64(size))
	}
	return mem, entry
}

func (d *DB) newFlushableEntry(f flushable, logNum FileNum, logSeqNum uint64) *flushableEntry {
	entry := &flushableEntry{
		flushable: f,
		flushed:   make(chan struct{}),
		logNum:    logNum,
		logSeqNum: logSeqNum,
	}
	entry.readerRefs.Store(1)
	return entry
}

func (d *DB) makeRoomForWrite(b *Batch, needReport bool) error {
	force := b == nil || b.flushable != nil
	stalled := false
	for {
		if d.mu.mem.switching {
			d.mu.mem.cond.Wait()
			continue
		}
		if b != nil && b.flushable == nil {
			err := d.mu.mem.mutable.prepare(b, true)
			if err != arenaskl.ErrArenaFull && err != errMemExceedDelPercent {
				if stalled {
					d.opts.EventListener.WriteStallEnd()
				}
				return err
			}
		} else if !force {
			if stalled {
				d.opts.EventListener.WriteStallEnd()
			}
			return nil
		}

		{
			var size uint64
			for i := range d.mu.mem.queue {
				size += d.mu.mem.queue[i].totalBytes()
			}
			if size >= uint64(d.opts.MemTableStopWritesThreshold)*uint64(d.opts.MemTableSize) {

				if !stalled {
					stalled = true
					d.opts.EventListener.WriteStallBegin(WriteStallBeginInfo{
						Reason: "memtable count limit reached",
					})
				}
				d.mu.compact.cond.Wait()
				continue
			}
		}

		var newLogNum FileNum
		var newLogFile vfs.File
		var newLogSize uint64
		var prevLogSize uint64
		var err error

		if !d.opts.DisableWAL {
			jobID := d.mu.nextJobID
			d.mu.nextJobID++
			newLogNum = d.mu.meta.getNextFileNum()
			d.mu.mem.switching = true
			prevLogSize = uint64(d.mu.log.Size())

			if d.mu.log.queue[len(d.mu.log.queue)-1].fileSize < prevLogSize {
				d.mu.log.queue[len(d.mu.log.queue)-1].fileSize = prevLogSize
			}

			d.mu.Unlock()

			err = d.mu.log.Close()
			newLogName := base.MakeFilepath(d.opts.FS, d.walDirname, fileTypeLog, newLogNum)

			var recycleLog fileInfo
			var recycleOK bool
			if err == nil {
				recycleLog, recycleOK = d.logRecycler.peek()
				if recycleOK {
					recycleLogName := base.MakeFilepath(d.opts.FS, d.walDirname, fileTypeLog, recycleLog.fileNum)
					newLogFile, err = d.opts.FS.ReuseForWrite(recycleLogName, newLogName)
				} else {
					newLogFile, err = d.opts.FS.Create(newLogName)
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
				err = d.walDir.Sync()
			}

			if err != nil && newLogFile != nil {
				newLogFile.Close()
			} else if err == nil {
				newLogFile = vfs.NewSyncingFile(newLogFile, vfs.SyncingFileOptions{
					BytesPerSync:    d.opts.WALBytesPerSync,
					PreallocateSize: d.walPreallocateSize(),
				})
			}

			if recycleOK {
				err = utils.FirstError(err, d.logRecycler.pop(recycleLog.fileNum))
			}

			d.opts.EventListener.WALCreated(WALCreateInfo{
				JobID:           jobID,
				Path:            newLogName,
				FileNum:         newLogNum,
				RecycledFileNum: recycleLog.fileNum,
				Err:             err,
			})

			d.mu.Lock()
			d.mu.mem.switching = false
			d.mu.mem.cond.Broadcast()
		}

		if err != nil {
			d.opts.Logger.Errorf("panic: makeRoomForWrite err:%s", err)
			return err
		}

		if !d.opts.DisableWAL {
			d.mu.log.queue = append(d.mu.log.queue, fileInfo{fileNum: newLogNum, fileSize: newLogSize})
			d.mu.log.LogWriter = record.NewLogWriter(newLogFile, newLogNum)
			d.mu.log.LogWriter.SetMinSyncInterval(d.opts.WALMinSyncInterval)
		}

		immMem := d.mu.mem.mutable
		imm := d.mu.mem.queue[len(d.mu.mem.queue)-1]
		imm.logSize = prevLogSize
		imm.flushForced = imm.flushForced || (b == nil)

		if (b == nil) && uint64(immMem.availBytes()) > immMem.totalBytes()/2 {
			d.mu.mem.nextSize = int(immMem.totalBytes())
		}

		if b != nil && b.flushable != nil {
			entry := d.newFlushableEntry(b.flushable, imm.logNum, b.SeqNum())
			entry.releaseMemAccounting = func() {}
			d.mu.mem.queue = append(d.mu.mem.queue, entry)
			imm.logNum = 0
		}

		var logSeqNum uint64
		if b != nil {
			logSeqNum = b.SeqNum()
			if b.flushable != nil {
				logSeqNum += uint64(b.Count())
			}
		} else {
			logSeqNum = atomic.LoadUint64(&d.mu.meta.atomic.logSeqNum)
		}

		var entry *flushableEntry
		d.mu.mem.mutable, entry = d.newMemTable(newLogNum, logSeqNum)
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
		d.updateReadStateLocked()
		if immMem.writerUnref() {
			d.maybeScheduleFlush(needReport)
		}
		force = false
	}
}

func (d *DB) getEarliestUnflushedSeqNumLocked() uint64 {
	seqNum := InternalKeySeqNumMax
	for i := range d.mu.mem.queue {
		logSeqNum := d.mu.mem.queue[i].logSeqNum
		if seqNum > logSeqNum {
			seqNum = logSeqNum
		}
	}
	return seqNum
}

func (d *DB) LockTask() {
	d.dbState.LockTask()
}

func (d *DB) UnlockTask() {
	d.dbState.UnlockTask()
}

func (d *DB) Id() int {
	return d.opts.Id
}

func (d *DB) CheckIterReadAmplification(jobId int) {
	slowCount := d.iterSlowCount.Load()
	isFlush := false
	if slowCount > consts.IterReadAmplificationThreshold {
		d.iterSlowCount.Store(0)
		_ = d.Flush()
		isFlush = true
	}

	if slowCount > 0 && jobId%50 == 0 {
		d.opts.Logger.Infof("[READAMP %d] iterator check slowCount:%d isFlush:%v", jobId, slowCount, isFlush)
	}
}
