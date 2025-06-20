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

package record

import (
	"context"
	"encoding/binary"
	"io"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/crc"

	"github.com/zuoyebang/bitalosdb/internal/errors"
)

var walSyncLabels = pprof.Labels("bitalosdb", "wal-sync")

type block struct {
	// buf[:written] has already been filled with fragments. Updated atomically.
	written int32
	// buf[:flushed] has already been flushed to w.
	flushed int32
	buf     [blockSize]byte
}

type flusher interface {
	Flush() error
}

type syncer interface {
	Sync() error
}

const (
	syncConcurrencyBits = 9

	// SyncConcurrency is the maximum number of concurrent sync operations that
	// can be performed. Note that a sync operation is initiated either by a call
	// to SyncRecord or by a call to Close. Exported as this value also limits
	// the commit concurrency in commitPipeline.
	SyncConcurrency = 1 << syncConcurrencyBits
)

type syncSlot struct {
	wg  *sync.WaitGroup
	err *error
}

// syncQueue is a lock-free fixed-size single-producer, single-consumer
// queue. The single-producer can push to the head, and the single-consumer can
// pop multiple values from the tail. Popping calls Done() on each of the
// available *sync.WaitGroup elements.
type syncQueue struct {
	// headTail packs together a 32-bit head index and a 32-bit tail index. Both
	// are indexes into slots modulo len(slots)-1.
	//
	// tail = index of oldest data in queue
	// head = index of next slot to fill
	//
	// Slots in the range [tail, head) are owned by consumers.  A consumer
	// continues to own a slot outside this range until it nils the slot, at
	// which point ownership passes to the producer.
	//
	// The head index is stored in the most-significant bits so that we can
	// atomically add to it and the overflow is harmless.
	headTail uint64

	// slots is a ring buffer of values stored in this queue. The size must be a
	// power of 2. A slot is in use until the tail index has moved beyond it.
	slots [SyncConcurrency]syncSlot

	// blocked is an atomic boolean which indicates whether syncing is currently
	// blocked or can proceed. It is used by the implementation of
	// min-sync-interval to block syncing until the min interval has passed.
	blocked uint32
}

const dequeueBits = 32

func (q *syncQueue) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (q *syncQueue) push(wg *sync.WaitGroup, err *error) {
	ptrs := atomic.LoadUint64(&q.headTail)
	head, tail := q.unpack(ptrs)
	if (tail+uint32(len(q.slots)))&(1<<dequeueBits-1) == head {
		panic("bitalosdb: queue is full")
	}

	slot := &q.slots[head&uint32(len(q.slots)-1)]
	slot.wg = wg
	slot.err = err

	// Increment head. This passes ownership of slot to dequeue and acts as a
	// store barrier for writing the slot.
	atomic.AddUint64(&q.headTail, 1<<dequeueBits)
}

func (q *syncQueue) setBlocked() {
	atomic.StoreUint32(&q.blocked, 1)
}

func (q *syncQueue) clearBlocked() {
	atomic.StoreUint32(&q.blocked, 0)
}

func (q *syncQueue) empty() bool {
	head, tail := q.load()
	return head == tail
}

func (q *syncQueue) load() (head, tail uint32) {
	if atomic.LoadUint32(&q.blocked) == 1 {
		return 0, 0
	}

	ptrs := atomic.LoadUint64(&q.headTail)
	head, tail = q.unpack(ptrs)
	return head, tail
}

func (q *syncQueue) pop(head, tail uint32, err error) error {
	if tail == head {
		// Queue is empty.
		return nil
	}

	for ; tail != head; tail++ {
		slot := &q.slots[tail&uint32(len(q.slots)-1)]
		wg := slot.wg
		if wg == nil {
			return errors.Errorf("nil waiter at %v", tail&uint32(len(q.slots)-1))
		}
		*slot.err = err
		slot.wg = nil
		slot.err = nil
		// We need to bump the tail count before signalling the wait group as
		// signalling the wait group can trigger release a blocked goroutine which
		// will try to enqueue before we've "freed" space in the queue.
		atomic.AddUint64(&q.headTail, 1)
		wg.Done()
	}

	return nil
}

// flusherCond is a specialized condition variable that allows its condition to
// change and readiness be signalled without holding its associated mutex. In
// particular, when a waiter is added to syncQueue atomically, this condition
// variable can be signalled without holding flusher.Mutex.
type flusherCond struct {
	mu   *sync.Mutex
	q    *syncQueue
	cond sync.Cond
}

func (c *flusherCond) init(mu *sync.Mutex, q *syncQueue) {
	c.mu = mu
	c.q = q
	// Yes, this is a bit circular, but that is intentional. flusherCond.cond.L
	// points flusherCond so that when cond.L.Unlock is called flusherCond.Unlock
	// will be called and we can check the !syncQueue.empty() condition.
	c.cond.L = c
}

func (c *flusherCond) Signal() {
	// Pass-through to the cond var.
	c.cond.Signal()
}

func (c *flusherCond) Wait() {
	// Pass-through to the cond var. Note that internally the cond var implements
	// Wait as:
	//
	//   t := notifyListAdd()
	//   L.Unlock()
	//   notifyListWait(t)
	//   L.Lock()
	//
	// We've configured the cond var to call flusherReady.Unlock() which allows
	// us to check the !syncQueue.empty() condition without a danger of missing a
	// notification. Any call to flusherReady.Signal() after notifyListAdd() is
	// called will cause the subsequent notifyListWait() to return immediately.
	c.cond.Wait()
}

func (c *flusherCond) Lock() {
	c.mu.Lock()
}

func (c *flusherCond) Unlock() {
	c.mu.Unlock()
	if !c.q.empty() {
		// If the current goroutine is about to block on sync.Cond.Wait, this call
		// to Signal will prevent that. The comment in Wait above explains a bit
		// about what is going on here, but it is worth reiterating:
		//
		//   flusherCond.Wait()
		//     sync.Cond.Wait()
		//       t := notifyListAdd()
		//       flusherCond.Unlock()    <-- we are here
		//       notifyListWait(t)
		//       flusherCond.Lock()
		//
		// The call to Signal here results in:
		//
		//     sync.Cond.Signal()
		//       notifyListNotifyOne()
		//
		// The call to notifyListNotifyOne() will prevent the call to
		// notifyListWait(t) from blocking.
		c.cond.Signal()
	}
}

type durationFunc func() time.Duration

// syncTimer is an interface for timers, modeled on the closure callback mode
// of time.Timer. See time.AfterFunc and LogWriter.afterFunc. syncTimer is used
// by tests to mock out the timer functionality used to implement
// min-sync-interval.
type syncTimer interface {
	Reset(time.Duration) bool
	Stop() bool
}

// LogWriter writes records to an underlying io.Writer. In order to support WAL
// file reuse, a LogWriter's records are tagged with the WAL's file
// number. When reading a log file a record from a previous incarnation of the
// file will return the error ErrInvalidLogNum.
type LogWriter struct {
	// w is the underlying writer.
	w io.Writer
	// c is w as a closer.
	c io.Closer
	// s is w as a syncer.
	s syncer
	// logNum is the low 32-bits of the log's file number.
	logNum uint32
	// blockNum is the zero based block number for the current block.
	blockNum int64
	// err is any accumulated error.
	err error
	// block is the current block being written. Protected by flusher.Mutex.
	block *block
	free  struct {
		sync.Mutex
		// Condition variable used to signal a block is freed.
		cond      sync.Cond
		blocks    []*block
		allocated int
	}

	flusher struct {
		sync.Mutex
		// Flusher ready is a condition variable that is signalled when there are
		// blocks to flush, syncing has been requested, or the LogWriter has been
		// closed. For signalling of a sync, it is safe to call without holding
		// flusher.Mutex.
		ready flusherCond
		// Set to true when the flush loop should be closed.
		close bool
		// Closed when the flush loop has terminated.
		closed chan struct{}
		// Accumulated flush error.
		err error
		// minSyncInterval is the minimum duration between syncs.
		minSyncInterval durationFunc
		pending         []*block
		syncQ           syncQueue
	}

	// afterFunc is a hook to allow tests to mock out the timer functionality
	// used for min-sync-interval. In normal operation this points to
	// time.AfterFunc.
	afterFunc func(d time.Duration, f func()) syncTimer
}

// NewLogWriter returns a new LogWriter.
func NewLogWriter(w io.Writer, logNum base.FileNum) *LogWriter {
	c, _ := w.(io.Closer)
	s, _ := w.(syncer)
	r := &LogWriter{
		w: w,
		c: c,
		s: s,
		// NB: we truncate the 64-bit log number to 32-bits. This is ok because a)
		// we are very unlikely to reach a file number of 4 billion and b) the log
		// number is used as a validation check and using only the low 32-bits is
		// sufficient for that purpose.
		logNum: uint32(logNum),
		afterFunc: func(d time.Duration, f func()) syncTimer {
			return time.AfterFunc(d, f)
		},
	}
	r.free.cond.L = &r.free.Mutex
	r.free.blocks = make([]*block, 0, 16)
	r.free.allocated = 1
	r.block = &block{}
	r.flusher.ready.init(&r.flusher.Mutex, &r.flusher.syncQ)
	r.flusher.closed = make(chan struct{})
	r.flusher.pending = make([]*block, 0, cap(r.free.blocks))
	go func() {
		pprof.Do(context.Background(), walSyncLabels, r.flushLoop)
	}()
	return r
}

// SetMinSyncInterval sets the closure to invoke for retrieving the minimum
// sync duration between syncs.
func (w *LogWriter) SetMinSyncInterval(minSyncInterval durationFunc) {
	f := &w.flusher
	f.Lock()
	f.minSyncInterval = minSyncInterval
	f.Unlock()
}

func (w *LogWriter) flushLoop(context.Context) {
	f := &w.flusher
	f.Lock()

	var syncTimer syncTimer
	defer func() {
		if syncTimer != nil {
			syncTimer.Stop()
		}
		close(f.closed)
		f.Unlock()
	}()

	// The flush loop performs flushing of full and partial data blocks to the
	// underlying writer (LogWriter.w), syncing of the writer, and notification
	// to sync requests that they have completed.
	//
	// - flusher.ready is a condition variable that is signalled when there is
	//   work to do. Full blocks are contained in flusher.pending. The current
	//   partial block is in LogWriter.block. And sync operations are held in
	//   flusher.syncQ.
	//
	// - The decision to sync is determined by whether there are any sync
	//   requests present in flusher.syncQ and whether enough time has elapsed
	//   since the last sync. If not enough time has elapsed since the last sync,
	//   flusher.syncQ.blocked will be set to 1. If syncing is blocked,
	//   syncQueue.empty() will return true and syncQueue.load() will return 0,0
	//   (i.e. an empty list).
	//
	// - flusher.syncQ.blocked is cleared by a timer that is initialized when
	//   blocked is set to 1. When blocked is 1, no syncing will take place, but
	//   flushing will continue to be performed. The on/off toggle for syncing
	//   does not need to be carefully synchronized with the rest of processing
	//   -- all we need to ensure is that after any transition to blocked=1 there
	//   is eventually a transition to blocked=0. syncTimer performs this
	//   transition. Note that any change to min-sync-interval will not take
	//   effect until the previous timer elapses.
	//
	// - Picking up the syncing work to perform requires coordination with
	//   picking up the flushing work. Specifically, flushing work is queued
	//   before syncing work. The guarantee of this code is that when a sync is
	//   requested, any previously queued flush work will be synced. This
	//   motivates reading the syncing work (f.syncQ.load()) before picking up
	//   the flush work (atomic.LoadInt32(&w.block.written)).

	// The list of full blocks that need to be written. This is copied from
	// f.pending on every loop iteration, though the number of elements is small
	// (usually 1, max 16).
	pending := make([]*block, 0, cap(f.pending))

	for {
		for {
			// Grab the portion of the current block that requires flushing. Note that
			// the current block can be added to the pending blocks list after we release
			// the flusher lock, but it won't be part of pending.
			written := atomic.LoadInt32(&w.block.written)
			if len(f.pending) > 0 || written > w.block.flushed || !f.syncQ.empty() {
				break
			}
			if f.close {
				// If the writer is closed, pretend the sync timer fired immediately so
				// that we can process any queued sync requests.
				f.syncQ.clearBlocked()
				if !f.syncQ.empty() {
					break
				}
				return
			}
			f.ready.Wait()
			continue
		}

		pending = pending[:len(f.pending)]
		copy(pending, f.pending)
		f.pending = f.pending[:0]

		// Grab the list of sync waiters. Note that syncQueue.load() will return
		// 0,0 while we're waiting for the min-sync-interval to expire. This
		// allows flushing to proceed even if we're not ready to sync.
		head, tail := f.syncQ.load()

		// Grab the portion of the current block that requires flushing. Note that
		// the current block can be added to the pending blocks list after we
		// release the flusher lock, but it won't be part of pending. This has to
		// be ordered after we get the list of sync waiters from syncQ in order to
		// prevent a race where a waiter adds itself to syncQ, but this thread
		// picks up the entry in syncQ and not the buffered data.
		written := atomic.LoadInt32(&w.block.written)
		data := w.block.buf[w.block.flushed:written]
		w.block.flushed = written

		// If flusher has an error, we propagate it to waiters. Note in spite of
		// error we consume the pending list above to free blocks for writers.
		if f.err != nil {
			f.syncQ.pop(head, tail, f.err)
			continue
		}
		f.Unlock()
		synced, err := w.flushPending(data, pending, head, tail)
		f.Lock()
		f.err = err
		if f.err != nil {
			f.syncQ.clearBlocked()
			continue
		}

		if synced && f.minSyncInterval != nil {
			// A sync was performed. Make sure we've waited for the min sync
			// interval before syncing again.
			if min := f.minSyncInterval(); min > 0 {
				f.syncQ.setBlocked()
				if syncTimer == nil {
					syncTimer = w.afterFunc(min, func() {
						f.syncQ.clearBlocked()
						f.ready.Signal()
					})
				} else {
					syncTimer.Reset(min)
				}
			}
		}
	}
}

func (w *LogWriter) flushPending(
	data []byte, pending []*block, head, tail uint32,
) (synced bool, err error) {
	defer func() {
		// Translate panics into errors. The errors will cause flushLoop to shut
		// down, but allows us to do so in a controlled way and avoid swallowing
		// the stack that created the panic if panic'ing itself hits a panic
		// (e.g. unlock of unlocked mutex).
		if r := recover(); r != nil {
			err = errors.Errorf("%v", r)
		}
	}()

	for _, b := range pending {
		if err = w.flushBlock(b); err != nil {
			break
		}
	}
	if err == nil && len(data) > 0 {
		_, err = w.w.Write(data)
	}

	synced = head != tail
	if synced {
		if err == nil && w.s != nil {
			err = w.s.Sync()
		}
		f := &w.flusher
		if popErr := f.syncQ.pop(head, tail, err); popErr != nil {
			return synced, popErr
		}
	}

	return synced, err
}

func (w *LogWriter) flushBlock(b *block) error {
	if _, err := w.w.Write(b.buf[b.flushed:]); err != nil {
		return err
	}
	b.written = 0
	b.flushed = 0
	w.free.Lock()
	w.free.blocks = append(w.free.blocks, b)
	w.free.cond.Signal()
	w.free.Unlock()
	return nil
}

// queueBlock queues the current block for writing to the underlying writer,
// allocates a new block and reserves space for the next header.
func (w *LogWriter) queueBlock() {
	// Allocate a new block, blocking until one is available. We do this first
	// because w.block is protected by w.flusher.Mutex.
	w.free.Lock()
	if len(w.free.blocks) == 0 {
		if w.free.allocated < cap(w.free.blocks) {
			w.free.allocated++
			w.free.blocks = append(w.free.blocks, &block{})
		} else {
			for len(w.free.blocks) == 0 {
				w.free.cond.Wait()
			}
		}
	}
	nextBlock := w.free.blocks[len(w.free.blocks)-1]
	w.free.blocks = w.free.blocks[:len(w.free.blocks)-1]
	w.free.Unlock()

	f := &w.flusher
	f.Lock()
	f.pending = append(f.pending, w.block)
	w.block = nextBlock
	f.ready.Signal()
	w.err = w.flusher.err
	f.Unlock()

	w.blockNum++
}

// Close flushes and syncs any unwritten data and closes the writer.
// Where required, external synchronisation is provided by commitPipeline.mu.
func (w *LogWriter) Close() error {
	f := &w.flusher

	// Emit an EOF trailer signifying the end of this log. This helps readers
	// differentiate between a corrupted entry in the middle of a log from
	// garbage at the tail from a recycled log file.
	w.emitEOFTrailer()

	// Signal the flush loop to close.
	f.Lock()
	f.close = true
	f.ready.Signal()
	f.Unlock()

	// Wait for the flush loop to close. The flush loop will not close until all
	// pending data has been written or an error occurs.
	<-f.closed

	// Sync any flushed data to disk. NB: flushLoop will sync after flushing the
	// last buffered data only if it was requested via syncQ, so we need to sync
	// here to ensure that all the data is synced.
	err := w.flusher.err
	if err == nil && w.s != nil {
		err = w.s.Sync()
	}

	if w.c != nil {
		cerr := w.c.Close()
		w.c = nil
		if cerr != nil {
			return cerr
		}
	}
	w.err = errors.New("bitalosdb/record: closed LogWriter")
	return err
}

// WriteRecord writes a complete record. Returns the offset just past the end
// of the record.
// External synchronisation provided by commitPipeline.mu.
func (w *LogWriter) WriteRecord(p []byte) (int64, error) {
	return w.SyncRecord(p, nil, nil)
}

// SyncRecord writes a complete record. If wg!= nil the record will be
// asynchronously persisted to the underlying writer and done will be called on
// the wait group upon completion. Returns the offset just past the end of the
// record.
// External synchronisation provided by commitPipeline.mu.
func (w *LogWriter) SyncRecord(p []byte, wg *sync.WaitGroup, err *error) (int64, error) {
	if w.err != nil {
		return -1, w.err
	}

	// The `i == 0` condition ensures we handle empty records. Such records can
	// possibly be generated for VersionEdits stored in the MANIFEST. While the
	// MANIFEST is currently written using Writer, it is good to support the same
	// semantics with LogWriter.

	for i := 0; i == 0 || len(p) > 0; i++ {
		p = w.emitFragment(i, p)
	}

	if wg != nil {
		// If we've been asked to persist the record, add the WaitGroup to the sync
		// queue and signal the flushLoop. Note that flushLoop will write partial
		// blocks to the file if syncing has been requested. The contract is that
		// any record written to the LogWriter to this point will be flushed to the
		// OS and synced to disk.
		f := &w.flusher
		f.syncQ.push(wg, err)
		f.ready.Signal()
	}

	offset := w.blockNum*blockSize + int64(w.block.written)
	// Note that we don't return w.err here as a concurrent call to Close would
	// race with our read. That's ok because the only error we could be seeing is
	// one to syncing for which the caller can receive notification of by passing
	// in a non-nil err argument.
	return offset, nil
}

// Size returns the current size of the file.
// External synchronisation provided by commitPipeline.mu.
func (w *LogWriter) Size() int64 {
	return w.blockNum*blockSize + int64(w.block.written)
}

func (w *LogWriter) emitEOFTrailer() {
	// Write a recyclable chunk header with a different log number.  Readers
	// will treat the header as EOF when the log number does not match.
	b := w.block
	i := b.written
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], 0) // CRC
	binary.LittleEndian.PutUint16(b.buf[i+4:i+6], 0) // Size
	b.buf[i+6] = recyclableFullChunkType
	binary.LittleEndian.PutUint32(b.buf[i+7:i+11], w.logNum+1) // Log number
	atomic.StoreInt32(&b.written, i+int32(recyclableHeaderSize))
}

func (w *LogWriter) emitFragment(n int, p []byte) []byte {
	b := w.block
	i := b.written
	first := n == 0
	last := blockSize-i-recyclableHeaderSize >= int32(len(p))

	if last {
		if first {
			b.buf[i+6] = recyclableFullChunkType
		} else {
			b.buf[i+6] = recyclableLastChunkType
		}
	} else {
		if first {
			b.buf[i+6] = recyclableFirstChunkType
		} else {
			b.buf[i+6] = recyclableMiddleChunkType
		}
	}

	binary.LittleEndian.PutUint32(b.buf[i+7:i+11], w.logNum)

	r := copy(b.buf[i+recyclableHeaderSize:], p)
	j := i + int32(recyclableHeaderSize+r)
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], crc.New(b.buf[i+6:j]).Value())
	binary.LittleEndian.PutUint16(b.buf[i+4:i+6], uint16(r))
	atomic.StoreInt32(&b.written, j)

	if blockSize-b.written < recyclableHeaderSize {
		// There is no room for another fragment in the block, so fill the
		// remaining bytes with zeros and queue the block for flushing.
		for i := b.written; i < blockSize; i++ {
			b.buf[i] = 0
		}
		w.queueBlock()
	}
	return p[r:]
}
