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
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/bitree"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/humanize"
	"github.com/zuoyebang/bitalosdb/v2/internal/iterator"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

type flushable interface {
	get(k []byte) ([]byte, bool, base.InternalKeyKind)
	exist(k []byte) (bool, base.InternalKeyKind)
	newIter(o *IterOptions) InternalKKVIterator
	newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator
	inuseBytes() uint64
	totalBytes() uint64
	empty() bool
	readyForFlush() bool
	getId() int
}

type memFlushableEntry struct {
	flushable
	flushed    chan struct{}
	obsolete   bool
	readerRefs atomic.Int32
	release    func()
}

func (e *memFlushableEntry) readerRef() {
	e.readerRefs.Add(1)
}

func (e *memFlushableEntry) readerUnref() {
	if e.readerRefs.Add(-1) == 0 {
		if e.release != nil {
			e.release()
			e.release = nil
		}
	}
}

type memFlushableList []*memFlushableEntry

func (e *memFlushableEntry) setObsolete() {
	e.obsolete = true
}

func newMemFlushableEntry(f flushable) *memFlushableEntry {
	entry := &memFlushableEntry{
		flushable: f,
		flushed:   make(chan struct{}),
	}
	entry.readerRefs.Store(1)
	return entry
}

func (ms *memTableShard) maybeScheduleFlush(isTask, isForce bool) {
	if ms.mem.compact.flushing || ms.db.IsClosed() || len(ms.mem.queue) <= 1 {
		return
	}

	ms.mem.compact.flushing = true

	if isTask {
		ms.db.memFlushTask.PushTask(&bitask.MemTableFlushTaskData{
			Index:   ms.index,
			IsForce: isForce,
		})
	} else {
		go ms.flush(false, isForce)
	}
}

func (ms *memTableShard) flush(isTask, isForce bool) {
	defer func() {
		if r := recover(); r != any(nil) {
			ms.db.opts.Logger.Errorf("%s flush panic error:%v stack:%s", ms.logTag, r, string(debug.Stack()))
		}
	}()

	ms.mem.Lock()
	defer ms.mem.Unlock()

	defer func() {
		ms.mem.compact.flushing = false
		ms.maybeScheduleFlush(isTask, false)
		ms.mem.compact.cond.Broadcast()
	}()

	var n int
	for ; n < len(ms.mem.queue)-1; n++ {
		if !ms.mem.queue[n].readyForFlush() {
			break
		}
	}
	if n == 0 {
		return
	}

	var flushed memFlushableList
	err := ms.runFlush(ms.mem.queue[:n], true, isForce)
	if err == nil {
		flushed = ms.mem.queue[:n]
		ms.mem.queue = ms.mem.queue[n:]
		ms.updateReadState()
	} else {
		ms.db.opts.Logger.Infof("%s flush error:%s", ms.logTag, err)
	}

	ms.mem.Unlock()
	defer ms.mem.Lock()

	for i := range flushed {
		flushed[i].readerUnref()
		close(flushed[i].flushed)
	}
}

func (ms *memTableShard) runFlush(flushing memFlushableList, checkFlush, isForce bool) (err error) {
	ms.mem.Unlock()
	defer ms.mem.Lock()

	var (
		flushIter           internalIterator
		bytesIterated       uint64
		bytesWritten        int64
		keyWritten          int64
		keyPrefixDeleteKind int64
		lastKeyVersion      uint64
	)

	flushNum := len(flushing)
	if flushNum == 1 {
		if flushing[0].empty() {
			return nil
		}

		flushIter = flushing[0].newFlushIter(nil, &bytesIterated)
	} else {
		iters := make([]internalIterator, 0, flushNum)
		for i := range flushing {
			if flushing[i].empty() {
				continue
			}
			iters = append(iters, flushing[i].newFlushIter(nil, &bytesIterated))
		}

		if len(iters) == 0 {
			return nil
		}

		flushIter = iterator.NewMergingIter(ms.db.opts.Logger, ms.db.cmp, iters...)
	}

	iter := &compactionIter{
		cmp:  ms.db.cmp,
		iter: flushIter,
	}
	defer func() {
		err = utils.FirstError(err, iter.Close())
	}()

	ms.db.dbState.SetMemTableHighPriority(ms.index, true)
	ms.db.dbState.LockKKVMemTableWrite(ms.index)
	defer func() {
		ms.db.dbState.SetMemTableHighPriority(ms.index, false)
		ms.db.dbState.UnlockKKVMemTableWrite(ms.index)
	}()

	if err = ms.db.memFlusher.flushStart(); err != nil {
		return err
	}

	defer func() {
		err = ms.db.memFlusher.flushFinish(checkFlush, isForce)
	}()

	logTag := fmt.Sprintf("%s flushing %d memtable to bitree", ms.logTag, flushNum)
	startTime := time.Now()
	ms.db.opts.Logger.Infof("%s start", logTag)
	defer func() {
		costTime := time.Since(startTime)
		cost := costTime.Seconds()
		ms.db.flushStats.setFlushMemTableTime(costTime.Nanoseconds())
		ms.db.opts.Logger.Infof("%s done iterated(%s) written(%s) keys(%d) keysPdKind(%d), in %.3fs, output rate %s/s",
			logTag,
			humanize.Uint64(bytesIterated),
			humanize.Int64(bytesWritten),
			keyWritten,
			keyPrefixDeleteKind,
			cost,
			humanize.Uint64(uint64(float64(bytesWritten)/cost)))
	}()

	checkKeyPrefixDelete := func(keyVersion uint64) bool {
		switch lastKeyVersion {
		case 0:
			return false
		case keyVersion:
			return true
		default:
			lastKeyVersion = 0
			return false
		}
	}

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		switch key.Kind() {
		case InternalKeyKindSet, InternalKeyKindDelete:
			if checkKeyPrefixDelete(kkv.DecodeKeyVersion(key.UserKey)) {
				continue
			}
		case InternalKeyKindPrefixDelete:
			lastKeyVersion = kkv.DecodeKeyVersion(key.UserKey)
			keyPrefixDeleteKind++
		default:
			continue
		}

		if err = ms.db.memFlusher.set(*key, val); err != nil {
			ms.db.opts.Logger.Fatalf("%s set fail key:%s err:%s", logTag, string(key.UserKey), err)
			continue
		}

		bytesWritten += int64(key.Size() + len(val))
		keyWritten++
	}

	return nil
}

type memFlushWriter struct {
	db      *DB
	mu      sync.Mutex
	writers map[uint16]*bitree.BitreeWriter
	err     error
}

func (w *memFlushWriter) set(key base.InternalKey, val []byte) error {
	slotId := kkv.DecodeSlotId(val)
	bitreeWriter, exist := w.writers[slotId]
	if !exist || bitreeWriter == nil {
		bt, err := w.db.getBitupleWrite(int(slotId))
		if err != nil {
			return err
		}
		bitreeWriter, err = bt.kkv.NewBitreeWriter()
		if err != nil {
			return err
		}
		w.writers[slotId] = bitreeWriter
	}

	err := bitreeWriter.Set(key, val[kkv.SlotIdLength:])
	if err != nil {
		w.err = err
	}
	return err
}

func (w *memFlushWriter) flushStart() error {
	return nil
}

func (w *memFlushWriter) flushFinish(checkFlush, isForce bool) error {
	var err error

	for i, writer := range w.writers {
		if writer != nil {
			if w.err != nil {
				writer.UnrefBdbTx()
			} else {
				err = utils.FirstError(err, writer.Finish(checkFlush, isForce))
			}
			w.writers[i] = nil
		}
	}

	return err
}

type stripeChangeType int

const (
	newStripe stripeChangeType = iota
	sameStripeSkippable
	sameStripeNonSkippable
)

type compactionIter struct {
	cmp       Compare
	iter      internalIterator
	err       error
	key       InternalKey
	valid     bool
	iterKey   *InternalKey
	iterValue []byte
	skip      bool
	pos       iterPos
	first     bool
	end       bool
}

func (i *compactionIter) IsVisitFirst() bool {
	return i.first == true
}

func (i *compactionIter) SetVisitEnd() {
	i.end = true
}

func (i *compactionIter) IsVisitEnd() bool {
	return i.end == true
}

func (i *compactionIter) First() (*InternalKey, []byte) {
	i.first = true
	if i.err != nil {
		return nil, nil
	}
	i.iterKey, i.iterValue = i.iter.First()
	i.pos = iterPosNext
	return i.Next()
}

func (i *compactionIter) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if i.pos == iterPosCurForward {
		if i.skip {
			i.skipInStripe()
		} else {
			i.nextInStripe()
		}
	}

	i.pos = iterPosCurForward
	i.valid = false
	for i.iterKey != nil {
		switch i.iterKey.Kind() {
		case InternalKeyKindSet, InternalKeyKindDelete, InternalKeyKindPrefixDelete:
			i.key.UserKey = i.iterKey.UserKey
			i.key.Trailer = i.iterKey.Trailer
			i.valid = true
			i.skip = true
			return &i.key, i.iterValue
		default:
			i.err = errors.Errorf("bitalosdb: invalid internal key kind %d", i.iterKey.Kind())
			i.valid = false
			return nil, nil
		}
	}

	return nil, nil
}

func (i *compactionIter) skipInStripe() {
	i.skip = true
	var change stripeChangeType
	for {
		change = i.nextInStripe()
		if change == sameStripeNonSkippable || change == newStripe {
			break
		}
	}
	if change == newStripe {
		i.skip = false
	}
}

func (i *compactionIter) nextInStripe() stripeChangeType {
	i.iterKey, i.iterValue = i.iter.Next()
	if i.iterKey == nil || i.cmp(i.key.UserKey, i.iterKey.UserKey) != 0 {
		return newStripe
	}

	if i.iterKey.Kind() == base.InternalKeyKindInvalid {
		return sameStripeNonSkippable
	}

	return sameStripeSkippable
}

func (i *compactionIter) Valid() bool {
	return i.valid
}

func (i *compactionIter) Error() error {
	return i.err
}

func (i *compactionIter) Close() error {
	err := i.iter.Close()
	if i.err == nil {
		i.err = err
	}
	return i.err
}
