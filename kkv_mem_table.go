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
	"fmt"
	"sync"
	"sync/atomic"

	arenaskl "github.com/zuoyebang/bitalosdb/v2/internal/arenaskl2"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
)

type memTableShard struct {
	db            *DB
	index         int
	tid           int
	maxSize       int
	iterSlowCount atomic.Uint64
	logTag        string

	readState struct {
		sync.RWMutex
		val *memReadState
	}

	mem struct {
		sync.RWMutex
		mutable *memTable
		queue   memFlushableList

		compact struct {
			cond     sync.Cond
			flushing bool
		}
	}
}

func newMemTableShard(d *DB, index int) *memTableShard {
	var memEntry *memFlushableEntry

	ms := &memTableShard{
		db:      d,
		index:   index,
		maxSize: d.opts.MemTableSize,
	}

	ms.mem.compact.cond.L = &ms.mem.RWMutex
	ms.logTag = fmt.Sprintf("[MEMSHARD %d]", ms.index)

	ms.mem.Lock()
	defer ms.mem.Unlock()

	ms.mem.mutable, memEntry = ms.newMemTable()
	ms.mem.queue = append(ms.mem.queue, memEntry)
	ms.updateReadState()

	return ms
}

func (ms *memTableShard) newMemTable() (*memTable, *memFlushableEntry) {
	size := ms.maxSize
	ms.tid++
	mem := &memTable{
		id:       ms.tid,
		cmp:      ms.db.opts.Comparer.Compare,
		equal:    ms.db.opts.Comparer.Equal,
		logger:   ms.db.opts.Logger,
		size:     uint32(size),
		arenaBuf: manual.New(size),
	}
	mem.writerRefs.Store(1)

	arena := arenaskl.NewArena(mem.arenaBuf)
	mem.skl.Reset(arena, mem.cmp)

	entry := newMemFlushableEntry(mem)
	entry.release = func() {
		manual.Free(mem.arenaBuf)
		mem.arenaBuf = nil
		ms.db.opts.Logger.Infof("%s memtable(%d) release success", ms.logTag, entry.getId())
	}
	ms.db.opts.Logger.Infof("%s memtable(%d) new success", ms.logTag, mem.id)
	return mem, entry
}

func (ms *memTableShard) makeRoomForWrite(newSize uint32, flushForce bool) {
	var size uint64
	var entry *memFlushableEntry
	force := newSize == 0
	stalled := false
	for {
		if newSize > 0 {
			err := ms.mem.mutable.prepare(newSize, true)
			if err == nil {
				if stalled {
					ms.db.opts.Logger.Infof("%s memtable write stall ending", ms.logTag)
				}
				return
			}
		} else if !force {
			if stalled {
				ms.db.opts.Logger.Infof("%s memtable write stall ending", ms.logTag)
			}
			return
		}

		size = 0
		for i := range ms.mem.queue {
			size += ms.mem.queue[i].totalBytes()
		}
		if size >= uint64(ms.db.opts.MemTableStopWritesThreshold*ms.maxSize) {
			if !stalled {
				stalled = true
				ms.db.opts.Logger.Infof("%s memtable write count limit reached stall beginning", ms.logTag)
			}
			ms.mem.compact.cond.Wait()
			continue
		}

		immMem := ms.mem.mutable
		ms.mem.mutable, entry = ms.newMemTable()
		ms.mem.queue = append(ms.mem.queue, entry)
		ms.updateReadState()
		if immMem.writerUnref() {
			ms.maybeScheduleFlush(true, flushForce)
		}
		force = false
	}
}

func (ms *memTableShard) asyncFlush(flushForce bool) (<-chan struct{}, error) {
	if ms.db.IsClosed() {
		return nil, ErrClosed
	}

	ms.mem.Lock()
	defer ms.mem.Unlock()
	empty := true
	for i := range ms.mem.queue {
		if !ms.mem.queue[i].empty() {
			empty = false
			break
		}
	}
	if empty {
		return nil, nil
	}
	flushed := ms.mem.queue[len(ms.mem.queue)-1].flushed
	ms.makeRoomForWrite(0, flushForce)
	return flushed, nil
}

func (ms *memTableShard) loadReadState() *memReadState {
	ms.readState.RLock()
	state := ms.readState.val
	state.ref()
	ms.readState.RUnlock()
	return state
}

func (ms *memTableShard) updateReadState() {
	rs := &memReadState{
		memtables: ms.mem.queue,
	}
	rs.refcnt.Store(1)

	for _, mem := range rs.memtables {
		mem.readerRef()
	}

	ms.readState.Lock()
	old := ms.readState.val
	ms.readState.val = rs
	ms.readState.Unlock()

	if old != nil {
		old.unref()
	}
}

func (ms *memTableShard) close() {
	ms.mem.Lock()
	defer ms.mem.Unlock()

	for ms.mem.compact.flushing {
		ms.mem.compact.cond.Wait()
	}

	if err := ms.runFlush(ms.mem.queue, false, false); err != nil {
		ms.db.opts.Logger.Errorf("%s close runFlush err:%s", ms.logTag, err)
	}

	ms.readState.val.unref()
	for _, mem := range ms.mem.queue {
		mem.readerUnref()
	}
}

func memTableEntrySize(keyBytes, valueBytes int) uint32 {
	return arenaskl.MaxNodeSize(uint32(keyBytes)+8, uint32(valueBytes))
}

var memTableEmptySize = func() uint32 {
	var pointSkl arenaskl.Skiplist
	arena := arenaskl.NewArena(make([]byte, 16<<10))
	pointSkl.Reset(arena, bytes.Compare)
	return arena.Size()
}()

type memTable struct {
	id         int
	cmp        Compare
	equal      Equal
	arenaBuf   []byte
	skl        arenaskl.Skiplist
	reserved   uint32
	writerRefs atomic.Int32
	logger     base.Logger
	size       uint32
	delCnt     atomic.Uint32
	totalCnt   atomic.Uint32
}

func (m *memTable) getId() int {
	return m.id
}

func (m *memTable) writerRef() {
	m.writerRefs.Add(1)
}

func (m *memTable) writerUnref() bool {
	switch v := m.writerRefs.Add(-1); {
	case v == 0:
		return true
	default:
		return false
	}
}

func (m *memTable) readyForFlush() bool {
	return m.writerRefs.Load() == 0
}

func (m *memTable) get(key []byte) ([]byte, bool, InternalKeyKind) {
	return m.skl.Get(key)
}

func (m *memTable) exist(key []byte) (bool, InternalKeyKind) {
	return m.skl.Exist(key)
}

func (m *memTable) add(key InternalKey, slotId uint16, valueSize int, values ...[]byte) error {
	var ins arenaskl.Inserter
	_, _, err := m.skl.FindForAdd(key, &ins)
	if err != nil {
		return err
	}
	_, _, err = m.skl.AddInserter(key, &ins, slotId, valueSize, values...)
	return err
}

func (m *memTable) set(
	key InternalKey, slotId uint16, addType int,
	seekNext func() (bool, InternalKeyKind),
	valueSize int, values ...[]byte,
) (bool, error) {
	var ins arenaskl.Inserter
	foundKey, foundKeyKind, err := m.skl.FindForAdd(key, &ins)
	if err != nil {
		return false, err
	}

	if addType == kkvWriteTypeDirectAdd {
		_, _, err = m.skl.AddInserter(key, &ins, slotId, valueSize, values...)
		return false, err
	}

	if !foundKey {
		foundKey, foundKeyKind = seekNext()
	}

	if foundKey && foundKeyKind == InternalKeyKindDelete {
		foundKey = false
	}

	if (addType == kkvWriteTypeNotExistToAdd && foundKey) ||
		(addType == kkvWriteTypeExistToAdd && !foundKey) {
		return foundKey, nil
	}

	addFound, addFoundKind, addErr := m.skl.AddInserter(key, &ins, slotId, valueSize, values...)
	if addErr != nil {
		return false, addErr
	}

	if !foundKey && addFound && addFoundKind == InternalKeyKindSet {
		foundKey = true
	}

	return foundKey, nil
}

func (m *memTable) newIter(o *IterOptions) InternalKKVIterator {
	return m.skl.NewIter(o.GetLowerBound(), o.GetUpperBound())
}

func (m *memTable) newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator {
	return m.skl.NewFlushIter(bytesFlushed)
}

func (m *memTable) availBytes() uint32 {
	a := m.skl.Arena()
	if m.writerRefs.Load() == 1 {
		m.reserved = a.Size()
	}
	return a.Capacity() - m.reserved
}

func (m *memTable) inuseBytes() uint64 {
	return uint64(m.skl.Size() - memTableEmptySize)
}

func (m *memTable) totalBytes() uint64 {
	return uint64(m.skl.Arena().Capacity())
}

func (m *memTable) deletePercent() float64 {
	delCnt := m.delCnt.Load()
	if delCnt == 0 {
		return 0
	}
	return float64(delCnt) / float64(m.totalCnt.Load())
}

func (m *memTable) close() error {
	return nil
}

func (m *memTable) empty() bool {
	return m.skl.Size() <= memTableEmptySize
}

func (m *memTable) prepare(newSize uint32, checkDelPercent bool) error {
	if newSize > m.availBytes() {
		return base.ErrTableFull
	}

	if checkDelPercent {
		delPercent := m.deletePercent()
		inuseSize := m.inuseBytes()
		if consts.CheckFlushDelPercent(delPercent, inuseSize, uint64(m.size)) {
			m.logger.Infof("memtable(%d) delete percent exceed inuse:%d size:%d delPercent:%.2f ", m.id, inuseSize, m.size, delPercent)
			return base.ErrMemTableExceedDelPercent
		}
	}

	m.reserved += newSize
	m.writerRef()
	return nil
}
