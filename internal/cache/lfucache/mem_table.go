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
	"fmt"
	"os"
	"sync/atomic"

	arenaskl2 "github.com/zuoyebang/bitalosdb/internal/cache/lfucache/internal/arenaskl"
	base2 "github.com/zuoyebang/bitalosdb/internal/cache/lfucache/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/manual"
)

func memTableEntrySize(keyBytes, valueBytes int) uint64 {
	return uint64(arenaskl2.MaxNodeSize(uint32(keyBytes)+8, uint32(valueBytes)))
}

var memTableEmptySize = func() uint32 {
	var pointSkl arenaskl2.Skiplist
	arena := arenaskl2.NewArena(make([]byte, 16<<10))
	pointSkl.Reset(arena)
	return arena.Size()
}()

type memTable struct {
	id         int64
	arenaBuf   []byte
	skl        arenaskl2.Skiplist
	size       int
	reserved   uint32
	writerRefs int32
	num        int
}

func checkMemTable(obj interface{}) {
	m := obj.(*memTable)
	if m.arenaBuf != nil {
		fmt.Fprintf(os.Stderr, "%p: memTable(%d) buffer was not freed\n", m.arenaBuf, m.id)
		os.Exit(1)
	}
}

func newMemTable(id int64, size int) *memTable {
	m := &memTable{
		id:         id,
		arenaBuf:   manual.New(size),
		size:       size,
		writerRefs: 1,
	}

	m.skl.Reset(arenaskl2.NewArena(m.arenaBuf))

	return m
}

func (m *memTable) writerRef() {
	switch v := atomic.AddInt32(&m.writerRefs, 1); {
	case v <= 1:
		panic(fmt.Sprintf("mcache: inconsistent reference count: %d", v))
	}
}

func (m *memTable) writerUnref() bool {
	switch v := atomic.AddInt32(&m.writerRefs, -1); {
	case v < 0:
		panic(fmt.Sprintf("mcache: inconsistent reference count: %d", v))
	case v == 0:
		return true
	default:
		return false
	}
}

func (m *memTable) readyForFlush() bool {
	return atomic.LoadInt32(&m.writerRefs) == 0
}

func (m *memTable) prepare(kvSize uint64) error {
	avail := m.availBytes()
	if kvSize > uint64(avail) {
		return arenaskl2.ErrArenaFull
	}
	m.reserved += uint32(kvSize)
	return nil
}

func (m *memTable) add(key []byte, value []byte, seqNum uint64, kind internalKeyKind) (err error) {
	ikey := base2.MakeInternalKey(key, seqNum, kind)
	var ins arenaskl2.Inserter
	err = ins.Add(&m.skl, ikey, value)
	if err == nil {
		m.num++
	}

	return err
}

func (m *memTable) get(key []byte) ([]byte, bool, internalKeyKind) {
	return m.skl.Get(key)
}

func (m *memTable) newIter(o *iterOptions) internalIterator {
	return m.skl.NewIter(o.GetLowerBound(), o.GetUpperBound())
}

func (m *memTable) newFlushIter(o *iterOptions, bytesFlushed *uint64) internalIterator {
	return m.skl.NewFlushIter(bytesFlushed)
}

func (m *memTable) availBytes() uint32 {
	a := m.skl.Arena()
	if atomic.LoadInt32(&m.writerRefs) == 1 {
		m.reserved = a.Size()
	}
	return a.Capacity() - m.reserved
}

func (m *memTable) inuseBytes() uint64 {
	return uint64(m.skl.Size())
}

func (m *memTable) totalBytes() uint64 {
	return uint64(m.skl.Arena().Capacity())
}

func (m *memTable) close() error {
	return nil
}

func (m *memTable) empty() bool {
	return m.skl.Size() == memTableEmptySize
}

func (m *memTable) getID() int64 {
	return m.id
}

func (m *memTable) count() int {
	return m.num
}
