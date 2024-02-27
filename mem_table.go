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
	"fmt"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/zuoyebang/bitalosdb/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

func memTableEntrySize(keyBytes, valueBytes int) uint64 {
	return uint64(arenaskl.MaxNodeSize(uint32(keyBytes)+8, uint32(valueBytes)))
}

var memTableEmptySize = func() uint32 {
	var pointSkl arenaskl.Skiplist
	arena := arenaskl.NewArena(make([]byte, 16<<10))
	pointSkl.Reset(arena, bytes.Compare)
	return arena.Size()
}()

type memTable struct {
	cmp        Compare
	formatKey  base.FormatKey
	equal      Equal
	arenaBuf   []byte
	skl        arenaskl.Skiplist
	reserved   uint32
	writerRefs atomic.Int32
	logSeqNum  uint64
	logger     base.Logger
	size       uint32
	delCnt     atomic.Uint32
	totalCnt   atomic.Uint32
}

type memTableOptions struct {
	*Options
	arenaBuf  []byte
	size      int
	logSeqNum uint64
}

func checkMemTable(obj interface{}) {
	m := obj.(*memTable)
	if m.arenaBuf != nil {
		fmt.Fprintf(os.Stderr, "%p: memTable buffer was not freed\n", m.arenaBuf)
		os.Exit(1)
	}
}

func newMemTable(opts memTableOptions) *memTable {
	if opts.size == 0 {
		opts.size = opts.MemTableSize
	}

	m := &memTable{
		cmp:       opts.Comparer.Compare,
		formatKey: opts.Comparer.FormatKey,
		equal:     opts.Comparer.Equal,
		arenaBuf:  opts.arenaBuf,
		logSeqNum: opts.logSeqNum,
		logger:    opts.Logger,
		size:      uint32(opts.size),
	}
	m.writerRefs.Store(1)

	if m.arenaBuf == nil {
		m.arenaBuf = make([]byte, opts.size)
	}

	arena := arenaskl.NewArena(m.arenaBuf)
	m.skl.Reset(arena, m.cmp)
	return m
}

func (m *memTable) writerRef() {
	m.writerRefs.Add(1)
}

func (m *memTable) writerUnref() bool {
	switch v := m.writerRefs.Add(-1); {
	case v < 0:
		m.logger.Errorf("panic: memTable inconsistent reference count: %d\n", v)
		return false
	case v == 0:
		return true
	default:
		return false
	}
}

func (m *memTable) readyForFlush() bool {
	return m.writerRefs.Load() == 0
}

func (m *memTable) get(key []byte) ([]byte, bool, base.InternalKeyKind) {
	return m.skl.Get(key)
}

func (m *memTable) getInternal(key []byte) ([]byte, error) {
	v, exist, kind := m.skl.Get(key)
	if exist && kind == base.InternalKeyKindSet {
		return v, nil
	}
	return nil, ErrNotFound
}

func (m *memTable) prepare(batch *Batch, checkDelPercent bool) error {
	avail := m.availBytes()
	if batch.memTableSize > uint64(avail) {
		return arenaskl.ErrArenaFull
	}

	if checkDelPercent {
		delPercent := m.deletePercent()
		inuseSize := m.inuseBytes()
		if consts.CheckFlushDelPercent(delPercent, inuseSize, uint64(m.size)) {
			m.logger.Infof("memtable delete percent exceed inuse:%d size:%d delPercent:%.2f ", inuseSize, m.size, delPercent)
			return errMemExceedDelPercent
		}
	}

	m.reserved += uint32(batch.memTableSize)
	m.writerRef()
	return nil
}

func (m *memTable) apply(batch *Batch, seqNum uint64) error {
	if seqNum < m.logSeqNum {
		return errors.Errorf("bitalosdb: batch seqnum %d is less than memtable creation seqnum %d", seqNum, m.logSeqNum)
	}

	var ins arenaskl.Inserter
	startSeqNum := seqNum
	for r := batch.Reader(); ; seqNum++ {
		kind, ukey, value, ok := r.Next()
		if !ok {
			break
		}
		var err error
		ikey := base.MakeInternalKey(ukey, seqNum, kind)
		switch kind {
		case InternalKeyKindLogData:
			seqNum--
		default:
			err = ins.Add(&m.skl, ikey, value)
		}
		if err != nil {
			return err
		}
		m.totalCnt.Add(1)
		if kind == InternalKeyKindDelete {
			m.delCnt.Add(1)
		}
	}
	if seqNum != startSeqNum+uint64(batch.Count()) {
		return errors.Errorf("bitalosdb: inconsistent batch count: %d vs %d", seqNum, startSeqNum+uint64(batch.Count()))
	}
	return nil
}

func (m *memTable) newIter(o *IterOptions) internalIterator {
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
