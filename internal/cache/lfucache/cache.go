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
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/internal/cache/lfucache/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/manual"
)

var (
	LFU_FREQ_BUF           = [2]uint8{0, 0}
	LFU_FREQ_LENGTH        = 2
	LFU_TM_LENGTH          = 2
	LFU_META_LENGTH        = LFU_FREQ_LENGTH + LFU_TM_LENGTH
	LFU_FREQ_MAX    uint16 = 65535
	FREQ_TM_STEP           = [5]uint16{2, 8, 16, 24, LFU_FREQ_MAX}
)

const (
	FREQ_TM_STEP_LEN   = len(FREQ_TM_STEP)
	FREQ_THRESHOLD_LEN = 2
	FREQ_LEVEL         = 656
)

type shard struct {
	atomic struct {
		memtableID  int64
		arrtableID  int64
		seqNum      uint64
		hits        int64
		misses      int64
		memtableNum int32
	}

	mu struct {
		sync.Mutex
		memMutable *memTable
		memQueue   flushableList
		arrtable   *flushableEntry
	}

	readState struct {
		sync.RWMutex
		val *readState
	}

	lc            *LfuCache
	index         int
	memSize       int
	maxSize       uint64
	flushCnt      int64
	freqLevelStat [FREQ_TM_STEP_LEN][FREQ_LEVEL]int
	freqThreshold [FREQ_TM_STEP_LEN][FREQ_THRESHOLD_LEN]uint64
}

func newCache(lc *LfuCache, index int, memSize int, maxSize uint64) *shard {
	s := &shard{
		lc:            lc,
		index:         index,
		memSize:       memSize,
		maxSize:       maxSize,
		freqLevelStat: [FREQ_TM_STEP_LEN][FREQ_LEVEL]int{},
		freqThreshold: [FREQ_TM_STEP_LEN][FREQ_THRESHOLD_LEN]uint64{},
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var entry *flushableEntry
	s.mu.memMutable, entry = s.newMemTable()
	s.mu.memQueue = append(s.mu.memQueue, entry)
	s.updateReadStateLocked()

	return s
}

func (s *shard) exist(key []byte) bool {
	_, found, closer := s.getInternal(key)
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	return found
}

func (s *shard) get(key []byte) ([]byte, func(), bool) {
	value, found, closer := s.getInternal(key)
	if found {
		vLen := len(value)
		if vLen > LFU_META_LENGTH {
			freq := binary.BigEndian.Uint16(value[vLen-LFU_FREQ_LENGTH:])
			if freq < LFU_FREQ_MAX {
				freq++
			}
			binary.BigEndian.PutUint16(value[vLen-LFU_FREQ_LENGTH:], freq)
			binary.BigEndian.PutUint16(value[vLen-LFU_META_LENGTH:], uint16((time.Now().Unix()-s.lc.launchTime)/3600))

			value = value[:vLen-LFU_META_LENGTH]
		}
		atomic.AddInt64(&s.atomic.hits, 1)
	} else {
		atomic.AddInt64(&s.atomic.misses, 1)
	}

	return value, closer, found
}

func (s *shard) getInternal(key []byte) ([]byte, bool, func()) {
	rs := s.loadReadState()
	closer := func() {
		rs.unref()
	}

	memIndex := len(rs.memtables) - 1
	for memIndex >= 0 {
		mt := rs.memtables[memIndex]
		val, exist, kind := mt.get(key)
		if exist {
			if kind == internalKeyKindSet {
				return val, true, closer
			} else if kind == internalKeyKindDelete {
				rs.unref()
				return nil, false, nil
			}
		}

		memIndex--
	}

	if rs.arrtable != nil {
		val, exist, _ := rs.arrtable.get(key)
		if exist {
			return val, true, closer
		}
	}

	rs.unref()
	return nil, false, nil
}

func (s *shard) getIter(key []byte) (val []byte, valPool func(), _ bool) {
	v, iter, found := s.getInternalIter(key)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if found {
		vLen := len(v)
		if vLen > LFU_META_LENGTH {
			freq := binary.BigEndian.Uint16(v[vLen-LFU_FREQ_LENGTH:])
			if freq < LFU_FREQ_MAX {
				freq++
			}
			binary.BigEndian.PutUint16(v[vLen-LFU_FREQ_LENGTH:], freq)
			binary.BigEndian.PutUint16(v[vLen-LFU_META_LENGTH:], uint16((time.Now().Unix()-s.lc.launchTime)/3600))

			v = v[:vLen-LFU_META_LENGTH]

			val, valPool = bytepools.DefaultBytePools.MakeValue(v)
		}
		atomic.AddInt64(&s.atomic.hits, 1)
	} else {
		atomic.AddInt64(&s.atomic.misses, 1)
	}

	return val, valPool, found
}

func (s *shard) getInternalIter(key []byte) ([]byte, internalIterator, bool) {
	var (
		iterKey   *internalKey
		iterValue []byte
		iter      internalIterator
		isSeekAt  bool
	)

	rs := s.loadReadState()
	defer rs.unref()

	memIndex := len(rs.memtables) - 1
	for {
		if iter != nil {
			if iterKey != nil && bytes.Equal(key, iterKey.UserKey) {
				break
			}
			_ = iter.Close()
			iter = nil
		}

		if memIndex >= 0 {
			m := rs.memtables[memIndex]
			iter = m.newIter(nil)
			iterKey, iterValue = iter.SeekGE(key)
			memIndex--
			continue
		}

		if !isSeekAt && rs.arrtable != nil {
			iter = rs.arrtable.newIter(nil)
			iterKey, iterValue = iter.SeekGE(key)
			isSeekAt = true
			continue
		}

		iterKey = nil
		iterValue = nil
		break
	}

	if iterKey == nil {
		return nil, iter, false
	}

	if iterKey.Kind() == internalKeyKindDelete {
		iterValue = nil
	}

	return iterValue, iter, true
}

func (s *shard) set(key []byte, value []byte) error {
	return s.setApply(key, value, internalKeyKindSet)
}

func (s *shard) delete(key []byte) error {
	return s.setApply(key, nil, internalKeyKindDelete)
}

func (s *shard) setApply(key []byte, value []byte, kind internalKeyKind) error {
	if kind == internalKeyKindSet && len(value) > 0 {
		accessTime := [2]uint8{0, 0}
		binary.BigEndian.PutUint16(accessTime[:], uint16((time.Now().Unix()-s.lc.launchTime)/3600))
		value = append(value, accessTime[:]...)
		value = append(value, LFU_FREQ_BUF[:]...)
	}

	kvSize := memTableEntrySize(len(key), len(value))
	mem, err := s.setPrepare(kvSize)
	if err != nil {
		return err
	}

	mem.writerRef()
	defer func() {
		mem.writerUnref()
	}()
	seqNum := atomic.AddUint64(&s.atomic.seqNum, 1)

	return mem.add(key, value, seqNum, kind)
}

func (s *shard) setPrepare(size uint64) (*memTable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.mu.memMutable.prepare(size)
	if err == arenaskl.ErrArenaFull {
		s.makeRoomForWrite()
	}
	mem := s.mu.memMutable
	return mem, nil
}

func (s *shard) makeRoomForWrite() {
	var entry *flushableEntry
	immMem := s.mu.memMutable
	s.mu.memMutable, entry = s.newMemTable()
	s.mu.memQueue = append(s.mu.memQueue, entry)
	s.updateReadStateLocked()
	immMem.writerUnref()
}

func (s *shard) newMemTable() (*memTable, *flushableEntry) {
	id := atomic.AddInt64(&s.atomic.memtableID, 1)
	mem := newMemTable(id, s.memSize)

	invariants.SetFinalizer(mem, checkMemTable)

	entry := s.newFlushableEntry(mem)
	entry.releaseMemAccounting = func() {
		manual.Free(mem.arenaBuf)
		mem.arenaBuf = nil
	}
	return mem, entry
}

func (s *shard) newArrayTable(size int) (*arrayTable, *flushableEntry) {
	id := atomic.AddInt64(&s.atomic.arrtableID, 1)
	at := newArrayTable(id, size)

	invariants.SetFinalizer(at, checkArrayTable)

	entry := s.newFlushableEntry(at)
	entry.releaseMemAccounting = func() {
		manual.Free(at.arenaBuf)
		at.arenaBuf = nil
	}
	return at, entry
}

func (s *shard) newFlushableEntry(f flushable) *flushableEntry {
	return &flushableEntry{
		flushable:  f,
		flushed:    make(chan struct{}),
		readerRefs: 1,
	}
}

func (s *shard) clearFreqLevelStat() {
	for i := 0; i < FREQ_TM_STEP_LEN; i++ {
		for j := 0; j < FREQ_LEVEL; j++ {
			s.freqLevelStat[i][j] = 0
		}
	}

	for i := 0; i < FREQ_TM_STEP_LEN; i++ {
		for j := 0; j < FREQ_THRESHOLD_LEN; j++ {
			s.freqThreshold[i][j] = 0
		}
	}
}

func (s *shard) updateFreqLevelStat(tm uint16, freq uint16, size int) {
	var i int
	for i = 0; i < FREQ_TM_STEP_LEN; i++ {
		if tm <= FREQ_TM_STEP[i] {
			break
		}
	}

	if i == FREQ_TM_STEP_LEN {
		return
	}

	for ; i < FREQ_TM_STEP_LEN; i++ {
		s.freqLevelStat[i][freq/100] += size
		s.freqThreshold[i][0] += uint64(freq)
		s.freqThreshold[i][1]++
	}
}

func (s *shard) calculateFreqLevel(size int) (uint16, uint16, uint16) {
	for i := 0; i < FREQ_TM_STEP_LEN; i++ {
		freqSize := 0
		j := FREQ_LEVEL - 1
		for ; j >= 0; j-- {
			if s.freqLevelStat[i][j] > 0 {
				freqSize += s.freqLevelStat[i][j]
				if freqSize >= size {
					break
				}
			}
		}

		if freqSize >= size {
			freqThreshold := uint16(0)
			if j > 0 {
				freqThreshold = uint16(j * 100)
			}

			freqAvg := uint16(0)
			if s.freqThreshold[i][0] > 0 && s.freqThreshold[i][1] > 0 {
				freqAvg = uint16(s.freqThreshold[i][0] / s.freqThreshold[i][1] / 2)
			}

			return freqThreshold, FREQ_TM_STEP[i], freqAvg
		}
	}

	return 0, 0, 0
}

func (s *shard) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readState.val.unref()
	for _, mem := range s.mu.memQueue {
		mem.readerUnref()
	}
}

func (s *shard) arrayTableInuseSize() uint64 {
	var size uint64
	if s.mu.arrtable != nil {
		size = s.mu.arrtable.inuseBytes()
	}
	return size
}

func (s *shard) arrayTableCount() int {
	var count int
	if s.mu.arrtable != nil {
		count = s.mu.arrtable.count()
	}
	return count
}

func (s *shard) memTableCount() int {
	var count int
	for i := range s.mu.memQueue {
		count += s.mu.memQueue[i].count()
	}
	return count
}

func (s *shard) memTableInuseSize() uint64 {
	var size uint64
	memNum := len(s.mu.memQueue)
	if memNum > 0 {
		if memNum > 1 {
			size += uint64((memNum - 1) * s.memSize)
		}
		size += s.mu.memMutable.inuseBytes()
	}
	return size
}

func (s *shard) inuseSize() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inuseSizeLocked()
}

func (s *shard) inuseSizeLocked() uint64 {
	return s.arrayTableInuseSize() + s.memTableInuseSize()
}

func (s *shard) setMemtableNum(n int32) {
	atomic.StoreInt32(&s.atomic.memtableNum, n)
}

func (s *shard) getMemtableNum() int32 {
	return atomic.LoadInt32(&s.atomic.memtableNum)
}
