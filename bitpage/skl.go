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

package bitpage

import (
	"bytes"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/fastrand"
	"github.com/zuoyebang/bitalosdb/internal/hash"
)

const (
	sklVersion1 uint16 = 1
)

const (
	maxHeight   = 20
	maxNodeSize = int(unsafe.Sizeof(node{}))
	linksSize   = int(unsafe.Sizeof(links{}))
	pValue      = 1 / math.E
	indexSize   = 1 << 20
)

const (
	sklHeaderLength        = 4
	sklHeaderOffset        = tableDataOffset
	sklHeaderVersionOffset = sklHeaderOffset
	sklHeaderHeightOffset  = sklHeaderVersionOffset + 2
	sklHeadNodeOffset      = 8
	sklTailNodeOffset      = 196
)

var ErrRecordExists = errors.New("record with this key already exists")

type skl struct {
	st          *sklTable
	tbl         *table
	cmp         base.Compare
	head        *node
	tail        *node
	version     uint16
	height      uint32
	useMapIndex bool
	testing     bool
	cache       struct {
		sync.RWMutex
		index map[uint32]uint32
	}
}

type Inserter struct {
	spl    [maxHeight]splice
	height uint32
}

func (ins *Inserter) Add(list *skl, key internalKey, value []byte) error {
	return list.addInternal(key, value, ins)
}

var (
	probabilities [maxHeight]uint32
)

func init() {
	p := float64(1.0)
	for i := 0; i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

func newSkl(tbl *table, st *sklTable, useMapIndex bool) (*skl, error) {
	headerOffset, err := tbl.alloc(sklHeaderLength)
	if err != nil || headerOffset != uint32(sklHeaderOffset) {
		return nil, ErrTableSize
	}

	head, err := newRawNode(tbl, maxHeight, 0, 0)
	if err != nil {
		return nil, errors.New("tblSize is not large enough to hold the head node")
	}

	tail, err := newRawNode(tbl, maxHeight, 0, 0)
	if err != nil {
		return nil, errors.New("tblSize is not large enough to hold the tail node")
	}

	head.keyOffset = 0
	tail.keyOffset = 0

	headOffset := tbl.getPointerOffset(unsafe.Pointer(head))
	tailOffset := tbl.getPointerOffset(unsafe.Pointer(tail))
	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset = tailOffset
		tail.tower[i].prevOffset = headOffset
	}

	sl := &skl{
		st:          st,
		tbl:         tbl,
		cmp:         bytes.Compare,
		head:        head,
		tail:        tail,
		height:      1,
		useMapIndex: useMapIndex,
	}

	sl.setHeader()

	if useMapIndex {
		sl.cache.index = make(map[uint32]uint32, indexSize)
	}

	return sl, nil
}

func openSkl(tbl *table, st *sklTable, useMapIndex bool) *skl {
	sl := &skl{
		st:          st,
		tbl:         tbl,
		cmp:         bytes.Compare,
		head:        (*node)(tbl.getPointer(sklHeadNodeOffset)),
		tail:        (*node)(tbl.getPointer(sklTailNodeOffset)),
		useMapIndex: useMapIndex,
	}

	sl.getHeader()

	if useMapIndex {
		sl.cache.index = make(map[uint32]uint32, indexSize)
	}

	return sl
}

func (s *skl) getHeader() {
	s.version = s.tbl.readAtUInt16(sklHeaderVersionOffset)
	s.height = s.getHeight()
}

func (s *skl) setHeader() {
	s.tbl.writeAtUInt16(sklVersion1, sklHeaderVersionOffset)
	s.setHeight()
}

func (s *skl) getHeight() uint32 {
	return uint32(s.tbl.readAtUInt16(sklHeaderHeightOffset))
}

func (s *skl) setHeight() {
	s.tbl.writeAtUInt16(uint16(s.Height()), sklHeaderHeightOffset)
}

func (s *skl) Height() uint32 { return atomic.LoadUint32(&s.height) }

func (s *skl) Table() *table { return s.tbl }

func (s *skl) Size() uint32 { return s.tbl.Size() }

func (s *skl) Get(key []byte, khash uint32) ([]byte, bool, internalKeyKind) {
	var nd *node
	var kind internalKeyKind
	var beFound bool

	if s.useMapIndex && s.cache.index != nil {
		s.cache.RLock()
		if ndOffset, ok := s.cache.index[khash]; ok {
			nd = (*node)(s.tbl.getPointer(ndOffset))
			if nd != s.tail {
				beFound, kind = s.compareKey(key, nd)
			}
		}
		s.cache.RUnlock()
	}

	if !beFound {
		_, nd, _ = s.seekForBaseSplice(key)
		if nd == s.tail {
			return nil, false, internalKeyKindInvalid
		}

		var exist bool = false
		exist, kind = s.compareKey(key, nd)
		if !exist {
			return nil, false, internalKeyKindInvalid
		}
	}

	if s.useMapIndex && !beFound && khash > 0 {
		s.cache.Lock()
		s.cache.index[khash] = s.tbl.getPointerOffset(unsafe.Pointer(nd))
		s.cache.Unlock()
	}

	if kind == internalKeyKindSet {
		value := s.tbl.getBytes(nd.keyOffset+nd.keySize, nd.valueSize)
		return value, true, kind
	} else if kind == internalKeyKindDelete {
		return nil, true, kind
	}

	return nil, false, internalKeyKindInvalid
}

func (s *skl) Add(key internalKey, value []byte) error {
	var ins Inserter
	return s.addInternal(key, value, &ins)
}

func (s *skl) addInternal(key internalKey, value []byte, ins *Inserter) error {
	if s.findSplice(key, ins) {
		return ErrRecordExists
	}

	if s.testing {
		runtime.Gosched()
	}

	nd, height, err := s.newNode(key, value)
	if err != nil {
		return err
	}

	ndOffset := s.tbl.getPointerOffset(unsafe.Pointer(nd))

	var found bool
	var invalidateSplice bool
	for i := 0; i < int(height); i++ {
		prev := ins.spl[i].prev
		next := ins.spl[i].next

		if prev == nil {
			if next != nil {
				return errors.New("bitpage: skl next is expected to be nil, since prev is nil")
			}

			prev = s.head
			next = s.tail
		}

		for {
			prevOffset := s.tbl.getPointerOffset(unsafe.Pointer(prev))
			nextOffset := s.tbl.getPointerOffset(unsafe.Pointer(next))
			nd.tower[i].init(prevOffset, nextOffset)

			nextPrevOffset := next.prevOffset(i)
			if nextPrevOffset != prevOffset {
				prevNextOffset := prev.nextOffset(i)
				if prevNextOffset == nextOffset {
					next.casPrevOffset(i, nextPrevOffset, prevOffset)
				}
			}

			if prev.casNextOffset(i, nextOffset, ndOffset) {
				if s.testing {
					runtime.Gosched()
				}

				next.casPrevOffset(i, prevOffset, ndOffset)
				break
			}

			prev, next, found = s.findSpliceForLevel(key, i, prev)
			if found {
				if i != 0 {
					panic("how can another thread have inserted a node at a non-base level?")
				}

				return ErrRecordExists
			}
			invalidateSplice = true
		}
	}

	s.setNodeSkipOffset(nd, ndOffset, key)

	if invalidateSplice {
		ins.height = 0
	} else {
		for i := uint32(0); i < height; i++ {
			ins.spl[i].prev = nd
		}
	}

	if s.useMapIndex && s.cache.index != nil {
		khash := hash.Crc32(key.UserKey)
		s.cache.Lock()
		s.cache.index[khash] = ndOffset
		s.cache.Unlock()
	}

	return nil
}

func (s *skl) setNodeSkipOffset(nd *node, ndOffset uint32, key internalKey) {
	nextNd := s.getNext(nd, 0)
	if nextNd == s.tail {
		return
	}

	offset, size := nextNd.keyOffset, nextNd.keySize
	nextKey := s.tbl.getBytes(offset, size)
	n := int32(size) - 8
	if n < 0 || s.cmp(key.UserKey, nextKey[:n]) != 0 {
		return
	}
	if key.Trailer <= binary.LittleEndian.Uint64(nextKey[n:]) {
		return
	}

	if s.st != nil && s.st.bp != nil {
		s.st.bp.deleteBithashKey(nextNd.getValue(s.tbl))
	}

	skipToFirstOffset := nextNd.skipToFirstOffset()
	if skipToFirstOffset > 0 {
		nd.setSkipToFirstOffset(skipToFirstOffset)

		skipToFirstNd := (*node)(s.tbl.getPointer(skipToFirstOffset))
		if skipToFirstNd == s.tail {
			return
		}

		skipToFirstNd.setSkipToLastOffset(ndOffset)
	} else {
		nextNdOffset := s.tbl.getPointerOffset(unsafe.Pointer(nextNd))
		nd.setSkipToFirstOffset(nextNdOffset)
	}
}

func (s *skl) NewIter(lower, upper []byte) *sklIterator {
	iter := &sklIterator{
		list: s,
		nd:   s.head,
	}
	return iter
}

func (s *skl) NewFlushIter() internalIterator {
	return s.NewIter(nil, nil)
}

func (s *skl) newNode(key internalKey, value []byte) (nd *node, height uint32, err error) {
	height = s.randomHeight()
	nd, err = newNode(s.tbl, height, key, value)
	if err != nil {
		return
	}

	listHeight := s.Height()
	for height > listHeight {
		if atomic.CompareAndSwapUint32(&s.height, listHeight, height) {
			s.setHeight()
			break
		}

		listHeight = s.Height()
	}

	return
}

func (s *skl) randomHeight() uint32 {
	rnd := fastrand.Uint32()

	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}

	return h
}

func (s *skl) isEmpty() bool {
	return s.getNext(s.head, 0) == s.tail
}

func (s *skl) findSplice(key internalKey, ins *Inserter) (found bool) {
	listHeight := s.Height()
	var level int

	prev := s.head
	if ins.height < listHeight {
		ins.height = listHeight
		level = int(ins.height)
	} else {
		for ; level < int(listHeight); level++ {
			spl := &ins.spl[level]
			if s.getNext(spl.prev, level) != spl.next {
				continue
			}
			if spl.prev != s.head && !s.keyIsAfterNode(spl.prev, key) {
				level = int(listHeight)
				break
			}
			if spl.next != s.tail && s.keyIsAfterNode(spl.next, key) {
				level = int(listHeight)
				break
			}
			prev = spl.prev
			break
		}
	}

	for level = level - 1; level >= 0; level-- {
		var next *node
		prev, next, found = s.findSpliceForLevel(key, level, prev)
		if next == nil {
			next = s.tail
		}
		ins.spl[level].init(prev, next)
	}

	return
}

func (s *skl) findSpliceForLevel(
	key internalKey, level int, start *node,
) (prev, next *node, found bool) {
	prev = start

	for {
		next = s.getNext(prev, level)
		if next == s.tail {
			break
		}

		offset, size := next.keyOffset, next.keySize
		nextKey := s.tbl.getBytes(offset, size)
		n := int32(size) - 8
		cmp := s.cmp(key.UserKey, nextKey[:n])
		if cmp < 0 {
			break
		}
		if cmp == 0 {
			var nextTrailer uint64
			if n >= 0 {
				nextTrailer = binary.LittleEndian.Uint64(nextKey[n:])
			} else {
				nextTrailer = uint64(internalKeyKindInvalid)
			}
			if key.Trailer == nextTrailer {
				found = true
				break
			}
			if key.Trailer > nextTrailer {
				break
			}
		}

		prev = next
	}

	return
}

func (s *skl) keyIsAfterNode(nd *node, key internalKey) bool {
	ndKey := s.tbl.getBytes(nd.keyOffset, nd.keySize)
	n := int32(nd.keySize) - 8
	cmp := s.cmp(ndKey[:n], key.UserKey)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	var ndTrailer uint64
	if n >= 0 {
		ndTrailer = binary.LittleEndian.Uint64(ndKey[n:])
	} else {
		ndTrailer = uint64(internalKeyKindInvalid)
	}
	if key.Trailer == ndTrailer {
		return false
	}
	return key.Trailer < ndTrailer
}

func (s *skl) getNext(nd *node, h int) *node {
	offset := atomic.LoadUint32(&nd.tower[h].nextOffset)
	return (*node)(s.tbl.getPointer(offset))
}

func (s *skl) getPrev(nd *node, h int) *node {
	offset := atomic.LoadUint32(&nd.tower[h].prevOffset)
	return (*node)(s.tbl.getPointer(offset))
}

func (s *skl) getSkipNext(nd *node) *node {
	var nextNd *node
	skipToFirstOffset := nd.skipToFirstOffset()
	if skipToFirstOffset > 0 {
		nextNd = (*node)(s.tbl.getPointer(skipToFirstOffset))
	} else {
		offset := atomic.LoadUint32(&nd.tower[0].nextOffset)
		nextNd = (*node)(s.tbl.getPointer(offset))
	}
	return nextNd
}

func (s *skl) getSkipPrev(nd *node) *node {
	var prevNd *node
	skipToLastOffset := nd.skipToLastOffset()
	if skipToLastOffset > 0 {
		prevNd = (*node)(s.tbl.getPointer(skipToLastOffset))
	} else {
		offset := atomic.LoadUint32(&nd.tower[0].prevOffset)
		prevNd = (*node)(s.tbl.getPointer(offset))
	}
	return prevNd
}

func (s *skl) compareKey(key []byte, nd *node) (bool, internalKeyKind) {
	b := s.tbl.getBytes(nd.keyOffset, nd.keySize)
	l := len(b) - 8
	if l < 0 || s.cmp(key, b[:l:l]) != 0 {
		return false, internalKeyKindInvalid
	}

	return true, internalKeyKind(binary.LittleEndian.Uint64(b[l:]) & 0xff)
}

func (s *skl) seekForBaseSplice(key []byte) (prev, next *node, found bool) {
	ikey := base.MakeSearchKey(key)
	level := int(s.Height() - 1)

	prev = s.head
	for {
		prev, next, found = s.findSpliceForLevel(ikey, level, prev)

		if found {
			if level != 0 {
				prev = s.getPrev(next, 0)
			}
			break
		}

		if level == 0 {
			break
		}

		level--
	}

	return
}
