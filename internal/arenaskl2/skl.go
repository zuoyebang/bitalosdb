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

package arenaskl2

import (
	"encoding/binary"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/fastrand"
)

const (
	maxHeight   = 19
	maxNodeSize = int(unsafe.Sizeof(node{}))
	linksSize   = int(unsafe.Sizeof(links{}))
	pValue      = 1 / math.E
)

type Skiplist struct {
	arena   *Arena
	cmp     base.Compare
	head    *node
	tail    *node
	height  uint32
	testing bool
}

type Inserter struct {
	spl    [maxHeight]splice
	height uint32
}

func (ins *Inserter) AddTest(list *Skiplist, key base.InternalKey, value []byte) error {
	_, _, err := list.FindForAdd(key, ins)
	if err == nil {
		_, _, err = list.AddInserter(key, ins, 1, len(value), value)
	}
	return err
}

var (
	probabilities [maxHeight]uint32
)

func init() {
	p := 1.0
	for i := 0; i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

func NewSkiplist(arena *Arena, cmp base.Compare) *Skiplist {
	skl := &Skiplist{}
	skl.Reset(arena, cmp)
	return skl
}

func (s *Skiplist) Reset(arena *Arena, cmp base.Compare) {
	head, err := newRawNode(arena, maxHeight, 0, 0)
	if err != nil {
		panic("arenaSize is not large enough to hold the head node")
	}
	head.keyOffset = 0
	head.skipToFirst = 0
	head.skipToLast = 0

	tail, err := newRawNode(arena, maxHeight, 0, 0)
	if err != nil {
		panic("arenaSize is not large enough to hold the tail node")
	}
	tail.keyOffset = 0
	tail.skipToFirst = 0
	tail.skipToLast = 0

	headOffset := arena.getPointerOffset(unsafe.Pointer(head))
	tailOffset := arena.getPointerOffset(unsafe.Pointer(tail))
	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset = tailOffset
		tail.tower[i].prevOffset = headOffset
	}

	*s = Skiplist{
		arena:  arena,
		cmp:    cmp,
		head:   head,
		tail:   tail,
		height: 1,
	}
}

func (s *Skiplist) Height() uint32 { return atomic.LoadUint32(&s.height) }

func (s *Skiplist) Arena() *Arena { return s.arena }

func (s *Skiplist) Size() uint32 { return s.arena.Size() }

func (s *Skiplist) AddTest(key base.InternalKey, value []byte) error {
	var ins Inserter
	_, _, err := s.FindForAdd(key, &ins)
	if err == nil {
		_, _, err = s.AddInserter(key, &ins, 1, len(value), value)
	}
	return err
}

func (s *Skiplist) AddInserter(
	key base.InternalKey, ins *Inserter, slotId uint16, valueSize int, values ...[]byte,
) (bool, base.InternalKeyKind, error) {
	nd, height, err := s.newNode(key, slotId, valueSize, values...)
	if err != nil {
		return false, 0, err
	}

	var invalidateSplice, foundIkey, foundKey bool
	var foundKeyKind base.InternalKeyKind
	ndOffset := s.arena.getPointerOffset(unsafe.Pointer(nd))

	for i := 0; i < int(height); i++ {
		prev := ins.spl[i].prev
		next := ins.spl[i].next

		if prev == nil {
			if next != nil {
				panic("next is expected to be nil, since prev is nil")
			}

			prev = s.head
			next = s.tail
		}

		for {
			prevOffset := s.arena.getPointerOffset(unsafe.Pointer(prev))
			nextOffset := s.arena.getPointerOffset(unsafe.Pointer(next))
			nd.tower[i].init(prevOffset, nextOffset)

			nextPrevOffset := next.prevOffset(i)
			if nextPrevOffset != prevOffset {
				prevNextOffset := prev.nextOffset(i)
				if prevNextOffset == nextOffset {
					next.casPrevOffset(i, nextPrevOffset, prevOffset)
				}
			}

			if prev.casNextOffset(i, nextOffset, ndOffset) {
				next.casPrevOffset(i, prevOffset, ndOffset)
				break
			}

			prev, next, foundIkey, foundKey, foundKeyKind = s.findSpliceForLevel(key, i, prev)
			if foundIkey {
				return foundKey, foundKeyKind, base.ErrRecordExists
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

	return foundKey, foundKeyKind, nil
}

func (s *Skiplist) setNodeSkipOffset(nd *node, ndOffset uint32, key base.InternalKey) {
	nextNd := s.getNext(nd, 0)
	if nextNd == s.tail {
		return
	}

	offset, size := nextNd.keyOffset, nextNd.keySize
	nextKey := s.arena.buf[offset : offset+size]
	n := int32(size) - 8
	if n < 0 || s.cmp(key.UserKey, nextKey[:n]) != 0 || key.Trailer <= binary.LittleEndian.Uint64(nextKey[n:]) {
		return
	}

	skipToFirstOffset := nextNd.skipToFirstOffset()
	if skipToFirstOffset > 0 {
		nd.setSkipToFirstOffset(skipToFirstOffset)

		skipToFirstNd := (*node)(s.arena.getPointer(skipToFirstOffset))
		if skipToFirstNd == s.tail {
			return
		}

		skipToFirstNd.setSkipToLastOffset(ndOffset)
	} else {
		nextNdOffset := s.arena.getPointerOffset(unsafe.Pointer(nextNd))
		nd.setSkipToFirstOffset(nextNdOffset)
	}
}

func (s *Skiplist) Get(key []byte) ([]byte, bool, base.InternalKeyKind) {
	var nd *node
	_, nd, _ = s.seekForBaseSplice(key)
	if nd == s.tail {
		return nil, false, base.InternalKeyKindInvalid
	}

	b := s.arena.getBytes(nd.keyOffset, nd.keySize)
	l := len(b) - 8
	if l < 0 || s.cmp(key, b[:l:l]) != 0 {
		return nil, false, base.InternalKeyKindInvalid
	}

	kind := base.InternalKeyKind(binary.LittleEndian.Uint64(b[l:]) & 0xff)
	value := nd.getValue(s.arena, true)
	return value, true, kind
}

func (s *Skiplist) Exist(key []byte) (bool, base.InternalKeyKind) {
	var nd *node
	_, nd, _ = s.seekForBaseSplice(key)
	if nd == s.tail {
		return false, base.InternalKeyKindInvalid
	}

	b := s.arena.getBytes(nd.keyOffset, nd.keySize)
	l := len(b) - 8
	if l < 0 || s.cmp(key, b[:l:l]) != 0 {
		return false, base.InternalKeyKindInvalid
	}

	kind := base.InternalKeyKind(binary.LittleEndian.Uint64(b[l:]) & 0xff)
	switch kind {
	case base.InternalKeyKindSet, base.InternalKeyKindDelete, base.InternalKeyKindPrefixDelete:
		return true, kind
	default:
		return false, base.InternalKeyKindInvalid
	}
}

func (s *Skiplist) seekForBaseSplice(key []byte) (prev, next *node, found bool) {
	ikey := base.MakeSearchKey(key)
	level := int(s.Height() - 1)

	prev = s.head
	for {
		prev, next, found, _, _ = s.findSpliceForLevel(ikey, level, prev)

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

func (s *Skiplist) NewIter(lower, upper []byte) *Iterator {
	it := iterPool.Get().(*Iterator)
	*it = Iterator{list: s, nd: s.head, lower: lower, upper: upper}
	return it
}

func (s *Skiplist) NewFlushIter(bytesFlushed *uint64) base.InternalIterator {
	return &flushIterator{
		Iterator:      Iterator{list: s, nd: s.head},
		bytesIterated: bytesFlushed,
	}
}

func (s *Skiplist) newNode(
	key base.InternalKey, slotId uint16, valueSize int, values ...[]byte,
) (nd *node, height uint32, err error) {
	height = s.randomHeight()
	nd, err = newNode(s.arena, height, key, slotId, valueSize, values...)
	if err != nil {
		return
	}

	listHeight := s.Height()
	for height > listHeight {
		if atomic.CompareAndSwapUint32(&s.height, listHeight, height) {
			break
		}

		listHeight = s.Height()
	}

	return
}

func (s *Skiplist) randomHeight() uint32 {
	rnd := fastrand.Uint32()

	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}

	return h
}

func (s *Skiplist) FindForAdd(key base.InternalKey, ins *Inserter) (
	foundKey bool, foundKeyKind base.InternalKeyKind, err error,
) {
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

	var foundIkey bool

	for level = level - 1; level >= 0; level-- {
		var next *node
		prev, next, foundIkey, foundKey, foundKeyKind = s.findSpliceForLevel(key, level, prev)
		if next == nil {
			next = s.tail
		}
		ins.spl[level].init(prev, next)
	}

	if foundIkey {
		err = base.ErrRecordExists
	}

	return
}

func (s *Skiplist) findSpliceForLevel(
	key base.InternalKey, level int, start *node,
) (prev, next *node, foundIkey, foundKey bool, foundKeyKind base.InternalKeyKind) {
	prev = start

	for {
		next = s.getNext(prev, level)
		if next == s.tail {
			break
		}

		offset, size := next.keyOffset, next.keySize
		nextKey := s.arena.buf[offset : offset+size]
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
				nextTrailer = uint64(base.InternalKeyKindInvalid)
			}

			foundKey = true
			foundKeyKind = base.DecodeKind(nextTrailer)

			if key.Trailer > nextTrailer {
				break
			} else if key.Trailer == nextTrailer {
				foundIkey = true
				break
			}
		}

		prev = next
	}

	return
}

func (s *Skiplist) keyIsAfterNode(nd *node, key base.InternalKey) bool {
	ndKey := s.arena.buf[nd.keyOffset : nd.keyOffset+nd.keySize]
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
		ndTrailer = uint64(base.InternalKeyKindInvalid)
	}
	if key.Trailer == ndTrailer {
		return false
	}
	return key.Trailer < ndTrailer
}

func (s *Skiplist) getNext(nd *node, h int) *node {
	offset := atomic.LoadUint32(&nd.tower[h].nextOffset)
	return (*node)(s.arena.getPointer(offset))
}

func (s *Skiplist) getPrev(nd *node, h int) *node {
	offset := atomic.LoadUint32(&nd.tower[h].prevOffset)
	return (*node)(s.arena.getPointer(offset))
}

func (s *Skiplist) getSkipNext(nd *node) *node {
	var nextNd *node
	skipToFirstOffset := nd.skipToFirstOffset()
	if skipToFirstOffset > 0 {
		nextNd = (*node)(s.arena.getPointer(skipToFirstOffset))
	} else {
		offset := atomic.LoadUint32(&nd.tower[0].nextOffset)
		nextNd = (*node)(s.arena.getPointer(offset))
	}
	return nextNd
}

func (s *Skiplist) getSkipPrev(nd *node) *node {
	var prevNd *node
	skipToLastOffset := nd.skipToLastOffset()
	if skipToLastOffset > 0 {
		prevNd = (*node)(s.arena.getPointer(skipToLastOffset))
	} else {
		offset := atomic.LoadUint32(&nd.tower[0].prevOffset)
		prevNd = (*node)(s.arena.getPointer(offset))
	}
	return prevNd
}
