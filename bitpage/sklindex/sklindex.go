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

package sklindex

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/fastrand"
	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
)

const (
	maxHeight      = 14
	maxNodeSize    = int(unsafe.Sizeof(node{}))
	linksSize      = int(unsafe.Sizeof(links{}))
	pValue         = 1 / math.E
	arriOffsetSize = 4
)

type Skiplist struct {
	arena              *Arena
	cmp                base.Compare
	head               *node
	tail               *node
	height             uint32
	itemCount          atomic.Int32
	refcnt             atomic.Int32
	arrayIndexLen      int
	arrayIndex         []uint32
	arrayIndexByte     []byte
	fetchIKeyCallback  func(uint32) base.InternalKey
	fetchValueCallback func(uint32) []byte
}

type Inserter struct {
	spl    [maxHeight]splice
	height uint32
	serial bool
}

func NewSerialInserter() *Inserter {
	return &Inserter{
		serial: true,
	}
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

func NewSkiplist(ikcb func(uint32) base.InternalKey, vcb func(uint32) []byte) *Skiplist {
	arena := NewArena()
	head, err := newNode(arena, maxHeight, 0, -1)
	if err != nil {
		panic("arenaSize is not large enough to hold the head node")
	}

	tail, err := newNode(arena, maxHeight, 0, -1)
	if err != nil {
		panic("arenaSize is not large enough to hold the tail node")
	}

	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset = tail.ndOffset
		tail.tower[i].prevOffset = head.ndOffset
	}

	s := &Skiplist{}
	s.arena = arena
	s.cmp = base.DefaultComparer.Compare
	s.head = head
	s.tail = tail
	s.height = 1
	s.refcnt.Store(1)
	s.itemCount.Store(0)
	s.fetchIKeyCallback = ikcb
	s.fetchValueCallback = vcb

	return s
}

func (s *Skiplist) Close() {
	s.arena.free()
	s.freeArrayIndex()
}

func (s *Skiplist) GetRefCnt() int32 {
	return s.refcnt.Load()
}

func (s *Skiplist) Ref() {
	s.refcnt.Add(1)
}

func (s *Skiplist) Unref() {
	if s.refcnt.Add(-1) == 0 {
		s.Close()
	}
}

func (s *Skiplist) Height() uint32 {
	return atomic.LoadUint32(&s.height)
}

func (s *Skiplist) Arena() *Arena {
	return s.arena
}

func (s *Skiplist) Size() uint32 {
	return s.arena.size()
}

func (s *Skiplist) SkiItemCount() uint32 {
	return uint32(s.itemCount.Load())
}

func (s *Skiplist) ItemCount() int {
	return int(s.itemCount.Load()) + s.arrayIndexLen
}

func (s *Skiplist) ArrayIndex() []uint32 {
	return s.arrayIndex
}

func (s *Skiplist) findArriOffset(key base.InternalKey, ins *Inserter) (int32, error) {
	var level int
	var foundKey bool
	var next *node

	listHeight := s.Height()
	insHeight := ins.height
	prev := s.head
	if insHeight < listHeight {
		ins.height = listHeight
		level = int(ins.height)
	} else {
		for ; level < int(listHeight); level++ {
			spl := &ins.spl[level]
			if s.getNext(spl.prev, level) != spl.next {
				continue
			}
			if spl.prev != s.head && !ins.serial && !s.keyIsAfterNode(spl.prev, key) {
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
		prev, next, foundKey, _ = s.findSpliceForLevel(key, level, prev)
		if next == nil {
			next = s.tail
		}
		ins.spl[level].init(prev, next)
	}

	if foundKey {
		return -1, base.ErrRecordExists
	}

	arriOffset := s.seekForArrayIndex(ins.spl[0].prev.arriOffset, ins.spl[0].next.arriOffset, key)
	return arriOffset, nil
}

func (s *Skiplist) Add(key base.InternalKey, itemOffset uint32, ins *Inserter) error {
	arriOffset, err := s.findArriOffset(key, ins)
	if err != nil {
		return err
	}

	var nd *node
	var height uint32
	nd, height, err = s.newNode(itemOffset, arriOffset)
	if err != nil {
		return err
	}

	var invalidateSplice, foundKey bool
	ndOffset := nd.ndOffset

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
			prevOffset := prev.ndOffset
			nextOffset := next.ndOffset
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

			prev, next, foundKey, _ = s.findSpliceForLevel(key, i, prev)
			if foundKey {
				return base.ErrRecordExists
			}

			invalidateSplice = true
		}
	}

	if invalidateSplice {
		ins.height = 0
	} else {
		for i := uint32(0); i < height; i++ {
			ins.spl[i].prev = nd
		}
	}

	s.itemCount.Add(1)

	return nil
}

func (s *Skiplist) seekForArrayIndex(start, end int32, key base.InternalKey) int32 {
	var offset int32

	offset = -1
	if s.arrayIndexLen == 0 {
		return offset
	}

	if start == -1 {
		start = 0
	}
	if end == -1 {
		end = int32(s.arrayIndexLen)
	}

	if start < end {
		pos := -1
		searchIdx := s.arrayIndex[start:end]
		searchIdxLen := len(searchIdx)
		if searchIdxLen > 0 {
			pos = sort.Search(searchIdxLen, func(i int) bool {
				ikey := s.fetchIKeyCallback(searchIdx[i])
				r := s.cmp(ikey.UserKey, key.UserKey)
				if r != 0 {
					return r != -1
				} else {
					return ikey.Trailer <= key.Trailer
				}
			})
			offset = int32(pos) + start
		}
	} else if start == end {
		offset = start
	}

	return offset
}

func (s *Skiplist) Get(key []byte) ([]byte, bool, base.InternalKeyKind, uint64) {
	exist, ndOffset := s.findKeyOffset(key)
	if !exist {
		return nil, false, base.InternalKeyKindInvalid, 0
	}

	ndKey := s.fetchIKeyCallback(ndOffset)
	if s.cmp(key, ndKey.UserKey) != 0 {
		return nil, false, base.InternalKeyKindInvalid, 0
	}

	kind := ndKey.Kind()
	sn := ndKey.SeqNum()
	switch kind {
	case base.InternalKeyKindSet:
		return s.fetchValueCallback(ndOffset), true, kind, sn
	case base.InternalKeyKindDelete, base.InternalKeyKindPrefixDelete:
		return nil, true, kind, sn
	default:
		return nil, false, base.InternalKeyKindInvalid, 0
	}
}

func (s *Skiplist) Exist(key []byte) (bool, base.InternalKeyKind) {
	exist, ndOffset := s.findKeyOffset(key)
	if !exist {
		return false, base.InternalKeyKindInvalid
	}

	ndKey := s.fetchIKeyCallback(ndOffset)
	if s.cmp(key, ndKey.UserKey) != 0 {
		return false, base.InternalKeyKindInvalid
	}

	return true, ndKey.Kind()
}

func (s *Skiplist) findKeyOffset(key []byte) (bool, uint32) {
	var prev, next *node
	var foundKey bool
	var arriOffset int32

	ikey := base.MakeSearchKey(key)
	level := int(s.Height() - 1)
	prev = s.head

	for {
		prev, next, _, foundKey = s.findSpliceForLevel(ikey, level, prev)
		if level == 0 {
			break
		}
		level--
	}

	arriOffset = -1

	if !foundKey {
		arriOffset = s.seekForArrayIndex(prev.arriOffset, next.arriOffset, ikey)
	}

	if next == s.tail && arriOffset == -1 {
		return false, 0
	}

	var itemOffset uint32
	if arriOffset == -1 {
		itemOffset = next.itemOffset
	} else {
		if int(arriOffset) >= s.arrayIndexLen {
			return false, 0
		}
		itemOffset = s.arrayIndex[arriOffset]
	}

	return true, itemOffset
}

func (s *Skiplist) seekForIterator(key []byte) (prev, next *node, arriOffset int32) {
	ikey := base.MakeSearchKey(key)
	level := int(s.Height() - 1)

	prev = s.head
	for {
		prev, next, _, _ = s.findSpliceForLevel(ikey, level, prev)
		if level == 0 {
			break
		}
		level--
	}

	arriOffset = s.seekForArrayIndex(prev.arriOffset, next.arriOffset, ikey)
	return
}

func (s *Skiplist) NewIter(lower, upper []byte) *Iterator {
	it := iterPool.Get().(*Iterator)
	*it = Iterator{
		list:  s,
		nd:    s.head,
		start: -1,
		end:   -1,
		lower: lower,
		upper: upper,
	}
	it.list.Ref()
	return it
}

func (s *Skiplist) GetAllIndexOffset() []uint32 {
	arri := make([]uint32, 0, s.ItemCount())
	iter := s.NewIter(nil, nil)
	defer iter.Close()
	for offset := iter.FirstOffset(); offset != 0; offset = iter.NextOffset() {
		arri = append(arri, offset)
	}
	return arri
}

func (s *Skiplist) allocArrayIndex(n int) {
	s.arrayIndexByte = manual.New(n * arriOffsetSize)
	s.arrayIndex = (*(*[math.MaxUint32]uint32)(unsafe.Pointer(&s.arrayIndexByte[0])))[:n:n]
}

func (s *Skiplist) freeArrayIndex() {
	if s.arrayIndexByte != nil {
		manual.Free(s.arrayIndexByte)
		s.arrayIndexByte = nil
	}
	s.arrayIndex = nil
	s.arrayIndexLen = 0
}

func (s *Skiplist) LoadArrayIndex(b []byte, n int) {
	s.allocArrayIndex(n)
	s.arrayIndexLen = n
	pos := 0
	for i := 0; i < n; i++ {
		s.arrayIndex[i] = binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
	}
}

func (s *Skiplist) FlushToNewSkl() *Skiplist {
	n := s.ItemCount() + rand.Intn(16)
	skli := NewSkiplist(s.fetchIKeyCallback, s.fetchValueCallback)
	skli.allocArrayIndex(n)

	iter := s.NewIter(nil, nil)
	defer iter.Close()
	i := 0
	for offset := iter.FirstOffset(); offset != 0; offset = iter.NextOffset() {
		skli.arrayIndex[i] = offset
		i++
	}

	if i < n {
		skli.arrayIndex = skli.arrayIndex[:i:i]
	}

	skli.arrayIndexLen = len(skli.arrayIndex)

	return skli
}

func (s *Skiplist) newNode(itemOffset uint32, arriOffset int32) (nd *node, height uint32, err error) {
	height = s.randomHeight()
	nd, err = newNode(s.arena, height, itemOffset, arriOffset)
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

func (s *Skiplist) findSpliceForLevel(
	key base.InternalKey, level int, start *node,
) (prev, next *node, foundIkey, foundKey bool) {
	prev = start

	for {
		next = s.getNext(prev, level)
		if next == s.tail {
			break
		}

		nextKey := s.fetchIKeyCallback(next.itemOffset)
		cmp := s.cmp(key.UserKey, nextKey.UserKey)
		if cmp < 0 {
			break
		}
		if cmp == 0 {
			foundKey = true
			if key.Trailer > nextKey.Trailer {
				break
			} else if key.Trailer == nextKey.Trailer {
				foundIkey = true
				break
			}
		}

		prev = next
	}

	return
}

func (s *Skiplist) keyIsAfterNode(nd *node, key base.InternalKey) bool {
	ndKey := s.fetchIKeyCallback(nd.itemOffset)
	cmp := s.cmp(ndKey.UserKey, key.UserKey)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}

	ndTrailer := ndKey.Trailer
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
