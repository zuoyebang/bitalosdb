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

package batchskl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/errors"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"golang.org/x/exp/rand"
)

const (
	maxHeight   = 20
	maxNodeSize = int(unsafe.Sizeof(node{}))
	linksSize   = int(unsafe.Sizeof(links{}))
)

var ErrExists = errors.New("record with this key already exists")

type links struct {
	next uint32
	prev uint32
}

type node struct {
	offset         uint32
	keyStart       uint32
	keyEnd         uint32
	abbreviatedKey uint64
	links          [maxHeight]links
}

type Skiplist struct {
	storage        *[]byte
	cmp            base.Compare
	abbreviatedKey base.AbbreviatedKey
	nodes          []byte
	head           uint32
	tail           uint32
	height         uint32
	rand           rand.PCGSource
}

var (
	probabilities [maxHeight]uint32
)

func init() {
	const pValue = 1 / math.E

	p := float64(1.0)
	for i := 0; i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

func NewSkiplist(storage *[]byte, cmp base.Compare, abbreviatedKey base.AbbreviatedKey) *Skiplist {
	s := &Skiplist{}
	s.Init(storage, cmp, abbreviatedKey)
	return s
}

func (s *Skiplist) Reset() {
	*s = Skiplist{
		nodes:  s.nodes[:0],
		height: 1,
	}
	const batchMaxRetainedSize = 1 << 20
	if cap(s.nodes) > batchMaxRetainedSize {
		s.nodes = nil
	}
}

func (s *Skiplist) Init(storage *[]byte, cmp base.Compare, abbreviatedKey base.AbbreviatedKey) {
	*s = Skiplist{
		storage:        storage,
		cmp:            cmp,
		abbreviatedKey: abbreviatedKey,
		nodes:          s.nodes[:0],
		height:         1,
	}
	s.rand.Seed(uint64(time.Now().UnixNano()))

	const initBufSize = 256
	if cap(s.nodes) < initBufSize {
		s.nodes = make([]byte, 0, initBufSize)
	}

	s.head = s.newNode(maxHeight, 0, 0, 0, 0)
	s.tail = s.newNode(maxHeight, 0, 0, 0, 0)

	headNode := s.node(s.head)
	tailNode := s.node(s.tail)
	for i := uint32(0); i < maxHeight; i++ {
		headNode.links[i].next = s.tail
		tailNode.links[i].prev = s.head
	}
}

func (s *Skiplist) Add(keyOffset uint32) error {
	data := (*s.storage)[keyOffset+1:]
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.Errorf("corrupted batch entry: %d", errors.Safe(keyOffset))
	}
	data = data[n:]
	if v > uint64(len(data)) {
		return errors.Errorf("corrupted batch entry: %d", errors.Safe(keyOffset))
	}
	keyStart := 1 + keyOffset + uint32(n)
	keyEnd := keyStart + uint32(v)
	key := data[:v]
	abbreviatedKey := s.abbreviatedKey(key)

	var spl [maxHeight]splice

	prev := s.getPrev(s.tail, 0)
	if prevNode := s.node(prev); prev == s.head ||
		abbreviatedKey > prevNode.abbreviatedKey ||
		(abbreviatedKey == prevNode.abbreviatedKey &&
			s.cmp(key, (*s.storage)[prevNode.keyStart:prevNode.keyEnd]) > 0) {
		for level := uint32(0); level < s.height; level++ {
			spl[level].prev = s.getPrev(s.tail, level)
			spl[level].next = s.tail
		}
	} else {
		s.findSplice(key, abbreviatedKey, &spl)
	}

	height := s.randomHeight()
	for ; s.height < height; s.height++ {
		spl[s.height].next = s.tail
		spl[s.height].prev = s.head
	}

	nd := s.newNode(height, keyOffset, keyStart, keyEnd, abbreviatedKey)
	newNode := s.node(nd)
	for level := uint32(0); level < height; level++ {
		next := spl[level].next
		prev := spl[level].prev
		newNode.links[level].next = next
		newNode.links[level].prev = prev
		s.node(next).links[level].prev = nd
		s.node(prev).links[level].next = nd
	}

	return nil
}

func (s *Skiplist) NewIter(lower, upper []byte) Iterator {
	return Iterator{list: s, lower: lower, upper: upper}
}

func (s *Skiplist) newNode(height,
	offset, keyStart, keyEnd uint32, abbreviatedKey uint64) uint32 {
	if height < 1 || height > maxHeight {
		panic("height cannot be less than one or greater than the max height")
	}

	unusedSize := (maxHeight - int(height)) * linksSize
	nodeOffset := s.alloc(uint32(maxNodeSize - unusedSize))
	nd := s.node(nodeOffset)

	nd.offset = offset
	nd.keyStart = keyStart
	nd.keyEnd = keyEnd
	nd.abbreviatedKey = abbreviatedKey
	return nodeOffset
}

func (s *Skiplist) alloc(size uint32) uint32 {
	offset := len(s.nodes)

	minAllocSize := offset + maxNodeSize
	if cap(s.nodes) < minAllocSize {
		allocSize := cap(s.nodes) * 2
		if allocSize < minAllocSize {
			allocSize = minAllocSize
		}
		tmp := make([]byte, len(s.nodes), allocSize)
		copy(tmp, s.nodes)
		s.nodes = tmp
	}

	newSize := uint32(offset) + size
	s.nodes = s.nodes[:newSize]
	return uint32(offset)
}

func (s *Skiplist) node(offset uint32) *node {
	return (*node)(unsafe.Pointer(&s.nodes[offset]))
}

func (s *Skiplist) randomHeight() uint32 {
	rnd := uint32(s.rand.Uint64())
	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}
	return h
}

func (s *Skiplist) findSplice(key []byte, abbreviatedKey uint64, spl *[maxHeight]splice) {
	prev := s.head

	for level := s.height - 1; ; level-- {
		next := s.getNext(prev, level)
		for next != s.tail {
			nextNode := s.node(next)
			nextAbbreviatedKey := nextNode.abbreviatedKey
			if abbreviatedKey < nextAbbreviatedKey {
				break
			}
			if abbreviatedKey == nextAbbreviatedKey {
				if s.cmp(key, (*s.storage)[nextNode.keyStart:nextNode.keyEnd]) <= 0 {
					break
				}
			}
			prev = next
			next = nextNode.links[level].next
		}

		spl[level].prev = prev
		spl[level].next = next
		if level == 0 {
			break
		}
	}
}

func (s *Skiplist) findSpliceForLevel(
	key []byte, abbreviatedKey uint64, level, start uint32,
) (prev, next uint32) {
	prev = start
	next = s.getNext(prev, level)

	for next != s.tail {
		nextNode := s.node(next)
		nextAbbreviatedKey := nextNode.abbreviatedKey
		if abbreviatedKey < nextAbbreviatedKey {
			break
		}
		if abbreviatedKey == nextAbbreviatedKey {
			if s.cmp(key, (*s.storage)[nextNode.keyStart:nextNode.keyEnd]) <= 0 {
				break
			}
		}

		prev = next
		next = nextNode.links[level].next
	}

	return
}

func (s *Skiplist) getKey(nd uint32) base.InternalKey {
	n := s.node(nd)
	kind := base.InternalKeyKind((*s.storage)[n.offset])
	key := (*s.storage)[n.keyStart:n.keyEnd]
	return base.MakeInternalKey(key, uint64(n.offset)|base.InternalKeySeqNumBatch, kind)
}

func (s *Skiplist) getNext(nd, h uint32) uint32 {
	return s.node(nd).links[h].next
}

func (s *Skiplist) getPrev(nd, h uint32) uint32 {
	return s.node(nd).links[h].prev
}

func (s *Skiplist) debug() string {
	var buf bytes.Buffer
	for level := uint32(0); level < s.height; level++ {
		var count int
		for nd := s.head; nd != s.tail; nd = s.getNext(nd, level) {
			count++
		}
		fmt.Fprintf(&buf, "%d: %d\n", level, count)
	}
	return buf.String()
}

var _ = (*Skiplist).debug
