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

package arenaskl

import (
	"math"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/cache/lfucache/internal/base"
)

func MaxNodeSize(keySize, valueSize uint32) uint32 {
	return uint32(maxNodeSize) + keySize + valueSize + Align4
}

type links struct {
	nextOffset uint32
	prevOffset uint32
}

func (l *links) init(prevOffset, nextOffset uint32) {
	l.nextOffset = nextOffset
	l.prevOffset = prevOffset
}

type node struct {
	keyOffset   uint32
	keySize     uint32
	valueSize   uint32
	allocSize   uint32
	skipToFirst uint32
	skipToLast  uint32
	tower       [maxHeight]links
}

func newNode(ar *Arena, height uint32, key base.InternalKey, value []byte) (nd *node, err error) {
	if height < 1 || height > maxHeight {
		panic("height cannot be less than one or greater than the max height")
	}
	keySize := key.Size()
	if int64(keySize) > math.MaxUint32 {
		panic("key is too large")
	}
	valueSize := len(value)
	if int64(len(value)) > math.MaxUint32 {
		panic("value is too large")
	}

	nd, err = newRawNode(ar, height, uint32(keySize), uint32(valueSize))
	if err != nil {
		return
	}

	key.Encode(nd.getKeyBytes(ar))
	copy(nd.getValue(ar), value)
	return
}

func newRawNode(ar *Arena, height uint32, keySize, valueSize uint32) (nd *node, err error) {
	unusedSize := uint32((maxHeight - int(height)) * linksSize)
	nodeSize := uint32(maxNodeSize) - unusedSize

	nodeOffset, allocSize, err := ar.alloc(nodeSize+keySize+valueSize, Align4, unusedSize)
	if err != nil {
		return
	}

	nd = (*node)(ar.GetPointer(nodeOffset))
	nd.keyOffset = nodeOffset + nodeSize
	nd.keySize = keySize
	nd.valueSize = valueSize
	nd.allocSize = allocSize
	nd.skipToFirst = 0
	nd.skipToLast = 0
	return
}

func (n *node) getKeyBytes(ar *Arena) []byte {
	return ar.GetBytes(n.keyOffset, n.keySize)
}

func (n *node) getValue(ar *Arena) []byte {
	return ar.GetBytes(n.keyOffset+n.keySize, n.valueSize)
}

func (n *node) nextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h].nextOffset)
}

func (n *node) skipToFirstOffset() uint32 {
	return atomic.LoadUint32(&n.skipToFirst)
}

func (n *node) setSkipToFirstOffset(val uint32) {
	atomic.StoreUint32(&n.skipToFirst, val)
}

func (n *node) skipToLastOffset() uint32 {
	return atomic.LoadUint32(&n.skipToLast)
}

func (n *node) setSkipToLastOffset(val uint32) {
	atomic.StoreUint32(&n.skipToLast, val)
}

func (n *node) prevOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h].prevOffset)
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h].nextOffset, old, val)
}

func (n *node) casPrevOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h].prevOffset, old, val)
}
