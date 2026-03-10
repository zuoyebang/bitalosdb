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
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

func MaxNodeSize(keySize, valueSize uint32) uint32 {
	return uint32(maxNodeSize) + keySize + valueSize + align4
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

func newNode(
	arena *Arena, height uint32, key base.InternalKey, slotId uint16, valueSize int, values ...[]byte,
) (nd *node, err error) {
	keySize := key.Size()
	nd, err = newRawNode(arena, height, uint32(keySize), uint32(valueSize+kkv.SlotIdLength))
	if err != nil {
		return
	}

	key.Encode(nd.getKeyBytes(arena))
	valueBuf := nd.getValue(arena, false)
	kkv.EncodeSlotId(valueBuf, slotId)
	pos := kkv.SlotIdLength
	for i := range values {
		pos += copy(valueBuf[pos:], values[i])
	}

	return
}

func newRawNode(arena *Arena, height uint32, keySize, valueSize uint32) (nd *node, err error) {
	unusedSize := uint32((maxHeight - int(height)) * linksSize)
	nodeSize := uint32(maxNodeSize) - unusedSize

	nodeOffset, allocSize, err := arena.alloc(nodeSize+keySize+valueSize, align4, unusedSize)
	if err != nil {
		return
	}

	nd = (*node)(arena.getPointer(nodeOffset))
	nd.keyOffset = nodeOffset + nodeSize
	nd.keySize = keySize
	nd.valueSize = valueSize
	nd.allocSize = allocSize
	nd.skipToFirst = 0
	nd.skipToLast = 0
	return
}

func (n *node) getKeyBytes(arena *Arena) []byte {
	return arena.getBytes(n.keyOffset, n.keySize)
}

func (n *node) getValue(arena *Arena, skipSlotId bool) []byte {
	value := arena.getBytes(n.keyOffset+n.keySize, n.valueSize)
	if skipSlotId {
		return value[kkv.SlotIdLength:]
	} else {
		return value
	}
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
