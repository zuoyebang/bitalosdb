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
	"sync/atomic"
)

func sklNodeMaxSize(keySize, valueSize uint32) uint32 {
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
	keyOffset uint32
	keySize   uint32
	valueSize uint32
	allocSize uint32

	skipToFirst uint32
	skipToLast  uint32

	tower [maxHeight]links
}

func newNode(tbl *table, height uint32, key internalKey, value []byte) (*node, error) {
	keySize := key.Size()
	valueSize := len(value)

	nd, err := newRawNode(tbl, height, uint32(keySize), uint32(valueSize))
	if err != nil {
		return nil, err
	}

	key.Encode(nd.getKeyBytes(tbl))
	copy(nd.getValue(tbl), value)
	return nd, nil
}

func newRawNode(tbl *table, height uint32, keySize, valueSize uint32) (*node, error) {
	unusedSize := uint32((maxHeight - int(height)) * linksSize)
	nodeSize := uint32(maxNodeSize) - unusedSize

	nodeOffset, allocSize, err := tbl.allocAlign(nodeSize+keySize+valueSize, align4, unusedSize)
	if err != nil {
		return nil, err
	}

	nd := (*node)(tbl.getPointer(nodeOffset))
	nd.keyOffset = nodeOffset + nodeSize
	nd.keySize = keySize
	nd.valueSize = valueSize
	nd.allocSize = allocSize
	nd.skipToFirst = 0
	nd.skipToLast = 0
	return nd, nil
}

func (n *node) getKeyBytes(tbl *table) []byte {
	return tbl.getBytes(n.keyOffset, n.keySize)
}

func (n *node) getValue(tbl *table) []byte {
	return tbl.getBytes(n.keyOffset+n.keySize, n.valueSize)
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
