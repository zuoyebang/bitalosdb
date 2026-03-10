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
	"sync/atomic"
)

type links struct {
	nextOffset uint32
	prevOffset uint32
}

func (l *links) init(prevOffset, nextOffset uint32) {
	l.nextOffset = nextOffset
	l.prevOffset = prevOffset
}

type node struct {
	ndOffset   uint32
	itemOffset uint32
	arriOffset int32
	tower      [maxHeight]links
}

func newNode(arena *Arena, height uint32, itemOffset uint32, arriOffset int32) (*node, error) {
	unusedSize := uint32((maxHeight - int(height)) * linksSize)
	nodeSize := uint32(maxNodeSize) - unusedSize

	nodeOffset, err := arena.alloc(nodeSize, unusedSize)
	if err != nil {
		return nil, err
	}

	nd := (*node)(arena.getPointer(nodeOffset))
	nd.ndOffset = nodeOffset
	nd.itemOffset = itemOffset
	nd.arriOffset = arriOffset
	return nd, nil
}

func (n *node) nextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h].nextOffset)
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
