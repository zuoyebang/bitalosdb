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

package bitpage

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	BitrieVersion    = 1
	BitrieHeaderSize = 18
	BitrieKeySize    = 1
	BitrieIndexSize  = 4
	PruneKeySize     = 2
	PruneKeyValFlag  = 1
)

const (
	InternalKindKeyPrune      uint8 = 1
	InternalKindHasValue      uint8 = 2
	InternalKindHasChildrenL1 uint8 = 4
	InternalKindHasChildrenL2 uint8 = 8
	InternalKindHasChildrenL3 uint8 = 16

	KindChildrenL1Step uint32 = 65536
	KindChildrenL2Step uint32 = 16777216
)

type Header struct {
	version     uint16
	reserved    uint16
	keyOffset   uint16
	indexOffset uint32
	dataOffset  uint32
	size        uint32
}

type Bitrie struct {
	header   Header
	length   uint32
	data     []byte
	children map[uint8]*trienode
}

type trienode struct {
	key      uint8
	prune    []byte
	value    []byte
	children map[uint8]*trienode
}

type disknode struct {
	prune      []byte
	value      []byte
	childCount uint8
	childIndex uint32
}

func NewBitrie() *Bitrie {
	root := &Bitrie{
		header:   Header{version: BitrieVersion},
		length:   1,
		data:     nil,
		children: nil,
	}

	return root
}

func (bt *Bitrie) InitWriter() {
	bt.length = 1
	bt.children = make(map[uint8]*trienode, 1<<10)
}

func (bt *Bitrie) SetReader(d []byte, offset uint32) bool {
	if d == nil {
		return false
	}

	bt.data = d
	bt.header = bt.readHeader(bt.data[offset:])

	return len(d) >= int(bt.header.size)
}

func (bt *Bitrie) Size() uint32 {
	return bt.header.size
}

func (bt *Bitrie) Add(key []byte, value []byte) {
	keyLength := len(key)
	if keyLength <= 0 || len(value) <= 0 {
		return
	}

	var ok bool
	var childNode *trienode

	children := bt.children

	for i := 0; i < keyLength; i++ {
		if childNode, ok = children[key[i]]; !ok {
			newNode := &trienode{
				key:      key[i],
				prune:    key[i+1:],
				value:    value,
				children: make(map[byte]*trienode, 1<<3),
			}
			children[key[i]] = newNode
			bt.length += 1
			break
		} else if pruneKeyLength := len(childNode.prune); pruneKeyLength > 0 {
			m := 0
			n := i + 1
			for m < pruneKeyLength && n < keyLength {
				if childNode.prune[m] != key[n] {
					break
				}

				m++
				n++
			}

			tailKeyLength := keyLength - i - 1
			if m == 0 {
				if pruneKeyLength > tailKeyLength {
					if n <= keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], value, childNode)
						childNode.prune = nil
						bt.length += 2
					} else if n > keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						childNode.prune = nil
						childNode.value = value
						bt.length += 1
					}
				} else if pruneKeyLength == tailKeyLength {
					if n <= keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], value, childNode)
						childNode.prune = nil
						bt.length += 2
					} else if n > keyLength-1 {
						childNode.value = value
					}
				} else if pruneKeyLength < tailKeyLength {
					bt.newPruneChildByNode(childNode, m)
					bt.newPruneChildByKey(key[n:], value, childNode)
					childNode.prune = nil
					bt.length += 2
				}
				break
			} else if m > 0 {
				if pruneKeyLength > tailKeyLength {
					if n <= keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], value, childNode)
						childNode.prune = childNode.prune[:m]
						bt.length += 2
					} else if n > keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						childNode.value = value
						childNode.prune = childNode.prune[:m]
						bt.length += 1
					}
					break
				} else if pruneKeyLength == tailKeyLength {
					if n <= keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], value, childNode)
						childNode.prune = childNode.prune[:m]
						bt.length += 2
					} else if n > keyLength-1 {
						childNode.value = value
					}
					break
				} else if pruneKeyLength < tailKeyLength {
					if m <= pruneKeyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], value, childNode)
						childNode.prune = childNode.prune[:m]
						bt.length += 2
						break
					} else if m > pruneKeyLength-1 {
						i += m
					}
				}
			}
		}

		children = childNode.children
	}
}

func (bt *Bitrie) Finish() {
	bt.children = nil
}

func (bt *Bitrie) Serialize(
	tblalloc func(uint32) uint32,
	tblbytes func(uint32, uint32) []byte,
	tblsize func() uint32) bool {
	if bt.length <= 0 {
		return false
	}

	itemIndex := uint32(1)

	headerOffset := tblalloc(BitrieHeaderSize + bt.length)
	keyOffset := headerOffset + BitrieHeaderSize

	idxSize := bt.length * BitrieIndexSize
	indexOffset := tblalloc(idxSize)

	dataOffset := indexOffset + idxSize

	bt.header.keyOffset = uint16(keyOffset)
	bt.header.indexOffset = indexOffset
	bt.header.dataOffset = dataOffset

	wrBuf := make([]byte, 256<<10)
	dkNode := disknode{
		prune:      nil,
		value:      nil,
		childCount: 0,
		childIndex: 0,
	}

	if len(bt.children) > 0 {
		bt.writeKey(tblbytes(keyOffset, BitrieKeySize), 0)
		keyOffset += BitrieKeySize

		bt.writeIndex(tblbytes(indexOffset, BitrieIndexSize), dataOffset)
		indexOffset += BitrieIndexSize

		dkNode.childIndex = itemIndex
		dkNode.childCount = uint8(len(bt.children) - 1)

		wbuf, wsize := bt.writeNode(wrBuf[0:], &dkNode)
		offset := tblalloc(wsize)
		copy(tblbytes(offset, wsize), wbuf)
		dataOffset += wsize
	} else {
		return false
	}

	Queue := list.New()
	bt.pushQueue(Queue, bt.children)

	for Queue.Len() > 0 {
		elem := Queue.Front()
		node := elem.Value.(*trienode)

		bt.writeKey(tblbytes(keyOffset, BitrieKeySize), node.key)
		keyOffset += BitrieKeySize

		bt.writeIndex(tblbytes(indexOffset, BitrieIndexSize), dataOffset)

		dkNode.prune = node.prune
		dkNode.value = node.value
		if len(node.children) > 0 {
			dkNode.childIndex = itemIndex + uint32(Queue.Len())
			dkNode.childCount = uint8(len(node.children) - 1)
		} else {
			dkNode.childIndex = 0
			dkNode.childCount = 0
		}
		itemIndex++

		wbuf, wsize := bt.writeNode(wrBuf[0:], &dkNode)
		offset := tblalloc(wsize)
		copy(tblbytes(offset, wsize), wbuf)

		indexOffset += BitrieIndexSize
		dataOffset += wsize

		bt.pushQueue(Queue, node.children)
		Queue.Remove(elem)
	}

	bt.header.size = tblsize()
	bt.writeHeader(tblbytes(headerOffset, BitrieHeaderSize), bt.header)

	return true
}

func (bt *Bitrie) Get(key []byte) ([]byte, bool) {
	keyOffset := uint32(bt.header.keyOffset)
	indexOffset := bt.header.indexOffset

	node := bt.readNode(bt.data[bt.header.dataOffset:], 0)
	childCount := node.childCount
	childIndex := node.childIndex

	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		tmpChildCount := uint32(childCount)
		if childIndex > 0 {
			tmpChildCount++
		}

		find, childPos := bt.findNode(key[i], bt.data[keyOffset+childIndex:], tmpChildCount)
		if find {
			childIndex += childPos
			nsize, offset := bt.getNodeSizeAndOffset(bt.data, indexOffset+childIndex*BitrieIndexSize)
			node = bt.readNode(bt.data[offset:], nsize)

			valueLength := len(node.value)
			pruneLength := len(node.prune)
			if pruneLength > 0 {
				curKeyPos := i + 1 + pruneLength
				if curKeyPos <= keyLength && bytes.Equal(node.prune, key[i+1:curKeyPos]) {
					i += pruneLength
					if i == keyLength-1 && valueLength > 0 {
						return node.value, true
					} else {
						childCount = node.childCount
						childIndex = node.childIndex
						continue
					}
				} else {
					return nil, false
				}
			}
			childCount = node.childCount
			childIndex = node.childIndex

			if i == keyLength-1 && valueLength > 0 {
				return node.value, true
			}
		} else {
			return nil, false
		}
	}

	return nil, false
}

func (bt *Bitrie) ToBytes() []byte {
	buf := make([]byte, 0, 1024)

	queue := list.New()
	bt.pushQueue(queue, bt.children)

	for queue.Len() > 0 {
		elem := queue.Front()
		node := elem.Value.(*trienode)

		buf = append(buf, fmt.Sprintf("key=%c; prune=%s; value=%s; ", node.key, node.prune, node.value)...)
		if len(node.children) > 0 {
			buf = append(buf, fmt.Sprintf("children[%d]=[", len(node.children))...)
			for k, _ := range node.children {
				buf = append(buf, fmt.Sprintf("k=%c, ", k)...)
			}
			buf = append(buf, "]\n"...)
		} else {
			buf = append(buf, "children=[0]\n"...)
		}

		bt.pushQueue(queue, node.children)
		queue.Remove(elem)
	}

	return buf
}

func (bt *Bitrie) AnalyzeBytes() []byte {
	buf := make([]byte, 0, 10<<10)

	keyOffset := uint32(bt.header.keyOffset)
	indexOffset := bt.header.indexOffset

	key := uint8(0)
	offset_next := uint32(0)

	count := indexOffset - keyOffset
	buf = append(buf, fmt.Sprintf("Header version=%d; keyOffset=%d; indexOffset=%d; dataOffset=%d; itemCount=%d; size=%d\n", bt.header.version, keyOffset, indexOffset, bt.header.dataOffset, count, bt.header.size)...)
	for i := uint32(0); i < count; i++ {
		tmp_kpos := keyOffset + i
		tmp_ipos := indexOffset + BitrieIndexSize*i
		tmp_dpos := binary.BigEndian.Uint32(bt.data[tmp_ipos:])
		if i == count-1 {
			offset_next = bt.header.size
		} else {
			offset_next = binary.BigEndian.Uint32(bt.data[tmp_ipos+BitrieIndexSize:])
		}
		node := bt.readNode(bt.data[tmp_dpos:], offset_next-tmp_dpos)

		key = bt.data[tmp_kpos]
		if key == 0 {
			key = ' '
		}

		if len(node.prune) == 0 {
			node.prune = []byte(" ")
		}
		if len(node.value) == 0 {
			node.value = []byte(" ")
		}

		tmpChildCount := uint32(node.childCount)
		if node.childIndex > 0 {
			tmpChildCount++
		}

		buf = append(buf, fmt.Sprintf("Item-%d keyOffset=%d; indexOffset=%d; dataOffset=%d; node.key=%c; node.prune=%s; node.value=%s; node.childCount=%v; node.childIndex=%v\n", i, tmp_ipos, tmp_ipos, tmp_dpos, key, node.prune, node.value, tmpChildCount, node.childIndex)...)
	}

	return buf
}

func (bt *Bitrie) newPruneChildByNode(node *trienode, offset int) *trienode {
	pruneKeyLen := len(node.prune) - 1
	if offset > pruneKeyLen {
		return nil
	}

	newNode := &trienode{
		key:   node.prune[offset],
		value: node.value,
	}

	if len(node.children) > 0 {
		newNode.children = node.children
		node.children = make(map[byte]*trienode, 1<<3)
	} else {
		newNode.children = make(map[byte]*trienode, 1<<3)
	}

	node.children[newNode.key] = newNode

	if offset < pruneKeyLen {
		newNode.prune = node.prune[offset+1:]
	} else {
		newNode.prune = nil
	}

	node.value = nil

	return newNode
}

func (bt *Bitrie) newPruneChildByKey(key []byte, value []byte, node *trienode) *trienode {
	newNode := &trienode{
		key:      key[0],
		prune:    key[1:],
		value:    value,
		children: make(map[byte]*trienode, 1<<3),
	}

	node.children[newNode.key] = newNode

	return newNode
}

func (bt *Bitrie) readHeader(buf []byte) Header {
	header := Header{
		version:     binary.BigEndian.Uint16(buf[0:]),
		reserved:    binary.BigEndian.Uint16(buf[2:]),
		keyOffset:   binary.BigEndian.Uint16(buf[4:]),
		indexOffset: binary.BigEndian.Uint32(buf[6:]),
		dataOffset:  binary.BigEndian.Uint32(buf[10:]),
		size:        binary.BigEndian.Uint32(buf[14:]),
	}

	return header
}

func (bt *Bitrie) writeHeader(buf []byte, header Header) {
	binary.BigEndian.PutUint16(buf[0:], header.version)
	binary.BigEndian.PutUint16(buf[2:], header.reserved)
	binary.BigEndian.PutUint16(buf[4:], header.keyOffset)
	binary.BigEndian.PutUint32(buf[6:], header.indexOffset)
	binary.BigEndian.PutUint32(buf[10:], header.dataOffset)
	binary.BigEndian.PutUint32(buf[14:], header.size)
}

func (bt *Bitrie) getNodeSizeAndOffset(buf []byte, offset uint32) (uint32, uint32) {
	lo := binary.BigEndian.Uint32(buf[offset:])

	var ro uint32
	offsetNext := offset + BitrieIndexSize
	if offsetNext < bt.header.dataOffset {
		ro = binary.BigEndian.Uint32(buf[offsetNext:])
	} else {
		ro = bt.header.size
	}

	return ro - lo, lo
}

func (bt *Bitrie) readNode(buf []byte, size uint32) disknode {
	dkNode := disknode{
		prune:      nil,
		value:      nil,
		childCount: 0,
		childIndex: 0,
	}

	kind := buf[0]
	offset := uint32(1)

	if kind&InternalKindKeyPrune == InternalKindKeyPrune {
		ksize := uint32(binary.BigEndian.Uint16(buf[offset:]))
		offset += 2
		dkNode.prune = buf[offset : offset+ksize]
		offset += ksize
	}

	if kind&InternalKindHasChildrenL1 == InternalKindHasChildrenL1 {
		dkNode.childCount = buf[offset]
		offset += 1
		dkNode.childIndex = uint32(binary.BigEndian.Uint16(buf[offset:]))
		offset += 2
	} else if kind&InternalKindHasChildrenL2 == InternalKindHasChildrenL2 {
		childIndex := binary.BigEndian.Uint32(buf[offset:])
		dkNode.childCount = uint8(childIndex & 0xff)
		dkNode.childIndex = childIndex >> 8
		offset += 4
	} else if kind&InternalKindHasChildrenL3 == InternalKindHasChildrenL3 {
		dkNode.childCount = buf[offset]
		offset += 1
		dkNode.childIndex = binary.BigEndian.Uint32(buf[offset:])
		offset += 4
	}

	if kind&InternalKindHasValue == InternalKindHasValue && offset < size {
		dkNode.value = buf[offset:size]
	}

	return dkNode
}

func (bt *Bitrie) writeKey(buf []byte, key uint8) {
	buf[0] = key
}

func (bt *Bitrie) writeIndex(buf []byte, idx uint32) {
	binary.BigEndian.PutUint32(buf[0:], idx)
}

func (bt *Bitrie) writeNode(buf []byte, dkNode *disknode) ([]byte, uint32) {
	kind := uint8(0)
	offset := uint32(1)

	pruneLength := uint32(len(dkNode.prune))
	if pruneLength > 0 {
		kind |= InternalKindKeyPrune
		binary.BigEndian.PutUint16(buf[offset:], uint16(pruneLength))
		offset += 2
		copy(buf[offset:offset+pruneLength], dkNode.prune)
		offset += pruneLength
	}

	if dkNode.childIndex > 0 {
		if dkNode.childIndex < KindChildrenL1Step {
			kind |= InternalKindHasChildrenL1
			buf[offset] = dkNode.childCount
			offset += 1
			binary.BigEndian.PutUint16(buf[offset:], uint16(dkNode.childIndex))
			offset += 2
		} else if dkNode.childIndex < KindChildrenL2Step {
			kind |= InternalKindHasChildrenL2
			childIndex := dkNode.childIndex<<8 | uint32(dkNode.childCount)
			binary.BigEndian.PutUint32(buf[offset:], childIndex)
			offset += 4
		} else {
			kind |= InternalKindHasChildrenL3
			buf[offset] = dkNode.childCount
			offset += 1
			binary.BigEndian.PutUint32(buf[offset:], dkNode.childIndex)
			offset += 4
		}
	}

	valueLength := uint32(len(dkNode.value))
	if valueLength > 0 {
		kind |= InternalKindHasValue
		copy(buf[offset:offset+valueLength], dkNode.value)
		offset += valueLength
	}

	buf[0] = kind

	return buf[0:offset], offset
}

func (bt *Bitrie) findNode(key uint8, buf []byte, n uint32) (bool, uint32) {
	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if buf[h] < key {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < n && buf[i] == key {
		return true, i
	}

	return false, 0
}

func (bt *Bitrie) pushQueue(queue *list.List, children map[uint8]*trienode) {
	childCount := len(children)
	if childCount <= 0 {
		return
	}

	sortedKeys := make([]int, 0, childCount)

	for k, _ := range children {
		sortedKeys = append(sortedKeys, int(k))
	}

	sort.Ints(sortedKeys)

	for _, v := range sortedKeys {
		queue.PushBack(children[uint8(v)])
	}
}
