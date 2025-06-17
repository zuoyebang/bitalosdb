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
	"container/list"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	childIndexMax uint32 = 4<<30 - 1
)

type Bitriev struct {
	header   Header
	length   uint32
	data     []byte
	children map[uint8]*trienodev
}

type trienodev struct {
	key      uint8
	prune    []byte
	voffset  uint32
	children map[uint8]*trienodev
}

type disknodev struct {
	prune      []byte
	voffset    uint32
	childCount uint8
	childIndex uint32
}

func NewBitriev() *Bitriev {
	root := &Bitriev{
		header:   Header{version: BitrieVersion},
		length:   1,
		data:     nil,
		children: nil,
	}

	return root
}

func (bt *Bitriev) InitWriter() {
	bt.length = 1
	bt.children = make(map[uint8]*trienodev, 1<<6)
}

func (bt *Bitriev) SetReader(d []byte, offset uint32) bool {
	if d == nil {
		return false
	}

	bt.data = d
	bt.header = bt.readHeader(bt.data[offset:])

	return len(d) >= int(bt.header.size)
}

func (bt *Bitriev) Size() uint32 {
	return bt.header.size
}

func (bt *Bitriev) Add(key []byte, voffset uint32) {
	keyLength := len(key)
	if keyLength <= 0 {
		return
	}

	var ok bool
	var childNode *trienodev

	children := bt.children

	for i := 0; i < keyLength; i++ {
		if childNode, ok = children[key[i]]; !ok {
			newNode := &trienodev{
				key:      key[i],
				prune:    key[i+1:],
				voffset:  voffset,
				children: make(map[byte]*trienodev, 1<<3),
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
						bt.newPruneChildByKey(key[n:], voffset, childNode)
						childNode.prune = nil
						bt.length += 2
					} else if n > keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						childNode.prune = nil
						childNode.voffset = voffset
						bt.length += 1
					}
				} else if pruneKeyLength == tailKeyLength {
					if n <= keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], voffset, childNode)
						childNode.prune = nil
						bt.length += 2
					} else if n > keyLength-1 {
						childNode.voffset = voffset
					}
				} else if pruneKeyLength < tailKeyLength {
					bt.newPruneChildByNode(childNode, m)
					bt.newPruneChildByKey(key[n:], voffset, childNode)
					childNode.prune = nil
					bt.length += 2
				}
				break
			} else if m > 0 {
				if pruneKeyLength > tailKeyLength {
					if n <= keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], voffset, childNode)
						childNode.prune = childNode.prune[:m]
						bt.length += 2
					} else if n > keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						childNode.voffset = voffset
						childNode.prune = childNode.prune[:m]
						bt.length += 1
					}
					break
				} else if pruneKeyLength == tailKeyLength {
					if n <= keyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], voffset, childNode)
						childNode.prune = childNode.prune[:m]
						bt.length += 2
					} else if n > keyLength-1 {
						childNode.voffset = voffset
					}
					break
				} else if pruneKeyLength < tailKeyLength {
					if m <= pruneKeyLength-1 {
						bt.newPruneChildByNode(childNode, m)
						bt.newPruneChildByKey(key[n:], voffset, childNode)
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

func (bt *Bitriev) Finish() {
	bt.children = nil
}

func (bt *Bitriev) Serialize(
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
	dkNode := disknodev{
		prune:      nil,
		voffset:    0,
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
		node := elem.Value.(*trienodev)

		bt.writeKey(tblbytes(keyOffset, BitrieKeySize), node.key)
		keyOffset += BitrieKeySize

		bt.writeIndex(tblbytes(indexOffset, BitrieIndexSize), dataOffset)

		dkNode.prune = node.prune
		dkNode.voffset = node.voffset
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

func (bt *Bitriev) Get(key []byte) (uint32, bool) {
	keyOffset := uint32(bt.header.keyOffset)
	indexOffset := bt.header.indexOffset

	node := bt.readNode(bt.data[bt.header.dataOffset:])
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
			offset := binary.BigEndian.Uint32(bt.data[indexOffset+childIndex*BitrieIndexSize:])
			node = bt.readNode(bt.data[offset:])

			pruneLength := len(node.prune)
			if pruneLength > 0 {
				curKeyPos := i + 1 + pruneLength
				if curKeyPos <= keyLength && bytes.Equal(node.prune, key[i+1:curKeyPos]) {
					i += pruneLength
					if i == keyLength-1 && node.voffset > 0 {
						return node.voffset, true
					} else {
						childCount = node.childCount
						childIndex = node.childIndex
						continue
					}
				} else {
					return 0, false
				}
			}
			childCount = node.childCount
			childIndex = node.childIndex

			if i == keyLength-1 && node.voffset > 0 {
				return node.voffset, true
			}
		} else {
			return 0, false
		}
	}

	return 0, false
}

func (bt *Bitriev) ToBytes() []byte {
	buf := make([]byte, 0, 1024)

	queue := list.New()
	bt.pushQueue(queue, bt.children)

	for queue.Len() > 0 {
		elem := queue.Front()
		node := elem.Value.(*trienodev)

		buf = append(buf, fmt.Sprintf("key=%c; prune=%s; value=%d; ", node.key, node.prune, node.voffset)...)
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

func (bt *Bitriev) AnalyzeBytes() []byte {
	buf := make([]byte, 0, 10<<10)

	keyOffset := uint32(bt.header.keyOffset)
	indexOffset := bt.header.indexOffset

	key := uint8(0)

	count := indexOffset - keyOffset
	buf = append(buf, fmt.Sprintf("Header version=%d; keyOffset=%d; indexOffset=%d; dataOffset=%d; itemCount=%d; size=%d\n", bt.header.version, keyOffset, indexOffset, bt.header.dataOffset, count, bt.header.size)...)
	for i := uint32(0); i < count; i++ {
		tmp_kpos := keyOffset + i
		tmp_ipos := indexOffset + BitrieIndexSize*i
		tmp_dpos := binary.BigEndian.Uint32(bt.data[tmp_ipos:])
		node := bt.readNode(bt.data[tmp_dpos:])

		key = bt.data[tmp_kpos]
		if key == 0 {
			key = ' '
		}

		if len(node.prune) == 0 {
			node.prune = []byte(" ")
		}

		tmpChildCount := uint32(node.childCount)
		if node.childIndex > 0 {
			tmpChildCount++
		}

		buf = append(buf, fmt.Sprintf("Item-%d keyOffset=%d; indexOffset=%d; dataOffset=%d; node.key=%c; node.prune=%s; node.voffset=%d; node.childCount=%v; node.childIndex=%v\n", i, tmp_ipos, tmp_ipos, tmp_dpos, key, node.prune, node.voffset, tmpChildCount, node.childIndex)...)
	}

	return buf
}

func (bt *Bitriev) newPruneChildByNode(node *trienodev, offset int) *trienodev {
	pruneKeyLen := len(node.prune) - 1
	if offset > pruneKeyLen {
		return nil
	}

	newNode := &trienodev{
		key:     node.prune[offset],
		voffset: node.voffset,
	}

	if len(node.children) > 0 {
		newNode.children = node.children
		node.children = make(map[byte]*trienodev, 1<<3)
	} else {
		newNode.children = make(map[byte]*trienodev, 1<<3)
	}

	node.children[newNode.key] = newNode

	if offset < pruneKeyLen {
		newNode.prune = node.prune[offset+1:]
	} else {
		newNode.prune = nil
	}

	node.voffset = 0

	return newNode
}

func (bt *Bitriev) newPruneChildByKey(key []byte, voffset uint32, node *trienodev) *trienodev {
	newNode := &trienodev{
		key:      key[0],
		prune:    key[1:],
		voffset:  voffset,
		children: make(map[byte]*trienodev, 1<<3),
	}

	node.children[newNode.key] = newNode

	return newNode
}

func (bt *Bitriev) readHeader(buf []byte) Header {
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

func (bt *Bitriev) writeHeader(buf []byte, header Header) {
	binary.BigEndian.PutUint16(buf[0:], header.version)
	binary.BigEndian.PutUint16(buf[2:], header.reserved)
	binary.BigEndian.PutUint16(buf[4:], header.keyOffset)
	binary.BigEndian.PutUint32(buf[6:], header.indexOffset)
	binary.BigEndian.PutUint32(buf[10:], header.dataOffset)
	binary.BigEndian.PutUint32(buf[14:], header.size)
}

func (bt *Bitriev) readNode(buf []byte) disknodev {
	dkNode := disknodev{
		prune:      nil,
		voffset:    0,
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

	if kind&InternalKindHasValue == InternalKindHasValue {
		dkNode.voffset = binary.BigEndian.Uint32(buf[offset:])
	}

	return dkNode
}

func (bt *Bitriev) writeKey(buf []byte, key uint8) {
	buf[0] = key
}

func (bt *Bitriev) writeIndex(buf []byte, idx uint32) {
	binary.BigEndian.PutUint32(buf[0:], idx)
}

func (bt *Bitriev) writeNode(buf []byte, dkNode *disknodev) ([]byte, uint32) {
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

	if dkNode.voffset > 0 {
		kind |= InternalKindHasValue
		binary.BigEndian.PutUint32(buf[offset:], dkNode.voffset)
		offset += 4
	}

	buf[0] = kind

	return buf[0:offset], offset
}

func (bt *Bitriev) findNode(key uint8, buf []byte, n uint32) (bool, uint32) {
	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if buf[h] < key {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < n {
		if buf[i] == key {
			return true, i
		} else {
			return false, i
		}
	}

	return false, childIndexMax
}

func (bt *Bitriev) pushQueue(queue *list.List, children map[uint8]*trienodev) {
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
