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
	"encoding/binary"
	"sort"
)

const (
	pbHeaderSize   = 10
	pbHeaderOffset = 0
	pbDataOffset   = pbHeaderOffset + pbHeaderSize
)

var (
	dataExpandBuf [2 << 10]byte
	intIndexSize  uint32 = 3 << 10
)

type pageBlock struct {
	header        pbHeader
	itemsOffset   []uint32
	data          []byte
	intIndex      []byte
	size          uint32
	num           int
	prevKey       []byte
	prevValue     []byte
	prevHasShared bool
	sharedKey     []byte
	sharedNum     int
}

type pbHeader struct {
	version        uint16
	num            uint32
	intIndexOffset uint32
}

/*var pbPool = sync.Pool{
	New: func() interface{} {
		return &pageBlock{}
	},
}*/

func newPageBlock(version uint16, blockSize uint32) *pageBlock {
	pb := &pageBlock{
		itemsOffset:   make([]uint32, 0, 1<<8),
		data:          make([]byte, blockSize+intIndexSize),
		size:          pbDataOffset,
		num:           0,
		prevKey:       nil,
		prevValue:     nil,
		prevHasShared: false,
		sharedKey:     nil,
		sharedNum:     0,
	}

	pb.header.version = version

	return pb
}

func openPageBlock(pb *pageBlock, buf []byte) {
	pb.data = buf

	pb.readHeader()
	pb.num = int(pb.header.num)
	pb.size = uint32(len(buf))
	pb.intIndex = pb.data[pb.header.intIndexOffset:pb.size]
}

func (p *pageBlock) reset(version uint16) {
	p.header.intIndexOffset = 0
	p.header.num = 0
	p.header.version = version

	p.itemsOffset = p.itemsOffset[0:0]
	p.size = pbDataOffset
	p.num = 0
	p.prevKey = nil
	p.prevValue = nil
	p.prevHasShared = false
	p.sharedKey = nil
	p.sharedNum = 0
}

func (p *pageBlock) writeHeader() {
	binary.BigEndian.PutUint16(p.data[0:2], p.header.version)
	binary.BigEndian.PutUint32(p.data[2:6], p.header.num)
	binary.BigEndian.PutUint32(p.data[6:10], p.header.intIndexOffset)
}

func (p *pageBlock) readHeader() {
	p.header.version = binary.BigEndian.Uint16(p.data[0:2])
	p.header.num = binary.BigEndian.Uint32(p.data[2:6])
	p.header.intIndexOffset = binary.BigEndian.Uint32(p.data[6:10])
}

func (p *pageBlock) getVersion() uint32 {
	return uint32(p.header.version)
}

func (p *pageBlock) getIntIndexSize() uint32 {
	return uint32(p.num * itemOffsetSize)
}

func (p *pageBlock) writeItem(key, value []byte) (n uint32) {
	if p.header.version == atVersionPrefixBlockCompress {
		n = p.writeItemPrefixCompress(key, value)
	} else {
		n = p.writeItemDefault(key, value)
	}

	p.num++
	return n
}

func (p *pageBlock) writeSharedInternal(shared int) uint32 {
	var keySize uint32

	if !p.prevHasShared {
		if shared >= itemSharedMinLength {
			keySize = uint32(len(p.prevKey)) + 3
		} else {
			keySize = uint32(len(p.prevKey)) + 1
		}
	} else {
		keySize = uint32(len(p.prevKey[len(p.sharedKey):])) + 1
	}

	valueSize := uint32(len(p.prevValue))
	sz := keySize + valueSize + itemHeaderLen
	nextSize := p.allocBuf(sz)

	p.itemsOffset = append(p.itemsOffset, p.size)
	itemBuf := p.data[p.size:nextSize]
	binary.BigEndian.PutUint16(itemBuf[0:itemHeaderLen], uint16(keySize))

	if !p.prevHasShared {
		if shared >= itemSharedMinLength {
			itemBuf[itemHeaderLen] = itemSharedMin
			binary.BigEndian.PutUint16(itemBuf[itemHeaderLen+1:itemHeaderLen+3], uint16(len(p.sharedKey)))
			copy(itemBuf[itemHeaderLen+3:itemHeaderLen+keySize], p.prevKey)
		} else {
			itemBuf[itemHeaderLen] = itemSharedMax
			copy(itemBuf[itemHeaderLen+1:itemHeaderLen+keySize], p.prevKey)
		}
	} else {
		itemBuf[itemHeaderLen] = uint8(p.num - p.sharedNum)
		copy(itemBuf[itemHeaderLen+1:itemHeaderLen+keySize], p.prevKey[len(p.sharedKey):])
	}

	copy(itemBuf[itemHeaderLen+keySize:sz], p.prevValue)

	p.size = nextSize

	return sz
}

func (p *pageBlock) writeItemDefault(key, value []byte) uint32 {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	sz := keySize + valueSize + itemHeaderLen
	nextSize := p.allocBuf(sz)

	p.itemsOffset = append(p.itemsOffset, p.size)
	itemBuf := p.data[p.size:nextSize]
	binary.BigEndian.PutUint16(itemBuf[0:itemHeaderLen], uint16(keySize))
	copy(itemBuf[itemHeaderLen:itemHeaderLen+keySize], key)
	copy(itemBuf[itemHeaderLen+keySize:sz], value)
	p.size = nextSize

	return sz
}

func (p *pageBlock) writeItemPrefixCompress(key, value []byte) uint32 {
	if p.prevKey == nil && p.prevValue == nil {
		p.prevKey = make([]byte, 0, 1<<7)
		p.prevKey = append(p.prevKey[:0], key...)
		p.prevValue = value
		p.prevHasShared = false
		p.sharedKey = make([]byte, 0, 1<<6)
		return 0
	}

	shared := 0
	n := len(key)
	if n > len(p.prevKey) {
		n = len(p.prevKey)
	}
	asUint64 := func(b []byte, i int) uint64 {
		return binary.LittleEndian.Uint64(b[i:])
	}

	for shared < n-7 && asUint64(key, shared) == asUint64(p.prevKey, shared) {
		shared += 8
	}
	for shared < n && key[shared] == p.prevKey[shared] {
		shared++
	}

	if shared > 0 {
		sharedKeyLength := len(p.sharedKey)
		if shared < itemSharedMinLength || shared < sharedKeyLength {
			shared = 0
		} else {
			if !p.prevHasShared {
				p.sharedKey = append(p.sharedKey[:0], key[:shared]...)
				p.sharedNum = p.num
			} else if shared > sharedKeyLength+4 || p.num-p.sharedNum >= itemSharedRestart {
				shared = 0
			} else {
				shared = sharedKeyLength
			}
		}
	}

	sz := p.writeSharedInternal(shared)

	p.prevKey = append(p.prevKey[:0], key...)
	p.prevValue = value
	if shared >= itemSharedMinLength {
		p.prevHasShared = true
	} else {
		p.prevHasShared = false
		p.sharedKey = p.sharedKey[:0]
		p.sharedNum = 0
	}

	return sz
}

func (p *pageBlock) writeFinish() {
	if p.header.version == atVersionPrefixBlockCompress && p.prevKey != nil && p.prevValue != nil {
		_ = p.writeSharedInternal(0)
	}

	nextSize := p.allocBuf(p.getIntIndexSize())

	intIndexBuf := p.data[p.size:nextSize]
	intIndexPos := 0
	for i := range p.itemsOffset {
		binary.BigEndian.PutUint32(intIndexBuf[intIndexPos:intIndexPos+itemOffsetSize], p.itemsOffset[i])
		intIndexPos += itemOffsetSize
	}

	p.header.num = uint32(p.num)
	p.header.intIndexOffset = p.size
	p.writeHeader()

	p.size = nextSize
	p.intIndex = p.data[p.header.intIndexOffset:p.size]

	p.prevKey = nil
	p.prevValue = nil
	p.sharedKey = nil
}

func (p *pageBlock) getSharedKey(i int, key []byte, sharedCache *sharedInfo) ([]byte, []byte) {
	offset := key[0]
	switch offset {
	case itemSharedMin:
		return key[3:], nil
	case itemSharedMax:
		return key[1:], nil
	default:
		idx := i - int(offset)

		if sharedCache != nil && sharedCache.idx == idx && len(sharedCache.key) >= itemSharedMinLength {
			return sharedCache.key, key[1:]
		}

		itemOffset := p.getItemOffset(idx)
		if itemOffset == 0 {
			return nil, nil
		}

		keyOffset := itemOffset + itemHeaderLen
		sharedKeySize := uint32(binary.BigEndian.Uint16(p.data[itemOffset:keyOffset]))
		sharedKey := p.data[keyOffset : keyOffset+sharedKeySize]

		sharedKeyOffset := sharedKey[0]
		if sharedKeyOffset != itemSharedMin {
			return nil, nil
		}

		sharedKeyLenght := uint32(binary.BigEndian.Uint16(sharedKey[1:]) + 3)
		if sharedKeyLenght > sharedKeySize {
			return nil, nil
		}

		if sharedCache != nil && sharedCache.idx != idx {
			sharedCache.idx = idx
			sharedCache.key = sharedKey[3:sharedKeyLenght]
		}

		return sharedKey[3:sharedKeyLenght], key[1:]
	}
}

func (p *pageBlock) getItemOffset(i int) uint32 {
	if i < 0 || i >= p.num {
		return 0
	}

	pos := i * itemOffsetSize
	itemOffset := binary.BigEndian.Uint32(p.intIndex[pos : pos+itemOffsetSize])

	return itemOffset
}

func (p *pageBlock) getKey(i int) ([]byte, []byte) {
	itemOffset := p.getItemOffset(i)
	if itemOffset == 0 {
		return nil, nil
	}

	keyOffset := itemOffset + itemHeaderLen
	keySize := uint32(binary.BigEndian.Uint16(p.data[itemOffset:keyOffset]))
	key := p.data[keyOffset : keyOffset+keySize]

	if p.header.version == atVersionPrefixBlockCompress {
		return p.getSharedKey(i, key, nil)
	}

	return key, nil
}

func (p *pageBlock) getMaxKey() []byte {
	pos := p.num - 1
	itemOffset := p.getItemOffset(pos)
	if itemOffset == 0 {
		return nil
	}

	keyOffset := itemOffset + itemHeaderLen
	keySize := uint32(binary.BigEndian.Uint16(p.data[itemOffset:keyOffset]))
	key := p.data[keyOffset : keyOffset+keySize]

	if p.header.version == atVersionPrefixBlockCompress {
		sharedKey1, sharedKey2 := p.getSharedKey(pos, key, nil)
		sk1l := len(sharedKey1)
		sk2l := len(sharedKey2)
		rawKey := make([]byte, sk1l+sk2l)
		copy(rawKey[:sk1l], sharedKey1)
		copy(rawKey[sk1l:], sharedKey2)

		return rawKey
	}

	return key
}

func (p *pageBlock) getKV(i int) ([]byte, []byte) {
	itemOffset := p.getItemOffset(i)
	if itemOffset == 0 {
		return nil, nil
	}

	var itemSize uint32
	if i == p.num-1 {
		itemSize = p.header.intIndexOffset - itemOffset
	} else {
		itemSize = p.getItemOffset(i+1) - itemOffset
	}

	keyOffset := itemOffset + itemHeaderLen
	keySize := uint32(binary.BigEndian.Uint16(p.data[itemOffset:keyOffset]))
	key := p.data[keyOffset : keyOffset+keySize]
	valueSize := itemSize - keySize - itemHeaderLen
	value := p.data[keyOffset+keySize : keyOffset+keySize+valueSize]

	return key, value
}

func (p *pageBlock) getSharedKV(i int, sharedCache *sharedInfo) ([]byte, []byte, []byte) {
	key, value := p.getKV(i)
	if key == nil {
		return nil, nil, nil
	}

	if p.header.version == atVersionPrefixBlockCompress {
		sharedKey1, sharedKey2 := p.getSharedKey(i, key, sharedCache)
		return sharedKey1, sharedKey2, value
	}

	return key, nil, value

}

func (p *pageBlock) get(key []byte) ([]byte, bool, internalKeyKind) {
	pos := p.findKeyByIntIndex(key)
	sharedKey1, sharedKey2, value := p.getSharedKV(pos, nil)
	if sharedKey1 == nil || !atEqual(sharedKey1, sharedKey2, key) {
		return nil, false, internalKeyKindInvalid
	}

	return value, true, internalKeyKindSet
}

func (p *pageBlock) newIter(o *iterOptions) *pageBlockIterator {
	iter := pbIterPool.Get().(*pageBlockIterator)
	iter.pb = p

	if p.header.version == atVersionPrefixBlockCompress {
		if cap(iter.keyBuf) == 0 {
			iter.keyBuf = make([]byte, 0, 1<<7)
		}
		if iter.sharedCache == nil {
			iter.sharedCache = &sharedInfo{idx: -1, key: nil}
		}
	}

	return iter
}

func (p *pageBlock) allocBuf(sz uint32) uint32 {
	newSize := int(p.size + sz)

	for {
		if newSize >= len(p.data) {
			p.data = append(p.data, dataExpandBuf[:]...)
		} else {
			break
		}
	}

	return uint32(newSize)
}

func (p *pageBlock) bytes() []byte {
	return p.data
}

func (p *pageBlock) inuseBytes() uint32 {
	return p.size
}

func (p *pageBlock) totalBytes() uint64 {
	return uint64(cap(p.data))
}

func (p *pageBlock) close() error {
	return nil
}

func (p *pageBlock) empty() bool {
	return p.num == 0
}

func (p *pageBlock) itemCount() int {
	return p.num
}

func (p *pageBlock) findKeyByIntIndex(key []byte) int {
	return sort.Search(p.num, func(i int) bool {
		sharedKey1, sharedKey2 := p.getKey(i)
		return atCompare(sharedKey1, sharedKey2, key) != -1
	})
}
