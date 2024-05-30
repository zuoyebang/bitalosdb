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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/golang/snappy"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bindex"
	"github.com/zuoyebang/bitalosdb/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/internal/cache/lrucache"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/hash"
)

const (
	atVersionDefault uint16 = iota + 1
	atVersionPrefixCompress
	atVersionBlockCompress
	atVersionPrefixBlockCompress
	atVersionBitrieCompress
)

const (
	atHeaderSize   = 18
	atHeaderOffset = tableDataOffset
	atDataOffset   = atHeaderOffset + atHeaderSize

	itemOffsetSize      = 4
	itemHeaderLen       = 2
	itemSharedMin       = 0
	itemSharedMax       = 255
	itemSharedRestart   = 254
	itemSharedMinLength = 6
)

type arrayTable struct {
	tbl           *table
	filename      string
	header        *atHeader
	block         *pageBlock
	blockSize     uint32
	blockCache    *lrucache.LruCache
	cacheID       uint64
	useMapIndex   bool
	itemsOffset   []uint32
	intIndex      []byte
	bitrieIndex   *Bitrie
	mapIndex      *bindex.HashIndex
	size          uint32
	num           int
	compressedBuf []byte
	prevKey       []byte
	prevValue     []byte
	prevHasShared bool
	sharedKey     []byte
	sharedNum     int
}

type atHeader struct {
	version        uint16
	num            uint32
	dataOffset     uint32
	intIndexOffset uint32
	mapIndexOffset uint32
}

type atOptions struct {
	useMapIndex       bool
	usePrefixCompress bool
	useBlockCompress  bool
	useBitrieCompress bool
	blockSize         uint32
}

type atCacheOptions struct {
	cache *lrucache.LruCache
	id    uint64
}

func checkArrayTable(obj interface{}) {
	s := obj.(*arrayTable)
	if s.tbl != nil {
		fmt.Fprintf(os.Stderr, "arrayTable(%s) buffer was not freed\n", s.tbl.path)
		os.Exit(1)
	}
}

func newArrayTable(path string, opts *atOptions, cacheOpts *atCacheOptions) (*arrayTable, error) {
	tbl, err := openTable(path, defaultTableOptions)
	if err != nil {
		return nil, err
	}

	at := &arrayTable{
		tbl:           tbl,
		filename:      base.GetFilePathBase(path),
		blockSize:     opts.blockSize,
		blockCache:    cacheOpts.cache,
		cacheID:       cacheOpts.id,
		useMapIndex:   opts.useMapIndex,
		num:           0,
		prevKey:       nil,
		prevValue:     nil,
		prevHasShared: false,
		sharedKey:     nil,
		sharedNum:     0,
	}

	var headerOffset uint32
	headerOffset, err = at.tbl.alloc(atHeaderSize)
	if err != nil {
		return nil, err
	}
	if headerOffset != atHeaderOffset {
		return nil, errors.New("tblSize is not large enough to hold the arrayTable header")
	}

	at.header = &atHeader{
		dataOffset: atDataOffset,
		version:    getAtVersionByOpts(opts),
	}

	if opts.useBlockCompress || opts.useBitrieCompress {
		at.useMapIndex = false
	}

	if opts.useBitrieCompress {
		at.bitrieIndex = NewBitrie()
		at.bitrieIndex.InitWriter()
	} else {
		at.itemsOffset = make([]uint32, 0, 1<<20)
	}

	if at.useMapIndex {
		at.mapIndex = bindex.NewHashIndex(true)
		at.mapIndex.InitWriter()
	}

	return at, nil
}

func openArrayTable(path string, cacheOpts *atCacheOptions) (*arrayTable, error) {
	tbl, err := openTable(path, &tableOptions{openType: tableReadMmap})
	if err != nil {
		return nil, err
	}

	at := &arrayTable{
		tbl:        tbl,
		filename:   base.GetFilePathBase(path),
		blockCache: cacheOpts.cache,
		cacheID:    cacheOpts.id,
	}

	at.readHeader()
	at.num = int(at.header.num)
	at.size = at.tbl.Size()

	if at.header.version == atVersionBitrieCompress {
		at.bitrieIndex = NewBitrie()
	} else if at.header.mapIndexOffset > 0 {
		at.useMapIndex = true
		at.mapIndex = bindex.NewHashIndex(true)
	}

	at.dataIntBuffer()

	return at, nil
}

func getAtVersionByOpts(opts *atOptions) uint16 {
	var version uint16
	if opts.useBitrieCompress {
		version = atVersionBitrieCompress
	} else if opts.usePrefixCompress {
		if opts.useBlockCompress {
			version = atVersionPrefixBlockCompress
		} else {
			version = atVersionPrefixCompress
		}
	} else {
		if opts.useBlockCompress {
			version = atVersionBlockCompress
		} else {
			version = atVersionDefault
		}
	}
	return version
}

func (a *arrayTable) writeHeader() {
	buf := a.tbl.getBytes(atHeaderOffset, atHeaderSize)
	binary.BigEndian.PutUint16(buf[0:2], a.header.version)
	binary.BigEndian.PutUint32(buf[2:6], a.header.num)
	binary.BigEndian.PutUint32(buf[6:10], a.header.dataOffset)
	binary.BigEndian.PutUint32(buf[10:14], a.header.intIndexOffset)
	binary.BigEndian.PutUint32(buf[14:18], a.header.mapIndexOffset)
}

func (a *arrayTable) readHeader() {
	buf := a.tbl.getBytes(atHeaderOffset, atHeaderSize)
	a.header = &atHeader{}
	a.header.version = binary.BigEndian.Uint16(buf[0:2])
	a.header.num = binary.BigEndian.Uint32(buf[2:6])
	a.header.dataOffset = binary.BigEndian.Uint32(buf[6:10])
	a.header.intIndexOffset = binary.BigEndian.Uint32(buf[10:14])
	a.header.mapIndexOffset = binary.BigEndian.Uint32(buf[14:18])
}

func (a *arrayTable) dataIntBuffer() {
	if a.header.version == atVersionBitrieCompress {
		a.bitrieIndex.SetReader(a.tbl.getBytes(0, a.size), atDataOffset)
	} else {
		a.intIndex = a.tbl.getBytes(a.header.intIndexOffset, a.getIntIndexSize())
	}

	if a.useMapIndex {
		a.mapIndex.SetReader(a.tbl.getBytes(a.header.mapIndexOffset, a.size-a.header.mapIndexOffset))
	}
}

func (a *arrayTable) getVersion() uint32 {
	return uint32(a.header.version)
}

func (a *arrayTable) getIntIndexSize() uint32 {
	return uint32(a.num * itemOffsetSize)
}

func (a *arrayTable) writeSharedInternal(shared int) (uint32, error) {
	var keySize uint32

	if !a.prevHasShared {
		if shared >= itemSharedMinLength {
			keySize = uint32(len(a.prevKey)) + 3
		} else {
			keySize = uint32(len(a.prevKey)) + 1
		}
	} else {
		keySize = uint32(len(a.prevKey[len(a.sharedKey):])) + 1
	}

	valueSize := uint32(len(a.prevValue))
	sz := keySize + valueSize + itemHeaderLen

	offset, err := a.tbl.alloc(sz)
	if err != nil {
		return 0, err
	}

	a.itemsOffset = append(a.itemsOffset, offset)
	itemBuf := a.tbl.getBytes(offset, sz)
	binary.BigEndian.PutUint16(itemBuf[0:itemHeaderLen], uint16(keySize))

	if !a.prevHasShared {
		if shared >= itemSharedMinLength {
			itemBuf[itemHeaderLen] = itemSharedMin
			binary.BigEndian.PutUint16(itemBuf[itemHeaderLen+1:itemHeaderLen+3], uint16(len(a.sharedKey)))
			copy(itemBuf[itemHeaderLen+3:itemHeaderLen+keySize], a.prevKey)
		} else {
			itemBuf[itemHeaderLen] = itemSharedMax
			copy(itemBuf[itemHeaderLen+1:itemHeaderLen+keySize], a.prevKey)
		}
	} else {
		itemBuf[itemHeaderLen] = uint8(a.num - a.sharedNum)
		copy(itemBuf[itemHeaderLen+1:itemHeaderLen+keySize], a.prevKey[len(a.sharedKey):])
	}

	copy(itemBuf[itemHeaderLen+keySize:sz], a.prevValue)

	a.num++

	if a.useMapIndex {
		a.mapIndex.Add(hash.Crc32(a.prevKey), uint32(a.num))
	}

	return sz, nil
}

func (a *arrayTable) writeItem(key, value []byte) (uint32, error) {
	switch a.header.version {
	case atVersionPrefixCompress:
		return a.writeItemPrefixCompress(key, value)
	case atVersionBlockCompress, atVersionPrefixBlockCompress:
		return a.writeItemBlockCompress(key, value)
	case atVersionBitrieCompress:
		return a.writeItemBitrieCompress(key, value)
	}

	return a.writeItemDefault(key, value)
}

func (a *arrayTable) writeItemDefault(key, value []byte) (uint32, error) {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	sz := keySize + valueSize + itemHeaderLen
	offset, err := a.tbl.alloc(sz)
	if err != nil {
		return 0, err
	}

	a.itemsOffset = append(a.itemsOffset, offset)
	itemBuf := a.tbl.getBytes(offset, sz)
	binary.BigEndian.PutUint16(itemBuf[0:itemHeaderLen], uint16(keySize))
	copy(itemBuf[itemHeaderLen:itemHeaderLen+keySize], key)
	copy(itemBuf[itemHeaderLen+keySize:sz], value)

	a.num++

	if a.useMapIndex {
		a.mapIndex.Add(hash.Crc32(key), uint32(a.num))
	}

	return sz, nil
}

func (a *arrayTable) writeItemPrefixCompress(key, value []byte) (uint32, error) {
	if a.prevKey == nil && a.prevValue == nil {
		a.prevKey = make([]byte, 0, 1<<7)
		a.prevKey = append(a.prevKey[:0], key...)
		a.prevValue = value
		a.prevHasShared = false
		a.sharedKey = make([]byte, 0, 1<<6)
		return 0, nil
	}

	shared := 0
	n := len(key)
	if n > len(a.prevKey) {
		n = len(a.prevKey)
	}
	asUint64 := func(b []byte, i int) uint64 {
		return binary.LittleEndian.Uint64(b[i:])
	}

	for shared < n-7 && asUint64(key, shared) == asUint64(a.prevKey, shared) {
		shared += 8
	}
	for shared < n && key[shared] == a.prevKey[shared] {
		shared++
	}

	if shared > 0 {
		sharedKeyLength := len(a.sharedKey)
		if shared < itemSharedMinLength || shared < sharedKeyLength {
			shared = 0
		} else {
			if !a.prevHasShared {
				a.sharedKey = append(a.sharedKey[:0], key[:shared]...)
				a.sharedNum = a.num
			} else if shared > sharedKeyLength+4 || a.num-a.sharedNum >= itemSharedRestart {
				shared = 0
			} else {
				shared = sharedKeyLength
			}
		}
	}

	sz, err := a.writeSharedInternal(shared)
	if err != nil {
		return 0, err
	}

	a.prevKey = append(a.prevKey[:0], key...)
	a.prevValue = value
	if shared >= itemSharedMinLength {
		a.prevHasShared = true
	} else {
		a.prevHasShared = false
		a.sharedKey = a.sharedKey[:0]
		a.sharedNum = 0
	}

	return sz, err
}

func (a *arrayTable) writeItemBlockCompress(key, value []byte) (uint32, error) {
	if a.block == nil {
		a.block = newPageBlock(a.header.version, a.blockSize)
	}

	a.block.writeItem(key, value)

	if a.block.inuseBytes() >= a.blockSize &&
		a.block.itemCount() >= consts.BitpageBlockMinItemCount {
		a.block.writeFinish()

		blockBuf := compressEncode(a.compressedBuf, a.block.bytes())
		if cap(blockBuf) > cap(a.compressedBuf) {
			a.compressedBuf = blockBuf[:cap(blockBuf)]
		}
		bufSize := len(blockBuf)

		keySize := len(key)
		sz := uint32(keySize + bufSize + itemHeaderLen)
		offset, err := a.tbl.alloc(sz)
		if err != nil {
			return 0, err
		}

		a.itemsOffset = append(a.itemsOffset, offset)
		itemBuf := a.tbl.getBytes(offset, sz)
		binary.BigEndian.PutUint16(itemBuf[0:itemHeaderLen], uint16(keySize))
		copy(itemBuf[itemHeaderLen:itemHeaderLen+keySize], key)
		copy(itemBuf[itemHeaderLen+keySize:sz], blockBuf)

		a.block.reset(a.header.version)
		a.num++

		return sz, nil
	}

	return 0, nil
}

func (a *arrayTable) writeItemBitrieCompress(key, value []byte) (uint32, error) {
	a.bitrieIndex.Add(key, value)

	a.num++

	return uint32(len(key) + len(value)), nil
}

func (a *arrayTable) writeFinish() error {
	switch a.header.version {
	case atVersionDefault, atVersionPrefixCompress:
		return a.writeFinishNonBlockCompress()
	case atVersionBlockCompress, atVersionPrefixBlockCompress:
		return a.writeFinishBlockCompress()
	case atVersionBitrieCompress:
		return a.writeFinishBitrieCompress()
	}

	return a.writeFinishNonBlockCompress()
}

func (a *arrayTable) writeFinishNonBlockCompress() error {
	if a.header.version == atVersionPrefixCompress && a.prevKey != nil && a.prevValue != nil {
		if _, err := a.writeSharedInternal(0); err != nil {
			return err
		}
	}

	intSize := a.getIntIndexSize()
	intIndexOffset, err := a.tbl.alloc(intSize)
	if err != nil {
		return err
	}
	intIndexBuf := a.tbl.getBytes(intIndexOffset, intSize)
	intIndexPos := 0
	for i := range a.itemsOffset {
		binary.BigEndian.PutUint32(intIndexBuf[intIndexPos:intIndexPos+itemOffsetSize], a.itemsOffset[i])
		intIndexPos += itemOffsetSize
	}

	var mapIndexOffset uint32
	if a.useMapIndex {
		mapSize := a.mapIndex.Size()
		mapIndexOffset, err = a.tbl.alloc(mapSize)
		if err != nil {
			return err
		}
		a.mapIndex.SetWriter(a.tbl.getBytes(mapIndexOffset, mapSize))
		if !a.mapIndex.Serialize() {
			return errors.New("hash_index write fail")
		}

		a.mapIndex.Finish()
	}

	a.size = a.tbl.Size()
	a.itemsOffset = nil
	a.header.num = uint32(a.num)
	a.header.intIndexOffset = intIndexOffset
	a.header.mapIndexOffset = mapIndexOffset
	a.writeHeader()

	if err = a.tbl.mmapReadTruncate(int(a.size)); err != nil {
		return err
	}

	a.dataIntBuffer()

	a.block = nil
	a.compressedBuf = nil
	a.prevKey = nil
	a.prevValue = nil
	a.sharedKey = nil

	return nil
}

func (a *arrayTable) writeFinishBlockCompress() error {
	if !a.block.empty() {
		a.block.writeFinish()

		blockBuf := compressEncode(a.compressedBuf, a.block.bytes())
		bufSize := len(blockBuf)

		key := consts.BdbMaxKey
		keySize := len(consts.BdbMaxKey)
		sz := uint32(keySize + bufSize + itemHeaderLen)
		offset, err := a.tbl.alloc(sz)
		if err != nil {
			return err
		}

		a.itemsOffset = append(a.itemsOffset, offset)
		itemBuf := a.tbl.getBytes(offset, sz)
		binary.BigEndian.PutUint16(itemBuf[0:itemHeaderLen], uint16(keySize))
		copy(itemBuf[itemHeaderLen:itemHeaderLen+keySize], key)
		copy(itemBuf[itemHeaderLen+keySize:sz], blockBuf)

		a.block.reset(a.header.version)

		a.num++
	}

	intSize := a.getIntIndexSize()
	intIndexOffset, err := a.tbl.alloc(intSize)
	if err != nil {
		return err
	}
	intIndexBuf := a.tbl.getBytes(intIndexOffset, intSize)
	intIndexPos := 0
	for i := range a.itemsOffset {
		binary.BigEndian.PutUint32(intIndexBuf[intIndexPos:intIndexPos+itemOffsetSize], a.itemsOffset[i])
		intIndexPos += itemOffsetSize
	}

	a.size = a.tbl.Size()
	a.itemsOffset = nil
	a.header.num = uint32(a.num)
	a.header.intIndexOffset = intIndexOffset
	a.header.mapIndexOffset = 0
	a.writeHeader()

	if err = a.tbl.mmapReadTruncate(int(a.size)); err != nil {
		return err
	}

	a.dataIntBuffer()

	a.block = nil
	a.compressedBuf = nil
	a.prevKey = nil
	a.prevValue = nil
	a.sharedKey = nil

	return nil
}

func (a *arrayTable) writeFinishBitrieCompress() error {
	tblalloc := func(size uint32) uint32 {
		offset, _ := a.tbl.alloc(size)
		return offset
	}

	tblbytes := func(offset uint32, size uint32) []byte {
		return a.tbl.getBytes(offset, size)
	}

	tblsize := func() uint32 {
		return a.tbl.Size()
	}

	if !a.bitrieIndex.Serialize(tblalloc, tblbytes, tblsize) {
		return errors.New("bitrie serialize fail")
	}
	a.bitrieIndex.Finish()

	a.size = a.tbl.Size()
	a.itemsOffset = nil
	a.header.num = uint32(a.num)
	a.header.intIndexOffset = 0
	a.header.mapIndexOffset = 0
	a.writeHeader()

	if err := a.tbl.mmapReadTruncate(int(a.size)); err != nil {
		return err
	}

	a.dataIntBuffer()

	a.block = nil
	a.compressedBuf = nil
	a.prevKey = nil
	a.prevValue = nil
	a.sharedKey = nil

	return nil
}

func (a *arrayTable) getSharedKey(i int, key []byte, sharedCache *sharedInfo) ([]byte, []byte) {
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

		itemOffset := a.getItemOffset(idx)
		if itemOffset == 0 {
			return nil, nil
		}

		sharedKeySize := uint32(binary.BigEndian.Uint16(a.tbl.getBytes(itemOffset, itemHeaderLen)))
		sharedKey := a.tbl.getBytes(itemOffset+itemHeaderLen, sharedKeySize)

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

func (a *arrayTable) checkPositon(i int) bool {
	return i >= 0 && i < a.num
}

func (a *arrayTable) getItemOffset(i int) uint32 {
	if i < 0 || i >= a.num {
		return 0
	}

	pos := i * itemOffsetSize
	itemOffset := binary.BigEndian.Uint32(a.intIndex[pos : pos+itemOffsetSize])

	return itemOffset
}

func (a *arrayTable) getKey(i int) ([]byte, []byte) {
	itemOffset := a.getItemOffset(i)
	if itemOffset == 0 {
		return nil, nil
	}

	keySize := uint32(binary.BigEndian.Uint16(a.tbl.getBytes(itemOffset, itemHeaderLen)))
	key := a.tbl.getBytes(itemOffset+itemHeaderLen, keySize)

	if a.header.version == atVersionPrefixCompress {
		return a.getSharedKey(i, key, nil)
	}

	return key, nil
}

func (a *arrayTable) getMaxKey() []byte {
	pos := a.num - 1
	itemOffset := a.getItemOffset(pos)
	if itemOffset == 0 {
		return nil
	}

	keySize := uint32(binary.BigEndian.Uint16(a.tbl.getBytes(itemOffset, itemHeaderLen)))
	key := a.tbl.getBytes(itemOffset+itemHeaderLen, keySize)

	if a.header.version == atVersionPrefixCompress {
		sharedKey1, sharedKey2 := a.getSharedKey(pos, key, nil)
		sk1l := len(sharedKey1)
		sk2l := len(sharedKey2)
		rawKey := make([]byte, sk1l+sk2l)
		copy(rawKey[:sk1l], sharedKey1)
		copy(rawKey[sk1l:], sharedKey2)

		return rawKey
	}

	return key
}

func (a *arrayTable) getKV(i int) ([]byte, []byte) {
	itemOffset := a.getItemOffset(i)
	if itemOffset == 0 {
		return nil, nil
	}

	var itemSize uint32
	if i == a.num-1 {
		itemSize = a.header.intIndexOffset - itemOffset
	} else {
		itemSize = a.getItemOffset(i+1) - itemOffset
	}

	keySize := uint32(binary.BigEndian.Uint16(a.tbl.getBytes(itemOffset, itemHeaderLen)))
	keyOffset := itemOffset + itemHeaderLen
	key := a.tbl.getBytes(keyOffset, keySize)
	valueSize := itemSize - keySize - itemHeaderLen
	value := a.tbl.getBytes(keyOffset+keySize, valueSize)

	return key, value
}

func (a *arrayTable) getSharedKV(i int, sharedCache *sharedInfo) ([]byte, []byte, []byte) {
	key, value := a.getKV(i)
	if key == nil {
		return nil, nil, nil
	}

	if a.header.version == atVersionPrefixCompress {
		sharedKey1, sharedKey2 := a.getSharedKey(i, key, sharedCache)
		return sharedKey1, sharedKey2, value
	}

	return key, nil, value

}

func (a *arrayTable) get(key []byte, khash uint32) ([]byte, bool, internalKeyKind, func()) {
	if a.header.version == atVersionBitrieCompress {
		value, exist := a.bitrieIndex.Get(key)
		if exist {
			return value, true, internalKeyKindSet, nil
		} else {
			return nil, false, internalKeyKindInvalid, nil
		}
	}

	var pos int
	if a.useMapIndex {
		pos = a.findKeyByMapIndex(khash)
		sharedKey1, sharedKey2 := a.getKey(pos)
		if sharedKey1 == nil {
			return nil, false, internalKeyKindInvalid, nil
		}
		if atEqual(sharedKey1, sharedKey2, key) {
			_, value := a.getKV(pos)
			return value, true, internalKeyKindSet, nil
		}
	}

	pos = a.findKeyByIntIndex(key)

	if a.isNonBlock() {
		sharedKey1, sharedKey2, value := a.getSharedKV(pos, nil)
		if sharedKey1 == nil || !atEqual(sharedKey1, sharedKey2, key) {
			return nil, false, internalKeyKindInvalid, nil
		}

		return value, true, internalKeyKindSet, nil
	}

	blockBuf, closer, blockExist := a.blockCache.GetBlock(a.cacheID, uint64(pos))
	if !blockExist {
		_, blockBuf = a.getKV(pos)
		if len(blockBuf) == 0 {
			return nil, false, internalKeyKindInvalid, nil
		}

		var compressedBuf []byte
		compressedBuf, closer = bytepools.ReaderBytePools.GetMaxBytePool()
		blockBuf, _ = compressDecode(compressedBuf, blockBuf)
		_ = a.blockCache.SetBlock(a.cacheID, uint64(pos), blockBuf)
	}

	pb := pageBlock{}
	openPageBlock(&pb, blockBuf)
	value, exist, kind := pb.get(key)
	if !exist && closer != nil {
		closer()
		closer = nil
	}

	return value, exist, kind, closer
}

func (a *arrayTable) isNonBlock() bool {
	return a.header.version == atVersionDefault ||
		a.header.version == atVersionPrefixCompress
}

func (a *arrayTable) newIter(opts *iterOptions) internalIterator {
	iter := atIterPool.Get().(*arrayTableIterator)
	iter.at = a

	if opts != nil {
		iter.disableCache = opts.DisableCache
	} else {
		iter.disableCache = false
	}

	if a.header.version == atVersionPrefixCompress {
		if cap(iter.keyBuf) == 0 {
			iter.keyBuf = make([]byte, 0, 1<<7)
		}
		if iter.sharedCache == nil {
			iter.sharedCache = &sharedInfo{idx: -1, key: nil}
		}
	}

	return iter
}

func (a *arrayTable) delPercent() float64 {
	return 0.0
}

func (a *arrayTable) itemCount() int {
	return a.num
}

func (a *arrayTable) inuseBytes() uint64 {
	return uint64(a.tbl.Size())
}

func (a *arrayTable) dataBytes() uint64 {
	return uint64(a.header.intIndexOffset - a.header.dataOffset)
}

func (a *arrayTable) close() error {
	if a.tbl == nil {
		return nil
	}

	if err := a.tbl.close(); err != nil {
		return err
	}

	a.tbl = nil
	return nil
}

func (a *arrayTable) readyForFlush() bool {
	return true
}

func (a *arrayTable) path() string {
	if a.tbl == nil {
		return ""
	}
	return a.tbl.path
}

func (a *arrayTable) idxFilePath() string {
	return ""
}

func (a *arrayTable) empty() bool {
	return a.num == 0
}

func (a *arrayTable) findKeyByMapIndex(khash uint32) int {
	var pos int
	v, exist := a.mapIndex.Get32(khash)
	if exist {
		pos = int(v - 1)
	} else {
		pos = a.num
	}
	return pos
}

func (a *arrayTable) findKeyByIntIndex(key []byte) int {
	return sort.Search(a.num, func(i int) bool {
		sharedKey1, sharedKey2 := a.getKey(i)
		return atCompare(sharedKey1, sharedKey2, key) != -1
	})
}

func atCompare(sharedKey1 []byte, sharedKey2 []byte, key []byte) int {
	if sharedKey1 != nil && sharedKey2 == nil {
		return bytes.Compare(sharedKey1, key)
	} else {
		sk1l := len(sharedKey1)
		kl := len(key)
		if sk1l <= kl {
			if r := bytes.Compare(sharedKey1, key[:sk1l]); r != 0 {
				return r
			} else {
				return bytes.Compare(sharedKey2, key[sk1l:])
			}
		} else {
			return bytes.Compare(sharedKey1, key)
		}
	}
}

func atEqual(sharedKey1 []byte, sharedKey2 []byte, key []byte) bool {
	if sharedKey1 != nil && sharedKey2 == nil {
		return bytes.Equal(sharedKey1, key)
	} else {
		sk1l := len(sharedKey1)
		sk2l := len(sharedKey2)
		kl := len(key)
		if sk1l+sk2l == kl && bytes.Equal(sharedKey1, key[:sk1l]) && bytes.Equal(sharedKey2, key[sk1l:]) {
			return true
		}
	}

	return false
}

func compressEncode(dst, src []byte) []byte {
	return snappy.Encode(dst, src)
}

func compressDecode(dst, src []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}
