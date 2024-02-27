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

package bindex

import (
	"arena"
	"encoding/binary"
	"sort"
)

const (
	MaxLowBit             = 0xFFFF
	HashIndexShardItemAvg = 1 << 8
	HashIndexShardsNum    = 64 << 10
	HashIndexShardSize    = 4
	HashIndexItem32Size   = 6
	HashIndexItem64Size   = 10
)

type HashIndex struct {
	header     Header
	size       uint32
	length     uint32
	itemOffset uint32
	type32     bool
	uniq       bool
	data       []byte
	data32     []FItem32Array
	data64     []FItem64Array
	arena      *arena.Arena
}

type FItem32 struct {
	key   uint16
	value uint32
}

type FItem64 struct {
	key   uint16
	value uint64
}

type FItem32Array []FItem32

func (i32 FItem32Array) Len() int {
	return len(i32)
}

func (i32 FItem32Array) Swap(i, j int) {
	i32[i], i32[j] = i32[j], i32[i]
}

func (i32 FItem32Array) Less(i, j int) bool {
	return i32[i].key < i32[j].key
}

type FItem64Array []FItem64

func (i64 FItem64Array) Len() int {
	return len(i64)
}

func (i64 FItem64Array) Swap(i, j int) {
	i64[i], i64[j] = i64[j], i64[i]
}

func (i64 FItem64Array) Less(i, j int) bool {
	return i64[i].key < i64[j].key
}

func NewHashIndex(type32 bool) *HashIndex {
	offset := uint32(SuccinctHeaderSize + HashIndexShardsNum*HashIndexShardSize)

	m := &HashIndex{
		header:     Header{version: SuccinctVersion, reserved: 0, shards: HashIndexShardsNum},
		size:       offset,
		length:     0,
		itemOffset: offset,
		type32:     type32,
		uniq:       false,
		data:       nil,
		data32:     nil,
		data64:     nil,
		arena:      nil,
	}

	return m
}

func (s *HashIndex) Size() uint32 {
	if !s.uniq {
		s.Unique()
	}
	return s.size
}

func (s *HashIndex) Length() uint32 {
	return s.length
}

func (s *HashIndex) GetData() []byte {
	return s.data
}

func (s *HashIndex) SetReader(d []byte) bool {
	if d == nil || len(d) <= int(s.itemOffset) {
		return false
	}

	s.data = d
	s.header = s.readHeader(s.data)

	return true
}

func (s *HashIndex) InitWriter() {
	s.arena = arena.NewArena()

	if s.type32 {
		s.data32 = arena.MakeSlice[FItem32Array](s.arena, int(s.header.shards), int(s.header.shards))
	} else {
		s.data64 = arena.MakeSlice[FItem64Array](s.arena, int(s.header.shards), int(s.header.shards))
	}
}

func (s *HashIndex) SetWriter(d []byte) bool {
	if d == nil || len(d) < int(s.size) || cap(d) < int(s.size) {
		return false
	}

	s.data = d

	return true
}

func (s *HashIndex) Add(key uint32, value any) {
	switch value.(type) {
	case uint32:
		if s.type32 {
			s.add32Internal(key, value.(uint32))
		}
		return
	case uint64:
		if !s.type32 {
			s.add64Internal(key, value.(uint64))
		}
		return
	default:
		return
	}
}

func (s *HashIndex) Unique() {
	if s.uniq {
		return
	}

	if s.type32 {
		s.unique32Internal()
	} else {
		s.unique64Internal()
	}

	s.uniq = true
}

func (s *HashIndex) Serialize() bool {
	if !s.uniq {
		s.Unique()
	}

	if s.type32 {
		return s.serialize32Internal()
	} else {
		return s.serialize64Internal()
	}
}

func (s *HashIndex) Get(key uint32) (any, bool) {
	if s.type32 {
		return s.Get32(key)
	} else {
		return s.Get64(key)
	}
}

func (s *HashIndex) add32Internal(key uint32, value uint32) {
	if s.header.shards <= 0 {
		return
	}

	hid := s.highbits(key)
	lid := s.lowbits(key)

	if len(s.data32[hid]) == 0 {
		s.data32[hid] = arena.MakeSlice[FItem32](s.arena, 0, HashIndexShardItemAvg)
	}

	s.data32[hid] = append(s.data32[hid], FItem32{key: lid, value: value})

	s.size += HashIndexItem32Size
	s.length++
}

func (s *HashIndex) add64Internal(key uint32, value uint64) {
	if s.header.shards <= 0 {
		return
	}

	hid := s.highbits(key)
	lid := s.lowbits(key)

	if len(s.data64[hid]) == 0 {
		s.data64[hid] = arena.MakeSlice[FItem64](s.arena, 0, HashIndexShardItemAvg)
	}

	s.data64[hid] = append(s.data64[hid], FItem64{key: lid, value: value})

	s.size += HashIndexItem64Size
	s.length++
}

func (s *HashIndex) unique32Internal() {
	if s.size <= s.itemOffset || s.length <= 0 || len(s.data32) <= 0 {
		return
	}

	for i := uint32(0); i < s.header.shards; i++ {
		itemsLen := uint32(len(s.data32[i]))
		if itemsLen > 1 {
			sort.Sort(s.data32[i])

			uniqFlag := false
			prevItem := int32(-1)
			for j := uint32(0); j < itemsLen; j++ {
				if prevItem == int32(s.data32[i][j].key) {
					itemsLen--
					s.length--
					s.size -= HashIndexItem32Size
					copy(s.data32[i][j:], s.data32[i][j+1:])
					uniqFlag = true
					continue
				}

				prevItem = int32(s.data32[i][j].key)
			}

			if uniqFlag {
				s.data32[i] = s.data32[i][0:itemsLen]
			}
		}
	}
}

func (s *HashIndex) unique64Internal() {
	if s.size <= s.itemOffset || s.length <= 0 || len(s.data64) <= 0 {
		return
	}

	for i := uint32(0); i < s.header.shards; i++ {
		itemsLen := uint32(len(s.data64[i]))
		if itemsLen > 1 {
			sort.Sort(s.data64[i])

			uniqFlag := false
			prevItem := int32(-1)
			for j := uint32(0); j < itemsLen; j++ {
				if prevItem == int32(s.data64[i][j].key) {
					itemsLen--
					s.length--
					s.size -= HashIndexItem64Size
					copy(s.data64[i][j:], s.data64[i][j+1:])
					uniqFlag = true
					continue
				}

				prevItem = int32(s.data64[i][j].key)
			}

			if uniqFlag {
				s.data64[i] = s.data64[i][0:itemsLen]
			}
		}
	}
}

func (s *HashIndex) serialize32Internal() bool {
	if s.size <= s.itemOffset || s.length <= 0 || len(s.data32) <= 0 {
		return false
	}

	shardOffset := uint32(0)
	itemOffset := s.itemOffset

	if s.data == nil {
		s.data = arena.MakeSlice[byte](s.arena, int(s.size), int(s.size))
	}

	s.writeHeader(s.data[shardOffset:], s.header)
	shardOffset += SuccinctHeaderSize

	totalCount := uint32(0)
	for i := uint32(0); i < s.header.shards; i++ {
		itemsLen := uint32(len(s.data32[i]))
		totalCount += itemsLen
		s.writeShard(s.data[shardOffset:], totalCount)
		shardOffset += HashIndexShardSize

		if itemsLen > 0 {
			for j := uint32(0); j < itemsLen; j++ {
				s.writeItem32(s.data[itemOffset:], s.data32[i][j])
				itemOffset += HashIndexItem32Size
			}
		}
	}

	return true
}

func (s *HashIndex) serialize64Internal() bool {
	if s.size <= s.itemOffset || s.length <= 0 || len(s.data64) <= 0 {
		return false
	}

	shardOffset := uint32(0)
	itemOffset := s.itemOffset

	if s.data == nil {
		s.data = arena.MakeSlice[byte](s.arena, int(s.size), int(s.size))
	}

	s.writeHeader(s.data[shardOffset:], s.header)
	shardOffset += SuccinctHeaderSize

	totalCount := uint32(0)
	for i := uint32(0); i < s.header.shards; i++ {
		itemsLen := uint32(len(s.data64[i]))
		totalCount += itemsLen
		s.writeShard(s.data[shardOffset:], totalCount)
		shardOffset += HashIndexShardSize

		if itemsLen > 0 {
			for j := uint32(0); j < itemsLen; j++ {
				s.writeItem64(s.data[itemOffset:], s.data64[i][j])
				itemOffset += HashIndexItem64Size
			}
		}
	}

	return true
}

func (s *HashIndex) Get32(key uint32) (uint32, bool) {
	if len(s.data) <= int(s.itemOffset) || s.header.shards <= 0 {
		return 0, false
	}

	hid := s.highbits(key)
	lid := s.lowbits(key)

	originCount := uint32(0)
	if hid > 0 {
		originOffset := uint32(SuccinctHeaderSize) + uint32(hid-1)*HashIndexShardSize
		originCount = s.readShard(s.data[originOffset:])
	}

	destOffset := uint32(SuccinctHeaderSize) + uint32(hid)*HashIndexShardSize
	destCount := s.readShard(s.data[destOffset:])
	if destCount <= originCount {
		return 0, false
	}

	itemLength := destCount - originCount
	curOffset := s.itemOffset + originCount*HashIndexItem32Size

	ok, idx := s.findItem(lid, s.data[curOffset:], int(itemLength), HashIndexItem32Size)
	if !ok {
		return 0, false
	}

	curOffset += uint32(idx * HashIndexItem32Size)
	value := s.readItem32Value(s.data[curOffset:])

	return value, true
}

func (s *HashIndex) Get64(key uint32) (uint64, bool) {
	if len(s.data) <= int(s.itemOffset) || s.header.shards <= 0 {
		return 0, false
	}

	hid := s.highbits(key)
	lid := s.lowbits(key)

	originCount := uint32(0)
	if hid > 0 {
		originOffset := uint32(SuccinctHeaderSize) + uint32(hid-1)*HashIndexShardSize
		originCount = s.readShard(s.data[originOffset:])
	}

	destOffset := uint32(SuccinctHeaderSize) + uint32(hid)*HashIndexShardSize
	destCount := s.readShard(s.data[destOffset:])
	if destCount <= originCount {
		return 0, false
	}

	itemLength := destCount - originCount
	curOffset := s.itemOffset + originCount*HashIndexItem64Size

	ok, idx := s.findItem(lid, s.data[curOffset:], int(itemLength), HashIndexItem64Size)
	if !ok {
		return 0, false
	}

	curOffset += uint32(idx * HashIndexItem64Size)
	value := s.readItem64Value(s.data[curOffset:])

	return value, true
}

func (s *HashIndex) Finish() {
	s.size = SuccinctHeaderSize
	s.length = 0
	s.uniq = false
	s.data32 = nil
	s.data64 = nil
	if s.arena != nil {
		s.arena.Free()
		s.arena = nil
	}
}

func (s *HashIndex) writeHeader(buf []byte, header Header) {
	binary.BigEndian.PutUint16(buf[0:], header.version)
	binary.BigEndian.PutUint16(buf[2:], header.reserved)
	binary.BigEndian.PutUint32(buf[4:], header.shards)
}

func (s *HashIndex) writeShard(buf []byte, count uint32) {
	binary.BigEndian.PutUint32(buf[0:], count)
}

func (s *HashIndex) writeItem32(buf []byte, item32 FItem32) {
	binary.BigEndian.PutUint16(buf[0:], item32.key)
	binary.BigEndian.PutUint32(buf[2:], item32.value)
}

func (s *HashIndex) writeItem64(buf []byte, item64 FItem64) {
	binary.BigEndian.PutUint16(buf[0:], item64.key)
	binary.BigEndian.PutUint64(buf[2:], item64.value)
}

func (s *HashIndex) readHeader(buf []byte) Header {
	header := Header{
		version:  binary.BigEndian.Uint16(buf[0:]),
		reserved: binary.BigEndian.Uint16(buf[2:]),
		shards:   binary.BigEndian.Uint32(buf[4:]),
	}

	return header
}

func (s *HashIndex) readShard(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf[0:])
}

func (s *HashIndex) readItem32Value(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf[2:])
}

func (s *HashIndex) readItem64Value(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf[2:])
}

func (s *HashIndex) findItem(key uint16, buf []byte, n int, step int) (bool, int) {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if binary.BigEndian.Uint16(buf[step*h:]) < key {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < n && binary.BigEndian.Uint16(buf[step*i:]) == key {
		return true, i
	}

	return false, 0
}

func (s *HashIndex) highbits(x uint32) uint16 {
	return uint16(x >> 16)
}

func (s *HashIndex) lowbits(x uint32) uint16 {
	return uint16(x & MaxLowBit)
}
