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

package kkv

import (
	"crypto/md5"
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/v2/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

func IsUseIterFirst(dataType uint8) bool {
	switch dataType {
	case DataTypeHash, DataTypeSet:
		return true
	default:
		return false
	}
}

func IsDataType(dataType uint8) bool {
	return dataType <= DataTypeDKSet
}

func IsDataTypeUseKeyHash(dataType uint8) bool {
	switch dataType {
	case DataTypeHash, DataTypeSet:
		return true
	default:
		return false
	}
}

func IsDataTypeList(dataType uint8) bool {
	switch dataType {
	case DataTypeList, DataTypeBitmap, DataTypeDKHash, DataTypeDKSet:
		return true
	default:
		return false
	}
}

func IsKeyUseMapIndex(key []byte) bool {
	dataType := key[SubKeyDataTypeOffset]
	switch dataType {
	case DataTypeNone, DataTypeZsetIndex:
		return false
	default:
		return true
	}
}

func IsUseBithash(dataType uint8) bool {
	switch dataType {
	case DataTypeBitmap:
		return false
	default:
		return true
	}
}

func GetKeyDataType(key []byte) uint8 {
	if len(key) <= SubKeyDataTypeOffset {
		return DataTypeNone
	}
	return key[SubKeyDataTypeOffset]
}

func GetSiDataType(dt uint8) uint8 {
	if dt == DataTypeZset {
		return DataTypeZsetIndex
	}
	return dt
}

func GetSiPrefix(buf []byte) []byte {
	return buf[0:SiKeySubKeyDataOffset]
}

func GetSubKey(key []byte) []byte {
	if len(key) <= SiKeyDataOffset {
		return nil
	}
	return key[SiKeyDataOffset:]
}

func EncodeKeyVersion(buf []byte, version uint64) {
	binary.LittleEndian.PutUint64(buf[0:SubKeyVersionLength], version)
}

func DecodeKeyVersion(buf []byte) uint64 {
	if len(buf) < SubKeyVersionLength {
		return 0
	}
	return binary.LittleEndian.Uint64(buf[0:SubKeyVersionLength])
}

func GetKeyVersion(buf []byte) []byte {
	if len(buf) < SubKeyVersionLength {
		return nil
	}
	return buf[0:SubKeyVersionLength]
}

func EncodeSlotId(buf []byte, slotId uint16) {
	binary.LittleEndian.PutUint16(buf, slotId)
}

func DecodeSlotId(buf []byte) uint16 {
	return binary.LittleEndian.Uint16(buf)
}

func PutSubKeyHeader(buf []byte, dt uint8, version uint64) {
	EncodeKeyVersion(buf, version)
	buf[SubKeyDataTypeOffset] = dt
}

func EncodeSubKeyByPool(key []byte, dt uint8, version uint64) ([]byte, func()) {
	size := SubKeyHeaderLength + len(key)
	buf, closer := bytepools.ReaderBytePools.GetBytePool(size)
	PutSubKeyHeader(buf, dt, version)
	copy(buf[SubKeyDataOffset:size], key)
	return buf[:size], closer
}

func EncodeSubKey(subKey []byte, dt uint8, version uint64) []byte {
	key := make([]byte, SubKeyHeaderLength+len(subKey))
	size := SubKeyHeaderLength + len(subKey)
	PutSubKeyHeader(key, dt, version)
	copy(key[SubKeyDataOffset:size], subKey)
	return key
}

func EncodeSubKeyLowerBound(buf []byte, dt uint8, version uint64) {
	PutSubKeyHeader(buf, dt, version)
}

func EncodeSubKeyUpperBound(buf []byte, dt uint8, version uint64) {
	PutSubKeyHeader(buf, dt, version)
	if dt == DataTypeZsetIndex {
		copy(buf[SubKeyHeaderLength:SubKeyUpperBoundLength], MaxScoreBound)
	} else {
		copy(buf[SubKeyHeaderLength:SubKeyUpperBoundLength], MaxUpperBound)
	}
}

func EncodeListKey(buf []byte, dt uint8, version uint64, index uint64) {
	PutSubKeyHeader(buf, dt, version)
	binary.BigEndian.PutUint64(buf[SubKeyDataOffset:SubKeyListLength], index)
}

func DecodeListKeyIndex(buf []byte) uint64 {
	if len(buf) != SubKeyListIndexLength {
		return 0
	}
	return binary.BigEndian.Uint64(buf)
}

func EncodeZsetIndexKeyByPool(version uint64, score, member []byte) ([]byte, func()) {
	memberLen := len(member)
	size := SiKeyHeaderLength + memberLen
	buf, closer := bytepools.ReaderBytePools.GetBytePool(size)
	PutSubKeyHeader(buf, DataTypeZsetIndex, version)
	copy(buf[SiKeyDataOffset:SiKeySubKeyDataOffset], score)
	copy(buf[SiKeySubKeyDataOffset:SiKeySubKeyDataOffset+memberLen], member)
	return buf[:size], closer
}

func EncodeZsetIndexKey(version uint64, score float64, member []byte) []byte {
	key := make([]byte, SiKeyLowerBoundLength+len(member))
	PutSubKeyHeader(key, DataTypeZsetIndex, version)
	utils.Float64ToByteSort(score, key[SubKeyDataOffset:SiKeySubKeyDataOffset])
	copy(key[SiKeySubKeyDataOffset:], member)
	return key
}

func DecodeZsetIndexMember(key []byte) []byte {
	if len(key) <= SiKeyHeaderLength {
		return nil
	}
	return key[SiKeySubKeyDataOffset:]
}

func EncodeZsetDataKey(buf []byte, version uint64, member []byte) {
	PutSubKeyHeader(buf, DataTypeZset, version)
	memberMd5 := md5.Sum(member)
	copy(buf[SubKeyDataOffset:SubKeyZsetLength], memberMd5[:])
}

func MakeZsetDataKey(version, memberMd5 []byte) []byte {
	buf := make([]byte, SubKeyZsetLength)
	copy(buf[SubKeyVersionOffset:SubKeyDataTypeOffset], version)
	buf[SubKeyDataTypeOffset] = DataTypeZset
	copy(buf[SubKeyDataOffset:SubKeyZsetLength], memberMd5)
	return buf
}

func DecodeZsetDataKey(key []byte) []byte {
	if len(key) < SubKeyZsetLength {
		return nil
	}
	return key[SubKeyDataOffset:]
}

func EncodeZsetDataKeyOld(buf []byte, version uint64, member []byte) int {
	var n int
	PutSubKeyHeader(buf, DataTypeZset, version)
	if len(member) <= KeyMd5Length {
		n = SubKeyHeaderLength + len(member)
		copy(buf[SubKeyDataOffset:n], member)
	} else {
		n = SubKeyZsetLength
		memberMd5 := md5.Sum(member)
		copy(buf[SubKeyDataOffset:n], memberMd5[:])
	}
	return n
}

func EncodeZsetScore(float float64) []byte {
	return utils.Float64ToByteSort(float, nil)
}

func DecodeZsetScore(buf []byte) float64 {
	return utils.ByteSortToFloat64(buf)
}

func EncodeZsetIndexLowerBound(buf []byte, version uint64, score float64) {
	PutSubKeyHeader(buf, DataTypeZsetIndex, version)
	utils.Float64ToByteSort(score, buf[SubKeyDataOffset:SiKeyLowerBoundLength])
}

func EncodeZsetIndexUpperBound(buf []byte, version uint64, score float64) {
	PutSubKeyHeader(buf, DataTypeZsetIndex, version)
	utils.Float64ToByteSort(score, buf[SubKeyDataOffset:SiKeySubKeyDataOffset])
	copy(buf[SiKeySubKeyDataOffset:SiKeyUpperBoundLength], MaxUpperBound)
}

func EncodeExpireKey(buf []byte, timestamp, version uint64, sid uint16) {
	PutSubKeyHeader(buf, DataTypeExpireKey, timestamp)
	binary.LittleEndian.PutUint64(buf[SiKeyDataOffset:], version)
	binary.LittleEndian.PutUint16(buf[SiKeyDataOffset+8:], sid)
}

func DecodeExpireKey(key InternalKey) (timestamp, version uint64, dt uint8, sid uint16) {
	timestamp = binary.LittleEndian.Uint64(key.Version)
	dt = key.DataType
	version = binary.LittleEndian.Uint64(key.SubKey[0:8])
	sid = binary.LittleEndian.Uint16(key.SubKey[8:10])
	return
}
