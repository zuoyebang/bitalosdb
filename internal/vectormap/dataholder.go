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

package vectormap

import (
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

const (
	offsetMetaHashL     = 0
	offsetMetaHashHH    = 8
	offsetMetaTimestamp = 12
	offsetMetaSeqNum    = 20
	offsetMetaSlot      = 28
	offsetMetaVInfo     = 30
	offsetMetaVOffset   = 34
	metaSize            = 38
	metaSizeWithKey     = 44
	offsetMetaVersion   = 30
	offsetMetaSize      = 38
	metaSizeKKV         = 42
	metaSizeKKVWithKey  = 48
	offsetMetaListPre   = 42
	offsetMetaListNext  = 50
	metaSizeList        = 58
	metaSizeListWithKey = 64
	// offsetMetaKOffset   = ${metaSizeNoKey} // cal

	maskHigh8 uint64 = 0xff00_0000_0000_0000
	maskLow56 uint64 = 0x00ff_ffff_ffff_ffff

	overShortSize uint32 = 1 << 5
	maxOffset            = 16 << 30
)

type header struct {
	ver      uint16
	groupNum uint32
	resident uint32
	dead     uint32
	idxSeq   uint32
	dataSeq  uint32
	delKeys  uint32
	delVCap  uint32
	vFileIdx uint32
}

const (
	vInfoMaskType       uint32 = 0x8000_0000
	vInfoMaskMetaTag    uint32 = 0x4000_0000
	vInfoMaskReverse    uint32 = 0x3000_0000
	vInfoMaskLargeVSize uint32 = 0x0FFF_FFFF
	vInfoMaskSmallVCap  uint32 = 0x0FFF_0000
	vInfoMaskSmallVSize uint32 = 0x0000_FFFF
)

type vInfo uint32

func NewVInfo(size, cap, t uint32) vInfo {
	if t == 1 {
		return vInfo(t<<31 | size)
	} else {
		return vInfo(t<<31 | cap<<16 | size)
	}
}

//go:inline
func (v *vInfo) Info() (t uint32, size uint32, cap uint32) {
	vi := uint32(*v)
	t = (vi & vInfoMaskType) >> 31
	if t == 0 {
		cap = (vi & vInfoMaskSmallVCap) >> 16
		size = vi & vInfoMaskSmallVSize
	} else {
		size = vi & vInfoMaskLargeVSize
	}
	return
}

func MarshalKInfo(buf []byte, off uint32, l uint16) {
	binary.LittleEndian.PutUint32(buf, off)
	binary.LittleEndian.PutUint16(buf[4:], l)
}

func UnmarshalKInfo(buf []byte) (off uint32, l uint16) {
	off = binary.LittleEndian.Uint32(buf)
	l = binary.LittleEndian.Uint16(buf[4:])
	return
}

func MarshalMetaKV(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32,
	slot uint16, kOffset uint32, kLen uint16, metaKOff int) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint32(buf[offsetMetaVOffset:], vOffset)
	binary.LittleEndian.PutUint16(buf[offsetMetaSlot:], slot)
	MarshalKInfo(buf[metaKOff:], kOffset, kLen)
}

func MarshalMeta(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64,
	slot uint16, kOffset uint32, kLen uint16, metaKOff int) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(buf[offsetMetaSlot:], slot)
	MarshalKInfo(buf[metaKOff:], kOffset, kLen)
}

//go:inline
func buildUpdateTSAndVInfo(buf []byte, timestamp, seqNum uint64, vInfo vInfo) {
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaVInfo:], uint32(vInfo))
}

//go:inline
func buildUpdateMeta(buf []byte, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32) {
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint32(buf[offsetMetaVOffset:], vOffset)
}

type dataHolder struct {
	dataHolderNoKey
}

func NewDataHolder(data []byte) *dataHolder {
	ds := newDataSpaceWithData(data)
	dh := &dataHolder{}
	dh.dataSpace = ds
	return dh
}

func (dh *dataHolder) set(hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (metaOffset uint32, err error) {
	tsAndDT := combine(timestamp, dataType)

	lv := uint32(len(inV))
	lk := uint32(len(k))
	var vinfo vInfo
	if lv > overShortSize {
		vinfo = NewVInfo(lv, lv, 1)
		vOffset, err := dh.dataSpace.alloc(lk + lv + metaSizeWithKey)
		if err != nil {
			return 0, err
		}
		copy(dh.dataSpace.data[vOffset:], inV)
		kOffset := vOffset + lv
		copy(dh.dataSpace.data[kOffset:], k)
		metaOffset = kOffset + lk

		hashHH := uint32(hi >> 32)
		MarshalMetaKV(dh.dataSpace.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, vinfo, vOffset, slot, kOffset, uint16(lk), metaSize)
		return metaOffset, nil
	} else {
		vCap := CapForShortValue(lv)
		vinfo = NewVInfo(lv, vCap, 0)

		vOffset, err := dh.dataSpace.alloc(lk + vCap + metaSizeWithKey)
		if err != nil {
			return 0, err
		}
		copy(dh.dataSpace.data[vOffset:], inV)
		kOffset := vOffset + vCap
		copy(dh.dataSpace.data[kOffset:], k)
		metaOffset = kOffset + lk

		hashHH := uint32(hi >> 32)
		MarshalMetaKV(dh.dataSpace.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, vinfo, vOffset, slot, kOffset, uint16(lk), metaSize)
		return metaOffset, nil
	}
}

func (dh *dataHolder) setKKVMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64, slot uint16,
	version uint64, size uint32) (metaOffset uint32, err error) {
	lk := uint32(len(k))
	kOffset, err := dh.dataSpace.alloc(metaSizeKKVWithKey + lk)
	if err != nil {
		return 0, err
	}

	copy(dh.dataSpace.data[kOffset:], k)
	metaOffset = kOffset + lk
	tsAndDT := combine(timestamp, dataType)
	hashHH := uint32(hi >> 32)
	MarshalMeta(dh.dataSpace.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot, kOffset, uint16(lk), metaSizeKKV)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaVersion:], version)
	binary.LittleEndian.PutUint32(dh.dataSpace.data[metaOffset+offsetMetaSize:], size)
	return
}

func (dh *dataHolder) setKKVListMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64,
	slot uint16, version uint64, size uint32, pre, next uint64) (metaOffset uint32, err error) {
	lk := uint32(len(k))
	kOffset, err := dh.dataSpace.alloc(metaSizeListWithKey + lk)
	if err != nil {
		return 0, err
	}

	copy(dh.dataSpace.data[kOffset:], k)
	metaOffset = kOffset + lk

	tsAndDT := combine(timestamp, dataType)
	hashHH := uint32(hi >> 32)
	MarshalMeta(dh.dataSpace.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot, kOffset, uint16(lk), metaSizeList)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaVersion:], version)
	binary.LittleEndian.PutUint32(dh.dataSpace.data[metaOffset+offsetMetaSize:], size)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaListPre:], pre)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaListNext:], next)
	return
}

func (dh *dataHolder) getMeta(metaOffset uint32) (
	seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64,
	size uint32, pre, next uint64, err error,
) {
	seqNum = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaTimestamp:])
	slot = binary.LittleEndian.Uint16(dh.dataSpace.data[metaOffset+offsetMetaSlot:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		return
	}
	version = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaVersion:])
	size = binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaSize:])
	if kkv.IsDataTypeList(dataType) {
		pre = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaListPre:])
		next = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaListNext:])
	}
	return
}

func (dh *dataHolder) getKV(metaOffset uint32) (
	key, value []byte, seqNum uint64, dataType uint8, timestamp uint64,
	slot uint16, version uint64, size uint32, pre, next uint64, err error,
) {
	vOffset := uint64(binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaVOffset:]))
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaVInfo:]))
	slot = binary.LittleEndian.Uint16(dh.dataSpace.data[metaOffset+offsetMetaSlot:])
	seqNum = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	_, vsize, _ := ivInfo.Info()
	if dataType == kkv.DataTypeString {
		value = dh.dataSpace.data[vOffset : vOffset+uint64(vsize)]
		kOffset, kLen := UnmarshalKInfo(dh.dataSpace.data[metaOffset+metaSize:])
		key = dh.dataSpace.data[kOffset : kOffset+uint32(kLen)]
	} else {
		version = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaVersion:])
		size = binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaSize:])
		var (
			kOffset uint32
			kLen    uint16
		)
		if kkv.IsDataTypeList(dataType) {
			pre = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaListPre:])
			next = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaListNext:])
			kOffset, kLen = UnmarshalKInfo(dh.dataSpace.data[metaOffset+metaSizeList:])
		} else {
			kOffset, kLen = UnmarshalKInfo(dh.dataSpace.data[metaOffset+metaSizeKKV:])
		}
		key = dh.dataSpace.data[kOffset : kOffset+uint32(kLen)]
	}
	return
}

//go:inline
func CapForShortValue(vSize uint32) uint32 {
	return (vSize + 3) &^ 3
}

func CapForAlign4B(vSize uint64) uint64 {
	return (vSize + 3) &^ 3
}
