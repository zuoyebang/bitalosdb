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

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
)

type dataSpace struct {
	tail uint32
	size uint32
	data []byte
}

func newDataSpace(size int) *dataSpace {
	return &dataSpace{
		size: uint32(size),
		data: manual.New(size),
	}
}

func newDataSpaceWithData(data []byte) *dataSpace {
	return &dataSpace{
		size: uint32(len(data)),
		data: data,
	}
}

func (ds *dataSpace) alloc(size uint32) (off uint32, err error) {
	nTail := ds.tail + size
	if nTail > ds.size {
		return 0, base.ErrTableFull
	}
	off = ds.tail
	ds.tail = nTail
	return
}

type dataHolderNoKey struct {
	cap        uint32
	valUsed    uint32
	items      uint32
	limit      uint32
	dataSpace  *dataSpace
	compressor compress.Compressor
	compBuf    []byte
	logger     base.Logger
}

func NewDataHolderNokey(data []byte, logger base.Logger) *dataHolderNoKey {
	ds := newDataSpaceWithData(data)
	hf := &dataHolderNoKey{
		dataSpace: ds,
		logger:    logger,
	}
	return hf
}

func (dh *dataHolderNoKey) close() error {
	return nil
}

func (dh *dataHolderNoKey) set(hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (metaOffset uint32, err error) {
	lv := uint32(len(inV))
	var vinfo vInfo
	if lv > overShortSize {
		vOffset, err := dh.dataSpace.alloc(lv + metaSize)
		if err != nil {
			return 0, err
		}

		copy(dh.dataSpace.data[vOffset:], inV)
		metaOffset = vOffset + lv
		vinfo = NewVInfo(lv, lv, 1)
		hashHH := uint32(hi >> 32)
		tsAndDT := combine(timestamp, dataType)
		MarshalMetaValueNoKey(dh.dataSpace.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, vinfo, vOffset, slot)
		return metaOffset, nil
	} else {
		vCap := CapForShortValue(lv)
		vOffset, err := dh.dataSpace.alloc(vCap + metaSize)
		if err != nil {
			return 0, err
		}

		copy(dh.dataSpace.data[vOffset:], inV)
		metaOffset = vOffset + vCap
		vinfo = NewVInfo(lv, vCap, 0)
		hashHH := uint32(hi >> 32)
		tsAndDT := combine(timestamp, dataType)
		MarshalMetaValueNoKey(dh.dataSpace.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, vinfo, vOffset, slot)
		return metaOffset, nil
	}
}

func (dh *dataHolderNoKey) setKKVMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64, slot uint16,
	version uint64, size uint32) (metaOffset uint32, err error) {
	metaOffset, err = dh.dataSpace.alloc(metaSizeKKV)
	if err != nil {
		return 0, err
	}
	tsAndDT := combine(timestamp, dataType)
	hashHH := uint32(hi >> 32)
	MarshalMetaNoKey(dh.dataSpace.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaVersion:], version)
	binary.LittleEndian.PutUint32(dh.dataSpace.data[metaOffset+offsetMetaSize:], size)
	return
}

func (dh *dataHolderNoKey) setKKVListMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64,
	slot uint16, version uint64, size uint32, pre, next uint64) (metaOffset uint32, err error) {
	metaOffset, err = dh.dataSpace.alloc(metaSizeList)
	if err != nil {
		return 0, err
	}
	tsAndDT := combine(timestamp, dataType)
	hashHH := uint32(hi >> 32)
	MarshalMetaNoKey(dh.dataSpace.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaVersion:], version)
	binary.LittleEndian.PutUint32(dh.dataSpace.data[metaOffset+offsetMetaSize:], size)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaListPre:], pre)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaListNext:], next)
	return
}

func (dh *dataHolderNoKey) getValue(metaOffset uint32) (bs []byte, seqNum uint64, dt uint8, ts uint64, slot uint16, err error) {
	vOffset := binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaVInfo:]))
	slot = binary.LittleEndian.Uint16(dh.dataSpace.data[metaOffset+offsetMetaSlot:])
	_, vSize, _ := ivInfo.Info()
	seqNum = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaSeqNum:])
	ttl := binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaTimestamp:])
	ts, dt = split(ttl)
	if dt == kkv.DataTypeString {
		bs = dh.dataSpace.data[vOffset : vOffset+vSize]
	}
	return
}

func (dh *dataHolderNoKey) getMeta(metaOffset uint32) (
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

func (dh *dataHolderNoKey) getKV(metaOffset uint32) (
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
	} else {
		version = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaVersion:])
		size = binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaSize:])
		if kkv.IsDataTypeList(dataType) {
			pre = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaListPre:])
			next = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaListNext:])
		}
	}
	return
}

func (dh *dataHolderNoKey) update(mOffset uint32, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (err error) {
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.dataSpace.data[mOffset+offsetMetaVInfo:]))
	binary.LittleEndian.PutUint16(dh.dataSpace.data[mOffset+offsetMetaSlot:], slot)

	lv := uint32(len(inV))
	tsAndDT := combine(timestamp, dataType)
	t, _, c := ivInfo.Info()

	if lv <= overShortSize && t == 0 && lv <= c {
		vinfo := NewVInfo(lv, c, 0)
		vOffset := uint64(binary.LittleEndian.Uint32(dh.dataSpace.data[mOffset+offsetMetaVOffset:]))
		copy(dh.dataSpace.data[vOffset:], inV)
		buildUpdateTSAndVInfo(dh.dataSpace.data[mOffset:], tsAndDT, seqNum, vinfo)
		return
	}

	var vinfo vInfo
	if lv > overShortSize {
		vOffset, err := dh.dataSpace.alloc(lv)
		if err != nil {
			return err
		}

		vinfo = NewVInfo(lv, lv, 1)
		buildUpdateMeta(dh.dataSpace.data[mOffset:], tsAndDT, seqNum, vinfo, vOffset)
		copy(dh.dataSpace.data[vOffset:], inV)
		return nil
	} else {
		vCap := CapForShortValue(lv)
		vinfo = NewVInfo(lv, vCap, 0)
		vOffset, err := dh.dataSpace.alloc(vCap)
		if err != nil {
			return err
		}

		copy(dh.dataSpace.data[vOffset:], inV)
		buildUpdateMeta(dh.dataSpace.data[mOffset:], tsAndDT, seqNum, vinfo, vOffset)
		return nil
	}
}

func (dh *dataHolderNoKey) updateKKVMeta(
	mOffset uint32, dataType uint8,
	timestamp, seqNum uint64, slot uint16, version uint64, size uint32,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(dh.dataSpace.data[mOffset+offsetMetaSlot:], slot)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[mOffset+offsetMetaVersion:], version)
	binary.LittleEndian.PutUint32(dh.dataSpace.data[mOffset+offsetMetaSize:], size)
}

func (dh *dataHolderNoKey) updateKKVListMeta(
	mOffset uint32, dataType uint8,
	timestamp, seqNum uint64, slot uint16, version uint64,
	size uint32, pre, next uint64,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(dh.dataSpace.data[mOffset+offsetMetaSlot:], slot)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[mOffset+offsetMetaVersion:], version)
	binary.LittleEndian.PutUint32(dh.dataSpace.data[mOffset+offsetMetaSize:], size)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[mOffset+offsetMetaListPre:], pre)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[mOffset+offsetMetaListNext:], next)
}

func (dh *dataHolderNoKey) getHashL(metaOffset uint32) (hashL uint64) {
	return binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset:])
}

func (dh *dataHolderNoKey) getHashInfo(metaOffset uint32) (hashL uint64, hashHH uint32) {
	hashL = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaHashHH:])
	return
}

func (dh *dataHolderNoKey) getHashAndVersionDT(metaOffset uint32) (hashL uint64, hashHH uint32, ver uint64, dt uint8) {
	hashL = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.dataSpace.data[metaOffset+offsetMetaHashHH:])
	tsAndDT := binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaTimestamp:])
	dt = splitHigh8(tsAndDT)
	if dt != kkv.DataTypeString {
		ver = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaVersion:])
	}
	return
}

func (dh *dataHolderNoKey) checkSeq(metaOffset uint32, seqNum uint64) bool {
	oldSeq := binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaSeqNum:])
	return seqNum <= oldSeq
}

func (dh *dataHolderNoKey) getSeqNum(metaOffset uint32) (seqNum uint64) {
	return binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaSeqNum:])
}

func (dh *dataHolderNoKey) getTTL(metaOffset uint32) (seqNum, ts uint64, dt uint8) {
	seqNum = binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaSeqNum:])
	t := binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaTimestamp:])
	ts, dt = split(t)
	return
}

func (dh *dataHolderNoKey) setTTL(metaOffset uint32, seqNum, ttl uint64) {
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaTimestamp:], ttl)
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaSeqNum:], seqNum)
}

func (dh *dataHolderNoKey) setSize(metaOffset uint32, seqNum uint64, size uint32) {
	ottl := binary.LittleEndian.Uint64(dh.dataSpace.data[metaOffset+offsetMetaTimestamp:])
	if uint8(ottl>>56) == kkv.DataTypeString {
		return
	}
	binary.LittleEndian.PutUint64(dh.dataSpace.data[metaOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(dh.dataSpace.data[metaOffset+offsetMetaSize:], size)
}

func MarshalMetaValueNoKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32, slot uint16) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint32(buf[offsetMetaVOffset:], vOffset)
	binary.LittleEndian.PutUint16(buf[offsetMetaSlot:], slot)
}

func MarshalMetaNoKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, slot uint16) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(buf[offsetMetaSlot:], slot)
}
