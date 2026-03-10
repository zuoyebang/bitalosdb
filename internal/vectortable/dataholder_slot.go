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

package vectortable

import (
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/v2/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

const (
	offsetMetaStVInfo     = 28
	offsetMetaStSlot      = 32
	offsetMetaStVFIdx     = 36
	offsetMetaStKFIdx     = 38
	offsetMetaStVOffset   = 40
	offsetMetaStKOffsetS  = 44
	metaSizeStStrWithKey  = 48
	offsetMetaStKKVSlot   = 28
	offsetMetaStKKVKFIdx  = 30
	offsetMetaStVersion   = 32
	offsetMetaStSize      = 40
	offsetMetaStKOffsetK  = 44
	metaSizeStKKVWithKey  = 48
	offsetMetaStListPre   = 44
	offsetMetaStListNext  = 52
	offsetMetaStKOffsetL  = 60
	metaSizeStListWithKey = 64
	// offsetMetaKOffset   = 36 // cal
)

func MarshalMetaStKV(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, vInfo vInfo, slot uint16,
	vfi, kfi uint16, vOffset uint32, kOffset uint32) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaStVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint16(buf[offsetMetaStSlot:], slot)
	binary.LittleEndian.PutUint16(buf[offsetMetaStVFIdx:], vfi)
	binary.LittleEndian.PutUint16(buf[offsetMetaStKFIdx:], kfi)
	binary.LittleEndian.PutUint32(buf[offsetMetaStVOffset:], vOffset)
	binary.LittleEndian.PutUint32(buf[offsetMetaStKOffsetS:], kOffset)
}

func MarshalMetaStKKVWithKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, slot uint16, ver uint64, size uint32, kfi uint16, kOffset uint32) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(buf[offsetMetaStKKVSlot:], slot)
	binary.LittleEndian.PutUint16(buf[offsetMetaStKKVKFIdx:], kfi)
	binary.LittleEndian.PutUint64(buf[offsetMetaStVersion:], ver)
	binary.LittleEndian.PutUint32(buf[offsetMetaStSize:], size)
	binary.LittleEndian.PutUint32(buf[offsetMetaStKOffsetK:], kOffset)
}

func MarshalMetaStListWithKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, slot uint16, ver uint64, size uint32, kfi uint16, kOffset uint32, pre, next uint64) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(buf[offsetMetaStKKVSlot:], slot)
	binary.LittleEndian.PutUint16(buf[offsetMetaStKKVKFIdx:], kfi)
	binary.LittleEndian.PutUint64(buf[offsetMetaStVersion:], ver)
	binary.LittleEndian.PutUint32(buf[offsetMetaStSize:], size)
	binary.LittleEndian.PutUint64(buf[offsetMetaStListPre:], pre)
	binary.LittleEndian.PutUint64(buf[offsetMetaStListNext:], next)
	binary.LittleEndian.PutUint32(buf[offsetMetaStKOffsetL:], kOffset)
}

//go:inline
func buildUpdateMeta4MultiWithSlot(buf []byte, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32, fIdx uint16) {
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaStVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint16(buf[offsetMetaStVFIdx:], fIdx)
	binary.LittleEndian.PutUint32(buf[offsetMetaStVOffset:], vOffset)
}

type dataHolderSlot struct {
	dataHolderSlotNoKey
}

func NewDataHolderSlot(opts *options.VectorTableOptions, idxSeq, dataSeq, groups uint32, now uint64) (*dataHolderSlot, error) {
	var err error
RETRY:
	indexFile, metaFile, kDataFile, vDataFile := buildVTFileName(opts.Dirname, opts.Filename, idxSeq, dataSeq)
	d := dataHolderSlot{}
	d.Header = &header{
		idxSeq:  idxSeq,
		dataSeq: dataSeq,
	}
	d.compressor = opts.Compressor
	d.compBuf = make([]byte, 1024)
	d.logger = opts.Logger
	mmapBlockSize := opts.MmapBlockSize
	exManage := !opts.DisableExpireManage
	d.Header.tags.setExpireManage(exManage)

	size := 9*int(groups)*simd.GroupSize + headerSize
	d.Index, err = openTableDesignated(indexFile, &d.Header.indexTail, &tableOptions{
		openType:     tableWriteMmap,
		initMmapSize: size,
		logger:       opts.Logger,
	})
	if err != nil {
		return nil, errors.Errorf("vectortable: open index file err:%s", err)
	}

	defer func() {
		if err != nil {
			if d.Index != nil {
				d.Index.close()
			}
			if d.Meta != nil {
				d.Meta.close()
			}
			if d.KDataM != nil {
				d.KDataM.close()
			}
			if d.VDataM != nil {
				d.VDataM.close()
			}
		}
	}()

	var typeNew bool
	if d.Index.filesz == 0 {
		typeNew = true
		d.Header.ver = curVersion
		var offset uint64
		offset, err = d.Index.alloc(headerSize)
		if err != nil {
			err = errors.Wrapf(err, "vectortable: alloc header space fail")
			return nil, err
		}
		if offset != 0 {
			err = errors.Errorf("vectortable: invalid header offset:%d", offset)
			return nil, err
		}
	} else if d.Index.filesz < headerSize {
		err = errors.Errorf("vectortable: invalid header size:%d", d.Index.filesz)
		return nil, err
	} else {
		var h header
		h.Unmarshal(d.Index.data[0:headerSize])
		if h.indexTail.GetOffset() == 0 {
			typeNew = true
			d.Header.ver = curVersion
			var offset uint64
			offset, err = d.Index.alloc(headerSize)
			if err != nil {
				err = errors.Wrapf(err, "vectortable: alloc header space fail")
				return nil, err
			}
			if offset != 0 {
				err = errors.Errorf("vectortable: invalid header offset:%d", offset)
				return nil, err
			}
		} else {
			if h.ver == 0 || h.ver > curVersion {
				opts.Logger.Errorf("panic error index version: file:%s", indexFile)
				dataSeq++
				idxSeq = 0
				goto RETRY
			}
			d.Header = &h
			exManage = d.Header.tags.isExpireManage()
		}
	}

	// fix idxSeq
	if d.Header.idxSeq != idxSeq {
		d.Header.idxSeq = idxSeq
	}

	metaVer := uint16(1)
	d.Meta, err = openTableExtend(metaFile, &d.Header.metaTail, mmapBlockSize, metaVer, opts.Logger)
	if err != nil {
		err = errors.Errorf("vectortable: open meta file err:%s", err)
		return nil, err
	}

	if uint64(d.Meta.filesz) < d.Header.metaTail.GetOffset() {
		err = errors.Errorf("vectortable: invalid meta file:%s size:%d offset:%d", metaFile, d.Meta.filesz, d.Header.metaTail.GetOffset())
		return nil, err
	}

	if exManage {
		d.emOffset = uint64(sizeMetaHeaderVer + sizeMetaReuseSlot*len(VerMap[metaVer])*2)
		if typeNew {
			off, err := d.Meta.allocSize(uint64(sizeExpireManager))
			if err != nil {
				err = errors.Errorf("vectortable: meta file alloc EX space err:%s", err)
				return nil, err
			}
			if d.emOffset != off {
				return nil, errors.Errorf("vectortable: meta file alloc EX space offser err: expect %d, actual %d", d.emOffset, off)
			}
			d.em = newExpireManager(now)
		} else {
			d.unmarshalExpireManager(now)
		}
	}

	d.KDataM, err = openTableMulti(kDataFile, &d.Header.kDataTail, d.Header.kFileIdx, opts.Logger, opts.WriteBuffer)
	if err != nil {
		err = errors.Wrapf(err, "vectortable: openTable OpenFile fail file:%s", kDataFile)
		return nil, err
	}

	d.VDataM, err = openTableMulti(vDataFile, &d.Header.vDataTail, d.Header.vFileIdx, opts.Logger, opts.WriteBuffer)
	if err != nil {
		err = errors.Wrapf(err, "vectortable: openTable OpenFile fail file:%s", vDataFile)
		return nil, err
	}

	return &d, nil
}

func (dh *dataHolderSlot) setKKVMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64, slot uint16, version uint64, size uint32) (metaOffset uint64, err error) {
	kLen := len(k)
	kSize := kLen + 2
	var ksBuf [2]byte
	binary.LittleEndian.PutUint16(ksBuf[:], uint16(kLen))
	kfi, kOffset, _, err := dh.KDataM.WriteMulti(uint64(kSize), ksBuf[:], k)
	if kfi > dh.Header.kFileIdx {
		dh.Header.kFileIdx = kfi
	}
	if err != nil {
		return 0, err
	}

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeStKKVWithKey)
	if err != nil {
		return 0, err
	}
	hashHH := uint32(hi >> 32)
	tsAndDT := combine(timestamp, dataType)
	MarshalMetaStKKVWithKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot, version, size, kfi, kOffset)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(kSize))
	}

	return
}

func (dh *dataHolderSlot) setKKVListMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64, slot uint16, version uint64, size uint32, pre, next uint64) (metaOffset uint64, err error) {
	kLen := len(k)
	kSize := kLen + 2
	var ksBuf [2]byte
	binary.LittleEndian.PutUint16(ksBuf[:], uint16(kLen))
	kfi, kOffset, _, err := dh.KDataM.WriteMulti(uint64(kSize), ksBuf[:], k)
	if kfi > dh.Header.kFileIdx {
		dh.Header.kFileIdx = kfi
	}
	if err != nil {
		return 0, err
	}

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeStListWithKey)
	if err != nil {
		return 0, err
	}
	hashHH := uint32(hi >> 32)
	tsAndDT := combine(timestamp, dataType)
	MarshalMetaStListWithKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot, version, size, kfi, kOffset, pre, next)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(kSize))
	}

	return
}

func (dh *dataHolderSlot) set(hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (metaOffset uint64, err error) {
	kLen := len(k)
	kSize := kLen + 2
	var ksBuf [2]byte
	binary.LittleEndian.PutUint16(ksBuf[:], uint16(kLen))
	kfi, kOffset, _, err := dh.KDataM.WriteMulti(uint64(kSize), ksBuf[:], k)
	if kfi > dh.Header.kFileIdx {
		dh.Header.kFileIdx = kfi
	}
	if err != nil {
		return 0, errors.Wrapf(err, "vectortable: write kdata fail")
	}

	var needCompress uint32
	tsAndDT := combine(timestamp, dataType)
	v, comp := dh.compressor.EncodeOptimal(dh.compBuf, inV)
	if comp {
		needCompress = 1
		if cap(v) > cap(dh.compBuf) {
			dh.compBuf = v[:cap(v)]
		}
	}

	vfi, vOffset, _, err := dh.VDataM.Write(v)
	if vfi > dh.Header.vFileIdx {
		dh.Header.vFileIdx = vfi
	}
	if err != nil {
		return 0, errors.Wrapf(err, "vectortable: write vdata fail")
	}

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeStStrWithKey)
	if err != nil {
		return 0, err
	}

	hashHH := uint32(hi >> 32)
	lv := uint32(len(v))
	vinfo := NewVInfo(lv, needCompress)
	MarshalMetaStKV(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, vinfo, slot, vfi, kfi, vOffset, kOffset)
	dh.IncrExpire(timestamp, uint64(kSize)+uint64(lv))
	return
}

func (dh *dataHolderSlot) getKV(metaOffset uint64) (
	hashL uint64, hashHH uint32,
	key []byte, value []byte, seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64, size uint32,
	pre, next uint64, kCloser func(), vCloser func(), err error,
) {
	defer func() {
		if err != nil {
			if kCloser != nil {
				kCloser()
			}
			if vCloser != nil {
				vCloser()
			}
		}
	}()

	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStVInfo:]))
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+8:])
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStSlot:])
		kOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStKOffsetS:])
		kfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStKFIdx:])
		vfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStVFIdx:])
		dh.Meta.dataLock.RUnlock()

		vSize, comp := ivInfo.Info()
		value, vCloser = bytepools.ReaderBytePools.GetBytePool(int(vSize))
		value = value[:vSize]
		_, err = dh.VDataM.ReadAt(vfi, value, vOffset)
		if err != nil {
			err = errors.Wrapf(err, "vectortable: read vdata fail")
			return
		}

		if comp {
			value, err = dh.compressor.Decode(nil, value)
			vCloser()
			vCloser = nil
			if err != nil {
				err = errors.Wrapf(err, "vectortable: decode fail")
				return
			}
		}

		var ksBytes [2]byte
		_, err = dh.KDataM.ReadAt(kfi, ksBytes[:], kOffset)
		if err != nil {
			err = errors.Wrapf(err, "vectortable: read kdata fail")
			return
		}
		kSize := binary.LittleEndian.Uint16(ksBytes[0:2])
		key, kCloser = bytepools.ReaderBytePools.GetBytePool(int(kSize))
		key = key[:kSize]
		_, err = dh.KDataM.ReadAt(kfi, key, kOffset+2)
		return
	} else {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStKKVSlot:])
		version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStVersion:])
		size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStSize:])
		var offsetMetaKOffset uint64
		switch dataType {
		case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
			offsetMetaKOffset = offsetMetaStKOffsetK
		case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			offsetMetaKOffset = offsetMetaStKOffsetL
			pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStListPre:])
			next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStListNext:])
		default:
			dh.Meta.dataLock.RUnlock()
			err = errors.Errorf("set unsupported datatype %d", dataType)
			return
		}
		kOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaKOffset:])
		kfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStKKVKFIdx:])
		dh.Meta.dataLock.RUnlock()

		var ksBytes [2]byte
		_, err = dh.KDataM.ReadAt(kfi, ksBytes[:], kOffset)
		if err != nil {
			err = errors.Wrapf(err, "vectortable: read kdata fail")
			return
		}
		kSize := binary.LittleEndian.Uint16(ksBytes[0:2])
		key, kCloser = bytepools.ReaderBytePools.GetBytePool(int(kSize))
		key = key[:kSize]
		_, err = dh.KDataM.ReadAt(kfi, key, kOffset+2)
		return
	}
}

func (dh *dataHolderSlot) getKVCopy(metaOffset uint64) (
	key, value []byte, seqNum uint64, dataType uint8, timestamp uint64,
	slot uint16, version uint64, size uint32, pre, next uint64, err error,
) {
	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStVInfo:]))
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStSlot:])
		kOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStKOffsetS:])
		vfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStVFIdx:])
		kfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStKFIdx:])
		dh.Meta.dataLock.RUnlock()

		vsize, comp := ivInfo.Info()
		value = make([]byte, vsize)
		_, err = dh.VDataM.ReadAt(vfi, value, vOffset)
		if err != nil {
			err = errors.Wrapf(err, "vectortable: read vdata fail")
			return
		}
		if comp {
			value, err = dh.compressor.Decode(nil, value)
			if err != nil {
				err = errors.Wrapf(err, "vectortable: decode fail")
				return
			}
		}

		ksBytes := make([]byte, 2)
		_, err = dh.KDataM.ReadAt(kfi, ksBytes, kOffset)
		if err != nil {
			err = errors.Wrapf(err, "vectortable: read kdata fail")
			return
		}
		kSize := binary.LittleEndian.Uint16(ksBytes[0:2])
		key = make([]byte, kSize)
		_, err = dh.KDataM.ReadAt(kfi, key, kOffset+2)
		return
	} else {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStKKVSlot:])
		version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStVersion:])
		size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStSize:])
		var offsetMetaKOffset uint64
		switch dataType {
		case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
			offsetMetaKOffset = offsetMetaStKOffsetK
		case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			offsetMetaKOffset = offsetMetaStKOffsetL
			pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStListPre:])
			next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStListNext:])
		default:
			dh.Meta.dataLock.RUnlock()
			err = errors.Errorf("set unsupported datatype %d", dataType)
			return
		}
		kOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaKOffset:])
		kfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStKKVKFIdx:])
		dh.Meta.dataLock.RUnlock()

		ksBytes := make([]byte, 2)
		_, err = dh.KDataM.ReadAt(kfi, ksBytes, kOffset)
		if err != nil {
			err = errors.Wrapf(err, "vectortable: read kdata fail")
			return
		}
		kSize := binary.LittleEndian.Uint16(ksBytes[0:2])
		key = make([]byte, kSize)
		_, err = dh.KDataM.ReadAt(kfi, key, kOffset+2)
		return
	}
}

func (dh *dataHolderSlot) getValue(metaOffset uint64) (
	bs []byte, seqNum uint64, dt uint8, ts uint64,
	slot uint16, c func(), err error,
) {
	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStVInfo:]))
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ttl := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	fIdx := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStVFIdx:])
	dh.Meta.dataLock.RUnlock()

	ts, dt = split(ttl)
	if dt == kkv.DataTypeString {
		vSize, comp := ivInfo.Info()
		bs, c = bytepools.ReaderBytePools.GetBytePool(int(vSize))
		bs = bs[:vSize]
		_, err = dh.VDataM.ReadAt(fIdx, bs, vOffset)
		if err != nil {
			c()
			c = nil
			return nil, 0, 0, 0, 0, nil, err
		}
		if comp {
			bs, err = dh.compressor.Decode(nil, bs)
			c()
			c = nil
			if err != nil {
				return nil, 0, 0, 0, 0, nil, err
			}
		}
	}
	return
}

func (dh *dataHolderSlot) getMeta(metaOffset uint64) (
	seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64,
	size uint32, pre, next uint64, err error,
) {
	dh.Meta.dataLock.RLock()
	defer dh.Meta.dataLock.RUnlock()

	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStSlot:])
		return
	}
	slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStKKVSlot:])
	version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStVersion:])
	size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStSize:])
	if kkv.IsDataTypeList(dataType) {
		pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStListPre:])
		next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStListNext:])
	}
	return
}

func (dh *dataHolderSlot) del(itemOffset uint64) {
	vinfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[itemOffset+offsetMetaNkVInfo:]))
	s, _ := vinfo.Info()
	dt := splitHigh8(binary.LittleEndian.Uint64(dh.Meta.data[itemOffset+offsetMetaTimestamp:]))
	if dt == kkv.DataTypeString {
		dh.Header.delVCap += uint64(CapForShortValue(s))
		dh.Meta.ReuseFree(itemOffset, metaSizeStStrWithKey)
	} else if kkv.IsDataTypeList(dt) {
		dh.Meta.ReuseFree(itemOffset, metaSizeStListWithKey)
	} else {
		dh.Meta.ReuseFree(itemOffset, metaSizeStKKVWithKey)
	}
}

func (dh *dataHolderSlot) update(mOffset uint64, inV []byte, dataType uint8, timestamp, seqNum uint64) (err error) {
	var needCompress uint32
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[mOffset+offsetMetaStVInfo:]))
	v, comp := dh.compressor.EncodeOptimal(dh.compBuf, inV)
	if comp {
		needCompress = 1
		if cap(v) > cap(dh.compBuf) {
			dh.compBuf = v[:cap(v)]
		}
	}

	s, _ := ivInfo.Info()
	dh.Header.delVCap += uint64(CapForShortValue(s))
	vfi, vOffset, _, err := dh.VDataM.Write(v)
	if vfi > dh.Header.vFileIdx {
		dh.Header.vFileIdx = vfi
	}
	if err != nil {
		return errors.Wrapf(err, "vectortable: write vdata fail")
	}

	lv := uint32(len(v))
	vinfo := NewVInfo(lv, needCompress)
	tsAndDT := combine(timestamp, dataType)
	buildUpdateMeta4MultiWithSlot(dh.Meta.data[mOffset:], tsAndDT, seqNum, vinfo, vOffset, vfi)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(lv))
	}
	return
}

func (dh *dataHolderSlot) updateKKVMeta(
	mOffset uint64, dataType uint8,
	timestamp, seqNum, version uint64, size uint32,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaStVersion:], version)
	binary.LittleEndian.PutUint32(dh.Meta.data[mOffset+offsetMetaStSize:], size)
}

func (dh *dataHolderSlot) updateKKVListMeta(
	mOffset uint64, dataType uint8,
	timestamp, seqNum, version uint64,
	size uint32, pre, next uint64,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaStVersion:], version)
	binary.LittleEndian.PutUint32(dh.Meta.data[mOffset+offsetMetaStSize:], size)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaStListPre:], pre)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaStListNext:], next)
}

func (dh *dataHolderSlot) delDebug(itemOffset uint64, c chan int) {
	dh.del(itemOffset)
}

func (dh *dataHolderSlot) setDebug(hi, lo uint64, k, inV []byte, dataType uint8,
	timestamp, seqNum uint64, slot uint16, c chan int) (metaOffset uint64, err error) {
	return dh.set(hi, lo, k, inV, dataType, timestamp, seqNum, slot)
}
