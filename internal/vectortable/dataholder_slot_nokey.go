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

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

const (
	offsetMetaStNkVInfo    = 28
	offsetMetaStNkSlot     = 32
	offsetMetaStNkVFIdx    = 34
	offsetMetaStNkVOffset  = 36
	metaSizeStNkStr        = 40
	offsetMetaStNkSlotKKV  = 28
	offsetMetaStNkVersion  = 32
	offsetMetaStNkSize     = 40
	metaSizeStNkKKV        = 44
	offsetMetaStNkListPre  = 44
	offsetMetaStNkListNext = 52
	metaSizeStNkList       = 60
)

var _ DataHolder = &dataHolderSlotNoKey{}

//go:inline
func buildUpdateMeta4MultiStNk(buf []byte, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32, fIdx uint16) {
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaStNkVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint16(buf[offsetMetaStNkVFIdx:], fIdx)
	binary.LittleEndian.PutUint32(buf[offsetMetaStNkVOffset:], vOffset)
}

type dataHolderSlotNoKey struct {
	Header     *header
	Index      *tableDesignated
	Meta       *tableExtend
	KDataM     *tableMulti
	VDataM     *tableMulti
	em         *expireManager
	emOffset   uint64
	compressor compress.Compressor
	compBuf    []byte
	logger     base.Logger
}

func NewdataHolderSlotNoKey(opts *options.VectorTableOptions, idxSeq, dataSeq, groups uint32, now uint64) (*dataHolderSlotNoKey, error) {
	var err error
RETRY:
	indexFile, metaFile, _, vDataFile := buildVTFileName(opts.Dirname, opts.Filename, idxSeq, dataSeq)
	d := dataHolderSlotNoKey{
		Header: &header{
			idxSeq:   idxSeq,
			dataSeq:  dataSeq,
			groupNum: groups,
		},
		compressor: opts.Compressor,
		compBuf:    make([]byte, 1024),
		logger:     opts.Logger,
	}
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
				_ = d.Index.close()
			}
			if d.Meta != nil {
				_ = d.Meta.close()
			}
			if d.KDataM != nil {
				_ = d.KDataM.close()
			}
			if d.VDataM != nil {
				_ = d.VDataM.close()
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

	if exManage {
		d.emOffset = uint64(sizeMetaHeaderVer + sizeMetaReuseSlot*len(VerMap[metaVer])*2)
		if typeNew {
			off, err := d.Meta.allocSize(uint64(sizeExpireManager))
			if err != nil {
				err = errors.Errorf("vectortable: meta file alloc EX space err:%s", err)
				return nil, err
			}
			if d.emOffset != off {
				return nil, errors.Errorf("vectortable: meta file alloc EX space offser err: expect %d, actual %d",
					d.emOffset, off)
			}
			d.em = newExpireManager(now)
		} else {
			d.unmarshalExpireManager(now)
		}
	}

	if uint64(d.Meta.filesz) < d.Header.metaTail.GetOffset() {
		err = errors.Errorf("vectortable: invalid meta file:%s size:%d offset:%d", metaFile, d.Meta.filesz, d.Header.metaTail.GetOffset())
		return nil, err
	}

	d.VDataM, err = openTableMulti(vDataFile, &d.Header.vDataTail, d.Header.vFileIdx, opts.Logger, opts.WriteBuffer)
	if err != nil {
		err = errors.Wrapf(err, "vectortable: openTable OpenFile fail file:%s", vDataFile)
		return nil, err
	}

	return &d, nil
}

func (dh *dataHolderSlotNoKey) GetHeader() *header {
	return dh.Header
}
func (dh *dataHolderSlotNoKey) SyncHeader() {
	dh.Header.Marshal(dh.Index.data)
}
func (dh *dataHolderSlotNoKey) GetIndex() *tableDesignated {
	return dh.Index
}
func (dh *dataHolderSlotNoKey) GetMeta() *tableExtend {
	return dh.Meta
}
func (dh *dataHolderSlotNoKey) GetKDataM() *tableMulti {
	return dh.KDataM
}
func (dh *dataHolderSlotNoKey) GetVDataM() *tableMulti { return dh.VDataM }
func (dh *dataHolderSlotNoKey) GetEM() *expireManager {
	return dh.em
}

func (dh *dataHolderSlotNoKey) marshalExpireManager() {
	if dh.em != nil {
		bs := dh.Meta.data[dh.emOffset:]
		binary.LittleEndian.PutUint64(bs[0:8], dh.em.startTime)
		binary.LittleEndian.PutUint64(bs[8:16], dh.em.minExTime)
		for i := range dh.em.list {
			binary.LittleEndian.PutUint64(bs[16+(i<<3):], dh.em.list[i])
		}
	}
}

func (dh *dataHolderSlotNoKey) unmarshalExpireManager(now uint64) {
	if dh.em == nil {
		dh.em = newExpireManager(now)
	}
	bs := dh.Meta.data[dh.emOffset:]
	dh.em.startTime = binary.LittleEndian.Uint64(bs[0:8])
	dh.em.zeroOffset = time2idx(dh.em.startTime)
	dh.em.minExTime = binary.LittleEndian.Uint64(bs[8:16])
	for i := range dh.em.list {
		dh.em.list[i] = binary.LittleEndian.Uint64(bs[16+i<<3:])
	}
}

func (dh *dataHolderSlotNoKey) SwapHeader(h *header) {
	dh.Header = h
	dh.SyncHeader()
	dh.Index.updateOffset(&dh.Header.indexTail)
	dh.Meta.updateOffset(&dh.Header.metaTail)
	if dh.KDataM != nil {
		dh.KDataM.updateOffset(&dh.Header.kDataTail)
	}
	dh.VDataM.updateOffset(&dh.Header.vDataTail)
}

func (dh *dataHolderSlotNoKey) SwapIndex(i *tableDesignated) {
	dh.Index = i
}

func (dh *dataHolderSlotNoKey) GetStat() uint8 {
	return dh.Header.tags.getStat()
}

func (dh *dataHolderSlotNoKey) SetStat(stat uint8) {
	dh.Header.tags.setStat(stat)
}

func (dh *dataHolderSlotNoKey) GetExpireCap(now uint64) uint64 {
	if dh.Header.tags.isExpireManage() {
		var exCap = dh.em.list[expireManagerPeriods+1]
		off := time2offset(now, dh.em.startTime)
		for j := uint32(0); j < off; j++ {
			exCap += dh.em.get(j)
		}
		if dh.em.minExTime != 0 && dh.em.minExTime < now-uint64(expireManagerDurationMS) {
			exCap += dh.em.list[expireManagerPeriods]
		}
		return exCap
	}
	return 0
}

func (dh *dataHolderSlotNoKey) IncrExpire(expire, size uint64) {
	if expire == 0 {
		return
	}
	if dh.Header.tags.isExpireManage() {
		if expire < dh.em.startTime {
			dh.em.list[expireManagerPeriods+1] += size // expired
		}

		off := time2offset(expire, dh.em.startTime)
		dh.em.incr(off, size)
		if off == expireManagerPeriods && (dh.em.minExTime == 0 || expire < dh.em.minExTime) {
			dh.em.minExTime = expire
		}
		dh.em.writeCnt++
		if dh.em.writeCnt > expireManagerFlushCycle {
			dh.marshalExpireManager()
			dh.em.writeCnt = 0
		}
	}
}

func (dh *dataHolderSlotNoKey) MergeExpired(now uint64) {
	if dh.Header.tags.isExpireManage() {
		var exCap uint64
		last := time2offset(now, dh.em.startTime)
		for j := uint32(0); j < last; j++ {
			exCap += dh.em.get(j)
			dh.em.clear(j)
		}
		if last > 0 {
			dh.em.list[expireManagerPeriods+1] += exCap
			dh.em.startTime = time2startTime(now)
			dh.em.zeroOffset = time2idx(dh.em.startTime)
		}
	}
}

func (dh *dataHolderSlotNoKey) ClearExpired(now uint64) {
	if dh.Header.tags.isExpireManage() {
		last := time2offset(now, dh.em.startTime)
		for j := uint32(0); j < last; j++ {
			dh.em.clear(j)
		}
		dh.em.startTime = time2startTime(now)
		dh.em.zeroOffset = time2idx(now)
	}
}

func (dh *dataHolderSlotNoKey) Close() error {
	dh.Header.Marshal(dh.Index.data[0:headerSize])
	if dh.em != nil {
		dh.marshalExpireManager()
	}
	if dh.Index != nil {
		err := dh.Index.close()
		if err != nil {
			return errors.Errorf("vectortable: close index file fail: %s", err)
		}
	}
	if dh.Meta != nil {
		err := dh.Meta.close()
		if err != nil {
			return errors.Errorf("vectortable: close meta file fail: %s", err)
		}
	}
	if dh.KDataM != nil {
		err := dh.KDataM.close()
		if err != nil {
			return errors.Errorf("vectortable: close kdata file fail: %s", err)
		}
	}
	if dh.VDataM != nil {
		err := dh.VDataM.close()
		if err != nil {
			return errors.Errorf("vectortable: close vdata file fail: %s", err)
		}
	}

	//dh.wBuf = nil
	return nil
}

func (dh *dataHolderSlotNoKey) setKKVMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64, slot uint16, version uint64, size uint32) (metaOffset uint64, err error) {
	tsAndDT := combine(timestamp, dataType)
	hashHH := uint32(hi >> 32)
	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeStNkKKV)
	if err != nil {
		return 0, err
	}
	MarshalMetaStKKVNoKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot, version, size)
	return
}

func (dh *dataHolderSlotNoKey) setKKVListMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64, slot uint16, version uint64, size uint32, pre, next uint64) (metaOffset uint64, err error) {
	tsAndDT := combine(timestamp, dataType)

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeStNkList)
	if err != nil {
		return 0, err
	}

	hashHH := uint32(hi >> 32)
	MarshalMetaStKKVNoKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot, version, size)
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaStNkListPre:], pre)
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaStNkListNext:], next)
	return
}

func (dh *dataHolderSlotNoKey) set(hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (metaOffset uint64, err error) {
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

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeStNkStr)
	if err != nil {
		return 0, err
	}

	hashHH := uint32(hi >> 32)
	lv := uint32(len(v))
	vinfo := NewVInfo(lv, needCompress)
	MarshalMetaStNoKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, vinfo, slot, vOffset, vfi)
	dh.IncrExpire(timestamp, uint64(lv))
	return
}

func (dh *dataHolderSlotNoKey) getValue(metaOffset uint64) (
	bs []byte, seqNum uint64, dt uint8, ts uint64,
	slot uint16, c func(), err error,
) {
	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkVInfo:]))
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ttl := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	fIdx := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkVFIdx:])
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

func (dh *dataHolderSlotNoKey) getMeta(metaOffset uint64) (
	seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64,
	size uint32, pre, next uint64, err error,
) {
	dh.Meta.dataLock.RLock()
	defer dh.Meta.dataLock.RUnlock()

	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkSlot:])
		return
	}
	slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkSlotKKV:])
	version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkVersion:])
	size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkSize:])
	if kkv.IsDataTypeList(dataType) {
		pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkListPre:])
		next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkListNext:])
	}
	return
}

func (dh *dataHolderSlotNoKey) getKV(metaOffset uint64) (
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
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkVInfo:]))
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+8:])
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkSlot:])
		fIdx := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkVFIdx:])
		dh.Meta.dataLock.RUnlock()

		vSize, comp := ivInfo.Info()
		value, vCloser = bytepools.ReaderBytePools.GetBytePool(int(vSize))
		value = value[:vSize]
		_, err = dh.VDataM.ReadAt(fIdx, value, vOffset)
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
		return
	} else {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkSlotKKV:])
		version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkVersion:])
		size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkSize:])
		if kkv.IsDataTypeList(dataType) {
			pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkListPre:])
			next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkListNext:])
		}
		dh.Meta.dataLock.RUnlock()
		return
	}
}

func (dh *dataHolderSlotNoKey) getKVCopy(metaOffset uint64) (
	key, value []byte, seqNum uint64, dataType uint8,
	timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, err error,
) {
	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkVOffset:])
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkSlot:])
		ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkVInfo:]))
		fIdx := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkVFIdx:])
		dh.Meta.dataLock.RUnlock()

		vsize, comp := ivInfo.Info()
		value = make([]byte, vsize)
		_, err = dh.VDataM.ReadAt(fIdx, value, vOffset)
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
		return
	} else {
		slot = binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaStNkSlotKKV:])
		version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkVersion:])
		size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaStNkSize:])
		if kkv.IsDataTypeList(dataType) {
			pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkListPre:])
			next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkListNext:])
		}
		dh.Meta.dataLock.RUnlock()
		return
	}
}

func (dh *dataHolderSlotNoKey) update(mOffset uint64, inV []byte, dataType uint8, timestamp, seqNum uint64) (err error) {
	var needCompress uint32

	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[mOffset+offsetMetaStNkVInfo:]))
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
	buildUpdateMeta4MultiStNk(dh.Meta.data[mOffset:], tsAndDT, seqNum, vinfo, vOffset, vfi)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(lv))
	}
	return
}

func (dh *dataHolderSlotNoKey) updateKKVMeta(
	mOffset uint64, dataType uint8,
	timestamp, seqNum, version uint64, size uint32,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaStNkVersion:], version)
	binary.LittleEndian.PutUint32(dh.Meta.data[mOffset+offsetMetaStNkSize:], size)
}

func (dh *dataHolderSlotNoKey) updateKKVListMeta(
	mOffset uint64, dataType uint8,
	timestamp, seqNum, version uint64,
	size uint32, pre, next uint64,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaStNkVersion:], version)
	binary.LittleEndian.PutUint32(dh.Meta.data[mOffset+offsetMetaStNkSize:], size)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaStNkListPre:], pre)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaStNkListNext:], next)
}

func (dh *dataHolderSlotNoKey) getHashL(metaOffset uint64) (hashL uint64) {
	dh.Meta.dataLock.RLock()
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	dh.Meta.dataLock.RUnlock()
	return
}

func (dh *dataHolderSlotNoKey) getHashInfo(metaOffset uint64) (hashL uint64, hashHH uint32) {
	dh.Meta.dataLock.RLock()
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaHashHH:])
	dh.Meta.dataLock.RUnlock()
	return
}

func (dh *dataHolderSlotNoKey) getHashAndVersionDT(metaOffset uint64) (hashL uint64, hashHH uint32, ver uint64, dt uint8) {
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaHashHH:])
	tsAndDT := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	dt = splitHigh8(tsAndDT)
	if dt != kkv.DataTypeString {
		ver = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaStNkVersion:])
	}
	return
}

func (dh *dataHolderSlotNoKey) checkSeq(metaOffset uint64, seqNum uint64) bool {
	dh.Meta.dataLock.RLock()
	oldSeq := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	dh.Meta.dataLock.RUnlock()
	return seqNum <= oldSeq
}

func (dh *dataHolderSlotNoKey) del(itemOffset uint64) {
	vinfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[itemOffset+offsetMetaNkVInfo:]))
	s, _ := vinfo.Info()
	dt := splitHigh8(binary.LittleEndian.Uint64(dh.Meta.data[itemOffset+offsetMetaTimestamp:]))
	if dt == kkv.DataTypeString {
		dh.Header.delVCap += uint64(CapForShortValue(s))
		dh.Meta.ReuseFree(itemOffset, metaSizeStNkStr)
	} else if kkv.IsDataTypeList(dt) {
		dh.Meta.ReuseFree(itemOffset, metaSizeStNkList)
	} else {
		dh.Meta.ReuseFree(itemOffset, metaSizeStNkKKV)
	}
}

func (dh *dataHolderSlotNoKey) getSeqNum(metaOffset uint64) (seqNum uint64) {
	dh.Meta.dataLock.RLock()
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	dh.Meta.dataLock.RUnlock()
	return
}

func (dh *dataHolderSlotNoKey) getTTL(metaOffset uint64) (seqNum, ts uint64, dt uint8) {
	dh.Meta.dataLock.RLock()
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	t := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	dh.Meta.dataLock.RUnlock()
	ts, dt = split(t)
	return
}

func (dh *dataHolderSlotNoKey) setTTL(metaOffset uint64, seqNum, ttl uint64) {
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:], ttl)
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:], seqNum)
}

func (dh *dataHolderSlotNoKey) setSize(metaOffset uint64, seqNum uint64, size uint32) {
	ottl := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	if uint8(ottl>>56) == kkv.DataTypeString {
		return
	}
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(dh.Meta.data[metaOffset+offsetMetaStNkSize:], size)
}

func (dh *dataHolderSlotNoKey) sync() error {
	dh.Header.Marshal(dh.Index.data)
	dh.marshalExpireManager()
	if err := dh.syncBuffer(); err != nil {
		return errors.Wrapf(err, "sync buffer failed")
	}
	if err := dh.Meta.msync(); err != nil {
		return errors.Wrapf(err, "sync meta failed")
	}
	if err := dh.Index.msync(); err != nil {
		return errors.Wrapf(err, "sync index failed")
	}
	return nil
}

func (dh *dataHolderSlotNoKey) syncBuffer() error {
	if err := dh.VDataM.Flush(); err != nil {
		return errors.Wrapf(err, "sync vdata failed")
	}
	if dh.KDataM != nil {
		if err := dh.KDataM.Flush(); err != nil {
			return errors.Wrapf(err, "sync kdata failed")
		}
	}
	return nil
}

func MarshalMetaStNoKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, vInfo vInfo, slot uint16, vOffset uint32, fIdx uint16) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaStNkVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint16(buf[offsetMetaStNkSlot:], slot)
	binary.LittleEndian.PutUint32(buf[offsetMetaStNkVOffset:], vOffset)
	binary.LittleEndian.PutUint16(buf[offsetMetaStNkVFIdx:], fIdx)
}

func MarshalMetaStKKVNoKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, slot uint16, ver uint64, size uint32) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(buf[offsetMetaStNkSlotKKV:], slot)
	binary.LittleEndian.PutUint64(buf[offsetMetaStNkVersion:], ver)
	binary.LittleEndian.PutUint32(buf[offsetMetaStNkSize:], size)
}

func (dh *dataHolderSlotNoKey) delDebug(itemOffset uint64, c chan int) {
	dh.del(itemOffset)
}

func (dh *dataHolderSlotNoKey) setDebug(hi, lo uint64, k, inV []byte, dataType uint8,
	timestamp, seqNum uint64, slot uint16, c chan int) (metaOffset uint64, err error) {
	return dh.set(hi, lo, k, inV, dataType, timestamp, seqNum, slot)
}
