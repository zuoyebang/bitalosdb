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
	offsetMetaNkVInfo    = 28
	offsetMetaNkVFIdx    = 32
	offsetMetaNkVOffset  = 36
	metaSizeNkStr        = 40
	offsetMetaNkVersion  = 28
	offsetMetaNkSize     = 36
	metaSizeNkKKV        = 40
	offsetMetaNkListPre  = 40
	offsetMetaNkListNext = 48
	metaSizeNkList       = 56
)

type dataHolderNoKey struct {
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

func NewDataHolderNokey(opts *options.VectorTableOptions, idxSeq, dataSeq, groups uint32, now uint64) (*dataHolderNoKey, error) {
	var err error
RETRY:
	indexFile, metaFile, _, vDataFile := buildVTFileName(opts.Dirname, opts.Filename, idxSeq, dataSeq)
	d := dataHolderNoKey{
		Header: &header{
			idxSeq:  idxSeq,
			dataSeq: dataSeq,
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
			err = errors.Errorf("vectortable: alloc header space fail err:%s", err)
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
				err = errors.Errorf("vectortable: alloc header space fail err:%s", err)
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

func (dh *dataHolderNoKey) GetHeader() *header {
	return dh.Header
}

func (dh *dataHolderNoKey) SyncHeader() {
	dh.Header.Marshal(dh.Index.data)
}

func (dh *dataHolderNoKey) GetIndex() *tableDesignated {
	return dh.Index
}

func (dh *dataHolderNoKey) GetMeta() *tableExtend {
	return dh.Meta
}

func (dh *dataHolderNoKey) GetKDataM() *tableMulti {
	return dh.KDataM
}

func (dh *dataHolderNoKey) GetVDataM() *tableMulti {
	return dh.VDataM
}

func (dh *dataHolderNoKey) GetEM() *expireManager {
	return dh.em
}

func (dh *dataHolderNoKey) marshalExpireManager() {
	if dh.em != nil {
		bs := dh.Meta.data[dh.emOffset:]
		binary.LittleEndian.PutUint64(bs[0:8], dh.em.startTime)
		binary.LittleEndian.PutUint64(bs[8:16], dh.em.minExTime)
		for i := range dh.em.list {
			binary.LittleEndian.PutUint64(bs[16+(i<<3):], dh.em.list[i])
		}
	}
}

func (dh *dataHolderNoKey) unmarshalExpireManager(now uint64) {
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

func (dh *dataHolderNoKey) SwapHeader(h *header) {
	dh.Header = h
	dh.SyncHeader()
	dh.Index.updateOffset(&dh.Header.indexTail)
	dh.Meta.updateOffset(&dh.Header.metaTail)
	if dh.KDataM != nil {
		dh.KDataM.updateOffset(&dh.Header.kDataTail)
	}
	dh.VDataM.updateOffset(&dh.Header.vDataTail)
}

func (dh *dataHolderNoKey) SwapIndex(i *tableDesignated) {
	dh.Index = i
}

func (dh *dataHolderNoKey) GetStat() uint8 {
	return dh.Header.tags.getStat()
}

func (dh *dataHolderNoKey) SetStat(stat uint8) {
	dh.Header.tags.setStat(stat)
}

func (dh *dataHolderNoKey) GetExpireCap(now uint64) uint64 {
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

func (dh *dataHolderNoKey) IncrExpire(expire, size uint64) {
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

func (dh *dataHolderNoKey) MergeExpired(now uint64) {
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

func (dh *dataHolderNoKey) ClearExpired(now uint64) {
	if dh.Header.tags.isExpireManage() {
		last := time2offset(now, dh.em.startTime)
		for j := uint32(0); j < last; j++ {
			dh.em.clear(j)
		}
		dh.em.startTime = time2startTime(now)
		dh.em.zeroOffset = time2idx(now)
	}
}

func (dh *dataHolderNoKey) Close() error {
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

	return nil
}

func (dh *dataHolderNoKey) setKKVMeta(
	hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64,
	slot uint16, version uint64, size uint32,
) (metaOffset uint64, err error) {
	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeNkKKV)
	if err != nil {
		return 0, err
	}

	tsAndDT := combine(timestamp, dataType)
	hashHH := uint32(hi >> 32)
	MarshalMetaKKVNoKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, version, size)
	return
}

func (dh *dataHolderNoKey) setKKVListMeta(
	hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64,
	slot uint16, version uint64, size uint32, pre, next uint64,
) (metaOffset uint64, err error) {
	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeNkList)
	if err != nil {
		return 0, err
	}

	hashHH := uint32(hi >> 32)
	tsAndDT := combine(timestamp, dataType)
	MarshalMetaKKVNoKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, version, size)
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaNkListPre:], pre)
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaNkListNext:], next)
	return
}

func (dh *dataHolderNoKey) set(
	hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16,
) (metaOffset uint64, err error) {
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

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeNkStr)
	if err != nil {
		return 0, err
	}

	hashHH := uint32(hi >> 32)
	lv := uint32(len(v))
	vinfo := NewVInfo(lv, needCompress)
	MarshalMetaNoKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, vinfo, vOffset, vfi)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(lv))
	}
	return
}

func (dh *dataHolderNoKey) getValue(metaOffset uint64) (
	bs []byte, seqNum uint64, dt uint8,
	ts uint64, slot uint16, c func(), err error,
) {
	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkVInfo:]))
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ttl := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	fIdx := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaNkVFIdx:])
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

func (dh *dataHolderNoKey) getMeta(metaOffset uint64) (
	seqNum uint64, dataType uint8, timestamp uint64, slot uint16,
	version uint64, size uint32, pre, next uint64, err error,
) {
	dh.Meta.dataLock.RLock()
	defer dh.Meta.dataLock.RUnlock()

	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		return
	}
	version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkVersion:])
	size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkSize:])
	if kkv.IsDataTypeList(dataType) {
		pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkListPre:])
		next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkListNext:])
	}
	return
}

func (dh *dataHolderNoKey) getKV(metaOffset uint64) (
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
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkVInfo:]))
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+8:])
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		fIdx := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaNkVFIdx:])
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
		version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkVersion:])
		size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkSize:])
		if kkv.IsDataTypeList(dataType) {
			pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkListPre:])
			next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkListNext:])
		}
		dh.Meta.dataLock.RUnlock()
		return
	}
}

func (dh *dataHolderNoKey) getKVCopy(metaOffset uint64) (
	key, value []byte, seqNum uint64, dataType uint8,
	timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, err error,
) {
	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkVInfo:]))
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		fIdx := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaNkVFIdx:])
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
		version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkVersion:])
		size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaNkSize:])
		if kkv.IsDataTypeList(dataType) {
			pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkListPre:])
			next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkListNext:])
		}
		dh.Meta.dataLock.RUnlock()
		return
	}
}

func (dh *dataHolderNoKey) update(mOffset uint64, inV []byte, dataType uint8, timestamp, seqNum uint64) (err error) {
	var needCompress uint32

	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[mOffset+offsetMetaNkVInfo:]))
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
	buildUpdateMeta4MultiNoKey(dh.Meta.data[mOffset:], tsAndDT, seqNum, vinfo, vOffset, vfi)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(lv))
	}
	return
}

func (dh *dataHolderNoKey) updateKKVMeta(
	mOffset uint64, dataType uint8,
	timestamp, seqNum, version uint64, size uint32,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaNkVersion:], version)
	binary.LittleEndian.PutUint32(dh.Meta.data[mOffset+offsetMetaNkSize:], size)
}

func (dh *dataHolderNoKey) updateKKVListMeta(
	mOffset uint64, dataType uint8,
	timestamp, seqNum, version uint64,
	size uint32, pre, next uint64,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaNkVersion:], version)
	binary.LittleEndian.PutUint32(dh.Meta.data[mOffset+offsetMetaNkSize:], size)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaNkListPre:], pre)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaNkListNext:], next)
}

func (dh *dataHolderNoKey) getHashL(metaOffset uint64) (hashL uint64) {
	dh.Meta.dataLock.RLock()
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	dh.Meta.dataLock.RUnlock()
	return
}

func (dh *dataHolderNoKey) getHashInfo(metaOffset uint64) (hashL uint64, hashHH uint32) {
	dh.Meta.dataLock.RLock()
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaHashHH:])
	dh.Meta.dataLock.RUnlock()
	return
}

func (dh *dataHolderNoKey) getHashAndVersionDT(metaOffset uint64) (hashL uint64, hashHH uint32, ver uint64, dt uint8) {
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaHashHH:])
	tsAndDT := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	dt = splitHigh8(tsAndDT)
	if dt != kkv.DataTypeString {
		ver = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaNkVersion:])
	}
	return
}

func (dh *dataHolderNoKey) checkSeq(metaOffset uint64, seqNum uint64) bool {
	oldSeq := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	return seqNum <= oldSeq
}

// TODO check every del
func (dh *dataHolderNoKey) del(itemOffset uint64) {
	vinfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[itemOffset+offsetMetaNkVInfo:]))
	s, _ := vinfo.Info()
	dt := splitHigh8(binary.LittleEndian.Uint64(dh.Meta.data[itemOffset+offsetMetaTimestamp:]))
	if dt == kkv.DataTypeString {
		dh.Header.delVCap += uint64(CapForShortValue(s))
		dh.Meta.ReuseFree(itemOffset, metaSizeNkStr)
	} else if kkv.IsDataTypeList(dt) {
		dh.Meta.ReuseFree(itemOffset, metaSizeNkList)
	} else {
		dh.Meta.ReuseFree(itemOffset, metaSizeNkKKV)
	}
}

func (dh *dataHolderNoKey) getSeqNum(metaOffset uint64) (seqNum uint64) {
	dh.Meta.dataLock.RLock()
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	dh.Meta.dataLock.RUnlock()
	return
}

func (dh *dataHolderNoKey) getTTL(metaOffset uint64) (seqNum, ts uint64, dt uint8) {
	dh.Meta.dataLock.RLock()
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	t := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	dh.Meta.dataLock.RUnlock()
	ts, dt = split(t)
	return
}

func (dh *dataHolderNoKey) setTTL(metaOffset uint64, seqNum, ttl uint64) {
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:], ttl)
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:], seqNum)
}

func (dh *dataHolderNoKey) setSize(metaOffset uint64, seqNum uint64, size uint32) {
	ottl := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	if uint8(ottl>>56) == kkv.DataTypeString {
		return
	}
	binary.LittleEndian.PutUint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(dh.Meta.data[metaOffset+offsetMetaNkSize:], size)
}

func (dh *dataHolderNoKey) sync() error {
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

func (dh *dataHolderNoKey) syncBuffer() error {
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

func MarshalMetaNoKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32, fIdx uint16) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaNkVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint32(buf[offsetMetaNkVOffset:], vOffset)
	binary.LittleEndian.PutUint16(buf[offsetMetaNkVFIdx:], fIdx)
}

func MarshalMetaKKVNoKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, ver uint64, size uint32) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(buf[offsetMetaNkVersion:], ver)
	binary.LittleEndian.PutUint32(buf[offsetMetaNkSize:], size)
}

//go:inline
func buildUpdateMeta4MultiNoKey(buf []byte, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32, fIdx uint16) {
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaNkVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint32(buf[offsetMetaNkVOffset:], vOffset)
	binary.LittleEndian.PutUint16(buf[offsetMetaNkVFIdx:], fIdx)
}

func (dh *dataHolderNoKey) delDebug(itemOffset uint64, c chan int) {
	dh.del(itemOffset)
}

func (dh *dataHolderNoKey) setDebug(hi, lo uint64, k, inV []byte, dataType uint8,
	timestamp, seqNum uint64, slot uint16, c chan int) (metaOffset uint64, err error) {
	return dh.set(hi, lo, k, inV, dataType, timestamp, seqNum, slot)
}
