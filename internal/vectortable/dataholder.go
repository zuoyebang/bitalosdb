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
	"fmt"
	"strings"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

const (
	curVersion = 1

	offsetHeaderVer       = 0
	offsetHeaderTags      = 2
	offsetHeaderGroupNum  = 4
	offsetHeaderResident  = 8
	offsetHeaderDead      = 12
	offsetHeaderIdxSeq    = 16
	offsetHeaderDataSeq   = 20
	offsetHeaderIndexTail = 24
	offsetHeaderMetaTail  = 32
	offsetHeaderKDataTail = 40
	offsetHeaderVDataTail = 48
	offsetHeaderDelKeys   = 56
	offsetHeaderDelVCap   = 60
	offsetHeaderVFileIdx  = 68
	offsetHeaderKFileIdx  = 70
	headerSize            = 72

	offsetMetaMagic      = 4
	offsetMetaHashL      = 0
	offsetMetaHashHH     = 8
	offsetMetaTimestamp  = 12
	offsetMetaSeqNum     = 20
	offsetMetaMagic0     = 24
	offsetMetaMagic1     = 25
	offsetMetaMagic2     = 26
	offsetMetaMagic3     = 27
	offsetMetaWkVInfo    = 28
	offsetMetaWkVFIdx    = 32
	offsetMetaWkKFIdx    = 34
	offsetMetaWkVOffset  = 36
	offsetMetaWkKOffsetS = 40
	metaSizeStrWithKey   = 44
	// empty vMeta
	offsetMetaWkKKVKFIdx = 30
	offsetMetaWkVersion  = 32
	offsetMetaWkSize     = 40
	offsetMetaWkKOffsetK = 44
	metaSizeKKVWithKey   = 48
	offsetMetaWkListPre  = 44
	offsetMetaWkListNext = 52
	offsetMetaWkKOffsetL = 60
	metaSizeListWithKey  = 64
	// offsetMetaKOffset   = 36 // cal

	maskHigh8 uint64 = 0xff00_0000_0000_0000
	maskLow56 uint64 = 0x00ff_ffff_ffff_ffff

	maxGroups                = 1 << 31
	alignSize         uint64 = 4
	alignDisplacement        = 2
	overShortSize     uint32 = 1 << 5
	maxOffset                = 16 << 30
	maxU32Offset             = 4 << 30
)

const (
	fileExtIndex = "vti"
	fileExtMeta  = "vtm"
	fileExtKData = "vtk"
	fileExtVData = "vtv"
)

const (
	StatNormal uint8 = iota
	StatRehashing
	StatGC
)

const (
	tagMaskExpireManage uint16 = 0x8000
	tagMaskStat         uint16 = 0x00ff
)

var (
	expireManagerDuration          = 2                                         // per n hour
	expireManagerPeriods           = uint32(90 * (24 / expireManagerDuration)) // per 2 hour
	sizeExpireManager              = (expireManagerPeriods+2)<<3 + 16
	expireManagerDurationMS        = expireManagerDuration * 3600 * 1000
	expireManagerFlushCycle uint32 = 1 << 18
)

type Tags uint16

func (t *Tags) setStat(stat uint8) {
	*t = Tags((uint16(*t)>>8)<<8 | uint16(stat))
}

func (t *Tags) getStat() uint8 {
	return uint8(*t)
}

func (t *Tags) setExpireManage(b bool) {
	if b {
		*t = Tags(uint16(*t) | tagMaskExpireManage)
	} else {
		*t = Tags(uint16(*t) &^ tagMaskExpireManage)
	}
}

func (t *Tags) isExpireManage() bool {
	return uint16(*t)&tagMaskExpireManage != 0
}

type header struct {
	ver       uint16
	tags      Tags // 16 bit tag (reserve)
	groupNum  uint32
	resident  uint32
	dead      uint32
	idxSeq    uint32
	dataSeq   uint32
	indexTail OffsetKeeper
	metaTail  OffsetKeeper
	kDataTail OffsetKeeper
	vDataTail OffsetKeeper
	delVCap   uint64
	delKeys   uint32
	vFileIdx  uint16
	kFileIdx  uint16
}

func (h *header) Marshal(buf []byte) {
	binary.LittleEndian.PutUint16(buf[offsetHeaderVer:], h.ver)
	binary.LittleEndian.PutUint16(buf[offsetHeaderTags:], uint16(h.tags))
	binary.LittleEndian.PutUint32(buf[offsetHeaderGroupNum:], h.groupNum)
	binary.LittleEndian.PutUint32(buf[offsetHeaderResident:], h.resident)
	binary.LittleEndian.PutUint32(buf[offsetHeaderDead:], h.dead)
	binary.LittleEndian.PutUint32(buf[offsetHeaderIdxSeq:], h.idxSeq)
	binary.LittleEndian.PutUint32(buf[offsetHeaderDataSeq:], h.dataSeq)
	h.indexTail.Save(buf[offsetHeaderIndexTail:])
	h.metaTail.Save(buf[offsetHeaderMetaTail:])
	h.kDataTail.Save(buf[offsetHeaderKDataTail:])
	h.vDataTail.Save(buf[offsetHeaderVDataTail:])
	binary.LittleEndian.PutUint32(buf[offsetHeaderDelKeys:], h.delKeys)
	binary.LittleEndian.PutUint64(buf[offsetHeaderDelVCap:], h.delVCap)
	binary.LittleEndian.PutUint16(buf[offsetHeaderVFileIdx:], h.vFileIdx)
	binary.LittleEndian.PutUint16(buf[offsetHeaderKFileIdx:], h.kFileIdx)
}

func (h *header) MarshalSign(buf []byte, sign int) {
	binary.LittleEndian.PutUint16(buf[offsetHeaderVer:], h.ver)
	binary.LittleEndian.PutUint16(buf[offsetHeaderTags:], uint16(h.tags))
	binary.LittleEndian.PutUint32(buf[offsetHeaderGroupNum:], h.groupNum)
	binary.LittleEndian.PutUint32(buf[offsetHeaderResident:], h.resident)
	binary.LittleEndian.PutUint32(buf[offsetHeaderDead:], h.dead)
	binary.LittleEndian.PutUint32(buf[offsetHeaderIdxSeq:], h.idxSeq)
	binary.LittleEndian.PutUint32(buf[offsetHeaderDataSeq:], h.dataSeq)
	h.indexTail.Save(buf[offsetHeaderIndexTail:])
	h.metaTail.Save(buf[offsetHeaderMetaTail:])
	h.kDataTail.Save(buf[offsetHeaderKDataTail:])
	h.vDataTail.Save(buf[offsetHeaderVDataTail:])
	binary.LittleEndian.PutUint32(buf[offsetHeaderDelKeys:], h.delKeys)
	binary.LittleEndian.PutUint64(buf[offsetHeaderDelVCap:], h.delVCap)
	binary.LittleEndian.PutUint16(buf[offsetHeaderVFileIdx:], h.vFileIdx)
	binary.LittleEndian.PutUint16(buf[offsetHeaderKFileIdx:], h.kFileIdx)
}

func (h *header) Unmarshal(buf []byte) {
	h.ver = binary.LittleEndian.Uint16(buf[offsetHeaderVer:])
	h.tags = Tags(binary.LittleEndian.Uint16(buf[offsetHeaderTags:]))
	h.groupNum = binary.LittleEndian.Uint32(buf[offsetHeaderGroupNum:])
	h.resident = binary.LittleEndian.Uint32(buf[offsetHeaderResident:])
	h.dead = binary.LittleEndian.Uint32(buf[offsetHeaderDead:])
	h.idxSeq = binary.LittleEndian.Uint32(buf[offsetHeaderIdxSeq:])
	h.dataSeq = binary.LittleEndian.Uint32(buf[offsetHeaderDataSeq:])
	h.indexTail.Load(buf[offsetHeaderIndexTail:])
	h.metaTail.Load(buf[offsetHeaderMetaTail:])
	h.kDataTail.Load(buf[offsetHeaderKDataTail:])
	h.vDataTail.Load(buf[offsetHeaderVDataTail:])
	h.delKeys = binary.LittleEndian.Uint32(buf[offsetHeaderDelKeys:])
	h.delVCap = binary.LittleEndian.Uint64(buf[offsetHeaderDelVCap:])
	h.vFileIdx = binary.LittleEndian.Uint16(buf[offsetHeaderVFileIdx:])
	h.kFileIdx = binary.LittleEndian.Uint16(buf[offsetHeaderKFileIdx:])
}

const (
	vInfoMaskCompress uint32 = 0x8000_0000
	vInfoMaskVSize    uint32 = 0x7FFF_FFFF
)

type vInfo uint32

func NewVInfo(size, compress uint32) vInfo {
	return vInfo(compress<<31 | size)
}

//go:inline
func (v *vInfo) Info() (size uint32, compress bool) {
	vi := uint32(*v)
	size = vi & vInfoMaskVSize
	compress = (vi & vInfoMaskCompress) > 0
	return
}

func MarshalMetaKV(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, vInfo vInfo, slot uint16,
	vfi, kfi uint16, vOffset uint32, kOffset uint32) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaWkVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint16(buf[offsetMetaWkVFIdx:], vfi)
	binary.LittleEndian.PutUint16(buf[offsetMetaWkKFIdx:], kfi)
	binary.LittleEndian.PutUint32(buf[offsetMetaWkVOffset:], vOffset)
	binary.LittleEndian.PutUint32(buf[offsetMetaWkKOffsetS:], kOffset)
}

func MarshalMetaKKVWithKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, slot uint16,
	ver uint64, size uint32, kfi uint16, kOffset uint32) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(buf[offsetMetaWkKKVKFIdx:], kfi)
	binary.LittleEndian.PutUint64(buf[offsetMetaWkVersion:], ver)
	binary.LittleEndian.PutUint32(buf[offsetMetaWkSize:], size)
	binary.LittleEndian.PutUint32(buf[offsetMetaWkKOffsetK:], kOffset)
}

func MarshalMetaListWithKey(buf []byte, hashL uint64, hashHH uint32, timestamp, seqNum uint64, slot uint16,
	ver uint64, size uint32, kfi uint16, kOffset uint32, pre, next uint64) {
	binary.LittleEndian.PutUint64(buf[offsetMetaHashL:], hashL)
	binary.LittleEndian.PutUint32(buf[offsetMetaHashHH:], hashHH)
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint16(buf[offsetMetaWkKKVKFIdx:], kfi)
	binary.LittleEndian.PutUint64(buf[offsetMetaWkVersion:], ver)
	binary.LittleEndian.PutUint32(buf[offsetMetaWkSize:], size)
	binary.LittleEndian.PutUint64(buf[offsetMetaWkListPre:], pre)
	binary.LittleEndian.PutUint64(buf[offsetMetaWkListNext:], next)
	binary.LittleEndian.PutUint32(buf[offsetMetaWkKOffsetL:], kOffset)
}

// //go:inline
// func buildUpdateTSAndVInfo(buf []byte, timestamp, seqNum uint64, vInfo vInfo) {
// 	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
// 	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
// 	binary.LittleEndian.PutUint32(buf[offsetMetaWkVInfo:], uint32(vInfo))
// }

// //go:inline
// func buildUpdateMeta(buf []byte, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32) {
// 	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
// 	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
// 	binary.LittleEndian.PutUint32(buf[offsetMetaWkVInfo:], uint32(vInfo))
// 	binary.LittleEndian.PutUint32(buf[offsetMetaWkVOffset:], vOffset)
// }

//go:inline
func buildUpdateMeta4Multi(buf []byte, timestamp, seqNum uint64, vInfo vInfo, vOffset uint32, fIdx uint16) {
	binary.LittleEndian.PutUint64(buf[offsetMetaTimestamp:], timestamp)
	binary.LittleEndian.PutUint64(buf[offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint32(buf[offsetMetaWkVInfo:], uint32(vInfo))
	binary.LittleEndian.PutUint32(buf[offsetMetaWkVOffset:], vOffset)
	binary.LittleEndian.PutUint16(buf[offsetMetaWkVFIdx:], fIdx)
}

type expireManager struct {
	writeCnt   uint32
	zeroOffset uint32
	startTime  uint64
	minExTime  uint64
	list       []uint64
}

func newExpireManager(now uint64) *expireManager {
	startTime := time2startTime(now)
	em := &expireManager{
		startTime:  startTime,
		zeroOffset: time2idx(startTime),
		list:       make([]uint64, expireManagerPeriods+2), // overtime && expired
		writeCnt:   expireManagerFlushCycle,
	}
	return em
}

func (em *expireManager) getStartTime() uint64 {
	return em.startTime
}

func (em *expireManager) getMinExTime() uint64 {
	return em.minExTime
}

func (em *expireManager) getOverExSize() uint64 {
	return em.list[expireManagerPeriods]
}

func (em *expireManager) get(idx uint32) uint64 {
	return em.list[(idx+em.zeroOffset)%expireManagerPeriods]
}

func (em *expireManager) clear(idx uint32) {
	em.list[(idx+em.zeroOffset)%expireManagerPeriods] = 0
}

func (em *expireManager) incr(idx uint32, i uint64) {
	if idx == expireManagerPeriods {
		em.list[expireManagerPeriods] += i
	} else {
		em.list[(idx+em.zeroOffset)%expireManagerPeriods] += i
	}
}

func time2offset(t uint64, st uint64) uint32 {
	if t <= st {
		return 0
	}
	idx := uint32((t - st) / uint64(expireManagerDurationMS))
	if idx >= expireManagerPeriods {
		return expireManagerPeriods
	}
	return idx
}

func time2idx(t uint64) uint32 {
	return uint32(t/uint64(expireManagerDurationMS)) % expireManagerPeriods
}

func time2startTime(t uint64) uint64 {
	return t - t%uint64(expireManagerDurationMS)
}

type tmpIdx struct {
	path   string
	header *header
	ctrl   []simd.Metadata
	groups []group
	index  *tableDesignated
}

func NewTmpIndex(dir, filename string, h *header, groups uint32) (i *tmpIdx, err error) {
	i = &tmpIdx{
		path: dir,
		header: &header{
			ver:       h.ver,
			tags:      h.tags,
			groupNum:  groups,
			idxSeq:    h.idxSeq + 1,
			dataSeq:   h.dataSeq,
			metaTail:  h.metaTail,
			kDataTail: h.kDataTail,
			vDataTail: h.vDataTail,
		},
	}

	size := 9*int(groups)*simd.GroupSize + headerSize

	indexFile := buildVtIdxFileName(dir, filename, fileExtIndex, i.header.dataSeq, i.header.idxSeq)
	tblOpts := &tableOptions{
		openType:     tableWriteMmap,
		initMmapSize: size,
	}
	i.index, err = openTableDesignated(indexFile, &i.header.indexTail, tblOpts)
	if err != nil {
		return nil, errors.Errorf("open index file err: %s", err)
	}

	offset, err := i.index.alloc(uint64(size))
	if err != nil {
		return nil, errors.Errorf("alloc header space fail: %s", err)
	}
	if offset != 0 {
		return nil, errors.Errorf("invalid header offset: %d", offset)
	}

	slots := simd.GroupSize * uint64(i.header.groupNum)
	i.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&i.index.data[headerSize])))[:i.header.groupNum]
	groupOffset := uint64(headerSize) + slots
	i.groups = (*(*[maxGroups]group)(unsafe.Pointer(&i.index.data[groupOffset])))[:i.header.groupNum]
	for g := range i.ctrl {
		for s := range i.ctrl[g] {
			i.ctrl[g][s] = simd.Empty
		}
	}
	return i, nil
}

func (i *tmpIdx) Close() error {
	return nil
}

type dataHolder struct {
	dataHolderNoKey
}

func NewDataHolder(opts *options.VectorTableOptions, idxSeq, dataSeq, groups uint32, now uint64) (*dataHolder, error) {
	var err error
RETRY:
	indexFile, metaFile, kDataFile, vDataFile := buildVTFileName(opts.Dirname, opts.Filename, idxSeq, dataSeq)
	d := dataHolder{}
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

func (dh *dataHolder) setKKVMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64, slot uint16, version uint64, size uint32) (metaOffset uint64, err error) {
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

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeKKVWithKey)
	if err != nil {
		return 0, err
	}
	hashHH := uint32(hi >> 32)
	tsAndDT := combine(timestamp, dataType)
	MarshalMetaKKVWithKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot, version, size, kfi, kOffset)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(kSize))
	}
	return
}

func (dh *dataHolder) setKKVListMeta(hi, lo uint64, k []byte, dataType uint8, timestamp, seqNum uint64, slot uint16, version uint64, size uint32, pre, next uint64) (metaOffset uint64, err error) {
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

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeListWithKey)
	if err != nil {
		return 0, err
	}
	hashHH := uint32(hi >> 32)
	tsAndDT := combine(timestamp, dataType)
	MarshalMetaListWithKey(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, slot, version, size, kfi, kOffset, pre, next)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(kSize))
	}
	return
}

func (dh *dataHolder) set(hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (metaOffset uint64, err error) {
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

	metaOffset, err = dh.Meta.ReuseAlloc(metaSizeStrWithKey)
	if err != nil {
		return 0, err
	}

	hashHH := uint32(hi >> 32)
	lv := uint32(len(v))
	vinfo := NewVInfo(lv, needCompress)
	MarshalMetaKV(dh.Meta.data[metaOffset:], lo, hashHH, tsAndDT, seqNum, vinfo, slot, vfi, kfi, vOffset, kOffset)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(kSize)+uint64(lv))
	}
	return
}

func (dh *dataHolder) getKV(metaOffset uint64) (
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
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkVInfo:]))
	hashL = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset:])
	hashHH = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+8:])
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		vfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaWkVFIdx:])
		kOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkKOffsetS:])
		kfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaWkKFIdx:])
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
		version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkVersion:])
		size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkSize:])
		var offsetMetaKOffset uint64
		switch dataType {
		case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
			offsetMetaKOffset = offsetMetaWkKOffsetK
		case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			offsetMetaKOffset = offsetMetaWkKOffsetL
			pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkListPre:])
			next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkListNext:])
		default:
			dh.Meta.dataLock.RUnlock()
			err = errors.Errorf("set unsupported datatype %d", dataType)
			return
		}
		kOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaKOffset:])
		kfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaWkKKVKFIdx:])
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

func (dh *dataHolder) getKVCopy(metaOffset uint64) (
	key, value []byte, seqNum uint64, dataType uint8,
	timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, err error,
) {
	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkVInfo:]))
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		kOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkKOffsetS:])
		vfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaWkVFIdx:])
		kfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaWkKFIdx:])
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

		var ksBytes [2]byte
		_, err = dh.KDataM.ReadAt(kfi, ksBytes[:], kOffset)
		if err != nil {
			err = errors.Wrapf(err, "vectortable: read kdata fail")
			return
		}
		kSize := binary.LittleEndian.Uint16(ksBytes[0:2])
		key = make([]byte, kSize)
		_, err = dh.KDataM.ReadAt(kfi, key, kOffset+2)
		return
	} else {
		version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkVersion:])
		size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkSize:])
		var offsetMetaKOffset uint64
		switch dataType {
		case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
			offsetMetaKOffset = offsetMetaWkKOffsetK
		case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			offsetMetaKOffset = offsetMetaWkKOffsetL
			pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkListPre:])
			next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkListNext:])
		default:
			dh.Meta.dataLock.RUnlock()
			err = errors.Errorf("set unsupported datatype %d", dataType)
			return
		}
		kOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaKOffset:])
		kfi := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaWkKKVKFIdx:])
		dh.Meta.dataLock.RUnlock()

		var ksBytes [2]byte
		_, err = dh.KDataM.ReadAt(kfi, ksBytes[:], kOffset)
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

func (dh *dataHolder) getValue(metaOffset uint64) (bs []byte, seqNum uint64, dt uint8, ts uint64, slot uint16, c func(), err error) {
	dh.Meta.dataLock.RLock()
	vOffset := binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkVOffset:])
	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkVInfo:]))
	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ttl := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	fIdx := binary.LittleEndian.Uint16(dh.Meta.data[metaOffset+offsetMetaWkVFIdx:])
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

func (dh *dataHolder) getMeta(metaOffset uint64) (
	seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64,
	size uint32, pre, next uint64, err error,
) {
	dh.Meta.dataLock.RLock()
	defer dh.Meta.dataLock.RUnlock()

	seqNum = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaSeqNum:])
	ts := binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaTimestamp:])
	timestamp, dataType = split(ts)
	if dataType == kkv.DataTypeString {
		return
	}
	version = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkVersion:])
	size = binary.LittleEndian.Uint32(dh.Meta.data[metaOffset+offsetMetaWkSize:])
	if kkv.IsDataTypeList(dataType) {
		pre = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkListPre:])
		next = binary.LittleEndian.Uint64(dh.Meta.data[metaOffset+offsetMetaWkListNext:])
	}
	return
}

func (dh *dataHolder) del(itemOffset uint64) {
	vinfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[itemOffset+offsetMetaWkVInfo:]))
	s, _ := vinfo.Info()
	dt := splitHigh8(binary.LittleEndian.Uint64(dh.Meta.data[itemOffset+offsetMetaTimestamp:]))
	if dt == kkv.DataTypeString {
		dh.Header.delVCap += uint64(CapForShortValue(s))
		dh.Meta.ReuseFree(itemOffset, metaSizeStrWithKey)
	} else if kkv.IsDataTypeList(dt) {
		dh.Meta.ReuseFree(itemOffset, metaSizeListWithKey)
	} else {
		dh.Meta.ReuseFree(itemOffset, metaSizeKKVWithKey)
	}
}

func (dh *dataHolder) update(mOffset uint64, inV []byte, dataType uint8, timestamp, seqNum uint64) (err error) {
	var needCompress uint32

	ivInfo := vInfo(binary.LittleEndian.Uint32(dh.Meta.data[mOffset+offsetMetaWkVInfo:]))
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
	buildUpdateMeta4Multi(dh.Meta.data[mOffset:], tsAndDT, seqNum, vinfo, vOffset, vfi)

	if timestamp > 0 {
		dh.IncrExpire(timestamp, uint64(lv))
	}
	return
}

func (dh *dataHolder) updateKKVMeta(
	mOffset uint64, dataType uint8,
	timestamp, seqNum, version uint64, size uint32,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaWkVersion:], version)
	binary.LittleEndian.PutUint32(dh.Meta.data[mOffset+offsetMetaWkSize:], size)
}

func (dh *dataHolder) updateKKVListMeta(
	mOffset uint64, dataType uint8,
	timestamp, seqNum, version uint64,
	size uint32, pre, next uint64,
) {
	tsAndDT := combine(timestamp, dataType)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaTimestamp:], tsAndDT)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaSeqNum:], seqNum)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaWkVersion:], version)
	binary.LittleEndian.PutUint32(dh.Meta.data[mOffset+offsetMetaWkSize:], size)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaWkListPre:], pre)
	binary.LittleEndian.PutUint64(dh.Meta.data[mOffset+offsetMetaWkListNext:], next)
}

func buildVTFileName(dir, filename string, idxSeq, dataSeq uint32) (string, string, string, string) {
	return buildVtIdxFileName(dir, filename, fileExtIndex, dataSeq, idxSeq),
		buildVtDataFileName(dir, filename, fileExtMeta, dataSeq),
		buildVtDataFileName(dir, filename, fileExtKData, dataSeq),
		buildVtDataFileName(dir, filename, fileExtVData, dataSeq)
}

func buildFilename(dir, filename string) string {
	dir = strings.TrimSuffix(dir, "/")
	return dir + "/" + filename
}

func buildVtDataFileName(dir, filename, ext string, seq uint32) string {
	dir = strings.TrimSuffix(dir, "/")
	m := "%s/%s.%s.%d"
	return fmt.Sprintf(m, dir, filename, ext, seq)
}

func buildVtIdxFileName(dir, filename, ext string, seqData uint32, seqIdx uint32) string {
	dir = strings.TrimSuffix(dir, "/")
	m := "%s/%s.%s.%d.%d"
	return fmt.Sprintf(m, dir, filename, ext, seqData, seqIdx)
}

//go:inline
func CapForShortValue(vSize uint32) uint32 {
	return (vSize + 3) &^ 3
}

func CapForAlign4B(vSize uint64) uint64 {
	return (vSize + 3) &^ 3
}
