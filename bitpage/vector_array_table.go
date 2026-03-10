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
	"fmt"

	"github.com/zuoyebang/bitalosdb/v2/bitpage/vectorindex"
	"github.com/zuoyebang/bitalosdb/v2/bitpage/vectorindex64"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

const (
	vatOffsetLength            = 4
	vatHeaderSize              = 28
	vatDataOffset              = TblFileHeaderSize
	vatIndexHeaderOffset       = 4
	vatKeyVersionSize          = kkv.SubKeyVersionLength
	vatVersionIndex64ValueMask = 1<<32 - 1

	vatItemHeaderValueNil        = 0b1000_0000
	vatItemHeaderKeyVersionFirst = 0b0100_0000
	vatItemHeaderDtMask          = 0b0011_1111
	vatItemHeaderMaxLen          = 1 + binary.MaxVarintLen64

	vatZdataMemberSize = kkv.KeyMd5Length
	vatZdataValueSize  = kkv.SiKeyLength
	vatZdataItemLength = vatZdataMemberSize + vatZdataValueSize

	vatListIndexSize = kkv.SubKeyListIndexLength

	vatExpireKeySize = kkv.ExpireKeyLength - kkv.SubKeyHeaderLength
)

type vectorArrayTable struct {
	header   *vatHeader
	logger   base.Logger
	fn       FileNum
	id       string
	dataPath string
	dataFile *TableWriter
	idxPath  string
	idxFile  *TableWriter
	dataSize uint32
	idxSize  uint32

	hashSetWriter           *vatHashSetWriter
	hashSetItemIndex        *vectorindex.VectorIndex
	hashSetListVersionIndex *vectorindex64.VectorIndexV64
	listWriter              *vatRangeKeyWriter
	listOffsetIndex         vatOffsetIndex
	zindexWriter            *vatRangeKeyWriter
	zdataWriter             *vatFixedKeyWriter
	zsetIndex               *vatZsetIndex
	expireWriter            *vatFixedKeyWriter
	expireVersionIndex      *vectorindex64.VectorIndexV32
	lastVersion             []byte
	lastDataType            uint8
	itemHeaderBuf           [vatItemHeaderMaxLen]byte
	maxKey                  []byte
	keyBuf                  []byte

	keyStat struct {
		listKeys      uint32
		zsetKeys      uint32
		zsetIndexKeys uint32
		hashKeys      uint32
		setKeys       uint32
		expireKeys    uint32
	}
}

type vatHeader struct {
	headerOffset                  uint32
	version                       uint16
	num                           uint32
	zsetOffsetIndexOffset         uint32
	zsetOffsetIndexSize           uint32
	zsetVersionIndexOffset        uint32
	zsetVersionIndexSize          uint32
	listOffsetIndexOffset         uint32
	listOffsetIndexSize           uint32
	hashItemIndexOffset           uint32
	hashItemIndexSize             uint32
	hashSetListVersionIndexOffset uint32
	hashSetListVersionIndexSize   uint32
	expireVersionIndexOffset      uint32
	expireVersionIndexSize        uint32
}

func (a *vectorArrayTable) writeIdxToFile() error            { panic("unimplemented") }
func (a *vectorArrayTable) mmapRLock()                       { panic("unimplemented") }
func (a *vectorArrayTable) mmapRUnLock()                     { panic("unimplemented") }
func (a *vectorArrayTable) kindStatis(internalKeyKind)       { panic("unimplemented") }
func (a *vectorArrayTable) set(internalKey, ...[]byte) error { panic("unimplemented") }
func (a *vectorArrayTable) flushFinish() error               { panic("unimplemented") }
func (a *vectorArrayTable) flushIndexes() error              { panic("unimplemented") }

func newVectorArrayTable(p *page, path string, fn FileNum, exist bool) (*vectorArrayTable, error) {
	var err error

	idxPath := p.bp.makeFilePath(fileTypeVectorArrayTableIndex, p.pn, fn)
	at := &vectorArrayTable{
		id:       base.GetFilePathBase(path),
		logger:   p.bp.opts.Logger,
		fn:       fn,
		dataPath: path,
		idxPath:  idxPath,
		header:   &vatHeader{},
	}

	viOpt := &vectorindex.VectorIndexOptions{
		Logger:         at.logger,
		CompareKeyFunc: at.kkvItemIndexCompareKey,
	}
	vi64Opt := &vectorindex64.VectorIndexOptions{
		Logger: at.logger,
	}
	at.hashSetItemIndex = vectorindex.NewVectorIndex(viOpt)
	at.hashSetListVersionIndex = vectorindex64.NewVectorIndexV64(vi64Opt)
	at.expireVersionIndex = vectorindex64.NewVectorIndexV32(vi64Opt)
	at.zsetIndex = &vatZsetIndex{
		versionIndex: vectorindex64.NewVectorIndexV96(vi64Opt),
		offsetIndex:  vatOffsetIndex{},
	}
	at.listOffsetIndex = vatOffsetIndex{}

	tblOpts := &TableOptions{FileExist: exist}
	if exist {
		tblOpts.OpenType = TableTypeReadMmap
	} else {
		tblOpts.OpenType = TableTypeOnceWriteDisk
	}
	at.idxFile, err = NewTableWriter(at.idxPath, tblOpts)
	if err != nil {
		return nil, err
	}

	at.dataFile, err = NewTableWriter(at.dataPath, tblOpts)
	if err != nil {
		return nil, err
	}

	if exist {
		if err = at.readHeader(); err != nil {
			return nil, err
		}
	} else {
		at.header.version = at.idxFile.version
		if err = at.dataFile.SetWriter(consts.BitpageAtDataBufioWriterSize); err != nil {
			return nil, err
		}
	}

	return at, nil
}

func (a *vectorArrayTable) getID() string {
	return a.id
}

func (a *vectorArrayTable) readHeaderOffset() (uint32, error) {
	var buf [vatOffsetLength]byte
	if n, err := a.idxFile.readAt(buf[:], vatIndexHeaderOffset); err != nil {
		return 0, err
	} else if n != vatOffsetLength {
		return 0, errors.Errorf("readHeaderOffset fail read %d bytes", n)
	}
	offset := binary.BigEndian.Uint32(buf[:])
	return offset, nil
}

func (a *vectorArrayTable) writeHeaderOffset() error {
	var buf [vatOffsetLength]byte
	binary.BigEndian.PutUint32(buf[:], a.header.headerOffset)
	if n, err := a.idxFile.writeAt(buf[:], vatIndexHeaderOffset); err != nil {
		return err
	} else if n != vatOffsetLength {
		return errors.Errorf("writeHeaderOffset fail wrote %d bytes", n)
	}
	return nil
}

func (a *vectorArrayTable) readHeader() error {
	headerOffset, err := a.readHeaderOffset()
	if err != nil {
		return err
	}
	a.header.headerOffset = headerOffset
	a.header.version = a.idxFile.version
	header := a.idxFile.GetBytes(a.header.headerOffset, vatHeaderSize)
	pos := 0
	a.header.num = binary.BigEndian.Uint32(header[pos:])
	pos += 4
	a.header.zsetOffsetIndexOffset = binary.BigEndian.Uint32(header[pos:])
	pos += 4
	a.header.zsetVersionIndexOffset = binary.BigEndian.Uint32(header[pos:])
	pos += 4
	a.header.listOffsetIndexOffset = binary.BigEndian.Uint32(header[pos:])
	pos += 4
	a.header.hashItemIndexOffset = binary.BigEndian.Uint32(header[pos:])
	pos += 4
	a.header.hashSetListVersionIndexOffset = binary.BigEndian.Uint32(header[pos:])
	pos += 4
	a.header.expireVersionIndexOffset = binary.BigEndian.Uint32(header[pos:])
	a.header.zsetOffsetIndexSize = a.header.zsetVersionIndexOffset - a.header.zsetOffsetIndexOffset
	a.header.zsetVersionIndexSize = a.header.listOffsetIndexOffset - a.header.zsetVersionIndexOffset
	a.header.listOffsetIndexSize = a.header.hashItemIndexOffset - a.header.listOffsetIndexOffset
	a.header.hashItemIndexSize = a.header.hashSetListVersionIndexOffset - a.header.hashItemIndexOffset
	a.header.hashSetListVersionIndexSize = a.header.expireVersionIndexOffset - a.header.hashSetListVersionIndexOffset
	a.header.expireVersionIndexSize = a.header.headerOffset - a.header.expireVersionIndexOffset
	a.dataSize = a.dataFile.Size()
	a.idxSize = a.idxFile.Size()
	maxKeyOffset := a.header.headerOffset + vatHeaderSize
	maxKeySize := a.idxSize - maxKeyOffset
	a.maxKey = a.idxFile.GetBytes(maxKeyOffset, maxKeySize)
	a.setReader()
	return nil
}

func (a *vectorArrayTable) writeHeader() error {
	var headerBuf [vatHeaderSize]byte
	a.header.zsetOffsetIndexOffset = vatDataOffset
	a.header.zsetVersionIndexOffset = a.header.zsetOffsetIndexOffset + a.header.zsetOffsetIndexSize
	a.header.listOffsetIndexOffset = a.header.zsetVersionIndexOffset + a.header.zsetVersionIndexSize
	a.header.hashItemIndexOffset = a.header.listOffsetIndexOffset + a.header.listOffsetIndexSize
	a.header.hashSetListVersionIndexOffset = a.header.hashItemIndexOffset + a.header.hashItemIndexSize
	a.header.expireVersionIndexOffset = a.header.hashSetListVersionIndexOffset + a.header.hashSetListVersionIndexSize
	a.header.headerOffset = a.idxFile.Size()
	pos := 0
	binary.BigEndian.PutUint32(headerBuf[pos:], a.header.num)
	pos += 4
	binary.BigEndian.PutUint32(headerBuf[pos:], a.header.zsetOffsetIndexOffset)
	pos += 4
	binary.BigEndian.PutUint32(headerBuf[pos:], a.header.zsetVersionIndexOffset)
	pos += 4
	binary.BigEndian.PutUint32(headerBuf[pos:], a.header.listOffsetIndexOffset)
	pos += 4
	binary.BigEndian.PutUint32(headerBuf[pos:], a.header.hashItemIndexOffset)
	pos += 4
	binary.BigEndian.PutUint32(headerBuf[pos:], a.header.hashSetListVersionIndexOffset)
	pos += 4
	binary.BigEndian.PutUint32(headerBuf[pos:], a.header.expireVersionIndexOffset)
	if _, err := a.idxFile.Write(headerBuf[:]); err != nil {
		return err
	}
	if _, err := a.idxFile.Write(a.maxKey); err != nil {
		return err
	}
	if err := a.writeHeaderOffset(); err != nil {
		return err
	}
	return nil
}

func (a *vectorArrayTable) setReader() {
	if a.header.zsetOffsetIndexSize > 0 {
		buf := a.idxFile.GetBytes(a.header.zsetOffsetIndexOffset, a.header.zsetOffsetIndexSize)
		a.zsetIndex.setOffsetIndexReader(buf)
	}

	if a.header.zsetVersionIndexSize > 0 {
		buf := a.idxFile.GetBytes(a.header.zsetVersionIndexOffset, a.header.zsetVersionIndexSize)
		a.zsetIndex.setVersionIndexReader(buf)
	}

	if a.header.listOffsetIndexSize > 0 {
		buf := a.idxFile.GetBytes(a.header.listOffsetIndexOffset, a.header.listOffsetIndexSize)
		a.listOffsetIndex.setData(buf)
	}

	if a.header.hashItemIndexSize > 0 {
		buf := a.idxFile.GetBytes(a.header.hashItemIndexOffset, a.header.hashItemIndexSize)
		a.hashSetItemIndex.SetReader(buf)
	}

	if a.header.hashSetListVersionIndexSize > 0 {
		buf := a.idxFile.GetBytes(a.header.hashSetListVersionIndexOffset, a.header.hashSetListVersionIndexSize)
		a.hashSetListVersionIndex.SetReader(buf)
	}

	if a.header.expireVersionIndexSize > 0 {
		buf := a.idxFile.GetBytes(a.header.expireVersionIndexOffset, a.header.expireVersionIndexSize)
		a.expireVersionIndex.SetReader(buf)
	}
}

func (a *vectorArrayTable) getId() string {
	return a.id
}

func (a *vectorArrayTable) writeItemByHash(ikey *InternalKKVKey, value []byte) (uint32, error) {
	var keyHash uint32
	if kkv.IsDataTypeUseKeyHash(ikey.DataType) {
		a.keyBuf = ikey.MakeUserKeyByBuf(a.keyBuf)
		keyHash = hash.Fnv32(a.keyBuf)
	}
	return a.writeItem(ikey, value, keyHash)
}

func (a *vectorArrayTable) writeItem(ikey *InternalKKVKey, value []byte, keyHash uint32) (uint32, error) {
	var sz uint32
	var err error
	dataType := ikey.DataType
	if a.lastDataType != dataType {
		switch a.lastDataType {
		case kkv.DataTypeHash, kkv.DataTypeSet:
			a.hashSetWriter.writeVersion()
		case kkv.DataTypeList, kkv.DataTypeBitmap:
			a.listWriter.writeVersion()
		case kkv.DataTypeZsetIndex:
			a.zindexWriter.writeVersion()
		case kkv.DataTypeZset:
			if err = a.zdataWriter.writeVersion(); err != nil {
				return 0, err
			}
		case kkv.DataTypeExpireKey:
			if err = a.expireWriter.writeVersion(); err != nil {
				return 0, err
			}
		default:
		}
		a.lastDataType = dataType
	}

	itemOffset := a.dataFile.Size()
	switch dataType {
	case kkv.DataTypeZset:
		if a.zdataWriter == nil {
			a.zdataWriter = newFixedKeyWriter(a, dataType, vatZdataItemLength*256, a.zsetIndex.setZdataVersionIndex)
		}
		sz, err = a.zdataWriter.writeItem(ikey, value)
		if err != nil {
			return 0, err
		}
	case kkv.DataTypeZsetIndex:
		if a.zindexWriter == nil {
			a.zindexWriter = newRangeKeyWriter(a, a.zsetIndex.setZindexVersionIndex)
		}
		sz, err = a.zindexWriter.writeItem(ikey, nil, itemOffset)
		if err != nil {
			return 0, err
		}
	case kkv.DataTypeList, kkv.DataTypeBitmap:
		if a.listWriter == nil {
			a.listWriter = newRangeKeyWriter(a, a.setHashSetListVersionIndex)
		}
		sz, err = a.listWriter.writeItem(ikey, value, itemOffset)
		if err != nil {
			return 0, err
		}
	case kkv.DataTypeExpireKey:
		if a.expireWriter == nil {
			a.expireWriter = newFixedKeyWriter(a, dataType, vatExpireKeySize*256, a.expireVersionIndex.Set)
		}
		sz, err = a.expireWriter.writeItem(ikey, nil)
		if err != nil {
			return 0, err
		}
	case kkv.DataTypeHash, kkv.DataTypeSet:
		if a.hashSetWriter == nil {
			a.hashSetWriter = newHashSetWriter(a)
		}
		sz, err = a.hashSetWriter.writeItem(ikey, value, itemOffset)
		if err != nil {
			return 0, err
		}
		a.hashSetItemIndex.Add(keyHash, itemOffset)
	default:
		return 0, errors.Errorf("panic unknown data type %d", dataType)
	}

	a.header.num++
	a.statKeys(dataType)

	return sz, nil
}

func (a *vectorArrayTable) writeFinish(maxKey []byte) error {
	if maxKey == nil {
		return errors.New("maxKey is nil")
	}

	if a.hashSetWriter != nil {
		a.hashSetWriter.writeVersion()
		a.hashSetWriter = nil
	}

	if a.expireWriter != nil {
		if err := a.expireWriter.writeVersion(); err != nil {
			return err
		}
		a.expireWriter = nil
	}

	if a.zdataWriter != nil {
		if err := a.zdataWriter.writeVersion(); err != nil {
			return err
		}
		a.zdataWriter = nil
	}

	if a.zindexWriter != nil {
		zindexSize, err := a.zindexWriter.serialize(a.idxFile)
		if err != nil {
			return err
		}
		a.header.zsetOffsetIndexSize = uint32(zindexSize)
		a.zindexWriter = nil
	}

	if a.zsetIndex.versionIndex.Size() > 0 {
		n, err := a.zsetIndex.versionIndex.Serialize(a.idxFile)
		if err != nil {
			return err
		}
		a.header.zsetVersionIndexSize = uint32(n)
	}

	if a.listWriter != nil {
		listIndexSize, err := a.listWriter.serialize(a.idxFile)
		if err != nil {
			return err
		}
		a.header.listOffsetIndexSize = uint32(listIndexSize)
		a.listWriter = nil
	}

	n, err := a.hashSetItemIndex.Serialize(a.idxFile)
	if err != nil {
		return err
	}
	a.header.hashItemIndexSize = uint32(n)

	n, err = a.hashSetListVersionIndex.Serialize(a.idxFile)
	if err != nil {
		return err
	}
	a.header.hashSetListVersionIndexSize = uint32(n)

	n, err = a.expireVersionIndex.Serialize(a.idxFile)
	if err != nil {
		return err
	}
	a.header.expireVersionIndexSize = uint32(n)

	a.maxKey = maxKey
	if err = a.writeHeader(); err != nil {
		return err
	}

	a.dataSize = a.dataFile.Size()
	a.idxSize = a.idxFile.Size()
	if err = a.idxFile.Fdatasync(); err != nil {
		return err
	}
	if err = a.dataFile.Fdatasync(); err != nil {
		return err
	}

	if err = a.idxFile.MmapReadTruncate(int(a.idxSize)); err != nil {
		return err
	}
	if err = a.dataFile.MmapReadTruncate(int(a.dataSize)); err != nil {
		return err
	}

	headerOffset, err := a.readHeaderOffset()
	if err != nil {
		return err
	} else if headerOffset != a.header.headerOffset {
		return errors.Errorf("invalid header offset: expected %d, got %d", a.header.headerOffset, headerOffset)
	}

	a.setReader()
	a.idxFile.FreeWriter()
	a.dataFile.FreeWriter()
	return nil
}

func (a *vectorArrayTable) statKeys(dataType uint8) {
	switch dataType {
	case kkv.DataTypeZsetIndex:
		a.keyStat.zsetIndexKeys++
	case kkv.DataTypeZset:
		a.keyStat.zsetKeys++
	case kkv.DataTypeList, kkv.DataTypeBitmap:
		a.keyStat.listKeys++
	case kkv.DataTypeHash:
		a.keyStat.hashKeys++
	case kkv.DataTypeSet:
		a.keyStat.setKeys++
	case kkv.DataTypeExpireKey:
		a.keyStat.expireKeys++
	default:
	}
}

func (a *vectorArrayTable) encodeItemHeader(dt uint8, keySize, valueSize int, isFirst bool) int {
	var h byte
	n := 1
	if keySize > 0 {
		n += binary.PutUvarint(a.itemHeaderBuf[n:], uint64(keySize))
	}
	if valueSize > 0 {
		n += binary.PutUvarint(a.itemHeaderBuf[n:], uint64(valueSize))
		h = 0
	} else {
		h = vatItemHeaderValueNil
	}
	if isFirst {
		h |= vatItemHeaderKeyVersionFirst
	}
	h |= dt
	a.itemHeaderBuf[0] = h
	return n
}

func (a *vectorArrayTable) decodeItemHeader(offset uint32) (uint8, uint32, uint32, uint32, bool) {
	if a.dataFile.isEOF(offset) {
		return 0, 0, 0, 0, false
	}

	buf := a.dataFile.getData(offset)
	h := buf[0]
	isValueNil := (h >> 7) == 1
	isVersionFirst := ((h >> 6) & 1) == 1
	dt := h & vatItemHeaderDtMask
	if !kkv.IsDataType(dt) {
		a.logger.Fatalf("vat(%s) invalid dataType:%d offset:%d", a.id, dt, offset)
		return 0, 0, 0, 0, false
	}

	hl := uint32(1)
	var keySize, valueSize uint32

	switch dt {
	case kkv.DataTypeList, kkv.DataTypeBitmap:
		keySize = vatListIndexSize
	default:
		kz, n1 := binary.Uvarint(buf[hl:])
		hl += uint32(n1)
		keySize = uint32(kz)
	}

	if !isValueNil {
		vz, n2 := binary.Uvarint(buf[hl:])
		hl += uint32(n2)
		valueSize = uint32(vz)
	}

	return dt, keySize, valueSize, hl, isVersionFirst
}

func (a *vectorArrayTable) decodeItemHeaderOptimal(offset uint32) (uint8, uint32, uint32, uint32, bool) {
	dt, keySize, valueSize, hl, isVersionFirst := a.decodeItemHeader(offset)
	if isVersionFirst {
		hl += vatKeyVersionSize
	}
	return dt, keySize, valueSize, hl, isVersionFirst
}

func (a *vectorArrayTable) getItemByOffset(dataType uint8, offset uint32) ([]byte, []byte, uint32) {
	dt, keySize, valueSize, hl, _ := a.decodeItemHeaderOptimal(offset)
	if dt == 0 || dataType != dt {
		return nil, nil, 0
	}

	key, value := a.getKeyValue(offset+hl, keySize, valueSize)
	sz := hl + keySize + valueSize
	return key, value, sz
}

func (a *vectorArrayTable) getKeyValue(offset, keySize, valueSize uint32) ([]byte, []byte) {
	var value []byte
	key := a.dataFile.GetBytes(offset, keySize)
	if valueSize > 0 {
		value = a.dataFile.GetBytes(offset+keySize, valueSize)
	}
	return key, value
}

func (a *vectorArrayTable) getKeyByOffset(offset uint32) []byte {
	dt, keySize, _, hl, _ := a.decodeItemHeaderOptimal(offset)
	if dt == 0 {
		return nil
	}
	return a.dataFile.GetBytes(offset+hl, keySize)
}

func (a *vectorArrayTable) getValueByOffset(offset uint32) []byte {
	dt, _, valueSize, hl, _ := a.decodeItemHeaderOptimal(offset)
	if dt == 0 || valueSize == 0 {
		return nil
	}
	return a.dataFile.GetBytes(offset+hl, valueSize)
}

func (a *vectorArrayTable) get(key []byte, khash uint32) ([]byte, bool, internalKeyKind) {
	dataType := kkv.GetKeyDataType(key)
	switch dataType {
	case kkv.DataTypeHash, kkv.DataTypeSet:
		return a.getHashKey(key, khash)
	case kkv.DataTypeList, kkv.DataTypeBitmap:
		return a.getListKey(key, false)
	case kkv.DataTypeZset:
		return a.getZdataKey(key, false)
	case kkv.DataTypeZsetIndex:
		if found := a.isExistZindexKey(key); found {
			return nil, found, internalKeyKindSet
		}
		return nil, false, internalKeyKindInvalid
	case kkv.DataTypeExpireKey:
		if found := a.isExistExpireKey(key); found {
			return nil, found, internalKeyKindSet
		}
		return nil, false, internalKeyKindInvalid
	default:
		return nil, false, internalKeyKindInvalid
	}
}

func (a *vectorArrayTable) exist(key []byte, khash uint32) (bool, internalKeyKind) {
	var found bool
	dataType := kkv.GetKeyDataType(key)
	switch dataType {
	case kkv.DataTypeHash, kkv.DataTypeSet:
		_, found, _ = a.getHashKey(key, khash)
	case kkv.DataTypeList, kkv.DataTypeBitmap:
		_, found, _ = a.getListKey(key, true)
	case kkv.DataTypeZsetIndex:
		found = a.isExistZindexKey(key)
	case kkv.DataTypeZset:
		_, found, _ = a.getZdataKey(key, true)
	case kkv.DataTypeExpireKey:
		found = a.isExistExpireKey(key)
	default:
		found = false
	}
	if found {
		return true, internalKeyKindSet
	}
	return false, internalKeyKindInvalid
}

func (a *vectorArrayTable) getHashKey(key []byte, khash uint32) ([]byte, bool, internalKeyKind) {
	value, err := a.hashSetItemIndex.GetValue(key, khash)
	if err == nil {
		return value, true, internalKeyKindSet
	}
	return nil, false, internalKeyKindInvalid
}

func (a *vectorArrayTable) getListKey(key []byte, exist bool) ([]byte, bool, internalKeyKind) {
	keyVersion := kkv.GetKeyVersion(key)
	spi, epi, ok := a.getHashSetListVersionIndex(keyVersion)
	if !ok {
		return nil, false, internalKeyKindInvalid
	}

	var offset, valueOffset, valueSize uint32
	subKey := kkv.GetSubKey(key)
	sp := int(spi)
	ep := int(epi)
	num := ep - sp + 1
	_, found := binaryFind(num, func(p int) int {
		offset = a.listOffsetIndex.getOffset(sp + p)
		dt, ks, vs, hl, _ := a.decodeItemHeaderOptimal(offset)
		if !kkv.IsDataTypeList(dt) {
			return 1
		}
		offset += hl
		cmp := bytes.Compare(subKey, a.dataFile.GetBytes(offset, ks))
		if cmp == 0 && !exist {
			valueOffset = offset + ks
			valueSize = vs
		}
		return cmp
	})
	if !found {
		return nil, false, internalKeyKindInvalid
	} else if exist || valueSize == 0 {
		return nil, true, internalKeyKindSet
	}

	value := a.dataFile.GetBytes(valueOffset, valueSize)
	return value, true, internalKeyKindSet
}

func (a *vectorArrayTable) findListKeyRange(key []byte) (int, int, int, bool) {
	keyVersion := kkv.GetKeyVersion(key)
	spi, epi, ok := a.getHashSetListVersionIndex(keyVersion)
	if !ok {
		return 0, 0, 0, false
	}

	sp := int(spi)
	ep := int(epi)
	subKey := kkv.GetSubKey(key)
	num := ep - sp + 1
	pos := binarySearch(num, func(p int) int {
		offset := a.listOffsetIndex.getOffset(sp + p)
		return bytes.Compare(a.getKeyByOffset(offset), subKey)
	})
	return sp, ep, pos, true
}

func (a *vectorArrayTable) isExistExpireKey(key []byte) bool {
	keyVersion := kkv.GetKeyVersion(key)
	offset, err := a.expireVersionIndex.Get(keyVersion)
	if err != nil {
		return false
	}

	dt, keyNum, _, hl, isFirst := a.decodeItemHeaderOptimal(offset)
	if dt != kkv.DataTypeExpireKey || !isFirst {
		return false
	}

	offset += hl
	subKey := kkv.GetSubKey(key)
	_, found := binaryFind(int(keyNum), func(i int) int {
		p := offset + uint32(i*vatExpireKeySize)
		expireKey := a.dataFile.GetBytes(p, vatExpireKeySize)
		return bytes.Compare(subKey, expireKey)
	})
	return found
}

func (a *vectorArrayTable) getZdataKey(key []byte, exist bool) ([]byte, bool, internalKeyKind) {
	keyVersion := kkv.GetKeyVersion(key)
	offset, ok := a.zsetIndex.getZdataVersionIndex(keyVersion)
	if !ok {
		return nil, false, internalKeyKindInvalid
	}

	dt, keyNum, _, hl, isFirst := a.decodeItemHeaderOptimal(offset)
	if dt != kkv.DataTypeZset || !isFirst {
		return nil, false, internalKeyKindInvalid
	}

	offset += hl
	member := kkv.DecodeZsetDataKey(key)
	pos, found := binaryFind(int(keyNum), func(i int) int {
		p := offset + uint32(i*vatZdataItemLength)
		zdataKey := a.dataFile.GetBytes(p, vatZdataMemberSize)
		cmp := bytes.Compare(member, zdataKey)
		return cmp
	})
	if !found {
		return nil, false, internalKeyKindInvalid
	} else if exist {
		return nil, true, internalKeyKindSet
	}

	p := offset + uint32(pos*vatZdataItemLength) + vatZdataMemberSize
	value := a.dataFile.GetBytes(p, vatZdataValueSize)
	return value, true, internalKeyKindSet
}

func (a *vectorArrayTable) isExistZindexKey(key []byte) bool {
	keyVersion := kkv.GetKeyVersion(key)
	sp, ep, ok := a.zsetIndex.getZindexVersionIndex(keyVersion)
	if !ok {
		return false
	}

	subKey := kkv.GetSubKey(key)
	num := ep - sp + 1
	_, found := binaryFind(num, func(p int) int {
		offset := a.zsetIndex.getOffset(sp + p)
		return bytes.Compare(subKey, a.getKeyByOffset(offset))
	})
	return found
}

func (a *vectorArrayTable) findZindexKeyRange(key []byte) (int, int, int, bool) {
	keyVersion := kkv.GetKeyVersion(key)
	sp, ep, ok := a.zsetIndex.getZindexVersionIndex(keyVersion)
	if !ok {
		return 0, 0, 0, false
	}

	subKey := kkv.GetSubKey(key)
	num := ep - sp + 1
	pos := binarySearch(num, func(p int) int {
		offset := a.zsetIndex.getOffset(sp + p)
		return bytes.Compare(a.getKeyByOffset(offset), subKey)
	})
	return sp, ep, pos, true
}

func (a *vectorArrayTable) kkvItemIndexCompareKey(k []byte, offset uint32) ([]byte, int) {
	key, v, _ := a.getItemByOffset(kkv.GetKeyDataType(k), offset)
	if !bytes.Equal(kkv.GetSubKey(k), key) {
		return nil, 1
	}

	keyVersion := kkv.GetKeyVersion(k)
	startOffset, endOffset, exist := a.getHashSetListVersionIndex(keyVersion)
	if exist && startOffset <= offset && offset < endOffset {
		return v, 0
	}
	return nil, 1
}

func (a *vectorArrayTable) getMaxKey() []byte {
	return a.maxKey
}

func (a *vectorArrayTable) getDataOffset() uint32 {
	return TblFileHeaderSize
}

func (a *vectorArrayTable) getKeyStats() (int, int, int) {
	return 0, 0, 0
}

func (a *vectorArrayTable) itemCount() int {
	return int(a.header.num)
}

func (a *vectorArrayTable) inuseBytes() uint64 {
	return uint64(a.dataSize) + uint64(a.idxSize)
}

func (a *vectorArrayTable) dataBytes() uint64 {
	return uint64(a.dataSize)
}

func (a *vectorArrayTable) getModTime() int64 {
	return a.dataFile.getModTime()
}

func (a *vectorArrayTable) close() error {
	if a.dataFile == nil {
		return nil
	}
	if err := a.dataFile.Close(); err != nil {
		return err
	}
	if err := a.idxFile.Close(); err != nil {
		return err
	}
	a.dataFile = nil
	a.idxFile = nil
	a.zsetIndex = nil
	a.hashSetItemIndex = nil
	return nil
}

func (a *vectorArrayTable) readyForFlush() bool {
	return true
}

func (a *vectorArrayTable) getPath() string {
	return a.dataPath
}

func (a *vectorArrayTable) getFilePath() []string {
	return []string{a.dataPath, a.idxPath}
}

func (a *vectorArrayTable) idxFilePath() string {
	return ""
}

func (a *vectorArrayTable) empty() bool {
	return a.header.num == 0
}

func (a *vectorArrayTable) getFileType() FileType {
	return fileTypeVectorArrayTable
}

func (a *vectorArrayTable) headerInfo() string {
	return fmt.Sprintf("keys:%d(h:%d s:%d l:%d zd:%d zi:%d e:%d) idxSize:%s(hii:%s hvi:%s loi:%s zoi:%s zvi:%s) dataSize:%s",
		a.header.num,
		a.keyStat.hashKeys,
		a.keyStat.setKeys,
		a.keyStat.listKeys,
		a.keyStat.zsetKeys,
		a.keyStat.zsetIndexKeys,
		a.keyStat.expireKeys,
		utils.FmtSize(int64(a.idxSize)),
		utils.FmtSize(int64(a.header.hashItemIndexSize)),
		utils.FmtSize(int64(a.header.hashSetListVersionIndexSize)),
		utils.FmtSize(int64(a.header.listOffsetIndexSize)),
		utils.FmtSize(int64(a.header.zsetOffsetIndexSize)),
		utils.FmtSize(int64(a.header.zsetVersionIndexSize)),
		utils.FmtSize(int64(a.dataSize)))
}

func estimateVatItemSize(dataType uint8, key, value []byte) int {
	sz := 0
	switch dataType {
	case kkv.DataTypeZset:
		sz += vatZdataItemLength
	case kkv.DataTypeExpireKey:
		sz += vatExpireKeySize
	default:
		sz += sizeVarint(uint64(len(key))) + sizeVarint(uint64(len(value))) + 1
	}

	return sz
}
