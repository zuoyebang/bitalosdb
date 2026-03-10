// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitpage

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/iterator"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

const (
	xtiHeaderSize            = 28
	xtiDataOffset            = xtiHeaderSize
	xtiOffsetIndexLength     = 4
	xtiVersionIndexLength    = 12
	xtiKeyVersionLength      = 8
	xtiVersionIndexMiniCount = 192
	xtiVersionIndexMiniMax   = 256
)

type stIdxHeader struct {
	version   uint16
	useMiniVi uint16
	fileSize  uint32
	dataSize  uint32
	oiOffset  uint32
	oiSize    uint32
	viOffset  uint32
	viSize    uint32
}

type stIndexes struct {
	oi    []uint32
	oiNum int
	vi    []byte
	viNum int
}

func (s *superTable) readIndexes() *stIndexes {
	ptr := s.reading.Load()
	if ptr == nil {
		return nil
	}
	return ptr
}

func (s *superTable) mergeIndexes() error {
	if len(s.offsetPending) == 0 {
		return nil
	}

	if err := s.dataFile.SyncAndMmapReadGrow(); err != nil {
		return err
	}

	var lastVersion []byte
	var posBuf [xtiOffsetIndexLength]byte
	var offset uint32
	var oldOi []uint32

	newIndexes := &stIndexes{}
	oiPendingPos := 0
	oiPendingNum := len(s.offsetPending)
	oiPendingNext := s.getKey(s.offsetPending[0])
	oldIndexes := s.readIndexes()
	if oldIndexes != nil {
		oldOi = oldIndexes.oi
		if s.isUseVi && oldIndexes.viNum > 0 {
			newIndexes.vi = make([]byte, 0, oldIndexes.viNum*xtiVersionIndexLength)
		}
		newIndexes.oi = make([]uint32, 0, oldIndexes.oiNum+oiPendingNum)
	}

	addIndexes := func(key []byte, off uint32) {
		if s.isUseVi {
			keyVersion := kkv.GetKeyVersion(key)
			if !bytes.Equal(lastVersion, keyVersion) {
				lastVersion = keyVersion
				binary.BigEndian.PutUint32(posBuf[:], uint32(newIndexes.oiNum))
				newIndexes.vi = append(newIndexes.vi, keyVersion...)
				newIndexes.vi = append(newIndexes.vi, posBuf[:]...)
				newIndexes.viNum++
			}
		}
		newIndexes.oi = append(newIndexes.oi, off)
		newIndexes.oiNum++
	}

	oldOiNum := len(oldOi)
	oldOiPos := -1
	if oldOiNum > 0 {
		oldOiPos = binarySearch(oldOiNum, func(i int) int {
			ikey := s.getKey(oldOi[i])
			return bytes.Compare(ikey.UserKey, oiPendingNext.UserKey)
		})
	}

	for i := 0; i < oldOiPos; i++ {
		offset = oldOi[i]
		ikey := s.getKey(offset)
		addIndexes(ikey.UserKey, offset)
	}

	if oldOiPos >= 0 && oldOiPos < oldOiNum {
		oldNextKey := s.getKey(oldOi[oldOiPos])
		for {
			cmp := bytes.Compare(oldNextKey.UserKey, oiPendingNext.UserKey)
			if cmp < 0 {
				addIndexes(oldNextKey.UserKey, oldOi[oldOiPos])
				oldOiPos++
				if oldOiPos == oldOiNum {
					break
				}
				oldNextKey = s.getKey(oldOi[oldOiPos])
			} else if cmp > 0 {
				addIndexes(oiPendingNext.UserKey, s.offsetPending[oiPendingPos])
				oiPendingPos++
				if oiPendingPos == oiPendingNum {
					break
				}
				oiPendingNext = s.getKey(s.offsetPending[oiPendingPos])
			} else {
				addIndexes(oldNextKey.UserKey, s.offsetPending[oiPendingPos])

				if s.p != nil && s.p.bp != nil {
					s.p.bp.deleteBithashKey(s.getValue(oldOi[oldOiPos]))
				}

				oldOiPos++
				oiPendingPos++
				if oldOiPos == oldOiNum || oiPendingPos == oiPendingNum {
					break
				}

				oldNextKey = s.getKey(oldOi[oldOiPos])
				oiPendingNext = s.getKey(s.offsetPending[oiPendingPos])
			}
		}
	}

	for oldOiPos >= 0 && oldOiPos < oldOiNum {
		offset = oldOi[oldOiPos]
		ikey := s.getKey(offset)
		addIndexes(ikey.UserKey, offset)
		oldOiPos++
	}

	for oiPendingPos < oiPendingNum {
		offset = s.offsetPending[oiPendingPos]
		ikey := s.getKey(offset)
		addIndexes(ikey.UserKey, offset)
		oiPendingPos++
	}

	if s.isUseMiniVi && newIndexes.viNum > xtiVersionIndexMiniMax {
		s.mergeMiniVersionIndex(newIndexes)
	}

	s.reading.Store(newIndexes)
	s.offsetPending = s.offsetPending[:0]
	s.indexModified = true
	return nil
}

func (s *superTable) mergeMiniVersionIndex(indexes *stIndexes) {
	var keyCount, prevOiPos, nextOiPos uint32
	step := uint32(indexes.oiNum / xtiVersionIndexMiniCount)
	newVi := make([]byte, 0, xtiVersionIndexMiniCount*xtiVersionIndexLength)
	newVi = append(newVi, indexes.vi[0:xtiVersionIndexLength]...)
	prevOiPos = binary.BigEndian.Uint32(indexes.vi[xtiKeyVersionLength:])
	viPos := xtiVersionIndexLength
	newViNum := 1
	for i := 1; i < indexes.viNum; i++ {
		nextOiPos = binary.BigEndian.Uint32(indexes.vi[viPos+xtiKeyVersionLength:])
		keyCount += nextOiPos - prevOiPos
		if keyCount >= step || i == indexes.viNum-1 {
			newVi = append(newVi, indexes.vi[viPos:viPos+xtiVersionIndexLength]...)
			newViNum++
			keyCount = 0
		}
		prevOiPos = nextOiPos
		viPos += xtiVersionIndexLength
	}

	indexes.vi = nil
	indexes.vi = newVi
	indexes.viNum = newViNum
}

func (s *superTable) loadIdxFile() error {
	if os2.IsNotExist(s.idxPath) {
		if s.dataFile.empty() {
			return nil
		}
		return s.rebuildIndexes()
	}

	if err := s.openIdxFile(); err != nil {
		s.p.bp.opts.Logger.Errorf("superTable openIdxFile fail file:%s err:%v", s.id, err)
		return s.rebuildIndexes()
	}

	return nil
}

func (s *superTable) openIdxFile() error {
	idxFile, err := os.OpenFile(s.idxPath, os.O_RDONLY, consts.FileMode)
	if err != nil {
		return err
	}
	defer idxFile.Close()

	var headerBuf [xtiHeaderSize]byte
	if n, err := idxFile.ReadAt(headerBuf[:], 0); err != nil {
		return err
	} else if n != xtiHeaderSize {
		return errors.Errorf("header readAt fail n:%d", n)
	}

	pos := 0
	s.idxHeader.version = binary.BigEndian.Uint16(headerBuf[pos:])
	pos += 2
	s.idxHeader.useMiniVi = binary.BigEndian.Uint16(headerBuf[pos:])
	pos += 2
	s.idxHeader.fileSize = binary.BigEndian.Uint32(headerBuf[pos:])
	pos += 4
	s.idxHeader.dataSize = binary.BigEndian.Uint32(headerBuf[pos:])
	pos += 4
	s.idxHeader.oiOffset = binary.BigEndian.Uint32(headerBuf[pos:])
	pos += 4
	s.idxHeader.oiSize = binary.BigEndian.Uint32(headerBuf[pos:])
	pos += 4
	s.idxHeader.viOffset = binary.BigEndian.Uint32(headerBuf[pos:])
	pos += 4
	s.idxHeader.viSize = binary.BigEndian.Uint32(headerBuf[pos:])

	fstat, _ := idxFile.Stat()
	idxFileSize := fstat.Size()
	if idxFileSize != int64(s.idxHeader.fileSize) {
		return errors.Errorf("check fileSize not eq exp:%d act:%d", idxFileSize, s.idxHeader.fileSize)
	}
	if s.idxHeader.dataSize != s.dataFile.Size() {
		return errors.Errorf("check dataSize not eq exp:%d act:%d", s.dataFile.Size(), s.idxHeader.dataSize)
	}

	useMiniVi := false
	if s.idxHeader.useMiniVi == 1 {
		useMiniVi = true
	}
	if useMiniVi != s.isUseMiniVi {
		return errors.Errorf("check useMiniVi changed exp:%v act:%v", useMiniVi, s.isUseMiniVi)
	}

	indexes := stIndexes{
		oiNum: int(s.idxHeader.oiSize) / xtiOffsetIndexLength,
		oi:    nil,
		viNum: int(s.idxHeader.viSize) / xtiVersionIndexLength,
		vi:    nil,
	}

	if s.idxHeader.oiSize > 0 {
		oiBuf := make([]byte, s.idxHeader.oiSize)
		if n, err := idxFile.ReadAt(oiBuf, xtiDataOffset); err != nil {
			return err
		} else if n != int(s.idxHeader.oiSize) {
			return errors.Errorf("read offsetIndex fail exp:%d act:%d", s.idxHeader.oiSize, n)
		}

		indexes.oi = make([]uint32, indexes.oiNum)
		pos = 0
		for i := 0; i < indexes.oiNum; i++ {
			indexes.oi[i] = binary.BigEndian.Uint32(oiBuf[pos : pos+4])
			pos += 4
		}
	}

	if s.idxHeader.viSize > 0 {
		indexes.vi = make([]byte, s.idxHeader.viSize)
		if n, err := idxFile.ReadAt(indexes.vi, int64(xtiDataOffset+s.idxHeader.oiSize)); err != nil {
			return err
		} else if n != int(s.idxHeader.viSize) {
			return errors.Errorf("read versionIndex fail exp:%d n:%d", s.idxHeader.viSize, n)
		}
	}

	s.reading.Store(&indexes)
	return nil
}

func (s *superTable) writeIdxToFile() error {
	if !s.indexModified {
		return nil
	}

	idxFile, err := os.OpenFile(s.idxPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, consts.FileMode)
	if err != nil {
		return err
	}

	indexes := s.readIndexes()
	s.idxHeader.oiOffset = xtiDataOffset
	if s.isUseMiniVi {
		s.idxHeader.useMiniVi = 1
	} else {
		s.idxHeader.useMiniVi = 0
	}
	s.idxHeader.oiSize = uint32(indexes.oiNum) * xtiOffsetIndexLength
	s.idxHeader.viOffset = s.idxHeader.oiOffset + s.idxHeader.oiSize
	s.idxHeader.viSize = uint32(len(indexes.vi))
	idxSize := s.idxHeader.viOffset + s.idxHeader.viSize
	dataSize := s.dataFile.Size()
	if dataSize > 0 && indexes.oiNum == 0 {
		dataSize = 0
	}
	s.idxHeader.dataSize = dataSize
	s.idxHeader.fileSize = idxSize
	buf := make([]byte, idxSize)
	pos := 0
	binary.BigEndian.PutUint16(buf[pos:], s.idxHeader.version)
	pos += 2
	binary.BigEndian.PutUint16(buf[pos:], s.idxHeader.useMiniVi)
	pos += 2
	binary.BigEndian.PutUint32(buf[pos:], s.idxHeader.fileSize)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], s.idxHeader.dataSize)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], s.idxHeader.oiOffset)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], s.idxHeader.oiSize)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], s.idxHeader.viOffset)
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], s.idxHeader.viSize)
	pos += 4
	for i := 0; i < indexes.oiNum; i++ {
		binary.BigEndian.PutUint32(buf[pos:pos+4], indexes.oi[i])
		pos += 4
	}
	copy(buf[pos:], indexes.vi)

	if _, err = idxFile.WriteAt(buf, 0); err != nil {
		return err
	}
	if err = idxFile.Sync(); err != nil {
		return err
	}
	if err = idxFile.Close(); err != nil {
		return err
	}

	s.indexModified = false

	//s.p.bp.opts.Logger.Infof("superTable writeIdxFile finish file:%s idxSize:%d dataSize:%d oiOffset:%d viOffset:%d oiNum:%d viNum:%d",
	//	s.id, idxSize, dataSize,
	//	s.idxHeader.oiOffset,
	//	s.idxHeader.viOffset,
	//	indexes.oiNum,
	//	indexes.viNum)
	return nil
}

func (s *superTable) rebuildIndexes() (err error) {
	var n int
	dataFileSize := int(s.dataFile.Size())
	dataReadBuf := s.p.bp.getStKeyArenaBuf(dataFileSize)
	dataReadBuf = dataReadBuf[:dataFileSize]
	n, err = s.dataFile.readAt(dataReadBuf, 0)
	if err != nil {
		return err
	} else if n != dataFileSize {
		return errors.Errorf("superTable rebuild indexes dataFile return not eq %d != %d", n, dataFileSize)
	}

	var prevKey []byte
	var keyOffset, offset int
	var keySize uint32
	var valueSize uint16
	var its []internalIterator

	sstIter := &sstIterator{}
	offset = TblFileHeaderSize
	for {
		keyOffset = offset
		if len(dataReadBuf[offset:]) < TblItemKeySize {
			break
		}

		keySize, valueSize = decodeKeySize(dataReadBuf[offset:])
		if keySize == 0 {
			break
		}
		kz := int(keySize) + int(valueSize)
		if valueSize == 0 {
			offset += TblItemKeySize
		} else {
			offset += TblItemHeaderSize
		}

		if offset > dataFileSize || len(dataReadBuf[offset:]) < kz {
			break
		}

		ikey := base.DecodeInternalKey(dataReadBuf[offset : offset+int(keySize)])
		offset += kz

		if prevKey != nil && bytes.Compare(prevKey, ikey.UserKey) >= 0 {
			its = append(its, sstIter)
			sstIter = &sstIterator{}
		}
		sstIter.data = append(sstIter.data, sstItem{
			key:    ikey,
			offset: uint32(keyOffset),
		})
		sstIter.num += 1
		prevKey = ikey.UserKey
	}

	s.dataFile.setOffset(uint32(keyOffset))
	if err = s.dataFile.SeekWriter(); err != nil {
		return err
	}

	if len(sstIter.data) > 0 {
		its = append(its, sstIter)
	}
	if len(its) == 0 {
		return nil
	}
	iiter := iterator.NewMergingIter(s.logger, s.p.bp.opts.Cmp, its...)
	iter := &sstCompactionIter{
		iter: iiter,
	}
	defer iter.Close()

	for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
		s.offsetPending = append(s.offsetPending, binary.BigEndian.Uint32(iv))
	}

	if err = s.mergeIndexes(); err != nil {
		return err
	}

	s.logger.Infof("superTable rebuild indexes finish file:%s its:%d keyOffset:%d", s.id, len(its), keyOffset)

	return nil
}

type sstItem struct {
	key    internalKey
	offset uint32
}

type sstIterator struct {
	data     []sstItem
	num      int
	iterPos  int
	iterItem *sstItem
}

func (i *sstIterator) findItem() (*internalKey, []byte) {
	if i.iterPos < 0 || i.iterPos >= i.num {
		return nil, nil
	}

	item := i.data[i.iterPos]
	return &(item.key), utils.Uint32ToBytes(item.offset)
}

func (i *sstIterator) First() (*internalKey, []byte) {
	i.iterPos = 0
	return i.findItem()
}

func (i *sstIterator) Next() (*internalKey, []byte) {
	i.iterPos++
	return i.findItem()
}

func (i *sstIterator) Prev() (*internalKey, []byte) {
	i.iterPos--
	return i.findItem()
}

func (i *sstIterator) Last() (*internalKey, []byte) {
	i.iterPos = i.num - 1
	return i.findItem()
}

func (i *sstIterator) SeekGE(key []byte) (*internalKey, []byte) {
	return nil, nil
}

func (i *sstIterator) SeekLT(key []byte) (*internalKey, []byte) {
	return nil, nil
}

func (i *sstIterator) SetBounds(lower, upper []byte) {
}

func (i *sstIterator) Error() error {
	return nil
}

func (i *sstIterator) Close() error {
	i.data = nil
	i.iterItem = nil
	return nil
}

func (i *sstIterator) String() string {
	return "sstIterator"
}

type sstCompactionIter struct {
	iter      internalIterator
	key       internalKey
	iterKey   *internalKey
	iterValue []byte
	skip      bool
	pos       iterPos
}

func (i *sstCompactionIter) First() (*internalKey, []byte) {
	i.iterKey, i.iterValue = i.iter.First()
	i.pos = iterPosNext
	return i.Next()
}

func (i *sstCompactionIter) Next() (*internalKey, []byte) {
	if i.pos == iterPosCurForward {
		if i.skip {
			i.skipInStripe()
		} else {
			i.nextInStripe()
		}
	}

	i.pos = iterPosCurForward
	if i.iterKey == nil {
		return nil, nil
	}

	i.key.UserKey = i.iterKey.UserKey
	i.key.Trailer = i.iterKey.Trailer
	i.skip = true
	return &i.key, i.iterValue
}

func (i *sstCompactionIter) skipInStripe() {
	i.skip = true
	var change stripeChangeType
	for {
		change = i.nextInStripe()
		if change == sameStripeNonSkippable || change == newStripe {
			break
		}
	}

	if change == newStripe {
		i.skip = false
	}
}

func (i *sstCompactionIter) nextInStripe() stripeChangeType {
	i.iterKey, i.iterValue = i.iter.Next()
	if i.iterKey == nil || !bytes.Equal(i.key.UserKey, i.iterKey.UserKey) {
		return newStripe
	}

	if i.iterKey.Kind() == internalKeyKindInvalid {
		return sameStripeNonSkippable
	}

	return sameStripeSkippable
}

func (i *sstCompactionIter) Close() error {
	return i.iter.Close()
}
