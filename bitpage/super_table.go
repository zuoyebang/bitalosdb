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
	"os"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/internal/utils"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

const (
	stVersionDefault uint16 = 1 + iota
)

const (
	stHeaderSize          = 8
	stHeaderVersionOffset = 0
	stDataOffset          = stHeaderSize
	stItemKeySize         = 2
	stItemValueSize       = 4
	stItemHeaderSize      = stItemKeySize + stItemValueSize
)

const (
	stiVersionDefault uint16 = 1 + iota
)

const (
	stiHeaderSize           = 14
	stiHeaderVersionOffset  = 0
	stiHeaderFileSizeOffset = 2
	stiHeaderDataSizeOffset = 6
	stiHeaderIdxNumOffset   = 10
	stiIndexesOffset        = stiHeaderSize
)

type stIndexes []uint32

type superTable struct {
	p             *page
	tbl           *table
	writer        *tableWriter
	version       uint16
	fn            FileNum
	totalCount    float64
	delCount      float64
	filename      string
	idxPath       string
	indexModified bool
	reading       atomic.Pointer[stIndexes]
	pending       stIndexes
}

func checkSuperTable(obj interface{}) {
	s := obj.(*superTable)
	if s.tbl != nil {
		fmt.Fprintf(os.Stderr, "superTable(%s) buffer was not freed\n", s.path())
		os.Exit(1)
	}
}

func newSuperTable(p *page, path string, fn FileNum, exist bool) (*superTable, error) {
	tableOpts := &tableOptions{
		openType:     tableWriteDisk,
		initMmapSize: consts.BitpageInitMmapSize,
	}
	tbl, err := openTable(path, tableOpts)
	if err != nil {
		return nil, err
	}

	st := &superTable{
		p:             p,
		tbl:           tbl,
		fn:            fn,
		filename:      base.GetFilePathBase(path),
		pending:       make(stIndexes, 0, 1<<10),
		writer:        newTableWriter(tbl),
		indexModified: false,
	}
	st.idxPath = st.getIdxFilePath()
	if err = st.writer.reset(tbl.filesz); err != nil {
		return nil, err
	}

	if exist {
		if err = st.getHeader(); err != nil {
			return nil, err
		}

		err = st.loadIdxFromFile()
	} else {
		err = st.setHeader()
	}
	if err != nil {
		return nil, err
	}

	return st, nil
}

func (s *superTable) getHeader() error {
	var header [stHeaderSize]byte
	n, err := s.tbl.file.ReadAt(header[:], 0)
	if err != nil {
		return err
	}
	if n != stHeaderSize {
		return errors.Errorf("bitpage: superTable read header err n:%d", n)
	}

	s.version = binary.BigEndian.Uint16(header[stHeaderVersionOffset:])
	return nil
}

func (s *superTable) setHeader() error {
	version := stVersionDefault

	var header [stHeaderSize]byte
	for i := range header {
		header[i] = 0
	}
	binary.BigEndian.PutUint16(header[stHeaderVersionOffset:], version)
	n, err := s.writer.writer.Write(header[:])
	if err != nil {
		return err
	}
	if n != stHeaderSize {
		return errors.Errorf("bitpage: superTable write header err n:%d", n)
	}
	if err = s.writer.fdatasync(); err != nil {
		return err
	}

	s.version = version
	s.tbl.offset.Add(uint32(n))
	s.grow(n)
	return nil
}

func (s *superTable) set(key internalKey, value []byte) error {
	offset, err := s.writer.set(key, value)
	if err != nil {
		return err
	}

	s.pending = append(s.pending, offset)
	return nil
}

func (s *superTable) get(key []byte, _ uint32) ([]byte, bool, internalKeyKind, func()) {
	indexes := s.readIndexes()
	pos := s.findKeyIndexPos(indexes, key)
	if pos < 0 || pos >= len(indexes) {
		return nil, false, internalKeyKindInvalid, nil
	}

	ikey, value := s.getItem(indexes[pos])
	if !bytes.Equal(ikey.UserKey, key) {
		return nil, false, internalKeyKindInvalid, nil
	}

	return value, true, ikey.Kind(), nil
}

func (s *superTable) getKeyByPos(indexes stIndexes, pos int) internalKey {
	if pos < 0 || pos >= len(indexes) {
		return internalKey{}
	}
	return s.getKey(indexes[pos])
}

func (s *superTable) getKey(offset uint32) internalKey {
	keySize := uint32(binary.BigEndian.Uint16(s.tbl.getBytes(offset, stItemKeySize)))
	key := s.tbl.getBytes(offset+stItemHeaderSize, keySize)
	return base.DecodeInternalKey(key)
}

func (s *superTable) getValue(offset uint32) []byte {
	keySize := uint32(binary.BigEndian.Uint16(s.tbl.getBytes(offset, stItemKeySize)))
	valueSize := binary.BigEndian.Uint32(s.tbl.getBytes(offset+stItemKeySize, stItemValueSize))
	value := s.tbl.getBytes(offset+stItemHeaderSize+keySize, valueSize)
	return value
}

func (s *superTable) getItem(offset uint32) (internalKey, []byte) {
	keySize := uint32(binary.BigEndian.Uint16(s.tbl.getBytes(offset, stItemKeySize)))
	key := s.tbl.getBytes(offset+stItemHeaderSize, keySize)
	valueSize := binary.BigEndian.Uint32(s.tbl.getBytes(offset+stItemKeySize, stItemValueSize))
	value := s.tbl.getBytes(offset+stItemHeaderSize+keySize, valueSize)
	return base.DecodeInternalKey(key), value
}

func (s *superTable) findKeyIndexPos(indexes stIndexes, key []byte) int {
	num := len(indexes)
	if num == 0 {
		return -1
	}

	return sort.Search(num, func(i int) bool {
		ikey := s.getKeyByPos(indexes, i)
		return bytes.Compare(ikey.UserKey, key) != -1
	})
}

func (s *superTable) newIter(o *iterOptions) internalIterator {
	iter := &superTableIterator{
		st:      s,
		indexes: s.readIndexes(),
	}
	return iter
}

func (s *superTable) kindStatis(kind internalKeyKind) {
	s.totalCount++
	if kind == internalKeyKindDelete {
		s.delCount++
	}
}

func (s *superTable) delPercent() float64 {
	if s.delCount == 0 {
		return 0
	}
	return s.delCount / s.totalCount
}

func (s *superTable) itemCount() int {
	return len(s.readIndexes())
}

func (s *superTable) readyForFlush() bool {
	return true
}

func (s *superTable) inuseBytes() uint64 {
	return uint64(s.tbl.Size())
}

func (s *superTable) dataBytes() uint64 {
	return uint64(s.tbl.Size())
}

func (s *superTable) empty() bool {
	return s.tbl.Size() == stHeaderSize
}

func (s *superTable) close() error {
	if err := s.writeIdxToFile(); err != nil {
		return err
	}

	if err := s.tbl.close(); err != nil {
		return err
	}

	s.tbl = nil
	return nil
}

func (s *superTable) path() string {
	if s.tbl == nil {
		return ""
	}
	return s.tbl.path
}

func (s *superTable) idxFilePath() string {
	return s.idxPath
}

func (s *superTable) mmapRLock() {
	s.tbl.mmaplock.RLock()
}

func (s *superTable) mmapRUnLock() {
	s.tbl.mmaplock.RUnlock()
}

func (s *superTable) grow(sz int) {
	if sz > s.tbl.filesz {
		s.tbl.filesz = sz
	}
}

func (s *superTable) mergeIndexes() error {
	if len(s.pending) == 0 {
		return nil
	}

	if err := s.writer.fdatasync(); err != nil {
		return err
	}

	if _, err := s.tbl.mmapReadExpand(); err != nil {
		return err
	}

	oldIndexes := s.readIndexes()
	oldEnd := len(oldIndexes)
	pendingEnd := len(s.pending)
	pendingCurrent := 0
	pendingNextKey := s.getKey(s.pending[0])
	oldCurrent := s.findKeyIndexPos(oldIndexes, pendingNextKey.UserKey)

	newIndexes := make(stIndexes, 0, oldEnd+pendingEnd)
	addIndexes := func(index uint32) {
		newIndexes = append(newIndexes, index)
	}

	for i := 0; i <= oldCurrent-1; i++ {
		addIndexes(oldIndexes[i])
	}

	if oldCurrent >= 0 && oldCurrent < oldEnd {
		oldNextKey := s.getKey(oldIndexes[oldCurrent])
		for {
			cmp := bytes.Compare(oldNextKey.UserKey, pendingNextKey.UserKey)
			if cmp < 0 {
				addIndexes(oldIndexes[oldCurrent])
				oldCurrent++
				if oldCurrent >= oldEnd {
					break
				}
				oldNextKey = s.getKey(oldIndexes[oldCurrent])
			} else if cmp > 0 {
				addIndexes(s.pending[pendingCurrent])
				pendingCurrent++
				if pendingCurrent >= pendingEnd {
					break
				}
				pendingNextKey = s.getKey(s.pending[pendingCurrent])
			} else {
				addIndexes(s.pending[pendingCurrent])

				if s.p != nil && s.p.bp != nil {
					s.p.bp.deleteBithashKey(s.getValue(oldIndexes[oldCurrent]))
				}

				oldCurrent++
				pendingCurrent++
				if oldCurrent >= oldEnd || pendingCurrent >= pendingEnd {
					break
				}

				oldNextKey = s.getKey(oldIndexes[oldCurrent])
				pendingNextKey = s.getKey(s.pending[pendingCurrent])
			}
		}
	}

	for oldCurrent >= 0 && oldCurrent < oldEnd {
		addIndexes(oldIndexes[oldCurrent])
		oldCurrent++
	}

	for pendingCurrent < pendingEnd {
		addIndexes(s.pending[pendingCurrent])
		pendingCurrent++
	}

	s.indexModified = true
	s.reading.Store(&newIndexes)
	s.pending = s.pending[:0]
	s.grow(int(s.tbl.Size()))

	return nil
}

func (s *superTable) readIndexes() stIndexes {
	ptr := s.reading.Load()
	if ptr == nil {
		return nil
	}
	return *ptr
}

func (s *superTable) getIdxFilePath() string {
	return s.p.bp.makeFilePath(fileTypeSuperTableIndex, s.p.pn, s.fn)
}

func (s *superTable) loadIdxFromFile() error {
	if utils.IsFileNotExist(s.idxPath) {
		return s.rebuildIndexes()
	}

	err := func() error {
		idxFile, err := os.OpenFile(s.idxPath, os.O_CREATE|os.O_RDONLY, consts.FileMode)
		if err != nil {
			return err
		}

		fstat, err := idxFile.Stat()
		if err != nil {
			return err
		}
		idxFileSize := fstat.Size()
		if idxFileSize < stiHeaderSize {
			return errors.Errorf("bitpage: superTable header size small size:%d", idxFileSize)
		}

		var header [stiHeaderSize]byte
		n, err := idxFile.ReadAt(header[:], 0)
		if err != nil {
			return err
		}
		if n != stiHeaderSize {
			return errors.Errorf("bitpage: superTable header readAt fail n:%d", n)
		}

		fileSize := binary.BigEndian.Uint32(header[stiHeaderFileSizeOffset:])
		if idxFileSize != int64(fileSize) {
			return errors.Errorf("bitpage: superTable file size not eq fstat:%d rsize:%d", idxFileSize, fileSize)
		}

		stSize := s.tbl.Size()
		dataSize := binary.BigEndian.Uint32(header[stiHeaderDataSizeOffset:])
		if stSize != dataSize {
			return errors.Errorf("bitpage: superTable data size not eq filesz:%d dsize:%d", stSize, dataSize)
		}

		idxNum := int(binary.BigEndian.Uint32(header[stiHeaderIdxNumOffset:]))
		if idxNum > 0 {
			idxSize := idxNum * 4
			idxBuf := make([]byte, idxSize)
			n, err = idxFile.ReadAt(idxBuf, stiIndexesOffset)
			if err != nil {
				return err
			}
			if n != idxSize {
				return errors.Errorf("bitpage: superTable idx readAt fail n:%d exp:%d", n, idxSize)
			}

			indexes := make(stIndexes, idxNum)
			pos := 0
			for i := 0; i < idxNum; i++ {
				indexes[i] = binary.BigEndian.Uint32(idxBuf[pos : pos+4])
				pos += 4
			}

			s.reading.Store(&indexes)
		} else {
			s.reading.Store(nil)
		}

		s.p.bp.opts.Logger.Infof("superTable read indexes success file:%s idxNum:%d", s.filename, idxNum)
		return idxFile.Close()
	}()
	if err == nil {
		return nil
	}

	s.p.bp.opts.Logger.Errorf("superTable load indexes file fail file:%s err:%v", s.filename, err)
	return s.rebuildIndexes()
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
	idxNum := len(indexes)
	fileSize := stiHeaderSize + idxNum*4
	dataSize := s.tbl.Size()
	if dataSize > 0 && idxNum == 0 {
		dataSize = 0
	}

	buf := make([]byte, fileSize)
	binary.BigEndian.PutUint16(buf[stiHeaderVersionOffset:], stiVersionDefault)
	binary.BigEndian.PutUint32(buf[stiHeaderFileSizeOffset:], uint32(fileSize))
	binary.BigEndian.PutUint32(buf[stiHeaderDataSizeOffset:], dataSize)
	binary.BigEndian.PutUint32(buf[stiHeaderIdxNumOffset:], uint32(idxNum))
	pos := stiIndexesOffset
	for i := 0; i < idxNum; i++ {
		binary.BigEndian.PutUint32(buf[pos:pos+4], indexes[i])
		pos += 4
	}

	if _, err = idxFile.Write(buf); err != nil {
		return err
	}
	if err = idxFile.Sync(); err != nil {
		return err
	}
	if err = idxFile.Close(); err != nil {
		return err
	}

	s.indexModified = false
	s.p.bp.opts.Logger.Infof("superTable write indexes finish file:%s filesz:%d dsize:%d idxNum:%d", s.filename, fileSize, dataSize, idxNum)
	return nil
}
