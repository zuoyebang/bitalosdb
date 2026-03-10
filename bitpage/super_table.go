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
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

type superTable struct {
	p              *page
	id             string
	logger         base.Logger
	dataFile       *TableWriter
	dataPath       string
	idxPath        string
	idxHeader      *stIdxHeader
	fn             FileNum
	totalCount     int
	delCount       int
	prefixDelCount int
	indexModified  bool
	offsetPending  []uint32
	reading        atomic.Pointer[stIndexes]
	isUseVi        bool
	isUseMiniVi    bool
}

func checkSuperTable(obj interface{}) {
	s := obj.(*superTable)
	if s.dataFile.Table != nil {
		fmt.Fprintf(os.Stderr, "superTable(%s) buffer was not freed\n", s.dataFile.getPath())
		os.Exit(1)
	}
}

func newSuperTable(p *page, path string, fn FileNum, exist bool) (*superTable, error) {
	tableOpts := &TableOptions{
		OpenType:     TableTypeWriteDisk,
		InitMmapSize: consts.BitpageReadMmapSize,
		FileExist:    exist,
	}
	dataFile, err := NewTableWriter(path, tableOpts)
	if err != nil {
		return nil, err
	}
	if err = dataFile.SetWriter(consts.BitpageStBufioWriterSize); err != nil {
		return nil, err
	}
	if err = dataFile.MmapReadGrow(); err != nil {
		return nil, err
	}

	st := &superTable{
		p:             p,
		logger:        p.bp.opts.Logger,
		dataPath:      path,
		dataFile:      dataFile,
		fn:            fn,
		id:            base.GetFilePathBase(path),
		indexModified: false,
		isUseVi:       p.bp.opts.UseVi,
		isUseMiniVi:   false,
		idxHeader:     &stIdxHeader{},
	}

	if p.bp.opts.UseVi && !p.bp.opts.BitpageDisableMiniVi {
		st.isUseMiniVi = true
	}

	st.idxPath = st.getIdxFilePath()
	if exist {
		if err = st.loadIdxFile(); err != nil {
			return nil, err
		}
	}

	return st, nil
}

func (s *superTable) getID() string {
	return s.id
}

func (s *superTable) set(key internalKey, value ...[]byte) error {
	offset, err := s.dataFile.SetKeyValue(key, value...)
	if err != nil {
		return err
	}

	s.offsetPending = append(s.offsetPending, offset)

	return nil
}

func (s *superTable) get(key []byte, _ uint32) ([]byte, bool, internalKeyKind) {
	offset, found := s.findKeyInIndexes(key)
	if !found {
		return nil, false, internalKeyKindInvalid
	}

	ikey := s.getKey(offset)
	if !bytes.Equal(ikey.UserKey, key) {
		return nil, false, internalKeyKindInvalid
	}

	kind := ikey.Kind()
	switch kind {
	case internalKeyKindDelete, internalKeyKindPrefixDelete:
		return nil, true, kind
	default:
		return s.getValue(offset), true, kind
	}
}

func (s *superTable) exist(key []byte, _ uint32) (bool, internalKeyKind) {
	offset, found := s.findKeyInIndexes(key)
	if found {
		ikey := s.getKey(offset)
		if bytes.Equal(ikey.UserKey, key) {
			return true, ikey.Kind()
		}
	}

	return false, internalKeyKindInvalid
}

func (s *superTable) findKeyInIndexes(key []byte) (uint32, bool) {
	indexes := s.readIndexes()
	if indexes == nil || indexes.oiNum == 0 {
		return 0, false
	}

	var startPos, endPos, num int
	if s.isUseVi {
		var found bool
		startPos, endPos, found = s.findKeyInVersionIndex(indexes, key)
		if !found {
			return 0, false
		}
		num = endPos - startPos + 1
	} else {
		num = indexes.oiNum
	}

	pos := binarySearch(num, func(j int) int {
		offset := indexes.oi[startPos+j]
		ikey := s.getKey(offset)
		return bytes.Compare(ikey.UserKey, key)
	})
	if pos == num {
		return 0, false
	}

	offset := indexes.oi[startPos+pos]
	return offset, true
}

func (s *superTable) findKeyInVersionIndex(indexes *stIndexes, key []byte) (int, int, bool) {
	if indexes.viNum == 0 {
		return 0, 0, false
	}

	var startPos, endPos int

	keyVersion := kkv.GetKeyVersion(key)
	if s.isUseMiniVi {
		pos := binarySearch(indexes.viNum, func(j int) int {
			p := j * xtiVersionIndexLength
			return bytes.Compare(indexes.vi[p:p+xtiKeyVersionLength], keyVersion)
		})
		if pos == indexes.viNum {
			return 0, 0, false
		}

		p := pos * xtiVersionIndexLength
		if bytes.Equal(indexes.vi[p:p+xtiKeyVersionLength], keyVersion) {
			startPos = int(binary.BigEndian.Uint32(indexes.vi[p+xtiKeyVersionLength:]))
			if pos < indexes.viNum-1 {
				endPos = int(binary.BigEndian.Uint32(indexes.vi[p+xtiVersionIndexLength+xtiKeyVersionLength:])) - 1
			} else {
				endPos = indexes.oiNum - 1
			}
		} else {
			if p == 0 {
				return 0, 0, false
			}
			startPos = int(binary.BigEndian.Uint32(indexes.vi[p-xtiVersionIndexLength+xtiKeyVersionLength:]))
			endPos = int(binary.BigEndian.Uint32(indexes.vi[p+xtiKeyVersionLength:])) - 1
		}
	} else {
		pos, found := binaryFind(indexes.viNum, func(j int) int {
			p := j * xtiVersionIndexLength
			return bytes.Compare(keyVersion, indexes.vi[p:p+xtiKeyVersionLength])
		})
		if !found {
			return 0, 0, false
		}

		p := pos * xtiVersionIndexLength
		startPos = int(binary.BigEndian.Uint32(indexes.vi[p+xtiKeyVersionLength:]))
		if pos < indexes.viNum-1 {
			endPos = int(binary.BigEndian.Uint32(indexes.vi[p+xtiVersionIndexLength+xtiKeyVersionLength:])) - 1
		} else {
			endPos = indexes.oiNum - 1
		}
	}

	return startPos, endPos, true
}

func (s *superTable) getKey(offset uint32) internalKey {
	return s.dataFile.GetIKey(offset)
}

func (s *superTable) getValue(offset uint32) []byte {
	return s.dataFile.GetValue(offset)
}

func (s *superTable) getMaxKey() []byte {
	indexes := s.readIndexes()
	if indexes == nil || indexes.oiNum == 0 {
		return nil
	}
	offset := indexes.oi[indexes.oiNum-1]
	ikey := s.getKey(offset)
	return ikey.UserKey
}

func (s *superTable) kindStatis(kind internalKeyKind) {
	s.totalCount++
	switch kind {
	case internalKeyKindDelete:
		s.delCount++
	case internalKeyKindPrefixDelete:
		s.prefixDelCount++
	}
}

func (s *superTable) getKeyStats() (int, int, int) {
	return s.totalCount, s.delCount, s.prefixDelCount
}

func (s *superTable) itemCount() int {
	indexes := s.readIndexes()
	if indexes == nil {
		return 0
	}
	return len(indexes.oi)
}

func (s *superTable) getModTime() int64 {
	return s.dataFile.getModTime()
}

func (s *superTable) readyForFlush() bool {
	return true
}

func (s *superTable) inuseBytes() uint64 {
	return uint64(s.dataFile.Size())
}

func (s *superTable) dataBytes() uint64 {
	return uint64(s.dataFile.Size())
}

func (s *superTable) empty() bool {
	return s.dataFile.empty()
}

func (s *superTable) getFileType() FileType {
	return fileTypeSuperTable
}

func (s *superTable) close() error {
	if err := s.writeIdxToFile(); err != nil {
		s.logger.Errorf("superTable close writeIdxFile fail file:%s err:%s", s.id, err)
	}

	if err := s.dataFile.Close(); err != nil {
		return err
	}

	s.dataFile = nil
	return nil
}

func (s *superTable) getFilePath() []string {
	return []string{s.dataPath, s.idxPath}
}

func (s *superTable) idxFilePath() string {
	return s.idxPath
}

func (s *superTable) mmapRLock() {
	s.dataFile.MmapRLock()
}

func (s *superTable) mmapRUnLock() {
	s.dataFile.MmapRUnlock()
}

func (s *superTable) flushFinish() error {
	return s.mergeIndexes()
}

func (s *superTable) getIdxFilePath() string {
	return s.p.bp.makeFilePath(fileTypeSuperTableIndex, s.p.pn, s.fn)
}

func (s *superTable) flushIndexes() error {
	panic("implement me")
}
