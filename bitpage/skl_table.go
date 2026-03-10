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
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/zuoyebang/bitalosdb/v2/bitpage/sklindex"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
)

const (
	stiVersionDefault uint16 = 1 + iota
)

const (
	stIdxHeaderSize           = 14
	stIdxHeaderVersionOffset  = 0
	stIdxHeaderFileSizeOffset = 2
	stIdxHeaderDataSizeOffset = 6
	stIdxHeaderIdxNumOffset   = 10
	stIdxIndexesOffset        = stIdxHeaderSize
)

type sklTable struct {
	p              *page
	id             string
	dataFile       *TableWriter
	dataPath       string
	idxPath        string
	version        uint16
	fn             FileNum
	totalCount     int
	delCount       int
	prefixDelCount int
	indexModified  bool
	pending        []uint32

	skli struct {
		sync.RWMutex
		l       *sklindex.Skiplist
		maxSize uint32
	}
}

func checkSklTable(obj interface{}) {
	s := obj.(*sklTable)
	if s.dataFile.Table != nil {
		fmt.Fprintf(os.Stderr, "sklTable(%s) buffer was not freed\n", s.dataFile.getPath())
		os.Exit(1)
	}
}

func newSklTable(p *page, path string, fn FileNum, exist bool, stiCompressCount uint32) (*sklTable, error) {
	tableOpts := &TableOptions{
		OpenType:     TableTypeWriteDisk,
		InitMmapSize: consts.BitpageReadMmapSize,
		FileExist:    exist,
	}
	dataFile, err := NewTableWriter(path, tableOpts)
	if err != nil {
		return nil, err
	}

	st := &sklTable{
		p:        p,
		dataFile: dataFile,
		dataPath: path,
		fn:       fn,
		id:       base.GetFilePathBase(path),
		version:  dataFile.getVersion(),
		pending:  make([]uint32, 0, stiCompressCount),
	}

	st.idxPath = st.getIdxFilePath()
	st.skli.l = st.newSklIndex()
	st.skli.maxSize = stiCompressCount
	if exist {
		if err = st.loadIdxFromFile(); err != nil {
			return nil, err
		}
	}

	if err = st.dataFile.SetWriter(consts.BitpageStBufioWriterSize); err != nil {
		return nil, err
	}

	if err = st.dataFile.MmapReadGrow(); err != nil {
		return nil, err
	}

	return st, nil
}

func (s *sklTable) getID() string {
	return s.id
}

func (s *sklTable) set(key internalKey, value ...[]byte) error {
	offset, err := s.dataFile.SetKeyValue(key, value...)
	if err != nil {
		return err
	}

	s.pending = append(s.pending, offset)
	return nil
}

func (s *sklTable) flushFinish() error {
	pendingNum := len(s.pending)
	if pendingNum == 0 {
		return nil
	}

	if err := s.dataFile.SyncAndMmapReadGrow(); err != nil {
		return err
	}

	skli, skliCloser := s.getSklIndex()
	defer skliCloser()

	inserter := sklindex.NewSerialInserter()
	for _, offset := range s.pending {
		key := s.dataFile.GetIKey(offset)
		if err := skli.Add(key, offset, inserter); err != nil {
			return err
		}
	}

	if s.skli.l.SkiItemCount() >= s.skli.maxSize {
		s.flushSklIndex(skli)
		skli.Unref()
	}

	s.pending = s.pending[:0]
	s.indexModified = true

	return nil
}

func (s *sklTable) getKey(offset uint32) base.InternalKey {
	return s.dataFile.GetIKey(offset)
}

func (s *sklTable) getValue(offset uint32) []byte {
	return s.dataFile.GetValue(offset)
}

func (s *sklTable) newSklIndex() *sklindex.Skiplist {
	return sklindex.NewSkiplist(s.getKey, s.getValue)
}

func (s *sklTable) getSklIndex() (*sklindex.Skiplist, func()) {
	s.skli.RLock()
	skli := s.skli.l
	skli.Ref()
	s.skli.RUnlock()
	return skli, func() {
		skli.Unref()
	}
}

func (s *sklTable) setSklIndex(l *sklindex.Skiplist) {
	s.skli.Lock()
	s.skli.l = l
	s.skli.Unlock()
}

func (s *sklTable) flushSklIndex(l *sklindex.Skiplist) {
	newSkli := l.FlushToNewSkl()
	s.setSklIndex(newSkli)
}

func (s *sklTable) flushIndexes() error {
	skli, skliCloser := s.getSklIndex()
	s.flushSklIndex(skli)
	skliCloser()
	return nil
}

func (s *sklTable) get(key []byte, _ uint32) ([]byte, bool, internalKeyKind) {
	skli, skliCloser := s.getSklIndex()
	defer skliCloser()
	value, exist, kind, _ := skli.Get(key)
	return value, exist, kind
}

func (s *sklTable) exist(key []byte, _ uint32) (bool, internalKeyKind) {
	skli, skliCloser := s.getSklIndex()
	defer skliCloser()
	return skli.Exist(key)
}

func (s *sklTable) getMaxKey() []byte {
	iter := s.newIter(nil)
	key, _ := iter.Last()
	if key == nil {
		return nil
	}
	return key.MakeUserKey()
}

func (s *sklTable) newIter(o *iterOptions) InternalKKVIterator {
	s.skli.RLock()
	iter := s.skli.l.NewIter(o.GetLowerBound(), o.GetUpperBound())
	s.skli.RUnlock()
	return iter
}

func (s *sklTable) kindStatis(kind internalKeyKind) {
	s.totalCount++
	switch kind {
	case internalKeyKindDelete:
		s.delCount++
	case internalKeyKindPrefixDelete:
		s.prefixDelCount++
	}
}

func (s *sklTable) getKeyStats() (int, int, int) {
	return s.totalCount, s.delCount, s.prefixDelCount
}

func (s *sklTable) itemCount() int {
	skli, skliCloser := s.getSklIndex()
	n := skli.ItemCount()
	skliCloser()
	return n
}

func (s *sklTable) getModTime() int64 {
	return s.dataFile.getModTime()
}

func (s *sklTable) inuseBytes() uint64 {
	return uint64(s.dataFile.Size())
}

func (s *sklTable) dataBytes() uint64 {
	return s.inuseBytes()
}

func (s *sklTable) empty() bool {
	return s.dataFile.empty()
}

func (s *sklTable) getFileType() FileType {
	return fileTypeSklTable
}

func (s *sklTable) close() error {
	if s.dataFile == nil {
		return nil
	}

	if err := s.writeIdxToFile(); err != nil {
		s.p.bp.opts.Logger.Errorf("sklTable close writeIdxToFile fail file:%s err:%s", s.id, err)
	}

	if err := s.dataFile.Close(); err != nil {
		return err
	}

	s.skli.Lock()
	s.skli.l.Unref()
	s.skli.Unlock()

	s.dataFile = nil
	return nil
}

func (s *sklTable) getFilePath() []string {
	return []string{s.dataPath, s.idxPath}
}

func (s *sklTable) idxFilePath() string {
	return s.idxPath
}

func (s *sklTable) mmapRLock() {
	s.dataFile.MmapRLock()
}

func (s *sklTable) mmapRUnLock() {
	s.dataFile.MmapRUnlock()
}

func (s *sklTable) getIdxFilePath() string {
	return s.p.bp.makeFilePath(fileTypeSklTableIndex, s.p.pn, s.fn)
}

func (s *sklTable) loadIdxFromFile() error {
	if err := s.openIdxFile(); err != nil {
		return s.rebuildIndexes()
	}
	return nil
}

func (s *sklTable) openIdxFile() error {
	if os2.IsNotExist(s.idxPath) {
		return os.ErrNotExist
	}

	idxFile, err := os.OpenFile(s.idxPath, os.O_CREATE|os.O_RDONLY, consts.FileMode)
	if err != nil {
		return err
	}

	var fstat os.FileInfo
	fstat, err = idxFile.Stat()
	if err != nil {
		return err
	}
	idxFileSize := fstat.Size()
	if idxFileSize < stIdxHeaderSize {
		return errors.Errorf("sklTable header size error:%d", idxFileSize)
	}

	var header [stIdxHeaderSize]byte
	var n int
	n, err = idxFile.ReadAt(header[:], 0)
	if err != nil {
		return err
	} else if n != stIdxHeaderSize {
		return errors.Errorf("sklTable header readAt fail n:%d", n)
	}

	fileSize := binary.BigEndian.Uint32(header[stIdxHeaderFileSizeOffset:])
	if idxFileSize != int64(fileSize) {
		return errors.Errorf("sklTable file size not eq fstat:%d rsize:%d", idxFileSize, fileSize)
	}

	stSize := s.dataFile.Size()
	dataSize := binary.BigEndian.Uint32(header[stIdxHeaderDataSizeOffset:])
	if stSize != dataSize {
		return errors.Errorf("sklTable data size not eq filesz:%d dsize:%d", stSize, dataSize)
	}

	idxNum := int(binary.BigEndian.Uint32(header[stIdxHeaderIdxNumOffset:]))
	if idxNum > 0 {
		idxSize := idxNum * 4
		idxBuf := make([]byte, idxSize)
		n, err = idxFile.ReadAt(idxBuf, stIdxIndexesOffset)
		if err != nil {
			return err
		}
		if n != idxSize {
			return errors.Errorf("sklTable idx readAt fail n:%d exp:%d", n, idxSize)
		}

		s.skli.l.LoadArrayIndex(idxBuf, idxNum)
	}

	s.p.bp.opts.Logger.Infof("sklTable openIdxFile success file:%s fileSize:%d dsize:%d idxNum:%d",
		base.GetFilePathBase(s.idxPath), fileSize, dataSize, idxNum)

	return idxFile.Close()
}

func (s *sklTable) writeIdxToFile() error {
	if !s.indexModified {
		return nil
	}

	idxFile, err := os.OpenFile(s.idxPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, consts.FileMode)
	if err != nil {
		return err
	}

	skli, skliCloser := s.getSklIndex()
	defer skliCloser()

	arrIndex := skli.GetAllIndexOffset()
	idxNum := len(arrIndex)
	fileSize := stIdxHeaderSize + idxNum*4
	keyFileSize := s.dataFile.Size()
	if keyFileSize > 0 && idxNum == 0 {
		keyFileSize = 0
	}

	buf := make([]byte, fileSize)
	binary.BigEndian.PutUint16(buf[stIdxHeaderVersionOffset:], stiVersionDefault)
	binary.BigEndian.PutUint32(buf[stIdxHeaderFileSizeOffset:], uint32(fileSize))
	binary.BigEndian.PutUint32(buf[stIdxHeaderDataSizeOffset:], keyFileSize)
	binary.BigEndian.PutUint32(buf[stIdxHeaderIdxNumOffset:], uint32(idxNum))
	pos := stIdxIndexesOffset
	for i := 0; i < idxNum; i++ {
		binary.BigEndian.PutUint32(buf[pos:pos+4], arrIndex[i])
		pos += 4
	}

	if _, err = idxFile.Write(buf); err != nil {
		return err
	}

	s.p.bp.opts.Logger.Infof("sklTable writeIdxToFile finish file:%s filesz:%d keyFileSize:%d idxNum:%d",
		base.GetFilePathBase(s.idxPath), fileSize, keyFileSize, idxNum)

	if err = idxFile.Sync(); err != nil {
		return err
	}
	if err = idxFile.Close(); err != nil {
		return err
	}

	s.indexModified = false
	return nil
}

func (s *sklTable) rebuildIndexes() (err error) {
	var n int
	dataFileSize := int(s.dataFile.Size())
	dataReadBuf := s.p.bp.getStKeyArenaBuf(dataFileSize)
	dataReadBuf = dataReadBuf[:dataFileSize]
	n, err = s.dataFile.readAt(dataReadBuf, 0)
	if err != nil {
		return err
	}
	if n != dataFileSize {
		return errors.Errorf("sklTable rebuild indexes dataFile return not eq %d != %d", n, dataFileSize)
	}

	var ins sklindex.Inserter
	var valueSize uint16
	var keySize uint32
	var keyOffset, offset int

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

		if err = s.skli.l.Add(ikey, uint32(keyOffset), &ins); err != nil {
			return err
		}
	}

	s.dataFile.setOffset(uint32(keyOffset))

	if uint32(s.skli.l.ItemCount()) > consts.BitpageStiCompressCountDefault {
		s.flushSklIndex(s.skli.l)
	}

	s.indexModified = true
	s.p.bp.opts.Logger.Infof("sklTable rebuild indexes finish file:%s dataFileSize:%d idxNum:%d idxHeight:%d",
		s.id, s.dataFile.Size(), s.itemCount(), s.skli.l.Height())
	return nil
}
