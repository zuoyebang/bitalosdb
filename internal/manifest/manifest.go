// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manifest

import (
	"bufio"
	"sync"

	"github.com/cockroachdb/errors/oserror"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/mmap"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

const (
	metaHeaderOffset = 0
	metaHeaderLen    = 16
	metaFieldOffset  = 16
	metaFieldLen     = 1024
	metaMagicLen     = 8
	metaFooterLen    = metaMagicLen
	metaMagic        = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	metaLen          = metaHeaderLen + metaFieldLen + metaMagicLen
	footerOffset     = metaLen - metaFooterLen
)

const (
	fieldOffsetMinUnflushedLogNumV1 = metaFieldOffset
	fieldOffsetNextFileNumV1        = fieldOffsetMinUnflushedLogNumV1 + 8
	fieldOffsetLastSeqNumV1         = fieldOffsetNextFileNumV1 + 8
	fieldOffsetIsBitableFlushedV1   = fieldOffsetLastSeqNumV1 + 8
)

const (
	fieldOffsetIsBitableFlushedV2          = metaFieldOffset
	fieldOffsetLastSeqNumV2                = fieldOffsetIsBitableFlushedV2 + 1
	fieldOffsetBitowerNextFileNumV2        = fieldOffsetLastSeqNumV2 + 8
	fieldOffsetBitowerMinUnflushedLogNumV2 = fieldOffsetBitowerNextFileNumV2 + 8*consts.DefaultBitowerNum
)

const (
	metaVersion1 uint16 = 1 + iota
	metaVersion2
)

type FileNum = base.FileNum

type Metadata struct {
	MetaEditor

	Bmes [consts.DefaultBitowerNum]BitowerMetaEditor

	header *metaHeader
	file   *mmap.MMap
	fs     vfs.FS
	mu     sync.RWMutex
}

type metaHeader struct {
	version uint16
}

type MetaEditor struct {
	LastSeqNum     uint64
	FlushedBitable uint8
}

type BitowerMetaEditor struct {
	Index              int
	MinUnflushedLogNum FileNum
	NextFileNum        FileNum
}

func (s *BitowerMetaEditor) MarkFileNumUsed(fileNum FileNum) {
	if s.NextFileNum < fileNum {
		s.NextFileNum = fileNum + 1
	}
}

func (s *BitowerMetaEditor) GetNextFileNum() FileNum {
	x := s.NextFileNum
	s.NextFileNum++
	return x
}

func NewMetadata(path string, fs vfs.FS) (*Metadata, error) {
	meta := &Metadata{fs: fs}

	if _, err := fs.Stat(path); oserror.IsNotExist(err) {
		if err = meta.create(path); err != nil {
			return nil, err
		}
	}

	if err := meta.load(path); err != nil {
		return nil, err
	}

	return meta, nil
}

func (m *Metadata) create(filename string) (err error) {
	var metaFile vfs.File
	var meta *bufio.Writer

	metaFile, err = m.fs.Create(filename)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = m.fs.Remove(filename)
		}
		if metaFile != nil {
			err = metaFile.Close()
		}
	}()

	var buf [metaLen]byte
	meta = bufio.NewWriterSize(metaFile, metaLen)
	copy(buf[footerOffset:footerOffset+metaFooterLen], metaMagic)
	if _, err = meta.Write(buf[:]); err != nil {
		return err
	}
	if err = meta.Flush(); err != nil {
		return err
	}
	if err = metaFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (m *Metadata) load(filename string) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.file, err = mmap.Open(filename, 0)
	if err != nil {
		return err
	}

	m.readHeader()
	m.updateV1toV2()
	m.readFields()

	return nil
}

func (m *Metadata) readHeader() {
	m.header = &metaHeader{}
	version := m.file.ReadUInt16At(metaHeaderOffset)
	if version == 0 {
		version = metaVersion2
		m.file.WriteUInt16At(version, metaHeaderOffset)
	}
	m.header.version = version
}

func (m *Metadata) writeHeader() {
	m.file.WriteUInt16At(m.header.version, metaHeaderOffset)
}

func (m *Metadata) readFields() {
	m.FlushedBitable = m.file.ReadUInt8At(fieldOffsetIsBitableFlushedV2)
	m.LastSeqNum = m.file.ReadUInt64At(fieldOffsetLastSeqNumV2)

	for i := range m.Bmes {
		m.Bmes[i].Index = i
		m.Bmes[i].NextFileNum = FileNum(m.file.ReadUInt64At(fieldOffsetBitowerNextFileNumV2 + i*8))
		m.Bmes[i].MinUnflushedLogNum = FileNum(m.file.ReadUInt64At(fieldOffsetBitowerMinUnflushedLogNumV2 + i*8))
	}
}

func (m *Metadata) Flush() error {
	return m.file.Flush()
}

func (m *Metadata) Close() error {
	return m.file.Close()
}

func (m *Metadata) GetFieldFlushedBitable() uint8 {
	m.mu.RLock()
	flushed := m.FlushedBitable
	m.mu.RUnlock()
	return flushed
}

func (m *Metadata) SetFieldFlushedBitable() {
	m.mu.Lock()
	m.FlushedBitable = 1
	m.file.WriteUInt8At(m.FlushedBitable, fieldOffsetIsBitableFlushedV2)
	m.Flush()
	m.mu.Unlock()
}

func (m *Metadata) writeMetaEditLocked(me *MetaEditor) {
	if me.LastSeqNum != 0 {
		m.LastSeqNum = me.LastSeqNum
		m.file.WriteUInt64At(m.LastSeqNum, fieldOffsetLastSeqNumV2)
		m.Flush()
	}
}

func (m *Metadata) writeBitowerMetaEditLocked(me *BitowerMetaEditor) {
	index := me.Index
	if me.NextFileNum != 0 {
		m.Bmes[index].NextFileNum = me.NextFileNum
		m.file.WriteUInt64At(uint64(me.NextFileNum), fieldOffsetBitowerNextFileNumV2+index*8)
	}
	if me.MinUnflushedLogNum != 0 {
		m.Bmes[index].MinUnflushedLogNum = me.MinUnflushedLogNum
		m.file.WriteUInt64At(uint64(me.MinUnflushedLogNum), fieldOffsetBitowerMinUnflushedLogNumV2+index*8)
	}
	m.Flush()
}

func (m *Metadata) Write(me *MetaEditor, bme *BitowerMetaEditor) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeMetaEditLocked(me)
	m.writeBitowerMetaEditLocked(bme)
}

func (m *Metadata) WriteMetaEdit(me *MetaEditor) {
	m.mu.Lock()
	m.writeMetaEditLocked(me)
	m.mu.Unlock()
}

func (m *Metadata) WriteBitowerMetaEdit(bme *BitowerMetaEditor) {
	m.mu.Lock()
	m.writeBitowerMetaEditLocked(bme)
	m.mu.Unlock()
}

func (m *Metadata) updateV1toV2() {
	if m.header.version != metaVersion1 {
		return
	}

	lastSeqNum := m.file.ReadUInt64At(fieldOffsetLastSeqNumV1)
	flushedBitable := m.file.ReadUInt8At(fieldOffsetIsBitableFlushedV1)
	minUnflushedLogNum := m.file.ReadUInt64At(fieldOffsetMinUnflushedLogNumV1)
	nextFileNum := m.file.ReadUInt64At(fieldOffsetNextFileNumV1)

	m.file.WriteUInt64At(lastSeqNum, fieldOffsetLastSeqNumV2)
	m.file.WriteUInt8At(flushedBitable, fieldOffsetIsBitableFlushedV2)

	for i := 0; i < len(m.Bmes); i++ {
		m.file.WriteUInt64At(nextFileNum, fieldOffsetBitowerNextFileNumV2+i*8)
		m.file.WriteUInt64At(minUnflushedLogNum, fieldOffsetBitowerMinUnflushedLogNumV2+i*8)
	}

	m.header.version = metaVersion2
	m.writeHeader()
	m.file.Flush()
}
