// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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
	fieldOffsetMinUnflushedLogNum = metaFieldOffset
	fieldOffsetNextFileNum        = fieldOffsetMinUnflushedLogNum + 8
	fieldOffsetLastSeqNum         = fieldOffsetNextFileNum + 8
	fieldOffsetIsBitableFlushed   = fieldOffsetLastSeqNum + 8
)

const (
	metaVersion1 uint16 = 1
)

type Metadata struct {
	MetaEdit

	header *metaHeader
	file   *mmap.MMap
	fs     vfs.FS
	mu     sync.RWMutex
}

type metaHeader struct {
	version uint16
}

type MetaEdit struct {
	MinUnflushedLogNum base.FileNum
	NextFileNum        base.FileNum
	LastSeqNum         uint64
	FlushedBitable     uint8
}

func NewMetadata(path string, fs vfs.FS) (*Metadata, error) {
	meta := &Metadata{
		fs: fs,
	}

	_, err := fs.Stat(path)
	if oserror.IsNotExist(err) {
		if err = meta.create(path); err != nil {
			return nil, err
		}
	}

	if err = meta.load(path); err != nil {
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
	m.readFields()

	return nil
}

func (m *Metadata) readHeader() {
	m.header = &metaHeader{}
	version := m.file.ReadUInt16At(metaHeaderOffset)
	if version == 0 {
		version = metaVersion1
		m.file.WriteUInt16At(version, metaHeaderOffset)
	}
	m.header.version = version
}

func (m *Metadata) writeHeader() {
	m.file.WriteUInt16At(m.header.version, metaHeaderOffset)
}

func (m *Metadata) readFields() {
	m.MinUnflushedLogNum = base.FileNum(m.file.ReadUInt64At(fieldOffsetMinUnflushedLogNum))
	m.NextFileNum = base.FileNum(m.file.ReadUInt64At(fieldOffsetNextFileNum))
	m.LastSeqNum = m.file.ReadUInt64At(fieldOffsetLastSeqNum)
	m.FlushedBitable = m.file.ReadUInt8At(fieldOffsetIsBitableFlushed)
}

func (m *Metadata) Close() error {
	return m.file.Close()
}

func (m *Metadata) GetFieldFlushedBitable() uint8 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.FlushedBitable
}

func (m *Metadata) SetFieldFlushedBitable() {
	m.mu.Lock()
	m.FlushedBitable = 1
	m.file.WriteUInt8At(m.FlushedBitable, fieldOffsetIsBitableFlushed)
	m.mu.Unlock()
}

func (m *Metadata) WriteMetaEdit(me *MetaEdit) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if me.MinUnflushedLogNum != 0 {
		m.MinUnflushedLogNum = me.MinUnflushedLogNum
		m.file.WriteUInt64At(uint64(m.MinUnflushedLogNum), fieldOffsetMinUnflushedLogNum)
	}

	if me.NextFileNum != 0 {
		m.NextFileNum = me.NextFileNum
		m.file.WriteUInt64At(uint64(m.NextFileNum), fieldOffsetNextFileNum)
	}

	if me.LastSeqNum != 0 {
		m.LastSeqNum = me.LastSeqNum
		m.file.WriteUInt64At(m.LastSeqNum, fieldOffsetLastSeqNum)
	}
}
