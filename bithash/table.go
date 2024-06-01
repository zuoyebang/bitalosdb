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

package bithash

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/internal/base"
)

const (
	blockHandleLen        = 8
	blockHandleSum        = 3
	bithashMagicLen       = 8
	bithashFooterLen      = 1 + 1*blockHandleLen + 4 + bithashMagicLen
	bithashMagicKey       = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	bithashMagicKeyOffset = bithashFooterLen - bithashMagicLen
	bithashVersionOffset  = bithashMagicKeyOffset - 4
	bithashFormatVersion2 = 2
	checksumCRC32c        = 1
)

const (
	MetaIndexHashBH   = "indexhash_blockhandle"
	MetaConflictBH    = "conflict_blockhandle"
	MetaDataBH        = "data_blockhandle"
	IndexHashData     = "indexhash_data"
	IndexHashChecksum = "indexhash_checksum"
)

// +------------+-------------+--------------+------------+
// | CRC (1B)   | MetaBH (4B) | Version (4B) | Magic (*B) |
// +------------+-------------+--------------+------------+
type footer struct {
	metaBH BlockHandle
}

func (f footer) encode(buf []byte) []byte {
	buf = buf[:bithashFooterLen]
	for i := range buf {
		buf[i] = 0
	}

	buf[0] = checksumCRC32c
	encodeBlockHandle(buf[1:], f.metaBH)
	binary.LittleEndian.PutUint32(buf[bithashVersionOffset:], bithashFormatVersion2)
	copy(buf[bithashMagicKeyOffset:], bithashMagicKey)

	return buf
}

func checkTableFooter(f ReadableFile) bool {
	stat, err := f.Stat()
	if err != nil {
		return false
	}

	footerOffset := stat.Size() - bithashFooterLen
	if footerOffset < 0 {
		return false
	}

	buf := [bithashMagicLen]byte{}
	n, err := f.ReadAt(buf[:], footerOffset+bithashMagicKeyOffset)
	if err != nil && err != io.EOF {
		return false
	}

	return bytes.Equal(buf[:n], []byte(bithashMagicKey))
}

func readTableFooter(f ReadableFile) (footer, error) {
	stat, err := f.Stat()
	if err != nil {
		return footer{}, errors.Errorf("bithash invalid table could not stat file err:%s", err.Error())
	}
	if stat.Size() < bithashFooterLen {
		return footer{}, ErrBhInvalidTableSize
	}

	var buf [bithashFooterLen]byte

	off := stat.Size() - bithashFooterLen
	n, err := f.ReadAt(buf[:], off)
	if err != nil && err != io.EOF {
		return footer{}, errors.Errorf("bithash invalid table could not read footer err:%s", err.Error())
	}
	if n < bithashFooterLen {
		return footer{}, errors.Errorf("bithash invalid table (footer too short):%d", len(buf))
	}

	return decodeTableFooter(buf[:n], uint64(stat.Size()))
}

func decodeTableFooter(buf []byte, end uint64) (footer, error) {
	var ft footer
	buf = buf[len(buf)-bithashFooterLen:]
	version := binary.LittleEndian.Uint32(buf[bithashVersionOffset:bithashMagicKeyOffset])
	if version != bithashFormatVersion2 {
		return ft, errors.Errorf("bithash unsupported format version:%d", version)
	}

	ft.metaBH = decodeBlockHandle(buf[1:])
	if uint64(ft.metaBH.Offset+ft.metaBH.Length) > end {
		return ft, ErrBhInvalidTableMeta
	}

	return ft, nil
}

func (b *Bithash) initTables() error {
	for fn, fileMeta := range b.meta.mu.filesMeta {
		filename := MakeFilepath(b.fs, b.dirname, fileTypeTable, fn)
		f, err := b.fs.Open(filename)
		if err != nil {
			return err
		}

		b.logger.Infof("bithash initTables file:%s fileMeta:%s", base.GetFilePathBase(filename), fileMeta.String())

		if fileMeta.state == fileMetaStateCompact {
			b.meta.freeFileMetadata(fn)
			b.stats.FileTotal.Add(^uint32(0))
			_ = b.fs.Remove(filename)
			continue
		}

		isRebuildTable := false

		if fileMeta.state != fileMetaStateImmutable {
			if checkTableFooter(f) {
				b.meta.updateFileState(fn, fileMetaStateImmutable)
			} else {
				isRebuildTable = true
			}
		}

		if !isRebuildTable {
			_, err = b.openTable(fn, f, filename)
			if err != nil {
				b.logger.Warnf("bithash initTables openTable fail file:%s err:%s", base.GetFilePathBase(filename), err)
				if err == ErrBhNewReaderFail {
					isRebuildTable = true
				} else if err = f.Close(); err != nil {
					return err
				}
			}
		}

		if isRebuildTable {
			if err = f.Close(); err != nil {
				return err
			}
			if err = b.rebuildTable(filename, fn); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *Bithash) rebuildTable(filename string, fn FileNum) (err error) {
	b.meta.updateFileState(fn, fileMetaStateWrite)

	f, err := b.fs.OpenWR(filename)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	w := newWriter(b, f, filename, fn)
	if err = w.rebuild(); err != nil {
		return err
	}

	b.stats.KeyTotal.Add(uint64(w.meta.keyNum))

	isFull := w.isWriteFull()
	if isFull {
		b.closeTableAsync(w, false)
	} else {
		b.pushMutableWriters(w)
		b.addRwwWriters(w)
	}

	b.logger.Infof("bithash rebuild table success file:%s isFull:%v", base.GetFilePathBase(filename), isFull)

	return nil
}

func (b *Bithash) openTable(fileNum FileNum, file File, filename string) (r *Reader, err error) {
	b.tLock.Lock()
	defer b.tLock.Unlock()

	if b.meta.getPos(fileNum) == 0 {
		return nil, ErrBhFileNumError
	}

	if file == nil {
		filename = MakeFilepath(b.fs, b.dirname, fileTypeTable, fileNum)
		file, err = b.fs.Open(filename)
		if err != nil {
			return nil, ErrBhOpenTableFile
		}
	}

	r, err = NewReader(b, file, FileReopenOpt{fs: b.fs, filename: filename, fileNum: fileNum, readOnly: true})
	if err != nil {
		b.logger.Errorf("bithash openTable NewReader file:%s err:%s", filename, err.Error())
		return nil, ErrBhNewReaderFail
	}

	b.addReaders(r)

	return r, nil
}

func (b *Bithash) closeTable(w *Writer, force bool) error {
	if !b.meta.isFileWriting(w.fileNum) {
		return nil
	}

	w.closing.Store(true)
	defer w.closing.Store(false)

	if err := w.writeTable(force); err != nil {
		b.logger.Errorf("bithash writeTable fail file:%s err:%s", w.filename, err)
		return err
	}

	b.meta.updateFileByClosed(w.fileNum, w.meta)

	if checkTableFooter(w.reader) {
		b.meta.updateFileState(w.fileNum, fileMetaStateImmutable)
	}

	if _, err := b.openTable(w.fileNum, nil, ""); err != nil {
		b.logger.Errorf("bithash openTable fail file:%s err:%s", w.filename, err)
		return err
	}

	b.deleteRwwWriters(w.fileNum)

	if err := w.close(); err != nil {
		b.logger.Errorf("bithash close writer fail file:%s err:%s", w.filename, err)
		return err
	}

	b.logger.Infof("[BITHASH %d] closeTable success file:%s", b.index, base.GetFilePathBase(w.filename))
	return nil
}

func (b *Bithash) closeTableAsync(w *Writer, force bool) {
	b.closeTableWg.Add(1)
	go func() {
		defer b.closeTableWg.Done()
		if err := b.closeTable(w, force); err != nil {
			w.b.logger.Errorf("bithash closeTable fail filename:%s err:%v", w.filename, err)
		}
	}()
}

func (b *Bithash) fileSize(fileNum FileNum) int64 {
	filename := MakeFilepath(b.fs, b.dirname, fileTypeTable, fileNum)
	if info, err := b.fs.Stat(filename); err != nil {
		return 0
	} else {
		return info.Size()
	}
}

func (b *Bithash) NewTableIter(fileNum FileNum) (*TableIterator, error) {
	if !b.meta.isFileImmutable(fileNum) {
		return nil, ErrBhFileNotImmutable
	}

	filename := MakeFilepath(b.fs, b.dirname, fileTypeTable, fileNum)
	f, err := b.fs.Open(filename)
	if err != nil {
		return nil, err
	}

	iter := &TableIterator{
		reader:  f,
		fileNum: fileNum,
		offset:  0,
		kvBuf:   make([]byte, 1024, 1024),
	}
	return iter, nil
}

type TableIterator struct {
	reader    ReadableFile
	fileNum   FileNum
	offset    int64
	br        block2Reader
	header    [recordHeaderSize]byte
	kvBuf     []byte
	iterKey   *InternalKey
	iterValue []byte
	eof       bool
	err       error
}

func (i *TableIterator) Valid() bool {
	if i.eof || i.err != nil {
		return false
	}
	return true
}

func (i *TableIterator) First() (key *InternalKey, value []byte, fileNum FileNum) {
	return i.findEntry()
}

func (i *TableIterator) Next() (key *InternalKey, value []byte, fileNum FileNum) {
	return i.findEntry()
}

func (i *TableIterator) Close() error {
	if i.reader == nil {
		return nil
	}
	i.err = i.reader.Close()
	i.reader = nil
	return i.err
}

func (i *TableIterator) findEntry() (key *InternalKey, value []byte, fileNum FileNum) {
	if !i.Valid() {
		return
	}

	n, err := i.reader.ReadAt(i.header[:], i.offset)
	if err != nil || n != recordHeaderSize {
		i.eof = true
		i.err = err
		return
	}
	ikeySize, valueSize, fn := i.br.readRecordHeader(i.header[:])
	if ikeySize <= 0 || valueSize <= 0 {
		i.eof = true
		return
	}

	kvLen := ikeySize + valueSize
	if cap(i.kvBuf) < int(kvLen) {
		i.kvBuf = make([]byte, 0, kvLen*2)
	}
	i.kvBuf = i.kvBuf[:kvLen]
	n, err = i.reader.ReadAt(i.kvBuf, i.offset+recordHeaderSize)
	if err != nil || n != int(kvLen) {
		i.eof = true
		i.err = err
		return
	}

	i.iterKey, i.iterValue = i.br.readKV(i.kvBuf, ikeySize, valueSize)
	i.offset += int64(recordHeaderSize + kvLen)

	return i.iterKey, i.iterValue, fn
}
