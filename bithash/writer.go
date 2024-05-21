// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bindex"
	"github.com/zuoyebang/bitalosdb/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/crc"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/unsafe2"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

const (
	maxKeySize           = 33 << 10
	maxValueSize         = 256 << 20
	blockRestartInterval = 16
)

type writeCloseSyncer interface {
	io.WriteCloser
	Sync() error
}

type WriterOption interface {
	writerApply(*Writer)
}

type hashHandle struct {
	bh       BlockHandle
	userKey  []byte
	conflict bool
}

type WriterMetadata struct {
	Size           uint32
	keyNum         uint32
	conflictKeyNum uint32
}

type Writer struct {
	b               *Bithash
	closed          bool
	closing         atomic.Bool
	writer          io.Writer
	syncer          writeCloseSyncer
	bufWriter       *bufio.Writer
	reader          ReadableFile
	meta            WriterMetadata
	err             error
	file            File
	filename        string
	fileNum         FileNum
	closeLock       sync.RWMutex
	indexLock       sync.RWMutex
	indexHash       map[uint32]*hashHandle
	indexArray      []hashHandle
	conflictKeys    map[string]BlockHandle
	footer          footer
	currentOffset   uint32
	dataBlockSize   uint32
	dataBlock       block2Writer
	dataBlockReader block2Reader
	metaBlock       blockWriter
	indexBlock      blockWriter
	conflictBlock   blockWriter
	indexHashBH     BlockHandle
	conflictBH      BlockHandle
	dataBH          BlockHandle
	compressedBuf   []byte
}

func newWriter(b *Bithash, f File, filename string, fileNum FileNum) *Writer {
	w := &Writer{
		b:             b,
		closed:        false,
		syncer:        f.(writeCloseSyncer),
		reader:        f.(ReadableFile),
		meta:          WriterMetadata{Size: 0},
		file:          f,
		filename:      filename,
		fileNum:       fileNum,
		dataBlockSize: uint32(b.tableMaxSize),
		dataBlock:     block2Writer{},
		indexBlock:    blockWriter{restartInterval: blockRestartInterval},
		metaBlock:     blockWriter{restartInterval: blockRestartInterval},
		conflictBlock: blockWriter{restartInterval: blockRestartInterval},
		indexHash:     make(map[uint32]*hashHandle, 1<<18),
		indexArray:    make([]hashHandle, 0, 1<<18),
		conflictKeys:  make(map[string]BlockHandle, 10),
		currentOffset: 0,
	}
	return w
}

func NewTableWriter(b *Bithash, isCompact bool) (*Writer, error) {
	fileNum := b.meta.getNextFileNum()
	filename := MakeFilepath(b.fs, b.dirname, fileTypeTable, fileNum)
	file, err := b.fs.Create(filename)
	if err != nil {
		return nil, ErrBhCreateTableFile
	}

	file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
		BytesPerSync: b.bytesPerSync,
	})

	w := newWriter(b, file, filename, fileNum)
	if err = w.setIoWriter(file); err != nil {
		return nil, err
	}

	b.meta.newFileMetadata(fileNum, isCompact)
	b.addRwwWriters(w)
	b.stats.FileTotal.Add(1)

	if !isCompact {
		b.SetFileNumMap(fileNum, fileNum)
	}

	return w, nil
}

func (w *Writer) setIoWriter(f writeCloseSyncer) error {
	if _, err := w.file.(io.Seeker).Seek(int64(w.currentOffset), io.SeekStart); err != nil {
		return err
	}

	w.bufWriter = bufio.NewWriterSize(f, consts.BufioWriterBufSize)
	w.writer = w.bufWriter
	w.dataBlock.wr = w.bufWriter
	return nil
}

func (w *Writer) fileStatSize() int64 {
	info, err := w.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

func (w *Writer) Get(key []byte, khash uint32) ([]byte, func(), error) {
	var err error
	var bh BlockHandle

	w.indexLock.RLock()
	if handle, ok := w.indexHash[khash]; ok {
		if handle.conflict {
			bh = w.conflictKeys[unsafe2.String(key)]
		} else {
			bh = handle.bh
		}
	}
	w.indexLock.RUnlock()

	if bh.Length <= 0 {
		return nil, nil, nil
	}

	length := int(bh.Length)
	buf, closer := bytepools.ReaderBytePools.GetBytePool(length)
	defer func() {
		if err != nil {
			closer()
		}
	}()
	buf = buf[:length]

	w.closeLock.RLock()
	defer w.closeLock.RUnlock()

	if w.closed {
		err = ErrBhWriterClosed
		return nil, nil, err
	}

	var n int
	n, err = w.reader.ReadAt(buf, int64(bh.Offset))
	if err != nil {
		return nil, nil, err
	}
	if n != length {
		err = ErrBhReadAtIncomplete
		return nil, nil, err
	}
	_, val, _ := w.dataBlockReader.readRecord(buf)
	if val == nil {
		err = ErrBhReadRecordNil
		return nil, nil, err
	}

	var v []byte
	v, err = w.b.compressor.Decode(nil, val)
	if err != nil {
		return nil, nil, err
	}

	return v, closer, nil
}

func (w *Writer) Add(ikey base.InternalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}

	var compressed []byte
	switch w.b.compressor.Type() {
	case compress.CompressTypeNo:
		compressed = value
	case compress.CompressTypeSnappy:
		compressed = w.b.compressor.Encode(w.compressedBuf, value)
		if cap(compressed) > cap(w.compressedBuf) {
			w.compressedBuf = compressed[:cap(compressed)]
		}
	}

	return w.add(ikey, compressed, hash.Crc32(ikey.UserKey), w.fileNum)
}

func (w *Writer) AddIkey(ikey InternalKey, value []byte, khash uint32, fileNum FileNum) error {
	if w.err != nil {
		return w.err
	}

	return w.add(ikey, value, khash, fileNum)
}

func (w *Writer) add(ikey InternalKey, value []byte, khash uint32, fileNum FileNum) error {
	if ikey.Size() > maxKeySize {
		return ErrBhKeyTooLarge
	} else if len(value) > maxValueSize {
		return ErrBhValueTooLarge
	}

	n, err := w.dataBlock.set(ikey, value, fileNum)
	if err != nil {
		return err
	}

	length := uint32(n)
	bh := BlockHandle{w.currentOffset, length}
	w.currentOffset += length
	w.meta.Size += length
	w.meta.keyNum++
	w.updateHash(ikey, khash, bh)
	return nil
}

func (w *Writer) updateHash(ikey InternalKey, khash uint32, bh BlockHandle) {
	w.indexLock.Lock()
	defer w.indexLock.Unlock()

	key := ikey.UserKey
	if ih, ok := w.indexHash[khash]; !ok {
		w.indexArray = append(w.indexArray, hashHandle{
			bh:       bh,
			userKey:  key,
			conflict: false,
		})
		w.indexHash[khash] = &(w.indexArray[len(w.indexArray)-1])
	} else {
		if ih.conflict {
			w.conflictKeys[unsafe2.String(key)] = bh
		} else {
			if bytes.Equal(ih.userKey, key) {
				ih.bh = bh
			} else {
				ih.conflict = true
				w.conflictKeys[unsafe2.String(key)] = bh
				w.conflictKeys[unsafe2.String(ih.userKey)] = ih.bh
			}
		}
	}
}

func (w *Writer) writeTable(force bool) (err error) {
	if w.err != nil {
		return w.err
	}

	if force || w.isWriteFull() {
		if err = w.writeData(); err != nil {
			return err
		}
		if err = w.writeConflict(); err != nil {
			return err
		}
		if err = w.writeIndexHash(); err != nil {
			return err
		}
		if err = w.writeMeta(); err != nil {
			return err
		}
		if err = w.writeFooter(); err != nil {
			return err
		}

		w.err = ErrBhWriterClosed
	}

	return w.Flush()
}

func (w *Writer) close() (err error) {
	if w.closed {
		return
	}

	w.closeLock.Lock()
	err = w.syncer.Close()
	w.closed = true
	w.syncer = nil
	w.indexHash = nil
	w.indexArray = nil
	w.conflictKeys = nil
	w.compressedBuf = nil
	w.closeLock.Unlock()
	return err
}

func (w *Writer) Close() (err error) {
	if w.closing.Load() {
		return nil
	}

	defer func() {
		err1 := w.close()
		if err == nil {
			err = err1
		}
	}()
	if w.err != nil {
		return w.err
	}

	return w.writeTable(false)
}

func (w *Writer) Flush() error {
	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			w.err = err
			return err
		}
	}

	if w.syncer != nil {
		if err := w.syncer.Sync(); err != nil {
			w.err = err
			return err
		}
	}

	return nil
}

func (w *Writer) writeData() error {
	n, err := w.dataBlock.setEmptyHeader()
	if err != nil {
		w.err = err
		return w.err
	}

	w.currentOffset += uint32(n)
	w.dataBH = BlockHandle{
		Offset: 0,
		Length: w.currentOffset,
	}

	return nil
}

func (w *Writer) writeConflict() error {
	var buf [blockHandleLen]byte

	w.meta.conflictKeyNum = uint32(len(w.conflictKeys))
	if w.meta.conflictKeyNum == 0 {
		w.conflictBH = BlockHandle{
			Offset: w.currentOffset,
			Length: 0,
		}
		return nil
	}

	keyStrs := make([]string, 0, w.meta.conflictKeyNum)
	for k := range w.conflictKeys {
		keyStrs = append(keyStrs, k)
	}

	sort.Strings(keyStrs)
	for _, k := range keyStrs {
		encodeBlockHandle(buf[:], w.conflictKeys[k])
		ikey := base.MakeInternalKey(unsafe2.ByteSlice(k), 1, InternalKeyKindSet)
		w.conflictBlock.add(ikey, buf[:])
	}

	b := w.conflictBlock.finish()
	n, err := w.writer.Write(b)
	if err != nil {
		w.err = err
		return w.err
	}

	length := uint32(n)
	w.conflictBH = BlockHandle{
		Offset: w.currentOffset,
		Length: length,
	}
	w.currentOffset += length
	return nil
}

func (w *Writer) writeIndexHash() error {
	var buf [blockHandleLen]byte
	var data []byte

	if len(w.indexHash) > 0 {
		hindex := bindex.NewHashIndex(false)
		hindex.InitWriter()

		for khash, ih := range w.indexHash {
			if ih.conflict {
				encodeBlockHandle(buf[:], w.conflictBH)
			} else {
				encodeBlockHandle(buf[:], ih.bh)
			}
			hindex.Add(khash, binary.LittleEndian.Uint64(buf[:]))
		}

		if r := hindex.Serialize(); !r {
			w.err = ErrBhHashIndexWriteFail
			return w.err
		}

		data = hindex.GetData()
		w.indexBlock.add(base.MakeInternalKey([]byte(IndexHashData), 1, InternalKeyKindSet), data)

		hindex.Finish()
	}

	checksum := crc.New(data).Value()
	w.indexBlock.add(base.MakeInternalKey([]byte(IndexHashChecksum), 1, InternalKeyKindSet), []byte(strconv.FormatUint(uint64(checksum), 10)))
	b := w.indexBlock.finish()
	if _, err := w.writer.Write(b); err != nil {
		w.err = err
		return w.err
	}

	indexLength := uint32(len(b))
	w.indexHashBH = BlockHandle{w.currentOffset, indexLength}
	w.currentOffset += indexLength

	return nil
}

func (w *Writer) writeMeta() error {
	var buf [blockHandleLen]byte

	w.footer.metaBH = BlockHandle{w.currentOffset, 0}

	encodeBlockHandle(buf[:], w.dataBH)
	ikey := base.MakeInternalKey([]byte(MetaDataBH), 1, InternalKeyKindSet)
	w.metaBlock.add(ikey, buf[:])

	encodeBlockHandle(buf[:], w.conflictBH)
	ikey = base.MakeInternalKey([]byte(MetaConflictBH), 1, InternalKeyKindSet)
	w.metaBlock.add(ikey, buf[:])

	encodeBlockHandle(buf[:], w.indexHashBH)
	ikey = base.MakeInternalKey([]byte(MetaIndexHashBH), 1, InternalKeyKindSet)
	w.metaBlock.add(ikey, buf[:])

	b := w.metaBlock.finish()
	n, err := w.writer.Write(b)
	if err != nil {
		w.err = err
		return w.err
	}

	length := uint32(n)
	w.footer.metaBH = BlockHandle{w.currentOffset, length}
	w.currentOffset += length

	return nil
}

func (w *Writer) writeFooter() error {
	footer := footer{
		metaBH: w.footer.metaBH,
	}
	footerBuf := [bithashFooterLen]byte{}
	if _, err := w.writer.Write(footer.encode(footerBuf[:])); err != nil {
		w.err = err
		return w.err
	}
	return nil
}

func (w *Writer) isWriteFull() bool {
	return w.meta.Size >= w.dataBlockSize
}

func (w *Writer) rebuild() (err error) {
	var (
		ikeySize  uint32
		valueSize uint32
		br        block2Reader
		headerBuf = [recordHeaderSize]byte{}
		recordLen uint32
		offset    = 0
		n         = 0
	)

	for {
		n, err = w.reader.ReadAt(headerBuf[:], int64(offset))
		if err != nil || n != recordHeaderSize {
			break
		}
		offset += n

		ikeySize, valueSize, _ = br.readRecordHeader(headerBuf[:])
		if ikeySize == 0 {
			break
		}

		key := make([]byte, ikeySize)
		n, err = w.reader.ReadAt(key, int64(offset))
		if err != nil || n != int(ikeySize) {
			break
		}
		offset += int(ikeySize + valueSize)

		recordLen = recordHeaderSize + ikeySize + valueSize
		bh := BlockHandle{w.currentOffset, recordLen}
		w.currentOffset += recordLen
		w.meta.Size += recordLen
		w.meta.keyNum++
		ikey := base.DecodeInternalKey(key)
		w.updateHash(ikey, hash.Crc32(ikey.UserKey), bh)
	}

	if err != nil && err != io.EOF {
		return err
	}

	return w.setIoWriter(w.file)
}

func (w *Writer) Remove() error {
	w.b.deleteRwwWriters(w.fileNum)
	w.b.meta.freeFileMetadata(w.fileNum)
	if utils.IsFileExist(w.filename) {
		return w.b.fs.Remove(w.filename)
	}
	return nil
}
