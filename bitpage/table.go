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
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/mmap"
	"golang.org/x/sys/unix"
)

const (
	maxMapSize    = 0xFFFFFFFFFFFF
	maxExpandStep = 128 << 20
)

const (
	align4            = 3
	tableHeaderOffset = 0
	tableHeaderSize   = 4
	tableDataOffset   = 4
)

const (
	tableWriteMmap = 1
	tableReadMmap  = 2
	tableWriteDisk = 3
)

type tableOptions struct {
	openType     int
	initMmapSize int
}

var defaultTableOptions = &tableOptions{
	openType:     tableWriteMmap,
	initMmapSize: consts.BitpageInitMmapSize,
}

type table struct {
	path     string
	file     *os.File
	offset   atomic.Uint32
	filesz   int
	data     []byte
	datasz   int
	opened   bool
	openType int
	mmaplock sync.RWMutex
	modTime  int64
}

func openTable(path string, opts *tableOptions) (*table, error) {
	var err error
	var fileStat os.FileInfo

	t := &table{
		opened: true,
	}

	defer func() {
		if err != nil {
			_ = t.close()
		}
	}()

	t.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, consts.FileMode)
	if err != nil {
		return nil, err
	}
	fileStat, err = t.file.Stat()
	if err != nil {
		return nil, err
	}

	t.path = t.file.Name()
	t.filesz = int(fileStat.Size())
	t.modTime = fileStat.ModTime().Unix()

	switch opts.openType {
	case tableWriteMmap:
		sz := opts.initMmapSize
		if sz == 0 {
			sz = consts.BitpageInitMmapSize
		}
		if t.filesz > sz {
			sz = t.filesz
		}
		if err = t.mmapWrite(sz); err != nil {
			return nil, err
		}
		if err = t.initHeader(); err != nil {
			return nil, err
		}
		t.offset.Store(t.getOffset())
	case tableReadMmap:
		if err = t.mmapRead(t.filesz); err != nil {
			return nil, err
		}
		t.offset.Store(t.getOffset())
	case tableWriteDisk:
		if err = t.mmapRead(opts.initMmapSize); err != nil {
			return nil, err
		}
		t.offset.Store(uint32(t.filesz))
	default:
		return nil, ErrTableOpenType
	}

	return t, nil
}

func (t *table) close() error {
	if !t.opened {
		return nil
	}

	t.opened = false

	if err := t.munmap(); err != nil {
		return err
	}

	if t.file != nil {
		if err := t.file.Sync(); err != nil {
			return err
		}
		if err := t.file.Close(); err != nil {
			return err
		}
		t.file = nil
	}
	return nil
}

func (t *table) Size() uint32 {
	return t.offset.Load()
}

func (t *table) Capacity() int {
	return t.datasz
}

func (t *table) calcExpandSize(size int) (int, error) {
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	if size > maxMapSize {
		return 0, errors.New("bitpage: table too large")
	}

	sz := int64(size)
	if remainder := sz % int64(maxExpandStep); remainder > 0 {
		sz += int64(maxExpandStep) - remainder
	}

	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

func (t *table) expandFileSize(size int) error {
	if size > t.filesz {
		sz, err := t.calcExpandSize(size)
		if err != nil {
			return err
		}
		if err = t.fileTruncate(sz); err != nil {
			return errors.Wrapf(err, "bitpage: table truncate fail file:%s", t.path)
		}
	}
	return nil
}

func (t *table) expandMmapSize(size int) error {
	if size > t.datasz {
		if err := t.mmapWrite(size); err != nil {
			return errors.Wrapf(err, "bitpage: table mmapWrite fail file:%s", t.path)
		}
	}
	return nil
}

func (t *table) checkTableFull(size int) error {
	if size+int(t.Size()) > t.datasz {
		return ErrTableFull
	}
	return nil
}

func (t *table) allocAlign(size, align, overflow uint32) (uint32, uint32, error) {
	padded := size + align
	newSize := t.offset.Add(padded)
	sz := int(newSize) + int(overflow)
	if sz > t.datasz {
		return 0, 0, ErrTableFull
	}
	if err := t.expandFileSize(sz); err != nil {
		return 0, 0, err
	}

	t.setOffset(newSize)
	offset := (newSize - padded + align) & ^align
	return offset, padded, nil
}

func (t *table) alloc(size uint32) (uint32, error) {
	newSize := t.offset.Add(size)
	sz := int(newSize)
	if err := t.expandFileSize(sz); err != nil {
		return 0, err
	}
	if err := t.expandMmapSize(sz); err != nil {
		return 0, err
	}

	t.setOffset(newSize)
	offset := newSize - size
	return offset, nil
}

func (t *table) initHeader() error {
	if t.filesz == 0 {
		if _, err := t.alloc(tableHeaderSize); err != nil {
			return err
		}
		t.setOffset(tableHeaderSize)
	}
	return nil
}

func (t *table) getOffset() uint32 {
	return t.readAtUInt32(tableHeaderOffset)
}

func (t *table) setOffset(val uint32) {
	t.writeAtUInt32(val, tableHeaderOffset)
}

func (t *table) writeAt(b []byte, offset uint32) (int, error) {
	size := uint32(len(b))
	n := copy(t.data[offset:offset+size], b)
	return n, nil
}

func (t *table) readAtUInt16(offset uint16) uint16 {
	return binary.BigEndian.Uint16(t.data[offset : offset+2])
}

func (t *table) writeAtUInt16(val uint16, offset uint32) {
	binary.BigEndian.PutUint16(t.data[offset:offset+2], val)
}

func (t *table) readAtUInt32(offset uint32) uint32 {
	return binary.BigEndian.Uint32(t.data[offset : offset+4])
}

func (t *table) writeAtUInt32(val uint32, offset uint32) {
	binary.BigEndian.PutUint32(t.data[offset:offset+4], val)
}

func (t *table) getBytes(offset uint32, size uint32) []byte {
	return t.data[offset : offset+size : offset+size]
}

func (t *table) getPointer(offset uint32) unsafe.Pointer {
	return unsafe.Pointer(&t.data[offset])
}

func (t *table) getData() []byte {
	return t.data[:]
}

func (t *table) getPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}
	return uint32(uintptr(ptr) - uintptr(unsafe.Pointer(&t.data[0])))
}

func (t *table) fileTruncate(size int) error {
	if err := t.file.Truncate(int64(size)); err != nil {
		return err
	}
	if err := t.file.Sync(); err != nil {
		return err
	}
	t.filesz = size
	return nil
}

func (t *table) fileStatSize() int64 {
	info, err := t.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

func (t *table) mmapWrite(sz int) error {
	size, err := t.calcExpandSize(sz)
	if err != nil {
		return err
	}

	if err = t.munmap(); err != nil {
		return err
	}

	if err = mmapFile(t, mmap.RDWR, size); err != nil {
		return err
	}

	return nil
}

func (t *table) mmapRead(sz int) error {
	if err := t.munmap(); err != nil {
		return err
	}

	return mmapFile(t, mmap.RDONLY, sz)
}

func (t *table) mmapReadExpand() (bool, error) {
	if t.filesz <= t.datasz {
		return false, nil
	}

	sz := t.datasz * 2

	t.mmaplock.Lock()
	defer t.mmaplock.Unlock()

	return true, t.mmapRead(sz)
}

func (t *table) mmapReadTruncate(sz int) error {
	fileSize := int(t.fileStatSize())
	if fileSize != sz {
		if err := t.fileTruncate(sz); err != nil {
			return err
		}
	}

	return t.mmapRead(sz)
}

func (t *table) munmap() error {
	if t.data == nil {
		return nil
	}

	if t.openType == tableWriteMmap {
		_ = unix.Msync(t.data, unix.MS_SYNC)
	}

	err := unix.Munmap(t.data)
	t.data = nil
	t.datasz = 0
	if err != nil {
		return errors.Wrapf(err, "bitpage: munmap fail")
	}
	return nil
}

func mmapFile(t *table, prot, length int) error {
	b, err := mmap.Map(t.file, prot, length)
	if err != nil {
		return err
	}

	err = unix.Madvise(b, syscall.MADV_RANDOM)
	if err != nil && err != syscall.ENOSYS {
		return errors.Wrapf(err, "bitpage: madvise fail")
	}

	t.data = b
	t.datasz = length
	return nil
}

type tableWriter struct {
	*table
	wbuf      []byte
	writer    io.Writer
	bufWriter *bufio.Writer
}

func newTableWriter(t *table) *tableWriter {
	return &tableWriter{table: t}
}

func (w *tableWriter) reset(offset int) error {
	if _, err := w.file.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	w.writer = nil
	w.bufWriter = nil
	w.bufWriter = bufio.NewWriterSize(w.file, consts.BufioWriterBufSize)
	w.writer = w.bufWriter
	w.filesz = offset
	w.offset.Store(uint32(offset))
	return nil
}

func (w *tableWriter) encodeHeader(buf []byte, keySize uint16, valueSize uint32) {
	binary.BigEndian.PutUint16(buf[0:2], keySize)
	binary.BigEndian.PutUint32(buf[2:6], valueSize)
}

func (w *tableWriter) decodeHeader(buf []byte) (uint16, uint32) {
	return binary.BigEndian.Uint16(buf[0:2]), binary.BigEndian.Uint32(buf[2:6])
}

func (w *tableWriter) set(key internalKey, value []byte) (uint32, error) {
	keySize := key.Size()
	valueSize := len(value)
	preSize := keySize + stItemHeaderSize
	wrn := 0

	if cap(w.wbuf) < preSize {
		w.wbuf = make([]byte, 0, preSize*2)
	}

	w.wbuf = w.wbuf[:preSize]
	w.encodeHeader(w.wbuf[:stItemHeaderSize], uint16(keySize), uint32(valueSize))
	key.Encode(w.wbuf[stItemHeaderSize:])
	n, err := w.writer.Write(w.wbuf)
	if err != nil {
		return 0, err
	}
	wrn += n

	if valueSize > 0 {
		n, err = w.writer.Write(value)
		if err != nil {
			return 0, err
		}
		wrn += n
	}

	addSize := uint32(wrn)
	w.wbuf = w.wbuf[:0]
	offset := w.offset.Load()
	w.offset.Add(addSize)
	return offset, nil
}

func (w *tableWriter) close() error {
	w.bufWriter = nil
	w.wbuf = nil
	w.writer = nil
	return nil
}

func (w *tableWriter) fdatasync() error {
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}

	return w.file.Sync()
}
