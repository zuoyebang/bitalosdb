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
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/mmap"
	"golang.org/x/sys/unix"
)

const (
	maxMapSize    = 0xFFFFFFFFFFFF
	maxExpandStep = 128 << 20
)

const (
	TblVersionDefault uint16 = 1 + iota
)

const (
	TblHeaderOffset = 0
	TblHeaderSize   = 4
	TblDataOffset   = 4

	TblFileHeaderSize   = 8
	TblItemKeySize      = 4
	TblItemValueSize    = 2
	TblItemHeaderSize   = TblItemKeySize + TblItemValueSize
	TblItemValueNil     = 1 << 31
	TblItemValueNilMask = TblItemValueNil - 1
)

const (
	TableTypeWriteMmap = 1 + iota
	TableTypeReadMmap
	TableTypeWriteDisk
	TableTypeOnceWriteDisk
)

var defaultTableOptions = &TableOptions{
	OpenType:     TableTypeWriteMmap,
	InitMmapSize: consts.BitpageReadMmapSize,
}

type TableOptions struct {
	OpenType     int
	InitMmapSize int
	BlockSize    uint32
	FileExist    bool
	Version      int
}

type TableReader struct {
	data []byte
	r    int
}

func (t *TableReader) ReadByte() (byte, error) {
	if t.r >= len(t.data) {
		return 0, io.EOF
	}
	c := t.data[t.r]
	t.r++
	return c, nil
}

type Table struct {
	file     *os.File
	path     string
	offset   uint32
	filesz   int
	data     []byte
	datasz   int
	opened   bool
	openType int
	modTime  int64
	version  uint16
	mmaplock sync.RWMutex
}

func OpenTable(path string, opts *TableOptions) (*Table, error) {
	var err error
	var fileStat os.FileInfo

	t := &Table{
		opened: true,
	}

	defer func() {
		if err != nil {
			_ = t.Close()
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

	switch opts.OpenType {
	case TableTypeWriteMmap:
		sz := opts.InitMmapSize
		if sz == 0 {
			sz = consts.BitpageReadMmapSize
		}
		if t.filesz > sz {
			sz = t.filesz
		}
		if err = t.mmapWrite(sz); err != nil {
			return nil, err
		}
		if t.filesz == 0 {
			if _, err = t.alloc(TblHeaderSize); err != nil {
				return nil, err
			}
			t.setOffsetOnDisk(TblHeaderSize)
		}
		t.offset = t.getOffsetOnDisk()
	case TableTypeReadMmap:
		if err = t.mmapRead(t.filesz); err != nil {
			return nil, err
		}
		t.offset = uint32(t.filesz)
	case TableTypeWriteDisk:
		sz := opts.InitMmapSize
		if sz == 0 {
			sz = consts.BitpageReadMmapSize
		}
		if t.filesz > sz {
			sz = t.filesz
		}
		if err = t.mmapRead(sz); err != nil {
			return nil, err
		}
		t.offset = uint32(t.filesz)
	case TableTypeOnceWriteDisk:
		t.offset = uint32(t.filesz)
	default:
		return nil, base.ErrTableOpenType
	}

	return t, nil
}

func (t *Table) Close() error {
	if !t.opened {
		return nil
	}

	t.opened = false

	t.munmap()

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

func (t *Table) getModTime() int64 {
	return t.modTime
}

func (t *Table) getPath() string {
	return t.path
}

func (t *Table) Size() uint32 {
	return t.offset
}

func (t *Table) isOverflow(n uint32) bool {
	return n > t.offset
}

func (t *Table) isEOF(n uint32) bool {
	return n < TblFileHeaderSize || n >= t.offset
}

func (t *Table) empty() bool {
	return t.Size() == TblFileHeaderSize
}

func (t *Table) getVersion() uint16 {
	return t.version
}

func (t *Table) addOffset(val uint32) uint32 {
	t.offset += val
	return t.offset
}

func (t *Table) setOffset(val uint32) {
	t.offset = val
}

func (t *Table) calcExpandSize(size int) (int, error) {
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

func (t *Table) growFileSize(sz int) {
	if sz > t.filesz {
		t.filesz = sz
	}
}

func (t *Table) expandFileSize(size int) error {
	if size > t.filesz {
		sz, err := t.calcExpandSize(size)
		if err != nil {
			return err
		}
		if err = t.fileTruncate(sz, true); err != nil {
			return errors.Errorf("bitpage: table truncate fail file:%s err:%s", t.path, err)
		}
	}
	return nil
}

func (t *Table) expandMmapSize(size int) error {
	if size > t.datasz {
		if err := t.mmapWrite(size); err != nil {
			return errors.Errorf("bitpage: table mmapWrite fail file:%s err:%s", t.path, err)
		}
	}
	return nil
}

func (t *Table) checkTableFull(size int) error {
	if size+int(t.Size()) > t.datasz {
		return base.ErrTableFull
	}
	return nil
}

func (t *Table) allocAlign(size, align, overflow uint32) (uint32, uint32, error) {
	padded := size + align
	newSize := t.addOffset(padded)
	sz := int(newSize) + int(overflow)
	if sz > t.datasz {
		return 0, 0, base.ErrTableFull
	}
	if err := t.expandFileSize(sz); err != nil {
		return 0, 0, err
	}

	t.setOffsetOnDisk(newSize)
	offset := (newSize - padded + align) & ^align
	return offset, padded, nil
}

func (t *Table) alloc(size uint32) (uint32, error) {
	newSize := t.addOffset(size)
	sz := int(newSize)
	if err := t.expandFileSize(sz); err != nil {
		return 0, err
	}
	if err := t.expandMmapSize(sz); err != nil {
		return 0, err
	}

	t.setOffsetOnDisk(newSize)
	offset := newSize - size
	return offset, nil
}

func (t *Table) getOffsetOnDisk() uint32 {
	return t.readAtUInt32(TblHeaderOffset)
}

func (t *Table) setOffsetOnDisk(val uint32) {
	t.writeAtUInt32(val, TblHeaderOffset)
}

func (t *Table) writeAtOffset(b []byte, offset uint32) (int, error) {
	size := uint32(len(b))
	n := copy(t.data[offset:offset+size], b)
	return n, nil
}

func (t *Table) readAtUInt16(offset uint16) uint16 {
	return binary.BigEndian.Uint16(t.data[offset : offset+2])
}

func (t *Table) writeAtUInt16(val uint16, offset uint32) {
	binary.BigEndian.PutUint16(t.data[offset:offset+2], val)
}

func (t *Table) readAtUInt32(offset uint32) uint32 {
	return binary.BigEndian.Uint32(t.data[offset : offset+4])
}

func (t *Table) writeAtUInt32(val uint32, offset uint32) {
	binary.BigEndian.PutUint32(t.data[offset:offset+4], val)
}

func (t *Table) writeAt(b []byte, offset int64) (int, error) {
	return t.file.WriteAt(b, offset)
}

func (t *Table) readAt(b []byte, offset int64) (int, error) {
	return t.file.ReadAt(b, offset)
}

func (t *Table) getData(offset uint32) []byte {
	return t.data[offset:t.offset]
}

func (t *Table) GetBytes(offset uint32, size uint32) []byte {
	return t.data[offset : offset+size : offset+size]
}

func (t *Table) getByte(offset uint32) byte {
	return t.data[offset]
}

func (t *Table) fileTruncate(size int, isSync bool) error {
	if err := t.file.Truncate(int64(size)); err != nil {
		return err
	}

	if isSync {
		if err := t.file.Sync(); err != nil {
			return err
		}
	}

	t.filesz = size
	return nil
}

func (t *Table) getFileSize() int64 {
	info, err := t.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

func (t *Table) mmapWrite(sz int) error {
	size, err := t.calcExpandSize(sz)
	if err != nil {
		return err
	}

	t.munmap()

	t.data, err = mmapFile(t.file, size, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	t.datasz = size

	return nil
}

func (t *Table) mmapRead(sz int) error {
	t.munmap()

	b, err := mmapFile(t.file, sz, mmap.RDONLY, 0)
	if err != nil {
		return err
	}

	t.data = b
	t.datasz = sz
	return nil
}

func (t *Table) MmapRLock() {
	t.mmaplock.RLock()
}

func (t *Table) MmapRUnlock() {
	t.mmaplock.RUnlock()
}

func (t *Table) MmapReadGrow() error {
	t.growFileSize(int(t.Size()))
	return t.MmapReadExpand()
}

func (t *Table) MmapReadExpand() error {
	if t.filesz <= t.datasz {
		return nil
	}

	sz := t.datasz * 2

	t.mmaplock.Lock()
	defer t.mmaplock.Unlock()

	return t.mmapRead(sz)
}

func (t *Table) MmapReadTruncate(sz int) error {
	fileSize := int(t.getFileSize())
	if fileSize != sz {
		if err := t.fileTruncate(sz, true); err != nil {
			return err
		}
	}
	return t.mmapRead(sz)
}

func (t *Table) munmap() {
	if t.data == nil {
		return
	}

	if t.openType == TableTypeWriteMmap {
		_ = unix.Msync(t.data, unix.MS_SYNC)
	}

	_ = unix.Munmap(t.data)
	t.data = nil
	t.datasz = 0
}

func mmapFile(f *os.File, length, prot int, offset int64) ([]byte, error) {
	b, err := mmap.MapRegion(f, length, prot, 0, offset)
	if err != nil {
		return nil, err
	}

	err = unix.Madvise(b, syscall.MADV_RANDOM)
	if err != nil && err != syscall.ENOSYS {
		return nil, errors.Errorf("bitpage: madvise fail err:%s", err)
	}

	return b, nil
}

func (t *Table) GetIKey(offset uint32) base.InternalKey {
	keySizeBuf := t.GetBytes(offset, TblItemKeySize)
	keySize := binary.BigEndian.Uint32(keySizeBuf)
	var hl uint32
	if checkIsValueNil(keySizeBuf) {
		keySize = keySize & TblItemValueNilMask
		hl = TblItemKeySize
	} else {
		hl = TblItemHeaderSize
	}
	key := t.GetBytes(offset+hl, keySize)
	return base.DecodeInternalKey(key)
}

func (t *Table) GetKV(offset uint32) ([]byte, []byte) {
	keySizeBuf := t.GetBytes(offset, TblItemKeySize)
	keySize := binary.BigEndian.Uint32(keySizeBuf)
	if checkIsValueNil(keySizeBuf) {
		keySize = keySize & TblItemValueNilMask
		key := t.GetBytes(offset+TblItemKeySize, keySize)
		return key, nil
	} else {
		valueSize := binary.BigEndian.Uint16(t.GetBytes(offset+TblItemKeySize, TblItemValueSize))
		offset += TblItemHeaderSize
		key := t.GetBytes(offset, keySize)
		value := t.GetBytes(offset+keySize, uint32(valueSize))
		return key, value
	}
}

func (t *Table) GetValue(offset uint32) []byte {
	keySizeBuf := t.GetBytes(offset, TblItemKeySize)
	if checkIsValueNil(keySizeBuf) {
		return nil
	}

	keySize := binary.BigEndian.Uint32(keySizeBuf)
	valueSize := binary.BigEndian.Uint16(t.GetBytes(offset+TblItemKeySize, TblItemValueSize))
	offset += TblItemHeaderSize + keySize
	value := t.GetBytes(offset, uint32(valueSize))
	return value
}

func encodeKeySize(buf []byte, keySize uint32, valueSize uint16) int {
	var hl int
	if valueSize == 0 {
		hl = TblItemKeySize
		binary.BigEndian.PutUint32(buf[0:TblItemKeySize], TblItemValueNil|keySize)
	} else {
		hl = TblItemHeaderSize
		binary.BigEndian.PutUint32(buf[0:TblItemKeySize], keySize)
		binary.BigEndian.PutUint16(buf[TblItemKeySize:TblItemHeaderSize], valueSize)
	}
	return hl
}

func decodeKeySize(buf []byte) (uint32, uint16) {
	isValueNil := checkIsValueNil(buf)
	keySize := binary.BigEndian.Uint32(buf[0:TblItemKeySize])
	if isValueNil {
		keySize = keySize & TblItemValueNilMask
		return keySize, 0
	} else {
		valueSize := binary.BigEndian.Uint16(buf[TblItemKeySize:TblItemHeaderSize])
		return keySize, valueSize
	}
}

func checkIsValueNil(buf []byte) bool {
	return (buf[0] >> 7) == 1
}
