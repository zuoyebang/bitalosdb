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

package vectortable

import (
	"encoding/binary"
	"os"
	"syscall"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/mmap"
	"golang.org/x/sys/unix"
)

const (
	initMmapSize  = 1 << 30
	maxMapSize    = 0xFFFFFFFFFFFF
	maxExpandStep = 128 << 20
)

type OffsetKeeper uint64

func (o *OffsetKeeper) Init(size uint64) {
	*o = OffsetKeeper(size)
}

//go:inline
func (o *OffsetKeeper) GetOffset() uint64 {
	return uint64(*o)
}

//go:inline
func (o *OffsetKeeper) AllocSpaceAlign4B(size uint64) (offset uint64) {
	offset = CapForAlign4B(uint64(*o))
	*o = OffsetKeeper(offset + size)
	return
}

//go:inline
func (o *OffsetKeeper) AllocSpace(size uint64) (offset uint64) {
	offset = uint64(*o)
	*o += OffsetKeeper(size)
	return
}

func (o *OffsetKeeper) Save(buf []byte) {
	binary.LittleEndian.PutUint64(buf, uint64(*o))
}

func (o *OffsetKeeper) Load(buf []byte) {
	*o = OffsetKeeper(binary.LittleEndian.Uint64(buf))
}

const (
	tableWriteMmap      = 1
	tableWriteDisk      = 2
	tableWriteBlockMmap = 3
)

type tableOptions struct {
	openType     int
	initMmapSize int
	blockSize    uint32
	logger       base.Logger
}

type table struct {
	path          string
	file          *os.File
	offset        *OffsetKeeper
	logger        base.Logger
	filesz        int
	data          []byte
	opened        bool
	openType      int
	mmapBlockSize uint64
}

func openTable(path string, off *OffsetKeeper, opts *tableOptions) (*table, error) {
	var err error
	var fileStat os.FileInfo

	t := &table{
		path:   path,
		opened: true,
		offset: off,
		logger: opts.logger,
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

	switch opts.openType {
	case tableWriteMmap:
		sz := opts.initMmapSize
		if sz == 0 {
			sz = initMmapSize
		}
		if t.filesz > sz {
			sz = t.filesz
		}
		if err = t.mmapWrite(sz, 0); err != nil {
			return nil, err
		}
	case tableWriteDisk:
		if err = t.mmapRead(opts.initMmapSize); err != nil {
			return nil, err
		}
		t.offset.Init(uint64(t.filesz))
	case tableWriteBlockMmap:
		t.mmapBlockSize = uint64(opts.blockSize)
	}

	return t, nil
}

func (t *table) close() error {
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

func (t *table) updateOffset(off *OffsetKeeper) {
	t.offset = off
}

func (t *table) release(releaseF func([]string)) error {
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
		releaseF([]string{t.path})
		t.file = nil
	}
	return nil
}

func (t *table) Size() uint64 {
	return t.offset.GetOffset()
}

func (t *table) FileSize() int {
	return t.filesz
}

func (t *table) alloc(size uint64) (uint64, error) {
	sz := int(t.offset.GetOffset() + size)
	if err := t.expandFileSize(sz); err != nil {
		return 0, err
	}

	offset := t.offset.AllocSpace(size)
	if t.offset.GetOffset() > maxOffset {
		t.offset.Init(offset)
		return 0, base.ErrFileFull
	}
	return offset, nil
}

func (t *table) fileTruncate(size int, isSync bool) error {
	if err := t.file.Truncate(int64(size)); err != nil {
		return err
	}

	t.filesz = size

	if isSync {
		if err := t.file.Sync(); err != nil {
			return err
		}
	}

	return nil
}

func (t *table) calcExpandSize(size int) (int, error) {
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	if size > maxMapSize {
		return 0, errors.New("table too large")
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
		if err = t.fileTruncate(sz, false); err != nil {
			return errors.Wrapf(err, "vectortable: table truncate fail file:%s", t.path)
		}
	}
	return nil
}

func (t *table) expandFileDesignatedSize(size int) error {
	if size > t.filesz {
		if err := t.fileTruncate(size, false); err != nil {
			return errors.Wrapf(err, "vectortable: table truncate fail file:%s", t.path)
		}
	}
	return nil
}

func (t *table) mmapWrite(size int, offset int64) (err error) {
	t.munmap()

	t.data, err = mmapFile(t.file, size, mmap.RDWR, offset)
	if err != nil {
		return err
	}

	return nil
}

func (t *table) mmapRead(sz int) error {
	t.munmap()

	b, err := mmapFile(t.file, sz, mmap.RDONLY, 0)
	if err != nil {
		return err
	}

	t.data = b

	return nil
}

func (t *table) msync() error {
	if t.data != nil {
		return unix.Msync(t.data, unix.MS_SYNC)
	}
	return nil
}

func (t *table) munmap() {
	if t.data != nil {
		if t.openType == tableWriteMmap || t.openType == tableWriteBlockMmap {
			_ = unix.Msync(t.data, unix.MS_SYNC)
		}
		_ = unix.Munmap(t.data)
		t.data = nil
	}
}

func mmapFile(f *os.File, length, prot int, offset int64) ([]byte, error) {
	b, err := mmap.MapRegion(f, length, prot, 0, offset)
	if err != nil {
		return nil, err
	}

	err = unix.Madvise(b, syscall.MADV_RANDOM)
	if err != nil && err != syscall.ENOSYS {
		return nil, errors.Wrapf(err, "vectortable: madvise fail")
	}

	return b, nil
}

type tableDesignated struct {
	*table
}

func openTableDesignated(path string, off *OffsetKeeper, opts *tableOptions) (*tableDesignated, error) {
	var (
		td  tableDesignated
		err error
	)
	if td.table, err = openTable(path, off, opts); err != nil {
		return nil, err
	}

	if err = td.expandFileDesignatedSize(opts.initMmapSize); err != nil {
		_ = td.close()
		return nil, err
	}
	return &td, nil
}

func (t *tableDesignated) alloc(size uint64) (uint64, error) {
	sz := int(t.offset.GetOffset() + size)
	if sz > t.filesz {
		return 0, errors.New("size too large")
	}

	offset := t.offset.AllocSpace(size)
	if t.offset.GetOffset() > maxOffset {
		t.offset.Init(offset)
		return 0, base.ErrFileFull
	}
	return offset, nil
}
