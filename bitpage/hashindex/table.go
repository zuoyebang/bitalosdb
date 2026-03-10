// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this table except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashindex

import (
	"os"
	"syscall"

	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/mmap"
	"golang.org/x/sys/unix"
)

const (
	TblHeaderOffset = 0
	TblHeaderSize   = 4
)

type Table struct {
	file    *os.File
	path    string
	filesz  int
	data    []byte
	datasz  int
	opened  bool
	modTime int64
}

func OpenTable(path string, initMmapSize int) (*Table, bool, error) {
	var (
		err      error
		fileStat os.FileInfo
		isNew    bool
	)

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
		return nil, false, err
	}
	fileStat, err = t.file.Stat()
	if err != nil {
		return nil, false, err
	}

	t.path = t.file.Name()
	t.filesz = int(fileStat.Size())
	t.modTime = fileStat.ModTime().Unix()

	sz := initMmapSize
	if t.filesz > sz {
		sz = t.filesz
	}
	if sz == 0 {
		return nil, false, errors.New("table size zero")
	} else if t.filesz == 0 {
		isNew = true
		if err := t.file.Truncate(int64(initMmapSize)); err != nil {
			return nil, false, err
		}
	} else if t.filesz != sz {
		return nil, false, errors.Errorf("table size error: input:%d file:%d", initMmapSize, t.filesz)
	}

	if err = t.mmapWrite(sz); err != nil {
		return nil, false, err
	}

	return t, isNew, nil
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

func (t *Table) ExpandSize(size int) (err error) {
	size += t.datasz
	t.munmap()
	if err = t.expandFileSize(size); err != nil {
		return err
	}
	return t.expandMmapSize(size)
}

func (t *Table) expandFileSize(size int) error {
	if size > t.filesz {
		if err := t.fileTruncate(size, true); err != nil {
			return errors.Errorf("hashindex: table truncate fail file:%s err:%s", t.path, err)
		}
	}
	return nil
}

func (t *Table) expandMmapSize(size int) error {
	if size > t.datasz {
		if err := t.mmapWrite(size); err != nil {
			return errors.Errorf("hashindex: table mmapWrite fail file:%s err:%s", t.path, err)
		}
	}
	return nil
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

func (t *Table) mmapWrite(size int) (err error) {
	t.munmap()

	t.data, err = mmapFile(t.file, size, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	t.datasz = size
	return nil
}

func (t *Table) munmap() {
	if t.data == nil {
		return
	}
	_ = unix.Msync(t.data, unix.MS_SYNC)
	_ = unix.Munmap(t.data)
	t.data = nil
	t.datasz = 0
}

func (t *Table) Sync() error {
	if t.data == nil {
		return nil
	}
	if err := unix.Msync(t.data, unix.MS_SYNC); err != nil {
		return err
	}
	if err := t.file.Sync(); err != nil {
		return err
	}
	return nil
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
