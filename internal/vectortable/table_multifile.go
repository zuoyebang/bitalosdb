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
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/zuoyebang/bitalosdb/v2/internal/manual"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
)

type ReadWriterMulti interface {
	ReadAt(fIdx uint8, b []byte, off uint64) (n int, err error)
	WriteAt(fIdx uint8, b []byte, off uint64) (n int, err error)
}

type tableMulti struct {
	path        string
	maxIdx      uint16
	files       []*os.File
	curFile     *os.File
	offset      *OffsetKeeper
	logger      base.Logger
	opened      bool
	buf         bool
	bufWriter   *bufio.Writer
	multiBuf    []byte
	maxFileSize uint64
	totalSize   uint64
}

func openTableMulti(path string, off *OffsetKeeper, maxIdx uint16, logger base.Logger, buf bool) (*tableMulti, error) {
	var (
		err      error
		fileStat os.FileInfo
	)

	t := &tableMulti{
		maxFileSize: maxU32Offset,
		path:        path,
		maxIdx:      maxIdx,
		opened:      true,
		buf:         buf,
		offset:      off,
		logger:      logger,
		files:       make([]*os.File, maxIdx+1),
	}

	defer func() {
		if err != nil {
			_ = t.close()
		}
	}()

	for i := uint16(0); i <= maxIdx; i++ {
		f, err := os.OpenFile(fmt.Sprintf("%s.%d", path, i), os.O_CREATE|os.O_RDWR, consts.FileMode)
		if err != nil {
			return nil, errors.Wrapf(err, "vectortable: openTableMulti OpenFile fail file:%s idx:%d", path, i)
		}
		if i < maxIdx {
			fileStat, err = f.Stat()
			if err != nil {
				return nil, errors.Wrapf(err, "vectortable: openTable Stat fail file:%s idx:%d", path, i)
			}
			t.totalSize += uint64(fileStat.Size())
		}
		t.files[i] = f
	}

	fileStat, err = t.files[maxIdx].Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "vectortable: openTable Stat fail file:%s idx:%d", path, maxIdx)
	}
	fSize := uint64(fileStat.Size())
	if off.GetOffset() != fSize {
		logger.Warnf("vectortable: data file %s idx %d size %d not equal, header value %d", path, maxIdx, fSize, off.GetOffset())
	}

	_, err = t.files[maxIdx].Seek(fileStat.Size(), io.SeekStart)
	if err != nil {
		return nil, errors.Wrapf(err, "vectortable: openTable Stat fail file:%s, idx:%d", path, maxIdx)
	}
	t.offset.Init(fSize)
	if t.buf {
		t.bufWriter = bufio.NewWriterSize(t.files[maxIdx], consts.VectorTableBufioWriterSize)
	} else {
		t.curFile = t.files[maxIdx]
	}

	return t, nil
}

func (t *tableMulti) TotalSize() uint64 {
	return t.totalSize + t.offset.GetOffset()
}

func (t *tableMulti) updateOffset(off *OffsetKeeper) {
	t.offset = off
}

func (t *tableMulti) FilesValid() error {
	for _, f := range t.files {
		if _, err := f.Stat(); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableMulti) ReadAt(fIdx uint16, b []byte, off uint32) (n int, err error) {
	if int(fIdx) > len(t.files)-1 {
		return 0, errors.Errorf("vectortable: tableMulti read non-existent files,file %s,idx %d", t.path, fIdx)
	}
	return t.files[fIdx].ReadAt(b, int64(off))
}

func (t *tableMulti) Write(b []byte) (fIdx uint16, offset uint32, n int, err error) {
	if t.buf {
		return t.writeBuffer(b)
	} else {
		return t.writeFile(b)
	}
}

func (t *tableMulti) writeBuffer(b []byte) (fIdx uint16, offset uint32, n int, err error) {
	if t.offset.GetOffset()+uint64(len(b)) > t.maxFileSize {
		t.totalSize += t.offset.GetOffset()
		filename := fmt.Sprintf("%s.%d", t.path, t.maxIdx+1)
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, consts.FileMode)
		if err != nil {
			return 0, 0, 0, errors.Wrapf(err, "tableMulti OpenFile fail file:%s", base.GetFilePathBase(filename))
		}
		t.files = append(t.files, f)
		t.maxIdx++
		t.offset.Init(0)
		if err := t.bufWriter.Flush(); err != nil {
			return 0, 0, 0, errors.Wrapf(err, "tableMulti flush fail file:%s idx:%d", base.GetFilePathBase(t.path), t.maxIdx)
		}
		t.bufWriter = bufio.NewWriterSize(f, consts.VectorTableBufioWriterSize)
	}
	offset = uint32(*t.offset)
	n, err = t.bufWriter.Write(b)
	if err != nil {
		return 0, 0, n, errors.Wrapf(err, "tableMulti write file buffer fail:%s idx:%d b:%d", base.GetFilePathBase(t.path), t.maxIdx, len(b))
	}
	t.offset.AllocSpace(uint64(n))
	fIdx = t.maxIdx
	return
}

func (t *tableMulti) writeFile(b []byte) (fIdx uint16, offset uint32, n int, err error) {
	if t.offset.GetOffset()+uint64(len(b)) > t.maxFileSize {
		t.totalSize += t.offset.GetOffset()
		filename := fmt.Sprintf("%s.%d", t.path, t.maxIdx+1)
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, consts.FileMode)
		if err != nil {
			return 0, 0, 0, errors.Wrapf(err, "tableMulti OpenFile fail file:%s", base.GetFilePathBase(filename))
		}
		t.files = append(t.files, f)
		t.maxIdx++
		t.offset.Init(0)
		t.curFile = f
	}
	offset = uint32(*t.offset)
	n, err = t.curFile.WriteAt(b, int64(offset))
	if err != nil {
		return 0, 0, n, errors.Wrapf(err, "tableMulti write file fail:%s idx:%d b:%d", base.GetFilePathBase(t.path), t.maxIdx, len(b))
	}
	t.offset.AllocSpace(uint64(n))
	fIdx = t.maxIdx
	return
}

func (t *tableMulti) WriteMulti(size uint64, values ...[]byte) (fIdx uint16, offset uint32, n int, err error) {
	if t.buf {
		return t.writeMultiBuffer(size, values...)
	} else {
		return t.writeMultiFile(size, values...)
	}
}

func (t *tableMulti) writeMultiBuffer(size uint64, values ...[]byte) (fIdx uint16, offset uint32, n int, err error) {
	if t.offset.GetOffset()+size > t.maxFileSize {
		t.totalSize += t.offset.GetOffset()
		filename := fmt.Sprintf("%s.%d", t.path, t.maxIdx+1)
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, consts.FileMode)
		if err != nil {
			return 0, 0, 0, errors.Wrapf(err, "tableMulti OpenFile fail file:%s", base.GetFilePathBase(filename))
		}
		t.files = append(t.files, f)
		t.maxIdx++
		t.offset.Init(0)
		if err := t.bufWriter.Flush(); err != nil {
			return 0, 0, 0, errors.Wrapf(err, "tableMulti flush fail file:%s idx:%d", base.GetFilePathBase(t.path), t.maxIdx)
		}
		t.bufWriter = bufio.NewWriterSize(f, consts.VectorTableBufioWriterSize)
	}
	offset = uint32(*t.offset)
	for _, value := range values {
		n, err = t.bufWriter.Write(value)
		if err != nil {
			return 0, 0, n, errors.Wrapf(err, "tableMulti write fail file:%s idx:%d", base.GetFilePathBase(t.path), t.maxIdx)
		} else if n != len(value) {
			return 0, 0, n, errors.Errorf("tableMulti write fail file:%s idx:%d exp:%d act:%d", base.GetFilePathBase(t.path), t.maxIdx, len(value), n)
		}
	}

	t.offset.AllocSpace(size)
	fIdx = t.maxIdx
	return
}

func (t *tableMulti) writeMultiFile(size uint64, values ...[]byte) (fIdx uint16, offset uint32, n int, err error) {
	if t.offset.GetOffset()+size > t.maxFileSize {
		t.totalSize += t.offset.GetOffset()
		filename := fmt.Sprintf("%s.%d", t.path, t.maxIdx+1)
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, consts.FileMode)
		if err != nil {
			return 0, 0, 0, errors.Wrapf(err, "tableMulti OpenFile fail file:%s", base.GetFilePathBase(filename))
		}
		t.files = append(t.files, f)
		t.maxIdx++
		t.offset.Init(0)
		t.curFile = f
	}
	offset = uint32(*t.offset)
	t.multiBuf = manual.New(int(size))
	m := 0
	for _, value := range values {
		m += copy(t.multiBuf[m:], value)
	}
	n, err = t.curFile.WriteAt(t.multiBuf, int64(offset))
	if err != nil {
		manual.Free(t.multiBuf)
		return 0, 0, n, errors.Wrapf(err, "tableMulti write file fail:%s idx:%d b:%d", base.GetFilePathBase(t.path), t.maxIdx, size)
	}
	manual.Free(t.multiBuf)
	t.multiBuf = nil
	t.offset.AllocSpace(size)
	fIdx = t.maxIdx
	return
}

func (t *tableMulti) Flush() error {
	if t.bufWriter == nil {
		return nil
	}
	return t.bufWriter.Flush()
}

func (t *tableMulti) close() error {
	if !t.opened {
		return nil
	}

	t.opened = false
	if t.bufWriter != nil {
		t.bufWriter.Flush()
	}
	for _, f := range t.files {
		if f != nil {
			if err := f.Sync(); err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	t.files = nil

	return nil
}
