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

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
)

type TableWriter struct {
	*Table
	writer    io.Writer
	bufWriter *bufio.Writer
}

func NewTableWriter(path string, opts *TableOptions) (*TableWriter, error) {
	tbl, err := OpenTable(path, opts)
	if err != nil {
		return nil, err
	}

	t := &TableWriter{
		Table:  tbl,
		writer: tbl.file,
	}

	var header [TblFileHeaderSize]byte
	var n int
	if opts.FileExist {
		if n, err = t.readAt(header[:], 0); err != nil {
			return nil, err
		} else if n != TblFileHeaderSize {
			return nil, errors.Errorf("table read header err n:%d", n)
		}

		t.setOffset(uint32(t.filesz))
		t.version = binary.BigEndian.Uint16(header[:])
	} else {
		t.version = TblVersionDefault
		for i := range header {
			header[i] = 0
		}
		binary.BigEndian.PutUint16(header[:], t.version)
		n, err = t.Write(header[:])
		if err != nil {
			return nil, err
		}
		t.growFileSize(n)
	}

	return t, nil
}

func (w *TableWriter) FreeWriter() {
	w.writer = nil
	w.bufWriter = nil
}

func (w *TableWriter) SetWriter(size int) error {
	if err := w.SeekWriter(); err != nil {
		return err
	}
	w.bufWriter = bufio.NewWriterSize(w.file, size)
	w.writer = w.bufWriter
	return nil
}

func (w *TableWriter) SeekWriter() error {
	offset := int64(w.Size())
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (w *TableWriter) SetValue(values ...[]byte) (uint32, error) {
	var valueSize int
	for i := range values {
		valueSize += len(values[i])
	}
	if valueSize == 0 {
		return 0, nil
	}

	var vs [TblItemValueSize]byte
	wrn := 0
	binary.BigEndian.PutUint32(vs[:], uint32(valueSize))
	n, err := w.writer.Write(vs[:])
	if err != nil {
		return 0, err
	}
	wrn += n
	for i := range values {
		n, err = w.writer.Write(values[i])
		if err != nil {
			return 0, err
		}
		wrn += n
	}

	offset := w.Size()
	w.addOffset(uint32(wrn))
	return offset, nil
}

func (w *TableWriter) SetKeyValue(key base.InternalKey, values ...[]byte) (uint32, error) {
	var header [TblItemHeaderSize]byte
	var keyTrailer [8]byte
	keySize := uint32(key.Size())
	var valueSize int
	for i := range values {
		valueSize += len(values[i])
	}
	wrn := 0
	hl := encodeKeySize(header[:], keySize, uint16(valueSize))
	n, err := w.writer.Write(header[:hl])
	if err != nil {
		return 0, err
	}
	wrn += n

	n, err = w.writer.Write(key.UserKey)
	if err != nil {
		return 0, err
	}
	wrn += n
	binary.LittleEndian.PutUint64(keyTrailer[:], key.Trailer)
	n, err = w.writer.Write(keyTrailer[:])
	if err != nil {
		return 0, err
	}
	wrn += n

	for i := range values {
		n, err = w.writer.Write(values[i])
		if err != nil {
			return 0, err
		}
		wrn += n
	}

	offset := w.offset
	w.addOffset(uint32(wrn))
	return offset, nil
}

func (w *TableWriter) Write(buf []byte) (int, error) {
	n, err := w.writer.Write(buf)
	if err != nil {
		return 0, err
	} else if n != len(buf) {
		return 0, errors.Errorf("table write err exp:%d act:%d", len(buf), n)
	}

	w.addOffset(uint32(n))
	return n, nil
}

func (w *TableWriter) Close() error {
	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			return err
		}
	}

	return w.Table.Close()
}

func (w *TableWriter) Fdatasync() error {
	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			return err
		}
	}

	return w.file.Sync()
}

func (w *TableWriter) SyncAndMmapReadGrow() error {
	if err := w.Fdatasync(); err != nil {
		return err
	}

	return w.MmapReadGrow()
}
