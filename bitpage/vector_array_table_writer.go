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
	"bytes"
	"encoding/binary"
	"io"

	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

type setRangeKeyVersionIndex func([]byte, uint32, uint32)
type setFixedKeyVersionIndex func([]byte, uint32)

type vatHashSetWriter struct {
	at         *vectorArrayTable
	curVersion []byte
	curOffset  uint32
}

func newHashSetWriter(a *vectorArrayTable) *vatHashSetWriter {
	return &vatHashSetWriter{
		at: a,
	}
}

func (w *vatHashSetWriter) writeItem(key *InternalKKVKey, value []byte, offset uint32) (uint32, error) {
	isFirst := false
	if w.curVersion == nil {
		w.curVersion = key.Version
		w.curOffset = offset
		isFirst = true
	} else if !bytes.Equal(w.curVersion, key.Version) {
		w.writeVersion()
		w.curVersion = key.Version
		w.curOffset = offset
		isFirst = true
	}

	keySize := len(key.SubKey)
	valueSize := len(value)
	hl := w.at.encodeItemHeader(key.DataType, keySize, valueSize, isFirst)
	if _, err := w.at.dataFile.Write(w.at.itemHeaderBuf[:hl]); err != nil {
		return 0, err
	}
	sz := keySize + valueSize + hl
	if isFirst {
		if _, err := w.at.dataFile.Write(key.Version); err != nil {
			return 0, err
		}
		sz += vatKeyVersionSize
	}
	if _, err := w.at.dataFile.Write(key.SubKey); err != nil {
		return 0, err
	}
	if valueSize > 0 {
		if _, err := w.at.dataFile.Write(value); err != nil {
			return 0, err
		}
	}

	return uint32(sz), nil
}

func (w *vatHashSetWriter) writeVersion() {
	if w.curVersion != nil {
		itemOffset := w.at.dataFile.Size()
		w.at.setHashSetListVersionIndex(w.curVersion, w.curOffset, itemOffset)
		w.curVersion = nil
	}
}

type vatRangeKeyWriter struct {
	at              *vectorArrayTable
	oiNum           int
	oiPending       []byte
	oiBuf           [vatOffsetLength]byte
	curVersion      []byte
	curVersionSp    int
	curVersionEp    int
	setVersionIndex setRangeKeyVersionIndex
}

func newRangeKeyWriter(a *vectorArrayTable, setIndex setRangeKeyVersionIndex) *vatRangeKeyWriter {
	return &vatRangeKeyWriter{
		at:              a,
		oiPending:       make([]byte, 0, vatOffsetLength*4096),
		curVersionSp:    -1,
		curVersionEp:    -1,
		setVersionIndex: setIndex,
	}
}

func (w *vatRangeKeyWriter) writeItem(key *InternalKKVKey, value []byte, offset uint32) (uint32, error) {
	isFirst := false
	if w.curVersion == nil {
		w.curVersion = key.Version
		isFirst = true
	} else if !bytes.Equal(w.curVersion, key.Version) {
		if w.curVersion != nil {
			w.writeVersion()
		}
		w.curVersion = key.Version
		isFirst = true
	}

	valueSize := len(value)
	keySize := 0
	if key.DataType == kkv.DataTypeZsetIndex {
		keySize = len(key.SubKey)
	}
	hl := w.at.encodeItemHeader(key.DataType, keySize, valueSize, isFirst)
	if _, err := w.at.dataFile.Write(w.at.itemHeaderBuf[:hl]); err != nil {
		return 0, err
	}
	sz := hl + len(key.SubKey) + valueSize
	if isFirst {
		if _, err := w.at.dataFile.Write(key.Version); err != nil {
			return 0, err
		}
		sz += vatKeyVersionSize
	}
	if _, err := w.at.dataFile.Write(key.SubKey); err != nil {
		return 0, err
	}
	if valueSize > 0 {
		if _, err := w.at.dataFile.Write(value); err != nil {
			return 0, err
		}
	}

	if w.curVersionSp == -1 {
		w.curVersionSp = w.oiNum
		w.curVersionEp = w.oiNum
	} else {
		w.curVersionEp++
	}
	binary.BigEndian.PutUint32(w.oiBuf[:], offset)
	w.oiPending = append(w.oiPending, w.oiBuf[:]...)
	w.oiNum++
	return uint32(sz), nil
}

func (w *vatRangeKeyWriter) writeVersion() {
	if w.curVersion != nil {
		w.setVersionIndex(w.curVersion, uint32(w.curVersionSp), uint32(w.curVersionEp))
		w.curVersionSp = -1
		w.curVersionEp = -1
		w.curVersion = nil
	}
}

func (w *vatRangeKeyWriter) serialize(writer io.Writer) (int, error) {
	w.writeVersion()

	if _, err := writer.Write(w.oiPending); err != nil {
		return 0, err
	}
	return len(w.oiPending), nil
}

type vatFixedKeyWriter struct {
	at              *vectorArrayTable
	dataType        uint8
	curVersion      []byte
	curVersionBuf   []byte
	curNum          int
	setVersionIndex setFixedKeyVersionIndex
}

func newFixedKeyWriter(a *vectorArrayTable, dataType uint8, size int, setIndex setFixedKeyVersionIndex) *vatFixedKeyWriter {
	return &vatFixedKeyWriter{
		at:              a,
		dataType:        dataType,
		setVersionIndex: setIndex,
		curVersionBuf:   make([]byte, 0, size),
	}
}

func (w *vatFixedKeyWriter) writeVersion() error {
	if w.curVersion == nil {
		return nil
	}

	dataOffset := w.at.dataFile.Size()
	w.setVersionIndex(w.curVersion, dataOffset)
	hl := w.at.encodeItemHeader(w.dataType, w.curNum, 0, true)
	if _, err := w.at.dataFile.Write(w.at.itemHeaderBuf[:hl]); err != nil {
		return err
	}
	if _, err := w.at.dataFile.Write(w.curVersion); err != nil {
		return err
	}
	if _, err := w.at.dataFile.Write(w.curVersionBuf); err != nil {
		return err
	}
	w.curVersionBuf = w.curVersionBuf[:0]
	w.curVersion = nil
	w.curNum = 0
	return nil
}

func (w *vatFixedKeyWriter) writeItem(key *InternalKKVKey, value []byte) (uint32, error) {
	isFirst := false
	if w.curVersion == nil {
		w.curVersion = key.Version
		isFirst = true
	} else if !bytes.Equal(w.curVersion, key.Version) {
		if err := w.writeVersion(); err != nil {
			return 0, err
		}
		w.curVersion = key.Version
		isFirst = true
	}

	w.curVersionBuf = append(w.curVersionBuf, key.SubKey...)
	valueSize := len(value)
	if valueSize > 0 {
		w.curVersionBuf = append(w.curVersionBuf, value...)
	}

	w.curNum++
	sz := len(key.SubKey) + valueSize
	if isFirst {
		sz += vatKeyVersionSize + 2
	}
	return uint32(sz), nil
}
