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
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/manual"
)

const recordHeaderSize = 12

type block2Reader struct {
}

func (i *block2Reader) readRecordHeader(buf []byte) (ikeySize, valueSize uint32, fileNum FileNum) {
	ikeySize = binary.LittleEndian.Uint32(buf[0:4])
	valueSize = binary.LittleEndian.Uint32(buf[4:8])
	fileNum = FileNum(binary.LittleEndian.Uint32(buf[8:12]))
	return
}

func (i *block2Reader) readKV(buf []byte, ikeySize, valueSize uint32) (*InternalKey, []byte) {
	ptr := unsafe.Pointer(&buf[0])

	key := getBytes(ptr, int(ikeySize))

	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(ikeySize))
	value := getBytes(ptr, int(valueSize))

	var k InternalKey
	if n := len(key) - 8; n >= 0 {
		k.Trailer = binary.LittleEndian.Uint64(key[n:])
		k.UserKey = key[:n:n]
	} else {
		k.Trailer = uint64(InternalKeyKindInvalid)
	}

	return &k, value
}

func (i *block2Reader) readRecord(buf []byte) (*InternalKey, []byte, FileNum) {
	ikeySize, valueSize, fileNum := i.readRecordHeader(buf)
	recordLen := int(recordHeaderSize + ikeySize + valueSize)
	if ikeySize == 0 || valueSize == 0 || len(buf) != recordLen {
		return nil, nil, FileNum(0)
	}

	k, value := i.readKV(buf[recordHeaderSize:], ikeySize, valueSize)
	return k, value, fileNum
}

type block2Writer struct {
	buf []byte
	wr  io.Writer
}

func (w *block2Writer) set(key InternalKey, value []byte, fileNum FileNum) (int, error) {
	keySize := key.Size()
	preSize := keySize + recordHeaderSize
	wrn := 0

	if cap(w.buf) < preSize {
		w.buf = make([]byte, 0, preSize*2)
	}

	w.buf = w.buf[:preSize]
	binary.LittleEndian.PutUint32(w.buf[0:4], uint32(keySize))
	binary.LittleEndian.PutUint32(w.buf[4:8], uint32(len(value)))
	binary.LittleEndian.PutUint32(w.buf[8:12], uint32(fileNum))

	key.Encode(w.buf[recordHeaderSize:])
	n, err := w.wr.Write(w.buf)
	if err != nil {
		return 0, err
	}
	wrn += n

	n, err = w.wr.Write(value)
	if err != nil {
		return 0, err
	}
	wrn += n

	w.buf = w.buf[:0]
	return wrn, nil
}

func (w *block2Writer) setEmptyHeader() (int, error) {
	var buf [recordHeaderSize]byte
	for i := range buf {
		buf[i] = 0
	}

	n, err := w.wr.Write(buf[:])
	if err != nil {
		return 0, err
	}
	return n, nil
}

func getBytes(ptr unsafe.Pointer, length int) []byte {
	return (*[manual.MaxArrayLen]byte)(ptr)[:length:length]
}
