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

package arenaskl

import (
	"errors"
	"math"
	"sync/atomic"
	"unsafe"
)

type Arena struct {
	n   uint64
	buf []byte
}

const (
	align4 = 3
)

var (
	ErrArenaFull = errors.New("allocation failed because arena is full")
)

func NewArena(buf []byte) *Arena {
	return &Arena{
		n:   1,
		buf: buf,
	}
}

func (a *Arena) Size() uint32 {
	s := atomic.LoadUint64(&a.n)
	if s > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(s)
}

func (a *Arena) Capacity() uint32 {
	return uint32(len(a.buf))
}

func (a *Arena) alloc(size, align, overflow uint32) (uint32, uint32, error) {
	origSize := atomic.LoadUint64(&a.n)
	if int(origSize) > len(a.buf) {
		return 0, 0, ErrArenaFull
	}

	padded := size + align

	newSize := atomic.AddUint64(&a.n, uint64(padded))
	if int(newSize)+int(overflow) > len(a.buf) {
		return 0, 0, ErrArenaFull
	}

	offset := (uint32(newSize) - padded + align) & ^align
	return offset, padded, nil
}

func (a *Arena) getBytes(offset uint32, size uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+size : offset+size]
}

func (a *Arena) getPointer(offset uint32) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	return unsafe.Pointer(&a.buf[offset])
}

func (a *Arena) getPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}
	return uint32(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}
