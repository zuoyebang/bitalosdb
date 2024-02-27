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

package bdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/RoaringBitmap/roaring/roaring64"
)

const (
	bitmapLenSize            = 8
	freelistLargeSize        = 8
	freelistBitmapHeaderSize = 16
)

func (f *freelist) copyallBitmap(dst unsafe.Pointer, rb *roaring64.Bitmap, rbLen uint64) error {
	bitmapLenBuf := unsafeByteSlice(dst, 0, 0, bitmapLenSize)
	binary.LittleEndian.PutUint64(bitmapLenBuf, rbLen)

	rbBytes, err := rb.ToBytes()
	if err != nil {
		return err
	}
	var c []byte
	dst = unsafeAdd(dst, bitmapLenSize)
	unsafeSlice(unsafe.Pointer(&c), dst, int(rbLen))
	copy(c, rbBytes)
	return nil
}

func (f *freelist) readFromBitmap(p *page) {
	if (p.flags & freelistPageFlag) == 0 {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.id, p.typ()))
	}
	var idx, count = 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		c := *(*pgid)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
		count = int(c)
		if count < 0 {
			panic(fmt.Sprintf("leading element count %d overflows int", c))
		}
	}

	if count == 0 {
		f.ids = nil
	} else {
		var ids []pgid
		var data unsafe.Pointer
		data = unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p), unsafe.Sizeof(ids[0]), idx)
		bitmapLenBuf := unsafeByteSlice(data, 0, 0, bitmapLenSize)
		bitmapLen := binary.LittleEndian.Uint64(bitmapLenBuf)

		data = unsafeAdd(data, bitmapLenSize)
		bitmapData := unsafeByteSlice(data, 0, 0, int(bitmapLen))

		buf := new(bytes.Buffer)
		buf.Write(bitmapData)
		rb := roaring64.NewBitmap()
		if _, err := rb.ReadFrom(buf); err != nil {
			panic(fmt.Sprintf("read freelist from bitmap. err: %s", err))
		}

		bitmapArray := rb.ToArray()
		if len(bitmapArray) != count {
			panic(fmt.Sprintf("read freelist count not match. page:%d, bitmap:%d", count, len(bitmapArray)))
		}
		idsCopy := *(*[]pgid)(unsafe.Pointer(&bitmapArray))

		f.readIDs(idsCopy)
	}
}

func (f *freelist) writeBitmap(p *page, rb *roaring64.Bitmap, rbLen uint64, freeCount int) error {
	p.flags |= freelistPageFlag

	l := freeCount
	if l == 0 {
		p.count = uint16(l)
	} else if l < 0xFFFF {
		p.count = uint16(l)
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		return f.copyallBitmap(data, rb, rbLen)
	} else {
		p.count = 0xFFFF
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		pgidCount := unsafeByteSlice(data, 0, 0, freelistLargeSize)
		binary.LittleEndian.PutUint64(pgidCount, uint64(l))
		data = unsafeAdd(data, freelistLargeSize)
		return f.copyallBitmap(data, rb, rbLen)
	}

	return nil
}
