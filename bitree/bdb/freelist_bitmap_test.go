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
	"testing"
	"unsafe"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

func Test_freelist_bitmap(t *testing.T) {
	rb := roaring64.NewBitmap()
	for i := pgid(10); i > pgid(0); i-- {
		rb.Add(uint64(i))
	}

	rbLen := rb.GetSerializedSizeInBytes()

	buf := make([]byte, 4096)
	p := (*page)(unsafe.Pointer(&buf[0]))
	data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))

	bitmapLenBuf := unsafeByteSlice(data, 0, 0, bitmapLenSize)
	binary.LittleEndian.PutUint64(bitmapLenBuf, rbLen)

	var c []byte
	data = unsafeAdd(data, bitmapLenSize)
	unsafeSlice(unsafe.Pointer(&c), data, int(rbLen))

	var rbBuf bytes.Buffer
	_, err := rb.WriteTo(&rbBuf)
	require.NoError(t, err)

	fmt.Println("write bitmapArray", rb.ToArray())
	fmt.Println("rbLen", rbLen, len(c), len(rbBuf.Bytes()))

	copy(c, rbBuf.Bytes())

	var ids []pgid
	idx := 0
	data = unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p), unsafe.Sizeof(ids[0]), idx)
	bLenBuf := unsafeByteSlice(data, 0, 0, bitmapLenSize)
	bitmapLen := binary.LittleEndian.Uint64(bLenBuf)

	fmt.Println("bitmapLen", bitmapLen)

	data = unsafeAdd(data, bitmapLenSize)
	bitmapData := unsafeByteSlice(data, 0, 0, int(bitmapLen))

	buf1 := new(bytes.Buffer)
	buf1.Write(bitmapData)
	rb1 := roaring64.NewBitmap()
	_, err = rb1.ReadFrom(buf1)
	require.NoError(t, err)

	bitmapArray := rb1.ToArray()
	idsCopy := *(*[]pgid)(unsafe.Pointer(&bitmapArray))
	fmt.Println("read bitmapArray", idsCopy)
}

func Test_freelist_largeCount(t *testing.T) {
	f := newFreelist(consts.BdbFreelistMapType)

	buf := make([]byte, 4096)
	p := (*page)(unsafe.Pointer(&buf[0]))

	p.flags |= freelistPageFlag

	p.count = 0xFFFF
	data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	pgidCount := unsafeByteSlice(data, 0, 0, freelistLargeSize)
	binary.LittleEndian.PutUint64(pgidCount, uint64(1234567))
	data = unsafeAdd(data, freelistLargeSize)

	rb := roaring64.NewBitmap()
	for i := pgid(10); i > pgid(0); i-- {
		rb.Add(uint64(i))
	}
	rbLen := rb.GetSerializedSizeInBytes()
	err := f.copyallBitmap(data, rb, rbLen)
	require.NoError(t, err)

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

	fmt.Println("readFromBitmap", idx, count)

	var ids []pgid
	var data1 unsafe.Pointer
	data1 = unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p), unsafe.Sizeof(ids[0]), idx)
	bitmapLenBuf := unsafeByteSlice(data1, 0, 0, bitmapLenSize)
	bitmapLen := binary.LittleEndian.Uint64(bitmapLenBuf)

	data1 = unsafeAdd(data1, bitmapLenSize)
	bitmapData := unsafeByteSlice(data1, 0, 0, int(bitmapLen))

	buf1 := new(bytes.Buffer)
	buf1.Write(bitmapData)
	rb1 := roaring64.NewBitmap()
	if _, err := rb1.ReadFrom(buf1); err != nil {
		panic(fmt.Sprintf("read freelist from bitmap. err: %s", err))
	}

	bitmapArray := rb1.ToArray()
	idsCopy := *(*[]pgid)(unsafe.Pointer(&bitmapArray))
	fmt.Println("readFromBitmap", idsCopy)
}
