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

package bithash

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/cache/lrucache"
)

type BlockHandle struct {
	Offset, Length uint32
}

func encodeBlockHandle(dst []byte, b BlockHandle) {
	binary.LittleEndian.PutUint32(dst[0:4], b.Offset)
	binary.LittleEndian.PutUint32(dst[4:8], b.Length)
}

func decodeBlockHandle(src []byte) BlockHandle {
	offset := binary.LittleEndian.Uint32(src[0:4])
	length := binary.LittleEndian.Uint32(src[4:8])
	return BlockHandle{offset, length}
}

type blockEntry struct {
	offset   int32
	keyStart int32
	keyEnd   int32
	valStart int32
	valSize  int32
}

type blockIter struct {
	cmp          Compare
	offset       int32
	nextOffset   int32
	restarts     int32
	numRestarts  int32
	globalSeqNum uint64
	ptr          unsafe.Pointer
	data         []byte
	key          []byte
	fullKey      []byte
	val          []byte
	ikey         InternalKey
	cached       []blockEntry
	cachedBuf    []byte
	cacheHandle  lrucache.Handle
	firstKey     InternalKey
}

type block []byte

func newBlockIter(cmp Compare, block block) (*blockIter, error) {
	i := &blockIter{}
	return i, i.init(cmp, block, 0)
}

func (i *blockIter) init(cmp Compare, block block, globalSeqNum uint64) error {
	numRestarts := int32(binary.LittleEndian.Uint32(block[len(block)-4:]))
	if numRestarts == 0 {
		return errors.New("bithash invalid table block has no restart points")
	}
	i.cmp = cmp
	i.restarts = int32(len(block)) - 4*(1+numRestarts)
	i.numRestarts = numRestarts
	i.globalSeqNum = globalSeqNum
	i.ptr = unsafe.Pointer(&block[0])
	i.data = block
	i.fullKey = i.fullKey[:0]
	i.val = nil
	i.clearCache()
	if i.restarts > 0 {
		if err := i.readFirstKey(); err != nil {
			return err
		}
	} else {
		i.firstKey = InternalKey{}
	}
	return nil
}

func (i *blockIter) isDataInvalidated() bool {
	return i.data == nil
}

func (i *blockIter) resetForReuse() blockIter {
	return blockIter{
		fullKey:   i.fullKey[:0],
		cached:    i.cached[:0],
		cachedBuf: i.cachedBuf[:0],
		data:      nil,
	}
}

func (i *blockIter) readEntry() {
	ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(i.offset))

	var shared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		shared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		shared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		shared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		shared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		shared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var value uint32
	if a := *((*uint8)(ptr)); a < 128 {
		value = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		value = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		value = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		value = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		value = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	unsharedKey := getBytes(ptr, int(unshared))
	i.fullKey = append(i.fullKey[:shared], unsharedKey...)
	if shared == 0 {
		i.key = unsharedKey
	} else {
		i.key = i.fullKey
	}
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(unshared))
	i.val = getBytes(ptr, int(value))
	i.nextOffset = int32(uintptr(ptr)-uintptr(i.ptr)) + int32(value)
}

func (i *blockIter) decodeInternalKey(key []byte) {
	if n := len(key) - 8; n >= 0 {
		i.ikey.Trailer = binary.LittleEndian.Uint64(key[n:])
		i.ikey.UserKey = key[:n:n]
		if i.globalSeqNum != 0 {
			i.ikey.SetSeqNum(i.globalSeqNum)
		}
	} else {
		i.ikey.Trailer = uint64(InternalKeyKindInvalid)
		i.ikey.UserKey = nil
	}
}

func (i *blockIter) clearCache() {
	i.cached = i.cached[:0]
	i.cachedBuf = i.cachedBuf[:0]
}

func (i *blockIter) readFirstKey() error {
	ptr := i.ptr

	if shared := *((*uint8)(ptr)); shared == 0 {
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else {
		panic("first key in block must have zero shared length")
	}

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	if a := *((*uint8)(ptr)); a < 128 {
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a := *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); a < 128 {
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if a := *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); a < 128 {
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if a := *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); a < 128 {
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	firstKey := getBytes(ptr, int(unshared))
	if n := len(firstKey) - 8; n >= 0 {
		i.firstKey.Trailer = binary.LittleEndian.Uint64(firstKey[n:])
		i.firstKey.UserKey = firstKey[:n:n]
		if i.globalSeqNum != 0 {
			i.firstKey.SetSeqNum(i.globalSeqNum)
		}
	} else {
		i.firstKey.Trailer = uint64(InternalKeyKindInvalid)
		i.firstKey.UserKey = nil
		return base.CorruptionErrorf("bitalosdb: invalid firstKey in block")
	}
	return nil
}

func (i *blockIter) cacheEntry() {
	var valStart int32
	valSize := int32(len(i.val))
	if valSize > 0 {
		valStart = int32(uintptr(unsafe.Pointer(&i.val[0])) - uintptr(i.ptr))
	}

	i.cached = append(i.cached, blockEntry{
		offset:   i.offset,
		keyStart: int32(len(i.cachedBuf)),
		keyEnd:   int32(len(i.cachedBuf) + len(i.key)),
		valStart: valStart,
		valSize:  valSize,
	})
	i.cachedBuf = append(i.cachedBuf, i.key...)
}

func (i *blockIter) SeekGE(key []byte) (*InternalKey, []byte) {
	i.clearCache()

	ikey := base.MakeSearchKey(key)

	i.offset = 0
	var index int32

	{
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1)
			offset := int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			var v1 uint32
			if a := *((*uint8)(ptr)); a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			if *((*uint8)(ptr)) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			s := getBytes(ptr, int(v1))
			var k InternalKey
			if n := len(s) - 8; n >= 0 {
				k.Trailer = binary.LittleEndian.Uint64(s[n:])
				k.UserKey = s[:n:n]
			} else {
				k.Trailer = uint64(InternalKeyKindInvalid)
			}

			if base.InternalCompare(i.cmp, ikey, k) >= 0 {
				index = h + 1
			} else {
				upper = h
			}
		}
	}

	if index > 0 {
		i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	}
	i.readEntry()
	i.decodeInternalKey(i.key)

	for ; i.Valid(); i.Next() {
		if base.InternalCompare(i.cmp, i.ikey, ikey) >= 0 {
			return &i.ikey, i.val
		}
	}

	return nil, nil
}

func (i *blockIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	panic("bitalosdb: SeekPrefixGE unimplemented")
}

func (i *blockIter) SeekLT(key []byte) (*InternalKey, []byte) {
	i.clearCache()

	ikey := base.MakeSearchKey(key)

	i.offset = 0
	var index int32

	{
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1)
			offset := int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			var v1 uint32
			if a := *((*uint8)(ptr)); a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			if *((*uint8)(ptr)) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			s := getBytes(ptr, int(v1))
			var k InternalKey
			if n := len(s) - 8; n >= 0 {
				k.Trailer = binary.LittleEndian.Uint64(s[n:])
				k.UserKey = s[:n:n]
			} else {
				k.Trailer = uint64(InternalKeyKindInvalid)
			}

			if base.InternalCompare(i.cmp, ikey, k) > 0 {
				index = h + 1
			} else {
				upper = h
			}
		}
	}

	targetOffset := i.restarts
	if index > 0 {
		i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
		if index < i.numRestarts {
			targetOffset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index):]))
		}
	} else if index == 0 {
		i.offset = -1
		i.nextOffset = 0
		return nil, nil
	}

	i.nextOffset = i.offset

	for {
		i.offset = i.nextOffset
		i.readEntry()
		i.decodeInternalKey(i.key)

		if i.cmp(i.ikey.UserKey, ikey.UserKey) >= 0 {
			i.Prev()
			return &i.ikey, i.val
		}

		if i.nextOffset >= targetOffset {
			break
		}

		i.cacheEntry()
	}

	if !i.Valid() {
		return nil, nil
	}
	return &i.ikey, i.val
}

func (i *blockIter) First() (*InternalKey, []byte) {
	i.offset = 0
	if !i.Valid() {
		return nil, nil
	}
	i.clearCache()
	i.readEntry()
	i.decodeInternalKey(i.key)
	return &i.ikey, i.val
}

func (i *blockIter) Last() (*InternalKey, []byte) {
	i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(i.numRestarts-1):]))
	if !i.Valid() {
		return nil, nil
	}

	i.readEntry()
	i.clearCache()

	for i.nextOffset < i.restarts {
		i.cacheEntry()
		i.offset = i.nextOffset
		i.readEntry()
	}

	i.decodeInternalKey(i.key)
	return &i.ikey, i.val
}

func (i *blockIter) Next() (*InternalKey, []byte) {
	if len(i.cachedBuf) > 0 {
		i.fullKey = append(i.fullKey[:0], i.key...)
		i.clearCache()
	}

	i.offset = i.nextOffset
	if !i.Valid() {
		return nil, nil
	}
	i.readEntry()

	if n := len(i.key) - 8; n >= 0 {
		i.ikey.Trailer = binary.LittleEndian.Uint64(i.key[n:])
		i.ikey.UserKey = i.key[:n:n]
		if i.globalSeqNum != 0 {
			i.ikey.SetSeqNum(i.globalSeqNum)
		}
	} else {
		i.ikey.Trailer = uint64(InternalKeyKindInvalid)
		i.ikey.UserKey = nil
	}
	return &i.ikey, i.val
}

func (i *blockIter) Prev() (*InternalKey, []byte) {
	if n := len(i.cached) - 1; n >= 0 {
		i.nextOffset = i.offset
		e := &i.cached[n]
		i.offset = e.offset
		i.val = getBytes(unsafe.Pointer(uintptr(i.ptr)+uintptr(e.valStart)), int(e.valSize))
		i.key = i.cachedBuf[e.keyStart:e.keyEnd]
		if n := len(i.key) - 8; n >= 0 {
			i.ikey.Trailer = binary.LittleEndian.Uint64(i.key[n:])
			i.ikey.UserKey = i.key[:n:n]
			if i.globalSeqNum != 0 {
				i.ikey.SetSeqNum(i.globalSeqNum)
			}
		} else {
			i.ikey.Trailer = uint64(InternalKeyKindInvalid)
			i.ikey.UserKey = nil
		}
		i.cached = i.cached[:n]
		return &i.ikey, i.val
	}

	i.clearCache()
	if i.offset <= 0 {
		i.offset = -1
		i.nextOffset = 0
		return nil, nil
	}

	targetOffset := i.offset
	var index int32

	{
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1)
			offset := int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			if offset < targetOffset {
				index = h + 1
			} else {
				upper = h
			}
		}
	}

	i.offset = 0
	if index > 0 {
		i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	}

	i.readEntry()

	for i.nextOffset < targetOffset {
		i.cacheEntry()
		i.offset = i.nextOffset
		i.readEntry()
	}

	i.decodeInternalKey(i.key)
	return &i.ikey, i.val
}

func (i *blockIter) Key() *InternalKey {
	return &i.ikey
}

func (i *blockIter) Value() []byte {
	return i.val
}

func (i *blockIter) Valid() bool {
	return i.offset >= 0 && i.offset < i.restarts
}

func (i *blockIter) Error() error {
	return nil
}

func (i *blockIter) Close() error {
	i.cacheHandle.Release()
	i.cacheHandle = lrucache.Handle{}
	i.val = nil
	return nil
}

func (i *blockIter) SetBounds(lower, upper []byte) {
	panic("bitalosdb: SetBounds unimplemented")
}

type blockWriter struct {
	restartInterval int
	nEntries        int
	nextRestart     int
	buf             []byte
	restarts        []uint32
	curKey          []byte
	curValue        []byte
	prevKey         []byte
	tmp             [4]byte
}

func (w *blockWriter) store(keySize int, value []byte) {
	shared := 0
	if w.nEntries == w.nextRestart {
		w.nextRestart = w.nEntries + w.restartInterval
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	} else {
		n := len(w.curKey)
		if n > len(w.prevKey) {
			n = len(w.prevKey)
		}
		asUint64 := func(b []byte, i int) uint64 {
			return binary.LittleEndian.Uint64(b[i:])
		}
		for shared < n-7 && asUint64(w.curKey, shared) == asUint64(w.prevKey, shared) {
			shared += 8
		}
		for shared < n && w.curKey[shared] == w.prevKey[shared] {
			shared++
		}
	}

	needed := 3*binary.MaxVarintLen32 + len(w.curKey[shared:]) + len(value)
	n := len(w.buf)
	if cap(w.buf) < n+needed {
		newCap := 2 * cap(w.buf)
		if newCap == 0 {
			newCap = 1024
		}
		for newCap < n+needed {
			newCap *= 2
		}
		newBuf := make([]byte, n, newCap)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}
	w.buf = w.buf[:n+needed]

	{
		x := uint32(shared)
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	{
		x := uint32(keySize - shared)
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	{
		x := uint32(len(value))
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	n += copy(w.buf[n:], w.curKey[shared:])
	n += copy(w.buf[n:], value)
	w.buf = w.buf[:n]

	w.curValue = w.buf[n-len(value):]

	w.nEntries++
}

func (w *blockWriter) add(key InternalKey, value []byte) {
	w.curKey, w.prevKey = w.prevKey, w.curKey

	size := key.Size()
	if cap(w.curKey) < size {
		w.curKey = make([]byte, 0, size*2)
	}
	w.curKey = w.curKey[:size]
	key.Encode(w.curKey)

	w.store(size, value)
}

func (w *blockWriter) finish() []byte {
	// Write the restart points to the buffer.
	if w.nEntries == 0 {
		// Every block must have at least one restart point.
		if cap(w.restarts) > 0 {
			w.restarts = w.restarts[:1]
			w.restarts[0] = 0
		} else {
			w.restarts = append(w.restarts, 0)
		}
	}
	tmp4 := w.tmp[:4]
	for _, x := range w.restarts {
		binary.LittleEndian.PutUint32(tmp4, x)
		w.buf = append(w.buf, tmp4...)
	}
	binary.LittleEndian.PutUint32(tmp4, uint32(len(w.restarts)))
	w.buf = append(w.buf, tmp4...)
	result := w.buf

	// Reset the block state.
	w.nEntries = 0
	w.nextRestart = 0
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	return result
}

func (w *blockWriter) estimatedSize() int {
	return len(w.buf) + 4*(len(w.restarts)+1)
}
