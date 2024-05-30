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

package bitalosdb

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/rawalloc"
)

const (
	batchCountOffset     = 8
	batchHeaderLen       = 12
	batchInitialSize     = 1 << 10
	batchMaxRetainedSize = 1 << 20
	invalidBatchCount    = 1<<32 - 1
	maxVarintLen32       = 5
)

var ErrInvalidBatch = errors.New("bitalosdb: invalid batch")

type DeferredBatchOp struct {
	Key, Value []byte
	offset     uint32
}

func (d DeferredBatchOp) Finish() error {
	return nil
}

type BatchBitower struct {
	db           *DB
	data         []byte
	cmp          Compare
	memTableSize uint64
	count        uint64
	deferredOp   DeferredBatchOp
	commit       sync.WaitGroup
	commitErr    error
	applied      uint32
	indexValid   bool
	index        int
}

var batchBitowerPool = sync.Pool{
	New: func() interface{} {
		return &BatchBitower{}
	},
}

func newBatchBitower(db *DB) *BatchBitower {
	b := batchBitowerPool.Get().(*BatchBitower)
	b.db = db
	return b
}

func newBatchBitowerByIndex(db *DB, index int) *BatchBitower {
	b := newBatchBitower(db)
	b.index = index
	b.indexValid = true
	return b
}

func (b *BatchBitower) release() {
	if b.db == nil {
		return
	}

	b.db = nil
	b.Reset()
	b.cmp = nil
	batchBitowerPool.Put(b)
}

func (b *BatchBitower) refreshMemTableSize() {
	b.memTableSize = 0
	if len(b.data) < batchHeaderLen {
		return
	}

	for r := b.Reader(); ; {
		_, key, value, ok := r.Next()
		if !ok {
			break
		}
		b.memTableSize += memTableEntrySize(len(key), len(value))
	}
}

func (b *BatchBitower) prepareDeferredKeyValueRecord(keyLen, valueLen int, kind InternalKeyKind) {
	if len(b.data) == 0 {
		b.init(keyLen + valueLen + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	b.count++
	b.memTableSize += memTableEntrySize(keyLen, valueLen)

	pos := len(b.data)
	b.deferredOp.offset = uint32(pos)
	b.grow(1 + 2*maxVarintLen32 + keyLen + valueLen)
	b.data[pos] = byte(kind)
	pos++

	{
		x := uint32(keyLen)
		for x >= 0x80 {
			b.data[pos] = byte(x) | 0x80
			x >>= 7
			pos++
		}
		b.data[pos] = byte(x)
		pos++
	}

	b.deferredOp.Key = b.data[pos : pos+keyLen]
	pos += keyLen

	{
		x := uint32(valueLen)
		for x >= 0x80 {
			b.data[pos] = byte(x) | 0x80
			x >>= 7
			pos++
		}
		b.data[pos] = byte(x)
		pos++
	}

	b.deferredOp.Value = b.data[pos : pos+valueLen]
	b.data = b.data[:pos+valueLen]
}

func (b *BatchBitower) prepareDeferredKeyRecord(keyLen int, kind InternalKeyKind) {
	if len(b.data) == 0 {
		b.init(keyLen + binary.MaxVarintLen64 + batchHeaderLen)
	}
	b.count++
	b.memTableSize += memTableEntrySize(keyLen, 0)

	pos := len(b.data)
	b.deferredOp.offset = uint32(pos)
	b.grow(1 + maxVarintLen32 + keyLen)
	b.data[pos] = byte(kind)
	pos++

	{
		x := uint32(keyLen)
		for x >= 0x80 {
			b.data[pos] = byte(x) | 0x80
			x >>= 7
			pos++
		}
		b.data[pos] = byte(x)
		pos++
	}

	b.deferredOp.Key = b.data[pos : pos+keyLen]
	b.deferredOp.Value = nil

	b.data = b.data[:pos+keyLen]
}

func (b *BatchBitower) setBitowerIndexByKey(key []byte) {
	if !b.indexValid && b.db != nil {
		b.index = b.db.getBitowerIndexByKey(key)
		b.indexValid = true
	}
}

func (b *BatchBitower) setBitowerIndex(index int) {
	b.index = index
}

func (b *BatchBitower) Set(key, value []byte, _ *WriteOptions) error {
	b.setBitowerIndexByKey(key)
	deferredOp := b.SetDeferred(len(key), len(value))
	copy(deferredOp.Key, key)
	copy(deferredOp.Value, value)
	return deferredOp.Finish()
}

func (b *BatchBitower) SetMultiValue(key []byte, values ...[]byte) error {
	b.setBitowerIndexByKey(key)
	var valueLen int
	for i := range values {
		valueLen += len(values[i])
	}
	deferredOp := b.SetDeferred(len(key), valueLen)
	copy(deferredOp.Key, key)
	pos := 0
	for j := range values {
		pos += copy(deferredOp.Value[pos:], values[j])
	}
	return deferredOp.Finish()
}

func (b *BatchBitower) SetDeferred(keyLen, valueLen int) *DeferredBatchOp {
	b.prepareDeferredKeyValueRecord(keyLen, valueLen, InternalKeyKindSet)
	return &b.deferredOp
}

func (b *BatchBitower) PrefixDeleteKeySet(key []byte, _ *WriteOptions) error {
	b.setBitowerIndexByKey(key)
	b.prepareDeferredKeyRecord(len(key), InternalKeyKindPrefixDelete)
	copy(b.deferredOp.Key, key)
	return b.deferredOp.Finish()
}

func (b *BatchBitower) Delete(key []byte, _ *WriteOptions) error {
	b.setBitowerIndexByKey(key)
	deferredOp := b.DeleteDeferred(len(key))
	copy(deferredOp.Key, key)
	return deferredOp.Finish()
}

func (b *BatchBitower) DeleteDeferred(keyLen int) *DeferredBatchOp {
	b.prepareDeferredKeyRecord(keyLen, InternalKeyKindDelete)
	return &b.deferredOp
}

func (b *BatchBitower) LogData(data []byte, _ *WriteOptions) error {
	origCount, origMemTableSize := b.count, b.memTableSize
	b.prepareDeferredKeyRecord(len(data), InternalKeyKindLogData)
	copy(b.deferredOp.Key, data)
	b.count, b.memTableSize = origCount, origMemTableSize
	return nil
}

func (b *BatchBitower) Empty() bool {
	return len(b.data) <= batchHeaderLen
}

func (b *BatchBitower) Repr() []byte {
	if len(b.data) == 0 {
		b.init(batchHeaderLen)
	}
	binary.LittleEndian.PutUint32(b.countData(), b.Count())
	return b.data
}

func (b *BatchBitower) SetRepr(data []byte) error {
	if len(data) < batchHeaderLen {
		return ErrInvalidBatch
	}
	b.data = data
	b.count = uint64(binary.LittleEndian.Uint32(b.countData()))
	if b.db != nil {
		b.refreshMemTableSize()
	}
	return nil
}

func (b *BatchBitower) Commit(o *WriteOptions) error {
	if b.Empty() {
		return nil
	}
	return b.db.ApplyBitower(b, o)
}

func (b *BatchBitower) Close() error {
	b.release()
	return nil
}

func (b *BatchBitower) init(cap int) {
	n := batchInitialSize
	for n < cap {
		n *= 2
	}
	b.data = rawalloc.New(batchHeaderLen, n)
	b.setCount(0)
	b.setSeqNum(0)
	b.data = b.data[:batchHeaderLen]
}

func (b *BatchBitower) Reset() {
	b.indexValid = false
	b.index = 0
	b.count = 0
	b.memTableSize = 0
	b.deferredOp = DeferredBatchOp{}
	b.commit = sync.WaitGroup{}
	b.commitErr = nil
	atomic.StoreUint32(&b.applied, 0)
	if b.data != nil {
		if cap(b.data) > batchMaxRetainedSize {
			b.data = nil
		} else {
			b.data = b.data[:batchHeaderLen]
			b.setSeqNum(0)
		}
	}
}

func (b *BatchBitower) seqNumData() []byte {
	return b.data[:8]
}

func (b *BatchBitower) countData() []byte {
	return b.data[8:12]
}

func (b *BatchBitower) grow(n int) {
	newSize := len(b.data) + n
	if newSize > cap(b.data) {
		newCap := 2 * cap(b.data)
		for newCap < newSize {
			newCap *= 2
		}
		newData := rawalloc.New(len(b.data), newCap)
		copy(newData, b.data)
		b.data = newData
	}
	b.data = b.data[:newSize]
}

func (b *BatchBitower) setSeqNum(seqNum uint64) {
	binary.LittleEndian.PutUint64(b.seqNumData(), seqNum)
}

func (b *BatchBitower) SeqNum() uint64 {
	if len(b.data) == 0 {
		b.init(batchHeaderLen)
	}
	return binary.LittleEndian.Uint64(b.seqNumData())
}

func (b *BatchBitower) setCount(v uint32) {
	b.count = uint64(v)
}

func (b *BatchBitower) Count() uint32 {
	return uint32(b.count)
}

func (b *BatchBitower) Reader() BatchBitowerReader {
	if len(b.data) == 0 {
		b.init(batchHeaderLen)
	}
	return b.data[batchHeaderLen:]
}

func batchDecodeStr(data []byte) (odata []byte, s []byte, ok bool) {
	var v uint32
	var n int
	ptr := unsafe.Pointer(&data[0])
	if a := *((*uint8)(ptr)); a < 128 {
		v = uint32(a)
		n = 1
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		v = uint32(b)<<7 | uint32(a)
		n = 2
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		v = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 3
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		v = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 4
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		v = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 5
	}

	data = data[n:]
	if v > uint32(len(data)) {
		return nil, nil, false
	}
	return data[v:], data[:v], true
}

type BatchBitowerReader []byte

func ReadBatchBitower(repr []byte) (r BatchBitowerReader, count uint32) {
	if len(repr) <= batchHeaderLen {
		return nil, count
	}
	count = binary.LittleEndian.Uint32(repr[batchCountOffset:batchHeaderLen])
	return repr[batchHeaderLen:], count
}

func (r *BatchBitowerReader) Next() (kind InternalKeyKind, ukey []byte, value []byte, ok bool) {
	if len(*r) == 0 {
		return 0, nil, nil, false
	}
	kind = InternalKeyKind((*r)[0])
	if kind > InternalKeyKindMax {
		return 0, nil, nil, false
	}
	*r, ukey, ok = batchDecodeStr((*r)[1:])
	if !ok {
		return 0, nil, nil, false
	}
	switch kind {
	case InternalKeyKindSet:
		*r, value, ok = batchDecodeStr(*r)
		if !ok {
			return 0, nil, nil, false
		}
	}
	return kind, ukey, value, true
}
