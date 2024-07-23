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
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/base"
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
	flushable    *flushableBatch
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
	b.flushable = nil
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

type flushableBatchEntry struct {
	offset   uint32
	index    uint32
	keyStart uint32
	keyEnd   uint32
}

type flushableBatch struct {
	cmp       Compare
	formatKey base.FormatKey
	data      []byte
	seqNum    uint64
	offsets   []flushableBatchEntry
}

var _ flushable = (*flushableBatch)(nil)

func newFlushableBatchBitower(batch *BatchBitower, comparer *Comparer) *flushableBatch {
	b := &flushableBatch{
		data:      batch.data,
		cmp:       comparer.Compare,
		formatKey: comparer.FormatKey,
		offsets:   make([]flushableBatchEntry, 0, batch.Count()),
	}
	if b.data != nil {
		b.seqNum = batch.SeqNum()
	}
	if len(b.data) > batchHeaderLen {
		var index uint32
		for iter := BatchBitowerReader(b.data[batchHeaderLen:]); len(iter) > 0; index++ {
			offset := uintptr(unsafe.Pointer(&iter[0])) - uintptr(unsafe.Pointer(&b.data[0]))
			_, key, _, ok := iter.Next()
			if !ok {
				break
			}
			entry := flushableBatchEntry{
				offset: uint32(offset),
				index:  index,
			}
			if keySize := uint32(len(key)); keySize == 0 {
				entry.keyStart = uint32(offset) + 2
				entry.keyEnd = entry.keyStart
			} else {
				entry.keyStart = uint32(uintptr(unsafe.Pointer(&key[0])) -
					uintptr(unsafe.Pointer(&b.data[0])))
				entry.keyEnd = entry.keyStart + keySize
			}
			b.offsets = append(b.offsets, entry)
		}
	}

	sort.Sort(b)

	return b
}

func (b *flushableBatch) setSeqNum(seqNum uint64) {
	b.seqNum = seqNum
}

func (b *flushableBatch) Len() int {
	return len(b.offsets)
}

func (b *flushableBatch) Less(i, j int) bool {
	ei := &b.offsets[i]
	ej := &b.offsets[j]
	ki := b.data[ei.keyStart:ei.keyEnd]
	kj := b.data[ej.keyStart:ej.keyEnd]
	switch c := b.cmp(ki, kj); {
	case c < 0:
		return true
	case c > 0:
		return false
	default:
		return ei.offset > ej.offset
	}
}

func (b *flushableBatch) Swap(i, j int) {
	b.offsets[i], b.offsets[j] = b.offsets[j], b.offsets[i]
}

func (b *flushableBatch) get(key []byte) ([]byte, bool, base.InternalKeyKind) {
	iter := b.newIter(nil)
	ik, v := iter.SeekGE(key)
	if ik != nil && b.cmp(ik.UserKey, key) == 0 && ik.Kind() == base.InternalKeyKindSet {
		return v, true, ik.Kind()
	}
	return nil, false, base.InternalKeyKindInvalid
}

func (b *flushableBatch) newIter(o *IterOptions) internalIterator {
	return &flushableBatchIter{
		batch:   b,
		data:    b.data,
		offsets: b.offsets,
		cmp:     b.cmp,
		index:   -1,
		lower:   o.GetLowerBound(),
		upper:   o.GetUpperBound(),
	}
}

func (b *flushableBatch) newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator {
	return &flushFlushableBatchIter{
		flushableBatchIter: flushableBatchIter{
			batch:   b,
			data:    b.data,
			offsets: b.offsets,
			cmp:     b.cmp,
			index:   -1,
		},
		bytesIterated: bytesFlushed,
	}
}

func (b *flushableBatch) empty() bool {
	return len(b.data) <= batchHeaderLen
}

func (b *flushableBatch) inuseBytes() uint64 {
	return uint64(len(b.data) - batchHeaderLen)
}

func (b *flushableBatch) totalBytes() uint64 {
	return uint64(cap(b.data))
}

func (b *flushableBatch) readyForFlush() bool {
	return true
}

type flushableBatchIter struct {
	batch   *flushableBatch
	data    []byte
	offsets []flushableBatchEntry
	cmp     Compare
	index   int
	key     InternalKey
	err     error
	lower   []byte
	upper   []byte
}

var _ base.InternalIterator = (*flushableBatchIter)(nil)

func (i *flushableBatchIter) String() string {
	return "flushable-batch"
}

func (i *flushableBatchIter) SeekGE(key []byte) (*InternalKey, []byte) {
	i.err = nil
	ikey := base.MakeSearchKey(key)
	i.index = sort.Search(len(i.offsets), func(j int) bool {
		return base.InternalCompare(i.cmp, ikey, i.getKey(j)) <= 0
	})
	if i.index >= len(i.offsets) {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	if i.upper != nil && i.cmp(i.key.UserKey, i.upper) >= 0 {
		i.index = len(i.offsets)
		return nil, nil
	}
	return &i.key, i.Value()
}

func (i *flushableBatchIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	return i.SeekGE(key)
}

func (i *flushableBatchIter) SeekLT(key []byte) (*InternalKey, []byte) {
	i.err = nil
	ikey := base.MakeSearchKey(key)
	i.index = sort.Search(len(i.offsets), func(j int) bool {
		return base.InternalCompare(i.cmp, ikey, i.getKey(j)) <= 0
	})
	i.index--
	if i.index < 0 {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	if i.lower != nil && i.cmp(i.key.UserKey, i.lower) < 0 {
		i.index = -1
		return nil, nil
	}
	return &i.key, i.Value()
}

func (i *flushableBatchIter) First() (*InternalKey, []byte) {
	i.err = nil
	if len(i.offsets) == 0 {
		return nil, nil
	}
	i.index = 0
	i.key = i.getKey(i.index)
	if i.upper != nil && i.cmp(i.key.UserKey, i.upper) >= 0 {
		i.index = len(i.offsets)
		return nil, nil
	}
	return &i.key, i.Value()
}

func (i *flushableBatchIter) Last() (*InternalKey, []byte) {
	i.err = nil
	if len(i.offsets) == 0 {
		return nil, nil
	}
	i.index = len(i.offsets) - 1
	i.key = i.getKey(i.index)
	if i.lower != nil && i.cmp(i.key.UserKey, i.lower) < 0 {
		i.index = -1
		return nil, nil
	}
	return &i.key, i.Value()
}

func (i *flushableBatchIter) Next() (*InternalKey, []byte) {
	if i.index == len(i.offsets) {
		return nil, nil
	}
	i.index++
	if i.index == len(i.offsets) {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	if i.upper != nil && i.cmp(i.key.UserKey, i.upper) >= 0 {
		i.index = len(i.offsets)
		return nil, nil
	}
	return &i.key, i.Value()
}

func (i *flushableBatchIter) Prev() (*InternalKey, []byte) {
	if i.index < 0 {
		return nil, nil
	}
	i.index--
	if i.index < 0 {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	if i.lower != nil && i.cmp(i.key.UserKey, i.lower) < 0 {
		i.index = -1
		return nil, nil
	}
	return &i.key, i.Value()
}

func (i *flushableBatchIter) getKey(index int) InternalKey {
	e := &i.offsets[index]
	kind := InternalKeyKind(i.data[e.offset])
	key := i.data[e.keyStart:e.keyEnd]
	return base.MakeInternalKey(key, i.batch.seqNum+uint64(e.index), kind)
}

func (i *flushableBatchIter) Key() *InternalKey {
	return &i.key
}

func (i *flushableBatchIter) Value() []byte {
	p := i.data[i.offsets[i.index].offset:]
	if len(p) == 0 {
		i.err = base.CorruptionErrorf("corrupted batch")
		return nil
	}
	kind := InternalKeyKind(p[0])
	if kind > InternalKeyKindMax {
		i.err = base.CorruptionErrorf("corrupted batch")
		return nil
	}
	var value []byte
	var ok bool
	switch kind {
	case InternalKeyKindSet:
		keyEnd := i.offsets[i.index].keyEnd
		_, value, ok = batchDecodeStr(i.data[keyEnd:])
		if !ok {
			i.err = base.CorruptionErrorf("corrupted batch")
			return nil
		}
	}
	return value
}

func (i *flushableBatchIter) Valid() bool {
	return i.index >= 0 && i.index < len(i.offsets)
}

func (i *flushableBatchIter) Error() error {
	return i.err
}

func (i *flushableBatchIter) Close() error {
	return i.err
}

func (i *flushableBatchIter) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

type flushFlushableBatchIter struct {
	flushableBatchIter
	bytesIterated *uint64
}

var _ base.InternalIterator = (*flushFlushableBatchIter)(nil)

func (i *flushFlushableBatchIter) String() string {
	return "flushable-batch"
}

func (i *flushFlushableBatchIter) SeekGE(key []byte) (*InternalKey, []byte) {
	return nil, nil
}

func (i *flushFlushableBatchIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	return nil, nil
}

func (i *flushFlushableBatchIter) SeekLT(key []byte) (*InternalKey, []byte) {
	return nil, nil
}

func (i *flushFlushableBatchIter) First() (*InternalKey, []byte) {
	i.err = nil
	key, val := i.flushableBatchIter.First()
	if key == nil {
		return nil, nil
	}
	entryBytes := i.offsets[i.index].keyEnd - i.offsets[i.index].offset
	*i.bytesIterated += uint64(entryBytes) + i.valueSize()
	return key, val
}

func (i *flushFlushableBatchIter) Next() (*InternalKey, []byte) {
	if i.index == len(i.offsets) {
		return nil, nil
	}
	i.index++
	if i.index == len(i.offsets) {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	entryBytes := i.offsets[i.index].keyEnd - i.offsets[i.index].offset
	*i.bytesIterated += uint64(entryBytes) + i.valueSize()
	return &i.key, i.Value()
}

func (i *flushFlushableBatchIter) Prev() (*InternalKey, []byte) {
	return nil, nil
}

func (i *flushFlushableBatchIter) valueSize() uint64 {
	p := i.data[i.offsets[i.index].offset:]
	if len(p) == 0 {
		i.err = base.CorruptionErrorf("corrupted batch")
		return 0
	}
	kind := InternalKeyKind(p[0])
	if kind > InternalKeyKindMax {
		i.err = base.CorruptionErrorf("corrupted batch")
		return 0
	}
	var length uint64
	switch kind {
	case InternalKeyKindSet:
		keyEnd := i.offsets[i.index].keyEnd
		v, n := binary.Uvarint(i.data[keyEnd:])
		if n <= 0 {
			i.err = base.CorruptionErrorf("corrupted batch")
			return 0
		}
		length = v + uint64(n)
	}
	return length
}
