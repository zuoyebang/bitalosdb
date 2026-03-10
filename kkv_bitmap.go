// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitalosdb

import (
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

const (
	bitmapFieldBits     = 64 << 10
	bitmapFieldBitsMask = bitmapFieldBits - 1
)

func getBitmapRange(start, end int) (uint64, uint64, bool) {
	if start < 0 {
		start = math.MaxInt64 + start + 1
	}

	if end < 0 {
		end = math.MaxInt64 + end + 1
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	return uint64(start), uint64(end), start <= end
}

func (d *DB) SetBit64(key []byte, slotId uint16, offset uint64, on int) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, bitSize, rindex, size, err := d.makeAliveMeta(hi, lo, DataTypeBitmap, slotId)
	if err != nil {
		return 0, err
	}

	var ret int64
	var changed bool
	var newValue []byte
	var skBuf [kkv.SubKeyListLength]byte

	rb := roaring.NewBitmap()
	segmentIndex := offset / bitmapFieldBits
	bitOffset := uint32(offset & bitmapFieldBitsMask)
	kkv.EncodeListKey(skBuf[:], dt, version, segmentIndex)
	oldValue, oldCloser, oldErr := d.kkvGetValue(skBuf[:], slotId)
	if oldErr != nil {
		size++
	} else {
		defer oldCloser()
		if _, err = rb.FromUnsafeBytes(oldValue); err != nil {
			return 0, ErrBitUnmarshal
		}

		if rb.Contains(bitOffset) {
			ret = 1
		}
	}

	if on == 1 {
		if ret == 1 {
			return ret, nil
		}
		rb.Add(bitOffset)
		bitSize++
		changed = true
	} else if on == 0 {
		if ret == 1 {
			rb.Remove(bitOffset)
			bitSize--
			changed = true
		}
	}

	newValue, err = rb.MarshalBinary()
	if err != nil {
		return 0, ErrBitMarshal
	}
	if err = d.kkvSet(skBuf[:], slotId, newValue); err != nil {
		return 0, err
	}

	if changed {
		if err = d.SetMeta(key, slotId, dt, hi, lo, timestamp, version, bitSize, rindex, size); err != nil {
			return 0, err
		}
	}

	return ret, nil
}

func (d *DB) GetBit64(key []byte, slotId uint16, offset uint64) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, _, _, _, err := d.kkvGetMeta(hi, lo, DataTypeBitmap, slotId)
	if err != nil {
		err = base.DisableErrNotFound(err)
		return 0, err
	}

	var skBuf [kkv.SubKeyListLength]byte
	segmentIndex := offset / bitmapFieldBits
	kkv.EncodeListKey(skBuf[:], dt, version, segmentIndex)
	value, valueCloser, valueErr := d.kkvGetValue(skBuf[:], slotId)
	if valueErr != nil {
		return 0, nil
	}
	defer valueCloser()

	rb := roaring.NewBitmap()
	if _, err = rb.FromUnsafeBytes(value); err != nil {
		return 0, ErrBitUnmarshal
	}

	if rb == nil {
		return 0, nil
	}

	bitOffset := uint32(offset & bitmapFieldBitsMask)
	if rb.Contains(bitOffset) {
		return 1, nil
	} else {
		return 0, nil
	}
}

func (d *DB) BitCount64(key []byte, slotId uint16, begin, end int) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, bitSize, _, _, err := d.kkvGetMeta(hi, lo, DataTypeBitmap, slotId)
	if err != nil {
		err = base.DisableErrNotFound(err)
		return 0, err
	}

	if begin == 0 && end == -1 {
		return int64(bitSize), nil
	}

	start, stop, hasRange := getBitmapRange(begin, end)
	if !hasRange {
		return 0, nil
	}

	var lowerBound, upperBound [kkv.SubKeyListLength]byte
	var iter *KKVIterator
	var count, mask uint64
	startIndex := start / bitmapFieldBits
	endIndex := stop / bitmapFieldBits
	kkv.EncodeListKey(lowerBound[:], dt, version, startIndex)
	kkv.EncodeListKey(upperBound[:], dt, version, endIndex+1)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   kkv.DataTypeBitmap,
	}
	iter = d.NewKKVIterator(iterOpts)
	defer iter.Close()
	rb := roaring.NewBitmap()
	for iter.First(); iter.Valid(); iter.Next() {
		rb.Clear()
		if _, err = rb.FromUnsafeBytes(iter.Value()); err != nil {
			continue
		}

		fc := rb.GetCardinality()
		mask = 0
		fieldIndex := iter.GetListIndex()

		if fieldIndex == startIndex {
			fieldStart := uint32(start & bitmapFieldBitsMask)
			rank := rb.Rank(fieldStart)
			if rank > 0 {
				if rb.Contains(fieldStart) {
					rank--
				}
				mask += rank
			}
		}

		if fieldIndex == endIndex {
			mask += fc - rb.Rank(uint32(end&bitmapFieldBitsMask))
		}

		count += fc - mask
	}

	return int64(count), nil
}

func (d *DB) BitPos64(key []byte, slotId uint16, on, begin, end int) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeBitmap, slotId)
	if size == 0 {
		if on == 1 {
			return -1, err
		}
		return 0, err
	}

	start, stop, hasRange := getBitmapRange(begin, end)
	if !hasRange {
		return -1, nil
	}

	var lowerBound, upperBound [kkv.SubKeyListLength]byte
	var iter *KKVIterator
	startIndex := start / bitmapFieldBits
	endIndex := stop / bitmapFieldBits
	kkv.EncodeListKey(lowerBound[:], dt, version, startIndex)
	kkv.EncodeListKey(upperBound[:], dt, version, endIndex+1)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   kkv.DataTypeBitmap,
	}
	iter = d.NewKKVIterator(iterOpts)
	defer iter.Close()
	rb := roaring.NewBitmap()
	for iter.First(); iter.Valid(); iter.Next() {
		rb.Clear()
		if _, err = rb.FromUnsafeBytes(iter.Value()); err != nil {
			continue
		}

		fieldIndex := iter.GetListIndex()
		fieldBitsBase := fieldIndex * bitmapFieldBits

		if on == 1 {
			if rb.IsEmpty() {
				continue
			}

			i := rb.Iterator()
			if fieldBitsBase <= start && start < fieldBitsBase+bitmapFieldBits {
				i.AdvanceIfNeeded(uint32(start & bitmapFieldBitsMask))
			}
			if i.HasNext() {
				x := i.Next()
				act := uint64(x) + fieldBitsBase
				if act <= stop {
					return int64(act), nil
				}
			}
			return -1, nil
		}

		if rb.IsEmpty() {
			if start <= fieldBitsBase {
				return int64(fieldBitsBase), nil
			}
			return int64(start), nil
		}

		s := uint32(0)
		if fieldBitsBase <= start && start < fieldBitsBase+bitmapFieldBits {
			i := rb.Iterator()
			i.AdvanceIfNeeded(uint32(start & bitmapFieldBitsMask))
			if !i.HasNext() {
				return -1, nil
			}
			x := i.Next()
			act := uint64(x) + fieldBitsBase
			if act > start {
				return int64(start), nil
			}
			s = x + 1
		}

		for s < bitmapFieldBits {
			act := uint64(s) + fieldBitsBase
			if !rb.Contains(s) {
				return int64(act), nil
			} else if act == stop {
				return -1, nil
			} else {
				s++
			}
		}

		if fieldIndex == endIndex {
			return -1, nil
		}
	}

	return -1, nil
}
