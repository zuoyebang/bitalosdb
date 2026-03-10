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
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

func (d *DB) ListPush(key []byte, slotId uint16, isLeft bool, isExist bool, values ...[]byte) (int64, error) {
	var dataType uint8
	var timestamp, version, lindex, rindex uint64
	var size uint32
	var err error

	hi, lo := hash.MD5Uint64(key)
	if isExist {
		dataType, timestamp, version, lindex, rindex, size, err = d.getAliveMeta(hi, lo, kkv.DataTypeList, slotId)
	} else {
		dataType, timestamp, version, lindex, rindex, size, err = d.makeAliveMeta(hi, lo, kkv.DataTypeList, slotId)
	}
	if err != nil {
		err = base.DisableErrNotFound(err)
		return 0, err
	}

	var n uint32
	var index uint64
	var skBuf [kkv.SubKeyListLength]byte

	for i := range values {
		if err = d.checkValueSize(values[i]); err != nil {
			return 0, err
		}

		if isLeft {
			index = lindex
		} else {
			index = rindex
		}

		kkv.EncodeListKey(skBuf[:], dataType, version, index)
		if err = d.kkvSet(skBuf[:], slotId, values[i]); err == nil {
			size++
			n++
			if isLeft {
				lindex--
			} else {
				rindex++
			}
		}
	}

	if n > 0 {
		if err = d.SetMeta(key, slotId, dataType, hi, lo, timestamp, version, lindex, rindex, size); err != nil {
			return 0, err
		}
	}

	return int64(size), nil
}

func (d *DB) ListSet(key []byte, slotId uint16, index int64, value []byte) error {
	hi, lo := hash.MD5Uint64(key)
	dataType, _, version, lindex, rindex, size, err := d.kkvGetMeta(hi, lo, DataTypeList, slotId)
	if err != nil {
		return err
	} else if size == 0 {
		return base.ErrNoSuchKey
	}

	size64 := int64(size)
	if index >= size64 || -index > size64 {
		return base.ErrIndexOutOfRange
	}

	var newIndex int64
	var skBuf [kkv.SubKeyListLength]byte
	if index >= 0 {
		newIndex = int64(lindex) + 1 + index
	} else {
		newIndex = int64(rindex) + index
	}
	kkv.EncodeListKey(skBuf[:], dataType, version, uint64(newIndex))
	return d.kkvSet(skBuf[:], slotId, value)
}

func (d *DB) ListPop(key []byte, slotId uint16, isLeft bool) ([]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	dataType, ts, version, lindex, rindex, size, err := d.getAliveMeta(hi, lo, DataTypeList, slotId)
	if err != nil {
		err = base.DisableErrNotFound(err)
		return nil, nil, err
	}

	if lindex == rindex-1 {
		return nil, nil, nil
	}

	var index uint64
	var esk [kkv.SubKeyListLength]byte
	if isLeft {
		index = lindex + 1
		lindex++
	} else {
		index = rindex - 1
		rindex--
	}

	kkv.EncodeListKey(esk[:], dataType, version, index)
	v, vcloser, verr := d.kkvGetValue(esk[:], slotId)
	if verr != nil {
		return nil, nil, base.ErrNotFound
	}

	size--
	if size == 0 {
		_, _, err = d.deleteKey(key, slotId, hi, lo, dataType, version, ts)
	} else {
		err = d.kkvDeleteKey(esk[:], slotId)
		if err == nil {
			err = d.SetMeta(key, slotId, dataType, hi, lo, ts, version, lindex, rindex, size)
		}
	}
	if err != nil {
		vcloser()
		return nil, nil, err
	}

	return v, vcloser, nil
}

func (d *DB) ListTrim(key []byte, slotId uint16, start, stop int64) (err error) {
	hi, lo := hash.MD5Uint64(key)
	dataType, ts, version, lindex, rindex, size, _ := d.getAliveMeta(hi, lo, DataTypeList, slotId)
	if size == 0 {
		return nil
	}

	llen := int64(size)
	if start < 0 {
		start = llen + start
	}
	if stop < 0 {
		stop = llen + stop
	}

	if start >= llen || start > stop {
		_, _, err = d.deleteKey(key, slotId, hi, lo, dataType, version, ts)
		return err
	}

	if start < 0 {
		start = 0
	}
	if stop >= llen {
		stop = llen - 1
	}

	var skBuf [kkv.SubKeyListLength]byte
	var index uint64

	if start > 0 {
		for i := int64(0); i < start; i++ {
			index = lindex + 1
			kkv.EncodeListKey(skBuf[:], dataType, version, index)
			d.kkvDeleteKey(skBuf[:], slotId)
			size--
			lindex++
		}
	}
	if stop < llen-1 {
		for i := stop + 1; i < llen; i++ {
			index = rindex - 1
			kkv.EncodeListKey(skBuf[:], dataType, version, index)
			d.kkvDeleteKey(skBuf[:], slotId)
			size--
			rindex--
		}
	}

	if llen != int64(size) {
		if size == 0 {
			_, _, err = d.deleteKey(key, slotId, hi, lo, dataType, version, ts)
		} else {
			err = d.SetMeta(key, slotId, dataType, hi, lo, ts, version, lindex, rindex, size)
		}
	}

	return err
}

func (d *DB) ListIndex(key []byte, slotId uint16, index int64) ([]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, lindex, rindex, size, err := d.kkvGetMeta(hi, lo, DataTypeList, slotId)
	if size == 0 {
		return nil, nil, err
	}

	size64 := int64(size)
	if index >= size64 || -index > size64 {
		return nil, nil, nil
	}

	var skBuf [kkv.SubKeyListLength]byte
	var newIndex int64
	if index >= 0 {
		newIndex = int64(lindex) + 1 + index
	} else {
		newIndex = int64(rindex) + index
	}

	kkv.EncodeListKey(skBuf[:], dt, version, uint64(newIndex))
	return d.kkvGetValue(skBuf[:], slotId)
}

func (d *DB) ListRange(key []byte, slotId uint16, start, stop int64) ([][]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, lindex, _, size, err := d.kkvGetMeta(hi, lo, DataTypeList, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var startIndex, stopIndex int64
	startIndex, stopIndex, err = getListRangeIndex(start, stop, int64(size))
	if err != nil {
		return nil, nil, nil
	}

	limit := int(stopIndex - startIndex + 1)
	if limit > kkv.ListReadMax {
		limit = kkv.ListReadMax
	}
	lowerIndex := lindex + 1 + uint64(startIndex)
	upperIndex := lowerIndex + uint64(limit)

	var lowerBound, upperBound [kkv.SubKeyListLength]byte
	var iter *KKVIterator
	res := make([][]byte, 0, limit)
	iterCnt := 0
	kkv.EncodeListKey(lowerBound[:], dt, version, lowerIndex)
	kkv.EncodeListKey(upperBound[:], dt, version, upperIndex)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   kkv.DataTypeList,
	}
	iter = d.NewKKVIterator(iterOpts)
	for iter.First(); iter.Valid(); iter.Next() {
		res = append(res, iter.Value())
		iterCnt++
		if iterCnt >= limit {
			break
		}
	}

	return res, func() { iter.Close() }, nil
}

func getListRangeIndex(start, stop, size int64) (startIndex, stopIndex int64, err error) {
	if stop < 0 {
		if tmp := size + stop; tmp < 0 {
			return 0, 0, base.ErrIndexOutOfRange
		} else {
			stopIndex = tmp
		}
	} else {
		stopIndex = stop
	}

	if start < 0 {
		if tmp := size + start; tmp < 0 {
			startIndex = 0
		} else {
			startIndex = tmp
		}
	} else {
		startIndex = start
	}

	if startIndex >= size || startIndex > stopIndex {
		return 0, 0, base.ErrIndexOutOfRange
	}

	if stopIndex >= size {
		stopIndex = size - 1
	}

	return
}
