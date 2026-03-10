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
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

const (
	hashIterTypeAll = iota
	hashIterTypeKeys
	hashIterTypeValues
)

func (d *DB) hgetall(key []byte, slotId uint16, t int) ([][]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeHash, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var res [][]byte
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		SlotId:     uint32(slotId),
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		DataType:   kkv.DataTypeHash,
	}
	iter := d.NewKKVIterator(iterOpts)
	for iter.First(); iter.Valid(); iter.Next() {
		switch t {
		case hashIterTypeKeys:
			res = append(res, iter.GetSubKey())
		case hashIterTypeValues:
			res = append(res, iter.Value())
		default:
			res = append(res, iter.GetSubKey(), iter.Value())
		}
	}

	return res, func() { iter.Close() }, nil
}

func (d *DB) HGetAll(key []byte, slotId uint16) ([][]byte, func(), error) {
	return d.hgetall(key, slotId, hashIterTypeAll)
}

func (d *DB) HKeys(key []byte, slotId uint16) ([][]byte, func(), error) {
	return d.hgetall(key, slotId, hashIterTypeKeys)
}

func (d *DB) HValues(key []byte, slotId uint16) ([][]byte, func(), error) {
	return d.hgetall(key, slotId, hashIterTypeValues)
}

func (d *DB) HIncrBy(key []byte, slotId uint16, subKey []byte, incr int64) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, lindex, rindex, size, err := d.makeAliveMeta(hi, lo, DataTypeHash, slotId)
	if err != nil {
		return 0, err
	}

	var res int64
	var exist bool

	n := incr
	esk, eskCloser := kkv.EncodeSubKeyByPool(subKey, dt, version)
	defer eskCloser()
	if size > 0 {
		val, valCloser, err := d.kkvGetValue(esk, slotId)
		if err == nil {
			n, err = utils.FmtSliceToInt64(val)
			valCloser()
			if err != nil || incr == 0 {
				return n, err
			}
			n += incr
			exist = true
		}
	}
	newValue := utils.FmtInt64ToSlice(n)
	res = n
	if err = d.kkvSet(esk, slotId, newValue); err != nil {
		return 0, err
	}

	if !exist {
		size++
		if err = d.SetMeta(key, slotId, dt, hi, lo, timestamp, version, lindex, rindex, size); err != nil {
			return 0, err
		}
	}

	return res, nil
}

func (d *DB) HGet(key []byte, slotId uint16, subKey []byte) ([]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeHash, slotId)
	if size == 0 {
		return nil, nil, err
	}

	value, valueCloser, svErr := d.kkvGetSubKeyValue(subKey, version, dt, slotId)
	if svErr != nil {
		return nil, nil, svErr
	}

	return value, valueCloser, nil
}

func (d *DB) HMGet(key []byte, slotId uint16, subKeys ...[]byte) ([][]byte, []func(), error) {
	hi, lo := hash.MD5Uint64(key)
	res := make([][]byte, len(subKeys))
	dt, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeHash, slotId)
	if size == 0 {
		return res, nil, err
	}

	var closers []func()
	for i, subKey := range subKeys {
		if err = d.checkSubKeySize(subKey); err != nil {
			continue
		}

		value, valueCloser, svErr := d.kkvGetSubKeyValue(subKey, version, dt, slotId)
		if svErr == nil {
			res[i] = value
			closers = append(closers, valueCloser)
		}
	}

	return res, closers, nil
}

func (d *DB) HMSet(key []byte, slotId uint16, kvs ...[]byte) (int64, error) {
	return d.HMSetX(key, slotId, false, kvs...)
}

func (d *DB) HMSetX(key []byte, slotId uint16, isExist bool, kvs ...[]byte) (int64, error) {
	var dataType uint8
	var timestamp, version, lindex, rindex uint64
	var size uint32
	var err error

	hi, lo := hash.MD5Uint64(key)
	if isExist {
		dataType, timestamp, version, lindex, rindex, size, err = d.getAliveMeta(hi, lo, DataTypeHash, slotId)
	} else {
		dataType, timestamp, version, lindex, rindex, size, err = d.makeAliveMeta(hi, lo, DataTypeHash, slotId)
	}
	if err != nil {
		return 0, err
	}

	var n int64
	var foundKey bool
	var field, value []byte
	for i := 0; i < len(kvs); i += 2 {
		field = kvs[i]
		value = kvs[i+1]
		if err = d.checkSubKeyValueSize(field, value); err != nil {
			continue
		}

		sk, skCloser := kkv.EncodeSubKeyByPool(field, dataType, version)
		foundKey, err = d.kkvFindToSet(sk, InternalKeyKindSet, slotId, kkvWriteTypeFindAndAdd, value)
		skCloser()
		if err == nil && !foundKey {
			n++
			size++
		}
	}

	if n > 0 {
		if err = d.SetMeta(key, slotId, dataType, hi, lo, timestamp, version, lindex, rindex, size); err != nil {
			return 0, err
		}
	}

	return n, nil
}

func (d *DB) HDel(key []byte, slotId uint16, subKeys ...[]byte) (int64, error) {
	return d.DeleteKKV(key, slotId, DataTypeHash, subKeys...)
}

func (d *DB) HDelX(key []byte, slotId uint16, subKeys ...[]byte) (int64, error) {
	return d.DeleteXKKV(key, slotId, DataTypeHash, subKeys...)
}
