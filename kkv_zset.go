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
	"bytes"

	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

func (d *DB) kkvDeleteZsetKey(key []byte, slotId uint16, version uint64) error {
	if err := d.kkvDeleteKey(key, slotId); err != nil {
		return err
	}
	if member := kkv.DecodeZsetIndexMember(key); member != nil {
		var zsetDataKey [kkv.SubKeyZsetLength]byte
		kkv.EncodeZsetDataKey(zsetDataKey[:], version, member)
		if err := d.kkvDeleteKey(zsetDataKey[:], slotId); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) ZAdd(key []byte, slotId uint16, kvs ...[]byte) (int64, error) {
	if len(kvs) == 0 {
		return 0, nil
	}

	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, lindex, rindex, size, err := d.makeAliveMeta(hi, lo, DataTypeZset, slotId)
	if err != nil {
		return 0, err
	}

	var n int64
	var update bool
	var score, member []byte
	var zsetDataKey [kkv.SubKeyZsetLength]byte
	for i := 0; i < len(kvs); i += 2 {
		score = kvs[i]
		member = kvs[i+1]
		if err = d.checkSubKeyAndSiKey(member, score); err != nil {
			continue
		}

		update = true
		kkv.EncodeZsetDataKey(zsetDataKey[:], version, member)
		oldScore, oldScoreCloser, oldErr := d.kkvGetValue(zsetDataKey[:], slotId)
		if oldErr != nil {
			n++
			size++
		} else if bytes.Equal(oldScore, score) {
			update = false
		} else {
			d.kkvDeleteZsetIndexKey(slotId, version, oldScore, member)
		}
		if oldScoreCloser != nil {
			oldScoreCloser()
		}
		if update {
			if err = d.kkvSet(zsetDataKey[:], slotId, score); err != nil {
				d.opts.Logger.Errorf("set zset key fail key:%s sk:%s err:%s", string(key), string(member), err)
			}
			if err = d.kkvSetZsetIndexKey(slotId, version, score, member); err != nil {
				d.opts.Logger.Errorf("set zsetindex key fail key:%s sk:%s err:%s", string(key), string(member), err)
			}
		}
	}

	if n > 0 {
		if err = d.SetMeta(key, slotId, dt, hi, lo, timestamp, version, lindex, rindex, size); err != nil {
			return 0, err
		}
	}

	return n, nil
}

func (d *DB) ZIncrBy(key []byte, slotId uint16, member []byte, incr float64) (float64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, lindex, rindex, size, err := d.makeAliveMeta(hi, lo, DataTypeZset, slotId)
	if err != nil {
		return 0, err
	}

	var exist bool
	var floatBuf [8]byte
	var zsetDataKey [kkv.SubKeyZsetLength]byte
	kkv.EncodeZsetDataKey(zsetDataKey[:], version, member)
	n := incr
	if size > 0 {
		score, scoreCloser, err := d.kkvGetValue(zsetDataKey[:], slotId)
		if err == nil {
			n = kkv.DecodeZsetScore(score)
			if incr == 0 {
				scoreCloser()
				return n, nil
			}
			d.kkvDeleteZsetIndexKey(slotId, version, score, member)
			scoreCloser()
			n += incr
			exist = true
		}
	}
	newScore := utils.Float64ToByteSort(n, floatBuf[:])
	if err = d.kkvSetZsetIndexKey(slotId, version, newScore, member); err != nil {
		return float64(0), err
	}
	if err = d.kkvSet(zsetDataKey[:], slotId, newScore); err != nil {
		return float64(0), err
	}

	if !exist {
		size++
		if err = d.SetMeta(key, slotId, dt, hi, lo, timestamp, version, lindex, rindex, size); err != nil {
			return float64(0), err
		}
	}

	return n, nil
}

func (d *DB) ZRem(key []byte, slotId uint16, members ...[]byte) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	hi, lo := hash.MD5Uint64(key)
	dt, ts, version, lindex, rindex, size, err := d.getAliveMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 || err != nil {
		return 0, nil
	}

	var delCnt uint32
	var zsetDataKey [kkv.SubKeyZsetLength]byte
	for i := range members {
		member := members[i]
		if err = d.checkSubKeySize(member); err != nil {
			continue
		}

		kkv.EncodeZsetDataKey(zsetDataKey[:], version, member)
		score, scoreCloser, siErr := d.kkvGetValue(zsetDataKey[:], slotId)
		if siErr == nil {
			d.kkvDeleteZsetIndexKey(slotId, version, score, member)
			d.kkvDeleteKey(zsetDataKey[:], slotId)
			delCnt++
			scoreCloser()
		}

		if delCnt >= size {
			break
		}
	}

	if delCnt > 0 {
		newSize := size - delCnt
		if newSize == 0 {
			_, _, err = d.deleteKey(key, slotId, hi, lo, dt, version, ts)
		} else {
			err = d.SetMeta(key, slotId, dt, hi, lo, ts, version, lindex, rindex, newSize)
		}
		if err != nil {
			return 0, err
		}
	}

	return int64(delCnt), nil
}

func (d *DB) ZScore(key []byte, slotId uint16, member []byte) (float64, error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return 0, ErrZsetMemberNil
	}
	var zsetDataKey [kkv.SubKeyZsetLength]byte
	kkv.EncodeZsetDataKey(zsetDataKey[:], version, member)
	value, closer, err := d.kkvGetValue(zsetDataKey[:], slotId)
	if err != nil {
		return 0, err
	}
	defer closer()

	if len(value) != kkv.SiKeyLength {
		return 0, ErrZsetMemberNil
	}

	return kkv.DecodeZsetScore(value), nil
}

func (d *DB) ZRange(key []byte, slotId uint16, start int64, stop int64, reverse bool) ([]ScorePair, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return nil, nil, err
	}
	sz := int64(size)
	startIndex, stopIndex := zParseLimit(sz, start, stop, reverse)
	if startIndex > stopIndex || startIndex >= sz || stopIndex < 0 {
		return nil, nil, nil
	}

	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	dt := kkv.DataTypeZsetIndex
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   dt,
	}
	iter := d.NewKKVIterator(iterOpts)

	nv := stopIndex - startIndex
	if nv > 256 {
		nv = 256
	}
	res := make([]ScorePair, 0, nv)
	curIndex := int64(0)
	if !reverse {
		for iter.First(); iter.Valid(); iter.Next() {
			if curIndex >= startIndex {
				score, member := iter.GetZsetKey()
				zp := ScorePair{
					Score:  score,
					Member: member,
				}
				res = append(res, zp)
			}
			curIndex++
			if curIndex > stopIndex {
				break
			}
		}
	} else {
		curIndex = sz - 1
		for iter.Last(); iter.Valid(); iter.Prev() {
			if curIndex <= stopIndex {
				score, member := iter.GetZsetKey()
				zp := ScorePair{
					Score:  score,
					Member: member,
				}
				res = append(res, zp)
			}
			curIndex--
			if curIndex < startIndex {
				break
			}
		}
	}

	return res, func() { iter.Close() }, nil
}

func (d *DB) ZRangeByScore(
	key []byte, slotId uint16, min float64, max float64,
	leftClose bool, rightClose bool, offset int, count int,
) ([]ScorePair, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var lowerBound [kkv.SiKeyLowerBoundLength]byte
	var upperBound [kkv.SiKeyUpperBoundLength]byte
	kkv.EncodeZsetIndexLowerBound(lowerBound[:], version, min)
	kkv.EncodeZsetIndexUpperBound(upperBound[:], version, max)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   kkv.DataTypeZsetIndex,
	}
	iter := d.NewKKVIterator(iterOpts)

	var index int64
	stopIndex := int64(size - 1)
	skipped := 0
	nv := count
	if nv <= 0 || nv > 256 {
		nv = 256
	}

	res := make([]ScorePair, 0, nv)
	for iter.First(); iter.Valid() && index <= stopIndex; iter.Next() {
		score, member := iter.GetZsetKey()
		if rightClose && score == max {
			break
		}
		if !leftClose || score > min {
			if skipped >= offset {
				res = append(res, ScorePair{Score: score, Member: member})
				if count > 0 && len(res) == count {
					break
				}
			}
			skipped++
		}

		index++
		if index > stopIndex {
			break
		}
	}

	return res, func() { iter.Close() }, nil
}

func (d *DB) ZRevRangeByScore(
	key []byte, slotId uint16, min float64, max float64,
	leftClose bool, rightClose bool, offset int, count int,
) ([]ScorePair, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var lowerBound [kkv.SiKeyLowerBoundLength]byte
	var upperBound [kkv.SiKeyUpperBoundLength]byte
	kkv.EncodeZsetIndexLowerBound(lowerBound[:], version, min)
	kkv.EncodeZsetIndexUpperBound(upperBound[:], version, max)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   kkv.DataTypeZsetIndex,
	}
	iter := d.NewKKVIterator(iterOpts)

	skipped := 0
	nv := count
	if nv <= 0 || nv > 256 {
		nv = 256
	}

	res := make([]ScorePair, 0, nv)
	left := size
	for iter.Last(); iter.Valid() && left > 0; iter.Prev() {
		left--
		leftPass := false
		rightPass := false
		score, member := iter.GetZsetKey()
		if (leftClose && min < score) || (!leftClose && min <= score) {
			leftPass = true
		}
		if (rightClose && score < max) || (!rightClose && score <= max) {
			rightPass = true
		}
		if leftPass && rightPass {
			if skipped < offset {
				skipped++
				continue
			}
			res = append(res, ScorePair{Score: score, Member: member})
			if count > 0 && len(res) == count {
				break
			}
		}
		if !leftPass || left <= 0 {
			break
		}
	}

	return res, func() { iter.Close() }, nil
}

func (d *DB) ZRank(key []byte, slotId uint16, member []byte, reverse bool) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, size, _ := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return 0, ErrZsetMemberNil
	}

	var zsetDataKey [kkv.SubKeyZsetLength]byte
	kkv.EncodeZsetDataKey(zsetDataKey[:], version, member)
	if exist := d.kkvExist(zsetDataKey[:], slotId); !exist {
		return 0, ErrZsetMemberNil
	}

	var find bool
	var index int64
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	dt := kkv.DataTypeZsetIndex
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   dt,
	}
	iter := d.NewKKVIterator(iterOpts)
	defer iter.Close()
	sz := int64(size)
	if !reverse {
		for iter.First(); iter.Valid(); iter.Next() {
			field := iter.GetZsetMember()
			if bytes.Equal(field, member) {
				find = true
				break
			}
			index++
			if index >= sz {
				break
			}
		}
	} else {
		for iter.Last(); iter.Valid(); iter.Prev() {
			field := iter.GetZsetMember()
			if bytes.Equal(field, member) {
				find = true
				break
			}
			index++
			if index >= sz {
				break
			}
		}
	}

	if find {
		return index, nil
	}
	return 0, ErrZsetMemberNil
}

func (d *DB) ZCount(key []byte, slotId uint16, min float64, max float64, leftClose bool, rightClose bool) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return 0, err
	}

	var n, index int64
	var lowerBound [kkv.SiKeyLowerBoundLength]byte
	var upperBound [kkv.SiKeyUpperBoundLength]byte
	kkv.EncodeZsetIndexLowerBound(lowerBound[:], version, min)
	kkv.EncodeZsetIndexUpperBound(upperBound[:], version, max)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   kkv.DataTypeZsetIndex,
	}
	iter := d.NewKKVIterator(iterOpts)
	defer iter.Close()

	sz := int64(size)
	for iter.First(); iter.Valid(); iter.Next() {
		score, ok := iter.GetZsetScore()
		if !ok {
			break
		}
		if rightClose && score == max {
			break
		}
		if !leftClose || score > min {
			n++
		}
		index++
		if index == sz {
			break
		}
	}
	return n, nil
}

func (d *DB) ZRangeByLex(
	key []byte, slotId uint16, min []byte, max []byte,
	leftClose bool, rightClose bool, offset int, count int,
) ([][]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var res [][]byte
	var index int64
	var leftNoLimit, rightNotLimit bool
	if bytes.Equal([]byte{'-'}, min) {
		leftNoLimit = true
	}
	if bytes.Equal([]byte{'+'}, max) {
		rightNotLimit = true
	}

	dt := kkv.DataTypeZsetIndex
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   dt,
	}
	iter := d.NewKKVIterator(iterOpts)

	skipped := 0
	stopIndex := int64(size) - 1

	for iter.First(); iter.Valid() && index <= stopIndex; iter.Next() {
		leftPass := false
		rightPass := false
		member := iter.GetZsetMember()
		if leftNoLimit ||
			(leftClose && bytes.Compare(min, member) < 0) ||
			(!leftClose && bytes.Compare(min, member) <= 0) {
			leftPass = true
		}
		if rightNotLimit ||
			(rightClose && bytes.Compare(max, member) > 0) ||
			(!rightClose && bytes.Compare(max, member) >= 0) {
			rightPass = true
		}
		if leftPass && rightPass {
			if skipped < offset {
				skipped++
				continue
			}
			res = append(res, member)
			if count > 0 && len(res) == count {
				break
			}
		}
		if !rightPass {
			break
		}
		index++
		if index > stopIndex {
			break
		}
	}

	return res, func() { iter.Close() }, nil
}

func (d *DB) ZRemRangeByRank(
	key []byte, slotId uint16, start int64, stop int64,
) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, lindex, rindex, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return 0, err
	}

	var index, delCnt int64
	sz := int64(size)
	startIndex, stopIndex := zParseLimit(sz, start, stop, false)
	if startIndex > stopIndex || startIndex >= sz || stopIndex < 0 {
		return 0, nil
	}

	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	dataType := kkv.DataTypeZsetIndex
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dataType, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dataType, version)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   dataType,
	}
	iter := d.NewKKVIterator(iterOpts)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if index >= startIndex {
			d.kkvDeleteZsetKey(iter.Key(), slotId, version)
			delCnt++
		}
		index++
		if index > stopIndex {
			break
		}
	}

	if delCnt > 0 {
		newSize := uint32(sz - delCnt)
		if newSize == 0 {
			_, _, err = d.deleteKey(key, slotId, hi, lo, dt, version, timestamp)
		} else {
			err = d.SetMeta(key, slotId, dt, hi, lo, timestamp, version, lindex, rindex, newSize)
		}
		if err != nil {
			return 0, err
		}
	}
	return delCnt, nil
}

func (d *DB) ZRemRangeByScore(
	key []byte, slotId uint16,
	min float64, max float64,
	leftClose bool, rightClose bool,
) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, lindex, rindex, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return 0, err
	}

	var index, delCnt int64
	var leftPass, rightPass bool
	sz := int64(size)
	var lowerBound [kkv.SiKeyLowerBoundLength]byte
	var upperBound [kkv.SiKeyUpperBoundLength]byte
	kkv.EncodeZsetIndexLowerBound(lowerBound[:], version, min)
	kkv.EncodeZsetIndexUpperBound(upperBound[:], version, max)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   kkv.DataTypeZsetIndex,
	}
	iter := d.NewKKVIterator(iterOpts)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		leftPass = false
		rightPass = false
		score, ok := iter.GetZsetScore()
		if !ok {
			break
		}
		if (leftClose && min < score) || (!leftClose && min <= score) {
			leftPass = true
		}
		if (rightClose && score < max) || (!rightClose && score <= max) {
			rightPass = true
		}
		if leftPass && rightPass {
			d.kkvDeleteZsetKey(iter.Key(), slotId, version)
			delCnt++
		}
		if !rightPass {
			break
		}
		index++
		if index == sz {
			break
		}
	}

	if delCnt > 0 {
		newSize := uint32(sz - delCnt)
		if newSize == 0 {
			_, _, err = d.deleteKey(key, slotId, hi, lo, dt, version, timestamp)
		} else {
			err = d.SetMeta(key, slotId, dt, hi, lo, timestamp, version, lindex, rindex, newSize)
		}
		if err != nil {
			return 0, err
		}
	}
	return delCnt, nil
}

func (d *DB) ZRemRangeByLex(
	key []byte, slotId uint16,
	min []byte, max []byte,
	leftClose bool, rightClose bool,
) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, lindex, rindex, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return 0, err
	}

	var index, delCnt int64
	var leftPass, rightPass bool
	var leftNoLimit, rightNotLimit bool
	if bytes.Equal([]byte{'-'}, min) {
		leftNoLimit = true
	}
	if bytes.Equal([]byte{'+'}, max) {
		rightNotLimit = true
	}

	sz := int64(size)
	dataType := kkv.DataTypeZsetIndex
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dataType, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dataType, version)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   dataType,
	}
	iter := d.NewKKVIterator(iterOpts)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		leftPass = false
		rightPass = false
		field := iter.GetZsetMember()
		if leftNoLimit ||
			(leftClose && bytes.Compare(min, field) < 0) ||
			(!leftClose && bytes.Compare(min, field) <= 0) {
			leftPass = true
		}
		if rightNotLimit ||
			(rightClose && bytes.Compare(max, field) > 0) ||
			(!rightClose && bytes.Compare(max, field) >= 0) {
			rightPass = true
		}
		if leftPass && rightPass {
			d.kkvDeleteZsetKey(iter.Key(), slotId, version)
			delCnt++
		}
		if !rightPass {
			break
		}
		index++
		if index == sz {
			break
		}
	}

	if delCnt > 0 {
		newSize := uint32(sz - delCnt)
		if newSize == 0 {
			_, _, err = d.deleteKey(key, slotId, hi, lo, dt, version, timestamp)
		} else {
			err = d.SetMeta(key, slotId, dt, hi, lo, timestamp, version, lindex, rindex, newSize)
		}
		if err != nil {
			return 0, err
		}
	}
	return delCnt, nil
}

func (d *DB) ZLexCount(
	key []byte, slotId uint16, min []byte, max []byte,
	leftClose bool, rightClose bool,
) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeZset, slotId)
	if size == 0 {
		return 0, err
	}

	var count int64
	var index int64
	var leftNoLimit, rightNotLimit bool
	if bytes.Equal([]byte{'-'}, min) {
		leftNoLimit = true
	}
	if bytes.Equal([]byte{'+'}, max) {
		rightNotLimit = true
	}

	dt := kkv.DataTypeZsetIndex
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		SlotId:     uint32(slotId),
		DataType:   dt,
	}
	iter := d.NewKKVIterator(iterOpts)
	defer iter.Close()

	stopIndex := int64(size) - 1

	for iter.First(); iter.Valid() && index <= stopIndex; iter.Next() {
		leftPass := false
		rightPass := false
		field := iter.GetZsetMember()
		if leftNoLimit ||
			(leftClose && bytes.Compare(min, field) < 0) ||
			(!leftClose && bytes.Compare(min, field) <= 0) {
			leftPass = true
		}
		if rightNotLimit ||
			(rightClose && bytes.Compare(max, field) > 0) ||
			(!rightClose && bytes.Compare(max, field) >= 0) {
			rightPass = true
		}
		if leftPass && rightPass {
			count++
		}
		if !rightPass {
			break
		}
		index++
		if index > stopIndex {
			break
		}
	}

	return count, nil
}

func zParseLimit(size, start, stop int64, reverse bool) (startIndex int64, stopIndex int64) {
	if !reverse {
		if start >= 0 {
			startIndex = start
		} else {
			startIndex = size + start
		}
		if stop >= 0 {
			stopIndex = stop
		} else {
			stopIndex = size + stop
		}
	} else {
		if stop >= 0 {
			startIndex = size - stop - 1
		} else {
			startIndex = -stop - 1
		}
		if start >= 0 {
			stopIndex = size - start - 1
		} else {
			stopIndex = -start - 1
		}
	}

	if startIndex <= 0 {
		startIndex = 0
	}
	if stopIndex >= size {
		stopIndex = size - 1
	}
	return
}
