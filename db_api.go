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

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/unsafe2"
)

func (d *DB) Exist(key []byte, slotId uint16) (bool, error) {
	hi, lo := hash.MD5Uint64(key)
	s := d.getVmShard(slotId)
	rs := s.loadReadState()
	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		_, kind, exist := m.exist(hi, lo)
		if exist {
			rs.unref()
			switch kind {
			case InternalKeyKindSet, InternalKeyKindExpireAt, InternalKeyKindPrefixDelete:
				return true, nil
			default:
				return false, base.ErrNotFound
			}
		}
	}
	rs.unref()

	bt, err := d.getBitupleRead(slotId)
	if err != nil {
		return false, err
	}
	return bt.exist(hi, lo)
}

func (d *DB) GetTimestamp(key []byte, slotId uint16) (uint8, uint64, error) {
	hi, lo := hash.MD5Uint64(key)
	s := d.getVmShard(slotId)
	rs := s.loadReadState()
	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		_, ts, dt, kind, exist := m.getTimestamp(hi, lo)
		if exist {
			rs.unref()
			switch kind {
			case InternalKeyKindSet, InternalKeyKindExpireAt, InternalKeyKindPrefixDelete:
				return dt, ts, nil
			default:
				return 0, 0, base.ErrNotFound
			}
		}
	}
	rs.unref()

	b, err := d.getBitupleRead(slotId)
	if err != nil {
		return 0, 0, err
	}
	return b.getTimestamp(hi, lo)
}

func (d *DB) Get(key []byte, slotId uint16) (value []byte, dataType uint8, timestamp uint64, closer func(), err error) {
	var (
		kind          InternalKeyKind
		expireAtKey   bool
		expireAtKeyTs uint64
		mCloser       func()
	)

	defer func() {
		if err == nil && expireAtKey {
			timestamp = expireAtKeyTs
		}
	}()

	hi, lo := hash.MD5Uint64(key)
	s := d.getVmShard(slotId)
	rs := s.loadReadState()
	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		value, _, dataType, _, timestamp, kind, mCloser, err = m.getValue(hi, lo)
		if err != nil {
			continue
		}

		switch kind {
		case InternalKeyKindExpireAt:
			if !expireAtKey {
				expireAtKey = true
				expireAtKeyTs = timestamp
			}
			if mCloser != nil {
				mCloser()
			}
		case InternalKeyKindSet, InternalKeyKindPrefixDelete:
			return value, dataType, timestamp, func() {
				if mCloser != nil {
					mCloser()
				}
				rs.unref()
			}, nil
		case InternalKeyKindDelete:
			if mCloser != nil {
				mCloser()
			}
			rs.unref()
			return nil, 0, 0, nil, ErrNotFound
		}
	}
	rs.unref()

	var b *Bituple
	b, err = d.getBitupleRead(slotId)
	if err == nil {
		value, dataType, timestamp, closer, err = b.get(hi, lo)
	}
	if err != nil {
		return nil, 0, 0, nil, err
	}

	return value, dataType, timestamp, closer, nil
}

func (d *DB) GetMeta(key []byte, slotId uint16) (dataType uint8, timestamp, version, lindex, rindex uint64, size uint32, err error) {
	hi, lo := hash.MD5Uint64(key)
	return d.getMeta(hi, lo, slotId)
}

func (d *DB) GetMetaSize(key []byte, slotId uint16) (size uint32, err error) {
	hi, lo := hash.MD5Uint64(key)
	_, _, _, _, _, size, err = d.getMeta(hi, lo, slotId)
	return
}

func (d *DB) getMeta(hi, lo uint64, slotId uint16) (dataType uint8, timestamp, version, lindex, rindex uint64, size uint32, err error) {
	var (
		kind          InternalKeyKind
		expireAtKey   bool
		expireAtKeyTs uint64
	)

	defer func() {
		if err == nil && expireAtKey {
			timestamp = expireAtKeyTs
		}
	}()

	vmShard := d.getVmShard(slotId)
	rs := vmShard.loadReadState()
	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		dataType, _, kind, _, timestamp, version, lindex, rindex, size, err = m.getMeta(hi, lo)
		if err == nil {
			switch kind {
			case InternalKeyKindExpireAt:
				expireAtKey = true
				expireAtKeyTs = timestamp
				continue
			case InternalKeyKindSet, InternalKeyKindPrefixDelete:
				rs.unref()
				return dataType, timestamp, version, lindex, rindex, size, nil
			default:
				rs.unref()
				return 0, 0, 0, 0, 0, 0, ErrNotFound
			}
		}
	}
	rs.unref()

	var b *Bituple
	b, err = d.getBitupleRead(slotId)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	return b.getMeta(hi, lo)
}

func (d *DB) checkExpireInVmTable(slotId uint16, hi, lo uint64) bool {
	s := d.getVmShard(slotId)
	rs := s.loadReadState()
	defer rs.unref()

	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		_, ts, _, kind, exist := m.getTimestamp(hi, lo)
		if exist {
			if kind == InternalKeyKindDelete || !d.isTimestampAlive(ts) {
				return true
			} else {
				return false
			}
		}
	}

	return true
}

func (d *DB) isTimestampAlive(ts uint64) bool {
	return ts == 0 || ts > d.opts.GetNowTimestamp()
}

func (d *DB) getAliveMeta(hi, lo uint64, dt uint8, slotId uint16) (
	dataType uint8, timestamp, version, lindex, rindex uint64, size uint32, err error,
) {
	dataType, timestamp, version, lindex, rindex, size, err = d.getMeta(hi, lo, slotId)
	if err != nil {
		return
	}

	if d.isTimestampAlive(timestamp) {
		if dt != DataTypeNone && dt != dataType {
			err = base.ErrWrongType
		}
		return
	}

	size = 0
	err = ErrNotFound
	return
}

func (d *DB) makeAliveMeta(hi, lo uint64, dt uint8, slotId uint16) (
	dataType uint8, timestamp, version, lindex, rindex uint64, size uint32, err error,
) {
	dataType, timestamp, version, lindex, rindex, size, err = d.getMeta(hi, lo, slotId)
	if err == nil && d.isTimestampAlive(timestamp) {
		if dt != DataTypeNone && dt != dataType {
			err = base.ErrWrongType
		}
		return
	}

	if err != nil && err != ErrNotFound {
		return
	}

	dataType = dt
	version = d.meta.getNextKeyVersion()
	timestamp = 0
	size = 0
	err = nil
	if dataType == DataTypeList {
		lindex = kkv.InitalLeftIndex
		rindex = kkv.InitalRightIndex
	}
	return
}

func (d *DB) callSetVm(key []byte, keyKind InternalKeyKind,
	hi, lo, timestamp, version, lindex, rindex uint64,
	dataType uint8, slotId uint16, size uint32, value []byte,
) (err error) {
	s := d.getVmShard(slotId)
	sn := d.meta.getNextSeqNum()
	seqNum := base.EncodeTrailer(sn, keyKind)
	isBreak := false
	for {
		s.vm.RLock()
		err = s.vm.mutable.set(key, hi, lo, seqNum, dataType, timestamp, slotId, version, size, lindex, rindex, value)
		if err == base.ErrTableFull {
			s.vm.mutable.setWriteFull()
		} else {
			isBreak = true
			if err != nil {
				d.opts.Logger.Errorf("callSetVm fail key:%s err:%s", string(key), err)
			}
		}
		s.vm.RUnlock()
		if isBreak {
			break
		}

		s.switchMutable()
	}

	return err
}

func (d *DB) Set(key []byte, slotId uint16, dataType uint8, timestamp uint64, value []byte) error {
	hi, lo := hash.MD5Uint64(key)
	return d.callSetVm(key, InternalKeyKindSet, hi, lo, timestamp, 0, 0, 0, dataType, slotId, 0, value)
}

func (d *DB) SetMeta(
	key []byte, slotId uint16, dataType uint8,
	hi, lo, timestamp, version uint64,
	lindex, rindex uint64, size uint32,
) error {
	return d.callSetVm(key, InternalKeyKindSet, hi, lo, timestamp, version, lindex, rindex, dataType, slotId, size, nil)
}

func (d *DB) Delete(key []byte, slotId uint16) (int64, uint8, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, ts, version, _, _, _, err := d.getAliveMeta(hi, lo, DataTypeNone, slotId)
	if err != nil {
		return 0, 0, err
	}

	return d.deleteKey(key, slotId, hi, lo, dt, version, ts)
}

func (d *DB) deleteKey(
	key []byte, slotId uint16, hi, lo uint64,
	dataType uint8, version, timestamp uint64,
) (int64, uint8, error) {
	err := d.callSetVm(key, InternalKeyKindDelete, hi, lo, timestamp, version, 0, 0, dataType, slotId, 0, nil)
	if err != nil {
		return 0, 0, nil
	}

	if dataType == DataTypeString {
		return 1, dataType, nil
	}

	if err = d.kkvSetPrefixDelete(slotId, version); err != nil {
		return 0, 0, nil
	}

	if version > 0 {
		d.eliTask.updateExpireKey(slotId, version, timestamp, 0)
	}

	return 1, dataType, nil
}

func (d *DB) ExpireAt(key []byte, slotId uint16, newTs uint64) (int64, uint8, error) {
	var kind InternalKeyKind
	var dataType uint8
	var oldTs, version, lindex, rindex uint64
	var size uint32
	var isMemFind bool
	var memMutableId int
	var err error

	hi, lo := hash.MD5Uint64(key)
	s := d.getVmShard(slotId)
	rs := s.loadReadState()
	defer rs.unref()

	memIdx := len(rs.memtables) - 1
	for n := memIdx; n >= 0; n-- {
		m := rs.memtables[n]
		dataType, _, kind, _, oldTs, version, lindex, rindex, size, err = m.getMeta(hi, lo)
		if err != nil {
			continue
		}

		if kind == InternalKeyKindDelete || !d.isTimestampAlive(oldTs) {
			return 0, 0, nil
		}

		isMemFind = true
		if n == memIdx && kind == InternalKeyKindSet {
			memMutableId = m.getId()
		}
		break
	}

	if !isMemFind {
		var b *Bituple
		b, err = d.getBitupleRead(slotId)
		if err != nil {
			return 0, 0, nil
		}
		dataType, oldTs, version, lindex, rindex, size, err = b.getMeta(hi, lo)
		if err != nil {
			return 0, 0, base.DisableErrNotFound(err)
		} else if !d.isTimestampAlive(oldTs) {
			return 0, 0, nil
		}
	}

	if newTs == 0 && oldTs == 0 {
		return 0, dataType, nil
	} else if newTs == oldTs {
		return 1, dataType, nil
	}

	var sn uint64
	seqNum := d.meta.getNextSeqNum()
	isBreak := false
	for {
		s.vm.RLock()
		if memMutableId == s.vm.mutable.getId() {
			sn = base.EncodeTrailer(seqNum, InternalKeyKindSet)
			err = s.vm.mutable.setTimestamp(hi, lo, sn, newTs, dataType)
		} else {
			if dataType == DataTypeString {
				sn = base.EncodeTrailer(seqNum, InternalKeyKindExpireAt)
			} else {
				sn = base.EncodeTrailer(seqNum, InternalKeyKindSet)
			}
			err = s.vm.mutable.set(key, hi, lo, sn, dataType, newTs, slotId, version, size, lindex, rindex, nil)
		}
		if err == base.ErrTableFull {
			s.vm.mutable.setWriteFull()
		} else {
			isBreak = true
			if err != nil {
				d.opts.Logger.Errorf("callSet fail key:%s err:%s", string(key), err)
			}
		}
		s.vm.RUnlock()
		if isBreak {
			break
		}

		s.switchMutable()
	}
	if err != nil {
		return 0, 0, nil
	}

	if dataType >= DataTypeBitmap {
		d.eliTask.updateExpireKey(slotId, version, oldTs, newTs)
	}

	return 1, dataType, nil
}

func (d *DB) Scan(cursor []byte, count int, dt uint8, cmp func(member string) bool) ([]byte, [][]byte, error) {
	var (
		key       []byte
		dataType  uint8
		timestamp uint64
	)

	if len(cursor) == 0 || bytes.Equal(cursor, kkv.ScanEndCurosr) {
		cursor = nil
	}

	if count <= 0 {
		count = 10
	}
	v := make([][]byte, 0, count)

	it := d.NewBitupleIter()
	defer it.Close()

	if cursor == nil {
		key, dataType, timestamp = it.First()
	} else {
		key, dataType, timestamp = it.Seek(cursor)
	}
	for i := 0; key != nil; key, dataType, timestamp = it.Next() {
		if !cmp(unsafe2.String(key)) {
			continue
		}

		if (dt > DataTypeNone && dt != dataType) || !d.isTimestampAlive(timestamp) {
			continue
		}

		v = append(v, key)
		i++
		if i == count {
			break
		}
	}

	if len(v) == count {
		cursor = it.GetCursor()
	} else {
		cursor = kkv.ScanEndCurosr
	}

	return cursor, v, nil
}
