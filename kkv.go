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

const (
	kkvWriteTypeDirectAdd int = 1 + iota
	kkvWriteTypeFindAndAdd
	kkvWriteTypeNotExistToAdd
	kkvWriteTypeExistToAdd
)

func (d *DB) checkSubKeySize(subKey []byte) error {
	skLen := len(subKey)
	if skLen > d.opts.MaxSubKeySize || skLen == 0 {
		return ErrSubKeySize
	}
	return nil
}

func (d *DB) checkValueSize(value []byte) error {
	if len(value) > d.opts.MaxValueSize {
		return ErrValueSize
	}
	return nil
}

func (d *DB) checkSubKeyValueSize(subKey, value []byte) error {
	skLen := len(subKey)
	if skLen > d.opts.MaxSubKeySize || skLen == 0 {
		return ErrSubKeySize
	} else if len(value) > d.opts.MaxValueSize {
		return ErrValueSize
	}
	return nil
}

func (d *DB) checkSubKeyAndSiKey(subKey, siKey []byte) error {
	skLen := len(subKey)
	if skLen > d.opts.MaxSubKeySize || skLen == 0 {
		return ErrSubKeySize
	} else if len(siKey) != kkv.SiKeyLength {
		return ErrSiKeySize
	}

	return nil
}

func (d *DB) kkvGetMeta(hi, lo uint64, dataType uint8, slotId uint16) (
	dt uint8, timestamp, version, lindex, rindex uint64, size uint32, err error,
) {
	dt, timestamp, version, lindex, rindex, size, err = d.getAliveMeta(hi, lo, dataType, slotId)
	if err != nil {
		err = base.DisableErrNotFound(err)
		size = 0
	}
	return
}

func (d *DB) kkvGetValue(key []byte, slotId uint16) ([]byte, func(), error) {
	ms := d.getMemShard(slotId)
	rs := ms.loadReadState()
	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		mValue, mExist, kind := m.get(key)
		if mExist {
			switch kind {
			case InternalKeyKindSet, InternalKeyKindPrefixDelete:
				return mValue, func() { rs.unref() }, nil
			case InternalKeyKindDelete:
				rs.unref()
				return nil, nil, ErrNotFound
			}
		}
	}

	rs.unref()

	btree := d.getBitreeRead(slotId)
	if btree == nil {
		return nil, nil, ErrNotFound
	}

	keyHash := hash.Fnv32(key)
	sv, skExist, svCloser := btree.Get(key, keyHash)
	if !skExist {
		return nil, nil, base.ErrNotFound
	}

	return sv, svCloser, nil
}

func (d *DB) kkvGetSubKeyValue(subKey []byte, version uint64, dt uint8, slotId uint16) ([]byte, func(), error) {
	esk, eskCloser := kkv.EncodeSubKeyByPool(subKey, dt, version)
	defer eskCloser()
	return d.kkvGetValue(esk, slotId)
}

func (d *DB) kkvExist(key []byte, slotId uint16) bool {
	ms := d.getMemShard(slotId)
	rs := ms.loadReadState()
	for n := len(rs.memtables) - 1; n >= 0; n-- {
		m := rs.memtables[n]
		_, mExist, kind := m.get(key)
		if mExist {
			rs.unref()
			switch kind {
			case InternalKeyKindSet, InternalKeyKindPrefixDelete:
				return true
			default:
				return false
			}
		}
	}

	rs.unref()

	btree := d.getBitreeRead(slotId)
	if btree == nil {
		return false
	}

	return btree.Exist(key, hash.Fnv32(key))
}

func (d *DB) kkvExistSubKey(subKey []byte, slotId uint16, dataType uint8, version uint64) bool {
	skey, skeyCloser := kkv.EncodeSubKeyByPool(subKey, dataType, version)
	defer skeyCloser()

	return d.kkvExist(skey, slotId)
}

func (d *DB) kkvExistSubKeys(key []byte, slotId uint16, dataType uint8, subKeys ...[]byte) []bool {
	hi, lo := hash.MD5Uint64(key)
	res := make([]bool, len(subKeys))
	dt, _, version, _, _, size, _ := d.kkvGetMeta(hi, lo, dataType, slotId)
	if size == 0 {
		return res
	}

	for i, subKey := range subKeys {
		res[i] = d.kkvExistSubKey(subKey, slotId, dt, version)
	}

	return res
}

func (d *DB) kkvFindToSet(
	key []byte, keyKind InternalKeyKind, slotId uint16, addType int, values ...[]byte,
) (found bool, err error) {
	valueSize := 0
	for _, value := range values {
		valueSize += len(value)
	}
	newSize := memTableEntrySize(len(key), valueSize+kkv.SlotIdLength)
	ms := d.getMemShard(slotId)
	ms.mem.Lock()
	ms.makeRoomForWrite(newSize, false)
	mem := ms.mem.mutable
	ms.mem.Unlock()

	defer func() {
		if mem.writerUnref() {
			ms.mem.Lock()
			ms.maybeScheduleFlush(true, false)
			ms.mem.Unlock()
		}
	}()

	seqNum := d.meta.getNextSeqNum()
	ikey := base.MakeInternalKey(key, seqNum, keyKind)
	if addType == kkvWriteTypeDirectAdd {
		err = mem.add(ikey, slotId, valueSize, values...)
	} else {
		seekNext := func() (bool, InternalKeyKind) {
			rs := ms.loadReadState()
			defer rs.unref()

			for i := len(rs.memtables) - 2; i >= 0; i-- {
				if exist, kind := rs.memtables[i].exist(key); exist {
					return true, kind
				}
			}

			btree := d.getBitreeRead(slotId)
			if btree != nil {
				if exist := btree.Exist(key, hash.Fnv32(key)); exist {
					return true, InternalKeyKindSet
				}
			}
			return false, InternalKeyKindInvalid
		}
		found, err = mem.set(ikey, slotId, addType, seekNext, valueSize, values...)
	}

	return
}

func (d *DB) kkvSet(key []byte, slotId uint16, values ...[]byte) error {
	_, err := d.kkvFindToSet(key, InternalKeyKindSet, slotId, kkvWriteTypeDirectAdd, values...)
	return err
}

func (d *DB) kkvDeleteKey(key []byte, slotId uint16) error {
	_, err := d.kkvFindToSet(key, InternalKeyKindDelete, slotId, kkvWriteTypeDirectAdd, nil)
	return err
}

func (d *DB) kkvSetPrefixDelete(slotId uint16, version uint64) error {
	var key [kkv.SubKeyDataTypeOffset]byte
	kkv.EncodeKeyVersion(key[:], version)
	_, err := d.kkvFindToSet(key[:], InternalKeyKindPrefixDelete, slotId, kkvWriteTypeDirectAdd, nil)
	return err
}

func (d *DB) kkvSetZsetIndexKey(slotId uint16, version uint64, score, member []byte) (err error) {
	key, closer := kkv.EncodeZsetIndexKeyByPool(version, score, member)
	defer closer()
	return d.kkvSet(key, slotId, nil)
}

func (d *DB) kkvDeleteZsetIndexKey(slotId uint16, version uint64, score, member []byte) error {
	key, closer := kkv.EncodeZsetIndexKeyByPool(version, score, member)
	defer closer()
	return d.kkvDeleteKey(key, slotId)
}

func (d *DB) DeleteKKV(key []byte, slotId uint16, dataType uint8, subKeys ...[]byte) (int64, error) {
	if len(subKeys) == 0 {
		return 0, nil
	}

	hi, lo := hash.MD5Uint64(key)
	dt, ts, version, lindex, rindex, size, err := d.getAliveMeta(hi, lo, dataType, slotId)
	if size == 0 || err != nil {
		return 0, nil
	}

	var delCnt uint32

	for i := range subKeys {
		subKey := subKeys[i]
		if err = d.checkSubKeySize(subKey); err != nil {
			continue
		}

		esk, eskCloser := kkv.EncodeSubKeyByPool(subKey, dt, version)
		exist, _ := d.kkvFindToSet(esk, InternalKeyKindDelete, slotId, kkvWriteTypeExistToAdd, nil)
		if exist {
			delCnt++
		}
		eskCloser()

		if delCnt >= size {
			break
		}
	}

	if delCnt > 0 {
		newSize := size - delCnt
		if newSize == 0 {
			_, _, err = d.deleteKey(key, slotId, hi, lo, dataType, version, ts)
		} else {
			err = d.SetMeta(key, slotId, dt, hi, lo, ts, version, lindex, rindex, newSize)
		}
		if err != nil {
			return 0, err
		}
	}

	return int64(delCnt), nil
}

func (d *DB) DeleteXKKV(key []byte, slotId uint16, dataType uint8, subKeys ...[]byte) (int64, error) {
	if len(subKeys) == 0 {
		return 0, nil
	}

	hi, lo := hash.MD5Uint64(key)
	dt, ts, version, lindex, rindex, size, err := d.getAliveMeta(hi, lo, dataType, slotId)
	if size == 0 || err != nil {
		return 0, nil
	}

	var delCnt uint32

	for i := range subKeys {
		subKey := subKeys[i]
		if err = d.checkSubKeySize(subKey); err != nil {
			continue
		}

		esk, eskCloser := kkv.EncodeSubKeyByPool(subKey, dt, version)
		exist, _ := d.kkvFindToSet(esk, InternalKeyKindDelete, slotId, kkvWriteTypeExistToAdd, nil)
		if exist {
			delCnt++
		}
		eskCloser()

		if delCnt >= size {
			break
		}
	}

	if delCnt > 0 {
		newSize := size - delCnt
		if err = d.SetMeta(key, slotId, dt, hi, lo, ts, version, lindex, rindex, newSize); err != nil {
			return 0, err
		}
	}

	return int64(delCnt), nil
}
