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

import "github.com/zuoyebang/bitalosdb/v2/internal/hash"

func (d *DB) DKCreate(key []byte, slotId uint16, dataType uint8, shardNum uint32) error {
	if shardNum == 0 {
		return ErrDKShardNum
	}

	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, sn, err := d.makeAliveMeta(hi, lo, dataType, slotId)
	if err != nil {
		return err
	} else if sn > 0 {
		return ErrDKExist
	}

	return d.SetMeta(key, slotId, dataType, hi, lo, 0, version, 0, 0, shardNum)
}

func (d *DB) DKCreateShard(key []byte, slotId uint16, dataType uint8) error {
	hi, lo := hash.MD5Uint64(key)
	_, _, version, _, _, _, err := d.makeAliveMeta(hi, lo, dataType, slotId)
	if err != nil {
		return err
	}
	return d.SetMeta(key, slotId, dataType, hi, lo, 0, version, 0, 0, 0)
}

func (d *DB) DKIncrBySize(key []byte, slotId uint16, incr int64) (int64, error) {
	if incr == 0 {
		return 0, nil
	}

	hi, lo := hash.MD5Uint64(key)
	dt, ts, version, size, ri, sn, err := d.getAliveMeta(hi, lo, DataTypeNone, slotId)
	if err != nil {
		return 0, err
	}

	if incr > 0 {
		size += uint64(incr)
	} else {
		sz := int64(size) + incr
		if sz < 0 {
			size = 0
		} else {
			size = uint64(sz)
		}
	}

	if err = d.SetMeta(key, slotId, dt, hi, lo, ts, version, size, ri, sn); err != nil {
		return 0, err
	}

	return int64(size), nil
}
