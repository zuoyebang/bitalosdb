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
	"math/rand"
	"sort"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

func (d *DB) SAdd(key []byte, slotId uint16, kvs ...[]byte) (int64, error) {
	return d.SAddX(key, slotId, false, kvs...)
}

func (d *DB) SAddX(key []byte, slotId uint16, isExist bool, kvs ...[]byte) (int64, error) {
	var dataType uint8
	var timestamp, version, lindex, rindex uint64
	var size uint32
	var err error

	hi, lo := hash.MD5Uint64(key)
	if isExist {
		dataType, timestamp, version, lindex, rindex, size, err = d.getAliveMeta(hi, lo, DataTypeSet, slotId)
	} else {
		dataType, timestamp, version, lindex, rindex, size, err = d.makeAliveMeta(hi, lo, DataTypeSet, slotId)
	}
	if err != nil {
		return 0, err
	}

	var n int64
	var foundKey bool
	for _, member := range kvs {
		if err = d.checkSubKeySize(member); err != nil {
			continue
		}

		esk, eskCloser := kkv.EncodeSubKeyByPool(member, dataType, version)
		foundKey, err = d.kkvFindToSet(esk, InternalKeyKindSet, slotId, kkvWriteTypeNotExistToAdd, nil)
		eskCloser()
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

func (d *DB) SRem(key []byte, slotId uint16, members ...[]byte) (int64, error) {
	return d.DeleteKKV(key, slotId, DataTypeSet, members...)
}

func (d *DB) SRemX(key []byte, slotId uint16, members ...[]byte) (int64, error) {
	return d.DeleteXKKV(key, slotId, DataTypeSet, members...)
}

func (d *DB) SPop(key []byte, slotId uint16, count int64) ([][]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, lindex, rindex, size, err := d.kkvGetMeta(hi, lo, DataTypeSet, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var delCnt int64
	sz := int64(size)
	if count > sz {
		count = sz
	}
	members := make([][]byte, 0, count)
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		SlotId:     uint32(slotId),
		DataType:   DataTypeSet,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	iter := d.NewKKVIterator(iterOpts)
	for iter.First(); iter.Valid(); iter.Next() {
		members = append(members, iter.GetSubKey())
		d.kkvDeleteKey(iter.Key(), slotId)
		delCnt++
		if delCnt == count {
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
			iter.Close()
			return nil, nil, err
		}
	}

	return members, func() { iter.Close() }, nil
}

func (d *DB) SPopX(key []byte, slotId uint16, count int64) ([][]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	dt, timestamp, version, lindex, rindex, size, err := d.kkvGetMeta(hi, lo, DataTypeSet, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var delCnt int64
	sz := int64(size)
	if count > sz {
		count = sz
	}
	members := make([][]byte, 0, count)
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		SlotId:     uint32(slotId),
		DataType:   DataTypeSet,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	iter := d.NewKKVIterator(iterOpts)
	for iter.First(); iter.Valid(); iter.Next() {
		members = append(members, iter.GetSubKey())
		d.kkvDeleteKey(iter.Key(), slotId)
		delCnt++
		if delCnt == count {
			break
		}
	}

	if delCnt > 0 {
		newSize := uint32(sz - delCnt)
		if err = d.SetMeta(key, slotId, dt, hi, lo, timestamp, version, lindex, rindex, newSize); err != nil {
			iter.Close()
			return nil, nil, err
		}
	}

	return members, func() { iter.Close() }, nil
}

func (d *DB) SMembers(key []byte, slotId uint16) ([][]byte, func(), error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeSet, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var members [][]byte
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		SlotId:     uint32(slotId),
		DataType:   DataTypeSet,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	iter := d.NewKKVIterator(iterOpts)
	for iter.First(); iter.Valid(); iter.Next() {
		members = append(members, iter.GetSubKey())
	}

	return members, func() { iter.Close() }, nil
}

func (d *DB) SRandMember(key []byte, slotId uint16, count int64) ([][]byte, func(), error) {
	if count == 0 {
		return nil, nil, nil
	}

	hi, lo := hash.MD5Uint64(key)
	dt, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeSet, slotId)
	if size == 0 {
		return nil, nil, err
	}

	var randCount int64
	var repeated bool
	if count > 0 {
		randCount = count
		repeated = false
	} else {
		randCount = -count
		repeated = true
	}
	keySize := int64(size)
	randSize := randCount * 2
	if randSize > keySize {
		randSize = keySize
	}
	randNumbers := genRandomNumber(0, int(randSize), int(randCount), repeated)
	members := make([][]byte, 0, randCount)

	var cnt int64
	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, version)
	kkv.EncodeSubKeyUpperBound(upperBound[:], dt, version)
	iterOpts := &IterOptions{
		SlotId:     uint32(slotId),
		DataType:   DataTypeSet,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	iter := d.NewKKVIterator(iterOpts)

	if !repeated {
		for iter.First(); iter.Valid(); iter.Next() {
			if len(randNumbers) <= 0 {
				break
			}
			if int(cnt) == randNumbers[0] {
				randNumbers = randNumbers[1:]
				members = append(members, iter.GetSubKey())
			}
			cnt++
			if cnt >= keySize {
				break
			}
		}
	} else {
		for iter.First(); iter.Valid(); {
			if len(randNumbers) <= 0 {
				break
			}
			if int(cnt) == randNumbers[0] {
				randNumbers = randNumbers[1:]
				member := iter.GetSubKey()
				if member != nil {
					members = append(members, iter.GetSubKey())
				}
			} else if int(cnt) < randNumbers[0] {
				cnt++
				if cnt >= keySize {
					break
				}
				iter.Next()
			}
		}
	}

	rand.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})

	return members, func() { iter.Close() }, nil
}

func genRandomNumber(start int, end int, count int, repeated bool) (nums []int) {
	gap := end - start
	if repeated {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < count; i++ {
			nums = append(nums, r.Intn(gap)+start)
		}
	} else {
		if gap <= count {
			for i := start; i < end; i++ {
				nums = append(nums, i)
			}
		} else {
			numsMap := make(map[int]bool, count)
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for len(nums) < count {
				num := r.Intn(gap) + start
				if !numsMap[num] {
					numsMap[num] = true
					nums = append(nums, num)
				}
			}
		}
	}

	sort.Ints(nums)
	return nums
}

func (d *DB) SIsMember(key []byte, slotId uint16, subKey []byte) (int64, error) {
	hi, lo := hash.MD5Uint64(key)
	dt, _, version, _, _, size, err := d.kkvGetMeta(hi, lo, DataTypeSet, slotId)
	if size == 0 {
		return 0, err
	}

	exist := d.kkvExistSubKey(subKey, slotId, dt, version)
	if exist {
		return 1, nil
	}
	return 0, nil
}
