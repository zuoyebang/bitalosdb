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

package sortedkv

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

type KKVItem struct {
	Key       []byte
	KeyHash   uint32
	Hi, Lo    uint64
	Sid       uint16
	SubKey    []byte
	Value     []byte
	DataType  uint8
	Timestamp uint64
	Version   uint64
	Lindex    uint64
	Rindex    uint64
	Size      uint32
	Kvs       [][]byte
}
type SortedKKVKeyList []*KKVItem

func (x SortedKKVKeyList) Len() int { return len(x) }
func (x SortedKKVKeyList) Less(i, j int) bool {
	return bytes.Compare(x[i].Key, x[j].Key) == -1
}
func (x SortedKKVKeyList) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func MakeKVList(num int, vsize int) []*KKVItem {
	keys := make([]*KKVItem, num)
	seqNum := uint64(1)
	nowTime := time.Now().UnixMilli()
	for i := 0; i < num; i++ {
		key := MakeSortedKey(i)
		hi, lo := hash.MD5Uint64(key)
		keys[i] = &KKVItem{
			Key:       key,
			Hi:        hi,
			Lo:        lo,
			Version:   0,
			DataType:  kkv.DataTypeString,
			Timestamp: uint64(nowTime) + 10000 + uint64(i),
			Value:     utils.FuncRandBytes(vsize),
		}
		seqNum++
	}

	return keys
}

func MakeKeySlotId(key []byte) uint16 {
	_, lo := hash.MD5Uint64(key)
	return uint16(lo % consts.SlotNum)
}

func MakeKKVList(kn, kkn int, dt uint8, ksize, vsize int) []*KKVItem {
	keys := make([]*KKVItem, kn)
	seqNum := uint64(1)
	nowTime := time.Now().UnixMilli()
	for i := 0; i < kn; i++ {
		key := MakeKKVKey(i)
		hi, lo := hash.MD5Uint64(key)
		keys[i] = &KKVItem{
			Key:       key,
			Sid:       MakeKeySlotId(key),
			Hi:        hi,
			Lo:        lo,
			SubKey:    MakeSortedSubKey(i),
			DataType:  dt,
			Timestamp: uint64(nowTime + 1000*1000),
			Size:      uint32(kkn),
		}

		switch dt {
		case kkv.DataTypeHash, kkv.DataTypeZset:
			keys[i].Kvs = make([][]byte, 0, kkn*2)
			for j := 0; j < kkn; j++ {
				keys[i].Kvs = append(keys[i].Kvs, utils.FuncRandBytes(ksize), utils.FuncRandBytes(vsize))
			}
		default:
			keys[i].Kvs = make([][]byte, 0, kkn)
			for j := 0; j < kkn; j++ {
				keys[i].Kvs = append(keys[i].Kvs, utils.FuncRandBytes(ksize))
			}
		}

		seqNum++
	}

	return keys
}

func MakeKKVItemList(kn, kkn, ksize, vsize int) []*KKVItem {
	keys := make([]*KKVItem, kn*4)
	keyId := 0
	for i := 0; i < kn; i++ {
		for _, dt := range []uint8{kkv.DataTypeHash, kkv.DataTypeList, kkv.DataTypeSet, kkv.DataTypeZset} {
			key := MakeKKVKey(keyId)
			keys[keyId] = &KKVItem{
				Key:      key,
				DataType: dt,
				Size:     uint32(kkn),
			}
			switch dt {
			case kkv.DataTypeHash:
				keys[keyId].Kvs = make([][]byte, 0, kkn*2)
				for j := 0; j < kkn; j++ {
					vs := vsize
					if j%2 == 0 {
						vs += consts.KvSeparateSize
					}
					keys[keyId].Kvs = append(keys[keyId].Kvs, utils.FuncRandBytes(ksize), utils.FuncRandBytes(vs))
				}
			case kkv.DataTypeList:
				keys[keyId].Kvs = make([][]byte, 0, kkn)
				for j := 0; j < kkn; j++ {
					vs := vsize
					if j%2 == 0 {
						vs += consts.KvSeparateSize
					}
					keys[keyId].Kvs = append(keys[keyId].Kvs, utils.FuncRandBytes(vs))
				}
			case kkv.DataTypeZset:
				keys[keyId].Kvs = make([][]byte, 0, kkn*2)
				for j := 0; j < kkn; j++ {
					keys[keyId].Kvs = append(keys[keyId].Kvs, kkv.EncodeZsetScore(float64(j+1)), utils.FuncRandBytes(vsize))
				}
			case kkv.DataTypeSet:
				keys[keyId].Kvs = make([][]byte, 0, kkn)
				for j := 0; j < kkn; j++ {
					keys[keyId].Kvs = append(keys[keyId].Kvs, utils.FuncRandBytes(ksize))
				}
			default:
			}
			keyId++
		}
	}
	return keys
}

func MakeSortedKKVList(kn, kkn int, dt uint8, ksize, vsize int, isKvSeparate bool) []*KKVItem {
	keys := make([]*KKVItem, kn)
	seqNum := uint64(1)
	nowTime := time.Now().UnixMilli()
	for i := 0; i < kn; i++ {
		key := MakeKKVKey(i)
		hi, lo := hash.MD5Uint64(key)
		keys[i] = &KKVItem{
			Key:       key,
			Hi:        hi,
			Lo:        lo,
			SubKey:    MakeSortedSubKey(i),
			DataType:  dt,
			Timestamp: uint64(nowTime + 1000*100),
			Size:      uint32(kkn),
		}

		switch dt {
		case kkv.DataTypeHash, kkv.DataTypeZset:
			skeys := make([][]byte, 0, kkn)
			for j := 0; j < kkn; j++ {
				if dt == kkv.DataTypeZset {
					skeys = append(skeys, kkv.EncodeZsetScore(rand.Float64()))
				} else {
					skeys = append(skeys, utils.FuncRandBytes(ksize))
				}
			}
			sort.Slice(skeys, func(i, j int) bool {
				return bytes.Compare(skeys[i], skeys[j]) < 0
			})

			keys[i].Kvs = make([][]byte, 0, kkn*2)
			for j := 0; j < kkn; j++ {
				vs := vsize
				if isKvSeparate && j%2 == 0 {
					vs += consts.KvSeparateSize
				}
				keys[i].Kvs = append(keys[i].Kvs, skeys[j], utils.FuncRandBytes(vs))
			}
		case kkv.DataTypeList:
			skeys := make([][]byte, 0, kkn)
			for j := 0; j < kkn-1; j++ {
				skeys = append(skeys, utils.FuncRandBytes(ksize))
			}
			skeys = append(skeys, []byte(""))
			sort.Slice(skeys, func(i, j int) bool {
				return bytes.Compare(skeys[i], skeys[j]) < 0
			})
			keys[i].Kvs = skeys
		default:
			skeys := make([][]byte, 0, kkn)
			for j := 0; j < kkn; j++ {
				skeys = append(skeys, utils.FuncRandBytes(ksize))
			}
			sort.Slice(skeys, func(i, j int) bool {
				return bytes.Compare(skeys[i], skeys[j]) < 0
			})
			keys[i].Kvs = skeys
		}

		seqNum++
	}

	return keys
}

func MakeSortedZsetList(kn, kkn int, start, step float64) []*KKVItem {
	var subKey []byte
	skPrefix := utils.FuncRandBytes(20)
	keys := make([]*KKVItem, kn)
	seqNum := uint64(1)
	nowTime := time.Now().UnixMilli()
	for i := 0; i < kn; i++ {
		key := MakeKKVKey(i)
		hi, lo := hash.MD5Uint64(key)
		keys[i] = &KKVItem{
			Key:       key,
			Hi:        hi,
			Lo:        lo,
			DataType:  kkv.DataTypeZset,
			SubKey:    MakeSortedSubKey(i),
			Timestamp: uint64(nowTime + 1000*100),
			Size:      uint32(kkn),
		}

		skeys := make([]float64, 0, kkn)
		score := start
		for j := 0; j < kkn; j++ {
			skeys = append(skeys, score)
			score += step
		}
		sort.Float64s(skeys)

		keys[i].Kvs = make([][]byte, 0, kkn*2)
		for j := 0; j < kkn; j++ {
			siKey := kkv.EncodeZsetScore(skeys[j])
			if j%2 == 0 {
				subKey = []byte(fmt.Sprintf("%s_%d", skPrefix, j))
			} else {
				subKey = []byte(fmt.Sprintf("%d", j))
			}
			keys[i].Kvs = append(keys[i].Kvs, siKey, subKey)
		}

		seqNum++
	}

	return keys
}

func MakeSortedZsetList1(kn, kkn int, start, step float64) []*KKVItem {
	keys := make([]*KKVItem, kn)
	seqNum := uint64(1)
	nowTime := time.Now().UnixMilli()
	for i := 0; i < kn; i++ {
		key := MakeKKVKey(i)
		hi, lo := hash.MD5Uint64(key)
		keys[i] = &KKVItem{
			Key:       key,
			Hi:        hi,
			Lo:        lo,
			DataType:  kkv.DataTypeZset,
			SubKey:    MakeSortedSubKey(i),
			Timestamp: uint64(nowTime + 1000*100),
			Size:      uint32(kkn),
		}

		skeys := make([]float64, 0, kkn)
		score := start
		for j := 0; j < kkn; j++ {
			skeys = append(skeys, score)
			score += step
		}
		sort.Float64s(skeys)

		keys[i].Kvs = make([][]byte, 0, kkn*2)
		for j := 0; j < kkn; j++ {
			siKey := kkv.EncodeZsetScore(skeys[j])
			keys[i].Kvs = append(keys[i].Kvs, siKey, siKey)
		}

		seqNum++
	}

	return keys
}

func MakeSortedZsetList2(kn, kkn int, gap int, start, step float64) []*KKVItem {
	keys := make([]*KKVItem, kn)
	seqNum := uint64(1)
	nowTime := time.Now().UnixMilli()
	for i := 0; i < kn; i++ {
		key := MakeKKVKey(i)
		hi, lo := hash.MD5Uint64(key)
		keys[i] = &KKVItem{
			Key:       key,
			Hi:        hi,
			Lo:        lo,
			DataType:  kkv.DataTypeZset,
			SubKey:    MakeSortedSubKey(i),
			Timestamp: uint64(nowTime + 1000*100),
			Size:      uint32(kkn),
		}

		skeys := make([]float64, 0, kkn)
		score := start
		for j := 1; j <= kkn; j++ {
			skeys = append(skeys, score)
			if j%gap == 0 {
				score += step
			}
		}
		sort.Float64s(skeys)

		keys[i].Kvs = make([][]byte, 0, kkn*2)
		for j := 0; j < kkn; j++ {
			siKey := kkv.EncodeZsetScore(skeys[j])
			keys[i].Kvs = append(keys[i].Kvs, siKey, []byte(fmt.Sprintf("%d", j)))
		}
		seqNum++
	}

	return keys
}

func MakeKKVKey(n int) []byte {
	return []byte(kkvKeyPrefix + strconv.Itoa(n))
}

func MakeKKVSubKey(dt uint8, version uint64, n int) []byte {
	return kkv.EncodeSubKey(MakeKKVKey(n), dt, version)
}
