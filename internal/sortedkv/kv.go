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
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

const (
	sortedKeyPrefix    = "sorted_key_prefix_"
	sortedSubKeyPrefix = "sorted_subkey_prefix_"
	kkvKeyPrefix       = "kkv_key_prefix_kkv_key_prefix_"
)

type SortedKVItem struct {
	Key     *base.InternalKey
	KeyHash uint32
	Value   []byte
	Sn      uint64
}
type SortedKVList []*SortedKVItem

func (x SortedKVList) Len() int { return len(x) }
func (x SortedKVList) Less(i, j int) bool {
	r := bytes.Compare(x[i].Key.UserKey, x[j].Key.UserKey)
	if r != 0 {
		return r == -1
	} else {
		return x[i].Key.Trailer >= x[j].Key.Trailer
	}
}
func (x SortedKVList) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func MakeSortedKVListForBitrie(start, end int, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		key := MakeSortedKeyForBitrie(i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, &SortedKVItem{
			Key:   &ikey,
			Value: utils.FuncRandBytes(vsize),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedKVList(start, end int, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		key := MakeKKVSubKey(kkv.DataTypeHash, seqNum+1, i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, &SortedKVItem{
			Key:     &ikey,
			Value:   utils.FuncRandBytes(vsize),
			Sn:      seqNum,
			KeyHash: hash.Fnv32(key),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedKKVKVList(dt uint8, start, end int, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		key := MakeKKVSubKey(dt, seqNum, i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, &SortedKVItem{
			Key:     &ikey,
			Value:   utils.FuncRandBytes(vsize),
			KeyHash: hash.Fnv32(key),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedListKey(version uint64, dt uint8, i int) []byte {
	key := make([]byte, kkv.SubKeyListLength)
	kkv.EncodeListKey(key, dt, version, uint64(i))
	return key
}

func MakeSortedListKVList(kn, kkn int, dt uint8, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	var version uint64
	for i := 0; i < kn; i++ {
		for j := 0; j < kkn; j++ {
			key := MakeSortedListKey(version, dt, j)
			ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
			kvList = append(kvList, &SortedKVItem{
				Key:     &ikey,
				Value:   utils.FuncRandBytes(vsize),
				KeyHash: hash.Fnv32(key),
			})
			seqNum++
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedZsetKey(version uint64, score float64, m int) []byte {
	member := []byte(fmt.Sprintf("zset_member_%d", m))
	key := kkv.EncodeZsetIndexKey(version, score, member)
	return key
}

func MakeSortedZsetKVList(kn, kkn, mn int, seqNum uint64) SortedKVList {
	var kvList SortedKVList
	var version uint64
	for i := 0; i < kn; i++ {
		for j := 0; j < kkn; j++ {
			for k := 0; k < mn; k++ {
				key := MakeSortedZsetKey(version, float64(j), k)
				ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
				kvList = append(kvList, &SortedKVItem{
					Key:     &ikey,
					KeyHash: hash.Fnv32(key),
					Value:   nil,
				})
				seqNum++
			}
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedHashKVList(kn, kkn, vsize int, seqNum uint64) SortedKVList {
	var kvList SortedKVList
	var key []byte
	var version uint64
	for i := 0; i < kn; i++ {
		for j := 0; j < kkn; j++ {
			key = MakeKKVSubKey(kkv.DataTypeHash, version, j)
			ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
			kvList = append(kvList, &SortedKVItem{
				Key:     &ikey,
				Value:   utils.FuncRandBytes(vsize),
				KeyHash: hash.Fnv32(key),
			})
			seqNum++
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedSetKVList(kn, kkn int, seqNum uint64) SortedKVList {
	var kvList SortedKVList
	var key []byte
	var version uint64
	for i := 0; i < kn; i++ {
		for j := 0; j < kkn; j++ {
			key = MakeKKVSubKey(kkv.DataTypeSet, version, j)
			ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
			kvList = append(kvList, &SortedKVItem{
				Key:     &ikey,
				Value:   nil,
				KeyHash: hash.Fnv32(key),
			})
			seqNum++
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedExpireKVList(kn, kkn int, seqNum uint64) SortedKVList {
	var kvList SortedKVList
	var key []byte
	var version uint64
	for i := 0; i < kn; i++ {
		for j := 0; j < kkn; j++ {
			key = make([]byte, kkv.ExpireKeyLength)
			kkv.EncodeExpireKey(key, version, version+1, uint16(j+1))
			ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
			kvList = append(kvList, &SortedKVItem{
				Key:     &ikey,
				Value:   nil,
				KeyHash: hash.Fnv32(key),
			})
			seqNum++
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedAllKVList(kn, kkn, mn, vsize int, seqNum uint64) SortedKVList {
	var kvList SortedKVList
	version := uint64(10000)
	for i := 0; i < kn; i++ {
		for _, dataType := range []uint8{
			kkv.DataTypeHash,
			kkv.DataTypeList,
			kkv.DataTypeBitmap,
			kkv.DataTypeSet,
			kkv.DataTypeZset,
			kkv.DataTypeExpireKey,
		} {
			switch dataType {
			case kkv.DataTypeZset:
				for j := 0; j < kkn; j++ {
					for k := 0; k < mn; k++ {
						score := float64(j)
						member := []byte(fmt.Sprintf("%s_%d_%d", kkvKeyPrefix, j, k))
						key := kkv.EncodeZsetIndexKey(version, score, member)
						ikey1 := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
						seqNum++
						kvList = append(kvList, &SortedKVItem{
							Key:     &ikey1,
							KeyHash: hash.Fnv32(key),
							Value:   []byte(""),
						})

						dataKey := make([]byte, kkv.SubKeyZsetLength)
						kkv.EncodeZsetDataKey(dataKey, version, member)
						ikey2 := base.MakeInternalKey(dataKey, seqNum, base.InternalKeyKindSet)
						kvItem := &SortedKVItem{
							Key:     &ikey2,
							KeyHash: hash.Fnv32(dataKey),
							Value:   kkv.EncodeZsetScore(score),
						}

						kvList = append(kvList, kvItem)
						seqNum++
					}
				}
			case kkv.DataTypeList, kkv.DataTypeBitmap:
				for j := 0; j < kkn; j++ {
					key := MakeSortedListKey(version, dataType, j)
					ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
					seqNum++
					kvList = append(kvList, &SortedKVItem{
						Key:     &ikey,
						Value:   utils.FuncRandBytes(vsize),
						KeyHash: hash.Fnv32(key),
					})
				}
			case kkv.DataTypeHash:
				for j := 0; j < kkn; j++ {
					key := MakeKKVSubKey(dataType, version, j)
					ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
					seqNum++
					kvList = append(kvList, &SortedKVItem{
						Key:     &ikey,
						Value:   utils.FuncRandBytes(vsize),
						KeyHash: hash.Fnv32(key),
					})
				}
			case kkv.DataTypeSet:
				for j := 0; j < kkn; j++ {
					key := MakeKKVSubKey(dataType, version, j)
					ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
					seqNum++
					kvList = append(kvList, &SortedKVItem{
						Key:     &ikey,
						Value:   []byte(""),
						KeyHash: hash.Fnv32(key),
					})
				}
			case kkv.DataTypeExpireKey:
				for j := 0; j < kkn; j++ {
					key := make([]byte, kkv.ExpireKeyLength)
					kkv.EncodeExpireKey(key, version, version+1, uint16(j+1))
					ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
					seqNum++
					kvList = append(kvList, &SortedKVItem{
						Key:     &ikey,
						Value:   []byte(""),
						KeyHash: hash.Fnv32(key),
					})
				}
			}
			version++
		}
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedAllKVListOld(kn, kkn, mn int, seqNum uint64) SortedKVList {
	var kvList SortedKVList
	version := uint64(10000)
	for i := 0; i < kn; i++ {
		for _, dataType := range []uint8{
			kkv.DataTypeHash,
			kkv.DataTypeList,
			kkv.DataTypeSet,
			kkv.DataTypeZset,
			kkv.DataTypeExpireKey,
		} {
			switch dataType {
			case kkv.DataTypeZset:
				for j := 0; j < kkn; j++ {
					for k := 0; k < mn; k++ {
						score := float64(j)
						member := []byte(fmt.Sprintf("member_%d_%d", j, k))
						key := kkv.EncodeZsetIndexKey(version, score, member)
						ikey1 := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
						seqNum++
						kvList = append(kvList, &SortedKVItem{
							Key:     &ikey1,
							KeyHash: hash.Fnv32(key),
							Value:   []byte{},
						})

						dataKey := make([]byte, kkv.SubKeyZsetLength)
						l := kkv.EncodeZsetDataKeyOld(dataKey, version, member)
						ikey2 := base.MakeInternalKey(dataKey[:l], seqNum, base.InternalKeyKindSet)
						kvItem := &SortedKVItem{
							Key:     &ikey2,
							KeyHash: hash.Fnv32(dataKey),
						}
						kvItem.Value = make([]byte, 9)
						kvItem.Value[0] = base.InternalValueKindSet
						copy(kvItem.Value[1:9], kkv.EncodeZsetScore(score))
						kvList = append(kvList, kvItem)
						seqNum++
					}
				}
			case kkv.DataTypeList:
				for j := 0; j < kkn; j++ {
					key := make([]byte, kkv.SubKeyListLength)
					kkv.EncodeListKey(key, kkv.DataTypeList, version, uint64(j))
					ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
					seqNum++
					var value []byte
					if j%3 == 0 {
						value = make([]byte, base.InternalValueBithashSize)
						value[0] = base.InternalValueKindBithash
						binary.LittleEndian.PutUint64(value[1:9], (uint64(j+1)<<8)|uint64(base.InternalKeyKindSetBithash))
						binary.LittleEndian.PutUint32(value[9:13], uint32(j+1))
					} else if j%3 == 1 {
						vsize := rand.Intn(20) + 2
						value = utils.FuncRandBytes(vsize)
						value[0] = base.InternalValueKindSet
					} else {
						value = []byte{}
					}
					kvList = append(kvList, &SortedKVItem{
						Key:     &ikey,
						Value:   value,
						KeyHash: hash.Fnv32(key),
					})
				}
			case kkv.DataTypeHash:
				for j := 0; j < kkn; j++ {
					key := MakeKKVSubKey(dataType, version, j)
					ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
					seqNum++
					var value []byte
					if j%3 == 0 {
						value = make([]byte, base.InternalValueBithashSize)
						value[0] = base.InternalValueKindBithash
						binary.LittleEndian.PutUint64(value[1:9], (uint64(j+1)<<8)|uint64(base.InternalKeyKindSetBithash))
						binary.LittleEndian.PutUint32(value[9:13], uint32(j+1))
					} else if j%3 == 1 {
						vsize := rand.Intn(20) + 2
						value = utils.FuncRandBytes(vsize)
						value[0] = base.InternalValueKindSet
					} else {
						value = []byte{}
					}
					kvList = append(kvList, &SortedKVItem{
						Key:     &ikey,
						Value:   value,
						KeyHash: hash.Fnv32(key),
					})
				}
			case kkv.DataTypeSet:
				for j := 0; j < kkn; j++ {
					key := MakeKKVSubKey(dataType, version, j)
					ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
					seqNum++
					kvList = append(kvList, &SortedKVItem{
						Key:     &ikey,
						Value:   []byte{},
						KeyHash: hash.Fnv32(key),
					})
				}
			case kkv.DataTypeExpireKey:
				for j := 0; j < kkn; j++ {
					key := make([]byte, kkv.ExpireKeyLength)
					kkv.EncodeExpireKey(key, version, version+1, uint16(j+1))
					ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
					seqNum++
					kvList = append(kvList, &SortedKVItem{
						Key:     &ikey,
						Value:   []byte(""),
						KeyHash: hash.Fnv32(key),
					})
				}
			}
			version++
		}
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSlotSortedKVList(start, end int, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		key := MakeSortedKey(i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, &SortedKVItem{
			Key:     &ikey,
			KeyHash: hash.Fnv32(key),
			Value:   utils.FuncRandBytes(vsize),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSlotSortedKVList2(start, end int, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		version := uint64(i/10 + 100)
		key := MakeKKVSubKey(kkv.DataTypeHash, version, i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, &SortedKVItem{
			Key:   &ikey,
			Value: utils.FuncRandBytes(vsize),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedSamePrefixDeleteKVList(start, end int, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		version := uint64(i/10 + 100)
		var ikey base.InternalKey
		if version%2 == 0 && IsLastVersionKey(i) {
			ikey = MakePrefixDeleteKey(version, seqNum)
		} else {
			key := MakeKKVSubKey(kkv.DataTypeHash, version, i)
			ikey = base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		}
		kvList = append(kvList, &SortedKVItem{
			Key:   &ikey,
			Value: utils.FuncRandBytes(vsize),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSameVersionSortedKVList(num int, version, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := 0; i < num; i++ {
		key := MakeKKVSubKey(kkv.DataTypeHash, version, i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, &SortedKVItem{
			Key:   &ikey,
			Value: utils.FuncRandBytes(vsize),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func IsPrefixDeleteKey(version uint64) bool {
	return version%2 == 0
}

func IsLastVersionKey(i int) bool {
	return i%10 == 9
}
