// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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
	"sort"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

const sortedKeyPrefix = "sorted_key_prefix_"

type SortedKVItem struct {
	Key   *base.InternalKey
	Value []byte
}
type SortedKVList []SortedKVItem

func (x SortedKVList) Len() int { return len(x) }
func (x SortedKVList) Less(i, j int) bool {
	return bytes.Compare(x[i].Key.UserKey, x[j].Key.UserKey) == -1
}
func (x SortedKVList) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func ParseSortedKeyInt(k []byte) int {
	strs := strings.Split(string(k), "_")
	n, _ := strconv.Atoi(strs[len(strs)-1])
	return n
}

func MakeSortedKey(n int) []byte {
	if n < 0 {
		return []byte(sortedKeyPrefix)
	} else {
		return []byte(sortedKeyPrefix + strconv.Itoa(n))
	}
}

func MakeSortedKeyForBitrie(n int) []byte {
	if n < 0 {
		return []byte(sortedKeyPrefix)
	} else {
		return []byte(sortedKeyPrefix + string(utils.FuncRandBytes(1)) + "_bitalosdb_" + string(utils.FuncRandBytes(64)) + strconv.Itoa(n))
	}
}

func MakeSortedKVListForBitrie(start, end int, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		key := MakeSortedKeyForBitrie(i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, SortedKVItem{
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
		key := MakeSortedKey(i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, SortedKVItem{
			Key:   &ikey,
			Value: utils.FuncRandBytes(vsize),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedKV2List(start, end int, seqNum uint64, vsize int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		version := uint64(i/10 + 100)
		slotId := uint16(version % 65535)
		key := utils.FuncMakeKey2([]byte(sortedKeyPrefix+strconv.Itoa(i)), slotId, version)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, SortedKVItem{
			Key:   &ikey,
			Value: utils.FuncRandBytes(vsize),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}

func MakeSortedSameKVList(start, end int, seqNum uint64, vsize int, slotId int) SortedKVList {
	var kvList SortedKVList
	for i := start; i < end; i++ {
		key := utils.FuncMakeSameKey([]byte(sortedKeyPrefix+strconv.Itoa(i)), uint16(slotId))
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, SortedKVItem{
			Key:   &ikey,
			Value: utils.FuncRandBytes(vsize),
		})
		seqNum++
	}

	sort.Sort(kvList)
	return kvList
}
