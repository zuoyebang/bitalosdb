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
	"encoding/binary"
	"strconv"

	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

func MakeKey(key []byte) []byte {
	slotId := uint16(hash.Crc32(key) % 1024)
	keyLen := 2 + len(key)
	newKey := make([]byte, keyLen)
	binary.BigEndian.PutUint16(newKey[0:2], slotId)
	copy(newKey[2:keyLen], key)
	return newKey
}

func MakeKey2(key []byte, slotId uint16, version uint64) []byte {
	keyLen := 10 + len(key)
	newKey := make([]byte, keyLen)
	binary.BigEndian.PutUint16(newKey[0:2], slotId)
	binary.LittleEndian.PutUint64(newKey[2:10], version)
	if key != nil {
		copy(newKey[10:keyLen], key)
	}
	return newKey
}

func MakeSlotKey(key []byte, slotId uint16) []byte {
	keyLen := 2 + len(key)
	newKey := make([]byte, keyLen)
	binary.BigEndian.PutUint16(newKey[0:2], slotId)
	copy(newKey[2:keyLen], key)
	return newKey
}

func MakeSortedKey(n int) []byte {
	if n < 0 {
		return []byte(sortedKeyPrefix)
	} else {
		return []byte(sortedKeyPrefix + strconv.Itoa(n))
	}
}

func MakeSortedSlotKey(n int, slotId uint16) []byte {
	if n < 0 {
		return MakeSlotKey([]byte(sortedKeyPrefix), slotId)
	} else {
		return MakeSlotKey([]byte(sortedKeyPrefix+strconv.Itoa(n)), slotId)
	}
}

func MakeSortedKeyForBitrie(n int) []byte {
	if n < 0 {
		return []byte(sortedKeyPrefix)
	} else {
		return []byte(sortedKeyPrefix + string(utils.FuncRandBytes(1)) + "_bitalosdb_" + string(utils.FuncRandBytes(32)) + strconv.Itoa(n))
	}
}
