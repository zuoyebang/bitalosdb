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

package base

import (
	"encoding/binary"
	"fmt"

	"github.com/zuoyebang/bitalosdb/v2/internal/bytepools"
)

type InternalKeyKind uint8

const (
	InternalKeyKindDelete       InternalKeyKind = 0
	InternalKeyKindSet          InternalKeyKind = 1
	InternalKeyKindSetBithash   InternalKeyKind = 2
	InternalKeyKindLogData      InternalKeyKind = 3
	InternalKeyKindPrefixDelete InternalKeyKind = 4
	InternalKeyKindExpireAt     InternalKeyKind = 5
	InternalKeyKindMax          InternalKeyKind = 18
	InternalKeyKindInvalid      InternalKeyKind = 255

	InternalKeySeqNumMax = uint64(1<<56 - 1)
)

const (
	InternalValueKindSet     = 0
	InternalValueKindBithash = 1

	InternalValueBithashSize = 13
)

var internalKeyKindNames = []string{
	InternalKeyKindDelete:       "DEL",
	InternalKeyKindSet:          "SET",
	InternalKeyKindSetBithash:   "SETBITHASH",
	InternalKeyKindLogData:      "LOGDATA",
	InternalKeyKindPrefixDelete: "PREFIXDELETE",
	InternalKeyKindInvalid:      "INVALID",
}

func (k InternalKeyKind) String() string {
	if int(k) < len(internalKeyKindNames) {
		return internalKeyKindNames[k]
	}
	return fmt.Sprintf("UNKNOWN:%d", k)
}

type InternalKey struct {
	UserKey []byte
	Trailer uint64
}

func MakeInternalKey(userKey []byte, seqNum uint64, kind InternalKeyKind) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (seqNum << 8) | uint64(kind),
	}
}

func MakeInternalKey2(userKey []byte, trailer uint64) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: trailer,
	}
}

func MakeInternalSetKey(userKey []byte) InternalKey {
	return MakeInternalKey(userKey, 1, InternalKeyKindSet)
}

func MakeSearchKey(userKey []byte) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (InternalKeySeqNumMax << 8) | uint64(InternalKeyKindMax),
	}
}

func DecodeInternalKey(encodedKey []byte) InternalKey {
	n := len(encodedKey) - 8
	var trailer uint64
	if n >= 0 {
		trailer = binary.LittleEndian.Uint64(encodedKey[n:])
		encodedKey = encodedKey[:n:n]
	} else {
		trailer = uint64(InternalKeyKindInvalid)
		encodedKey = nil
	}
	return InternalKey{
		UserKey: encodedKey,
		Trailer: trailer,
	}
}

func InternalCompare(userCmp Compare, a, b InternalKey) int {
	if x := userCmp(a.UserKey, b.UserKey); x != 0 {
		return x
	}
	if a.Trailer > b.Trailer {
		return -1
	}
	if a.Trailer < b.Trailer {
		return 1
	}
	return 0
}

func (k InternalKey) Encode(buf []byte) {
	i := copy(buf, k.UserKey)
	binary.LittleEndian.PutUint64(buf[i:], k.Trailer)
}

func (k InternalKey) EncodeTrailer() [8]byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], k.Trailer)
	return buf
}

func (k InternalKey) Size() int {
	return len(k.UserKey) + 8
}

func (k *InternalKey) SetSeqNum(seqNum uint64) {
	k.Trailer = (seqNum << 8) | (k.Trailer & 0xff)
}

func (k InternalKey) SeqNum() uint64 {
	return k.Trailer >> 8
}

func (k *InternalKey) SetKind(kind InternalKeyKind) {
	k.Trailer = (k.Trailer &^ 0xff) | uint64(kind)
}

func (k InternalKey) Kind() InternalKeyKind {
	return InternalKeyKind(k.Trailer & 0xff)
}

func (k InternalKey) Valid() bool {
	return k.Kind() <= InternalKeyKindMax
}

func (k InternalKey) Clone() InternalKey {
	if len(k.UserKey) == 0 {
		return k
	}
	return InternalKey{
		UserKey: append([]byte(nil), k.UserKey...),
		Trailer: k.Trailer,
	}
}

func (k InternalKey) String() string {
	return fmt.Sprintf("%s#%d,%d", FormatBytes(k.UserKey), k.SeqNum(), k.Kind())
}

type InternalValue struct {
	Header    uint64
	UserValue []byte
}

func (v InternalValue) Kind() InternalKeyKind {
	return InternalKeyKind(v.Header & 0xff)
}

func (v *InternalValue) SetKind(kind InternalKeyKind) {
	v.Header = (v.Header &^ 0xff) | uint64(kind)
}

func (v InternalValue) SeqNum() uint64 {
	return v.Header >> 8
}

func MakeInternalValue(value []byte, seqNum uint64, kind InternalKeyKind) InternalValue {
	return InternalValue{
		Header:    (seqNum << 8) | uint64(kind),
		UserValue: value,
	}
}

func EncodeInternalValue(value []byte, seqNum uint64, kind InternalKeyKind) ([]byte, func()) {
	vLen := len(value) + 8
	pool, closer := bytepools.ReaderBytePools.GetBytePool(vLen)
	binary.LittleEndian.PutUint64(pool[0:8], (seqNum<<8)|uint64(kind))
	if value != nil {
		copy(pool[8:], value)
	}
	return pool[:vLen], closer
}

func DecodeInternalValue(value []byte) (bool, []byte, InternalValue) {
	if len(value) < InternalValueBithashSize {
		return false, value, InternalValue{}
	}

	if value[0] == InternalValueKindSet {
		return false, value[1:], InternalValue{}
	}

	value = value[1:]
	n := len(value) - 8
	var header uint64
	if n >= 0 {
		header = binary.LittleEndian.Uint64(value[0:8])
		if n == 0 {
			value = value[:0:0]
		} else {
			value = value[8 : n+8 : n+8]
		}
	} else {
		header = uint64(InternalKeyKindInvalid)
		value = nil
	}

	return true, nil, InternalValue{
		Header:    header,
		UserValue: value,
	}
}

func EncodeTrailer(seqNum uint64, kind InternalKeyKind) uint64 {
	return (seqNum << 8) | uint64(kind)
}

func DecodeTrailer(trailer uint64) (uint64, InternalKeyKind) {
	return trailer >> 8, InternalKeyKind(trailer & 0xff)
}

func DecodeKind(trailer uint64) InternalKeyKind {
	return InternalKeyKind(trailer & 0xff)
}

func CheckValueBithashValid(v []byte) bool {
	return len(v) == 4
}
