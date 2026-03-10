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

package kkv

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
)

type InternalKeyKind = base.InternalKeyKind

type InternalKey struct {
	Version  []byte
	DataType uint8
	SubKey   []byte
	Trailer  uint64
}

func MakeInternalKey(key base.InternalKey) InternalKey {
	if len(key.UserKey) > SubKeyVersionLength {
		return InternalKey{
			Version:  key.UserKey[0:SubKeyVersionLength],
			DataType: key.UserKey[SubKeyDataTypeOffset],
			SubKey:   key.UserKey[SubKeyDataOffset:],
			Trailer:  key.Trailer,
		}
	} else {
		return InternalKey{
			Version:  key.UserKey,
			DataType: DataTypeNone,
			SubKey:   []byte{},
			Trailer:  key.Trailer,
		}
	}
}

func MakeInternalSetKey(key []byte) InternalKey {
	return InternalKey{
		Version:  key[0:SubKeyVersionLength],
		DataType: key[SubKeyDataTypeOffset],
		SubKey:   key[SubKeyDataOffset:],
		Trailer:  (1 << 8) | uint64(base.InternalKeyKindSet),
	}
}

func MakeInternalSetKeyOptimal(version, subKey []byte, dt uint8) InternalKey {
	return InternalKey{
		Version:  version,
		DataType: dt,
		SubKey:   subKey,
		Trailer:  (1 << 8) | uint64(base.InternalKeyKindSet),
	}
}

func DecodeInternalKey(key []byte) InternalKey {
	n := len(key) - 8
	ikey := InternalKey{
		Version:  nil,
		DataType: DataTypeNone,
		SubKey:   nil,
		Trailer:  uint64(base.InternalKeyKindInvalid),
	}
	if n >= SubKeyVersionLength {
		ikey.Trailer = binary.LittleEndian.Uint64(key[n:])
		key = key[:n:n]
		ikey.Version = key[0:SubKeyVersionLength]
		if ikey.Kind() != base.InternalKeyKindPrefixDelete {
			ikey.DataType = key[SubKeyDataTypeOffset]
			ikey.SubKey = key[SubKeyDataOffset:]
		}
	}
	return ikey
}

func InternalKeyEqual(a, b *InternalKey) bool {
	if UserKeyEqual(a, b) && a.Trailer == b.Trailer {
		return true
	}
	return false
}

func UserKeyEqual(a, b *InternalKey) bool {
	if bytes.Equal(a.Version, b.Version) &&
		a.DataType == b.DataType &&
		bytes.Equal(a.SubKey, b.SubKey) {
		return true
	}
	return false
}

func InternalKeyCompare(a, b InternalKey) int {
	if x := UserKeyCompare(a, b); x != 0 {
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

func UserKeyCompare(a, b InternalKey) int {
	if x := bytes.Compare(a.Version, b.Version); x != 0 {
		return x
	}
	if a.DataType != b.DataType {
		if a.DataType < b.DataType {
			return -1
		}
		return 1
	}
	return bytes.Compare(a.SubKey, b.SubKey)
}

func UserKeyCompare2(a *InternalKey, b []byte) int {
	if len(b) <= SubKeyVersionLength {
		return bytes.Compare(a.Version, b)
	}
	if x := bytes.Compare(a.Version, b[0:SubKeyVersionLength]); x != 0 {
		return x
	}
	if a.DataType != b[SubKeyDataTypeOffset] {
		if a.DataType < b[SubKeyDataTypeOffset] {
			return -1
		}
		return 1
	}
	return bytes.Compare(a.SubKey, b[SubKeyDataOffset:])
}

func UserKeyCompare3(a []byte, b *InternalKey) int {
	if len(a) <= SubKeyVersionLength {
		return bytes.Compare(a, b.Version)
	}
	if x := bytes.Compare(a[0:SubKeyVersionLength], b.Version); x != 0 {
		return x
	}
	if a[SubKeyDataTypeOffset] != b.DataType {
		if a[SubKeyDataTypeOffset] < b.DataType {
			return -1
		}
		return 1
	}
	return bytes.Compare(a[SubKeyDataOffset:], b.SubKey)
}

func IsUserKeyGEUpperBound(key *InternalKey, upper []byte) bool {
	return key != nil && upper != nil && UserKeyCompare2(key, upper) >= 0
}

func IsUserKeyLTLowerBound(key *InternalKey, lower []byte) bool {
	return key != nil && lower != nil && UserKeyCompare2(key, lower) < 0
}

func (k *InternalKey) SetKind(kind InternalKeyKind) {
	k.Trailer = (k.Trailer &^ 0xff) | uint64(kind)
}

func (k *InternalKey) SetTrailer(trailer uint64) {
	k.Trailer = trailer
}

func (k *InternalKey) Copy(key *InternalKey) {
	k.Version = key.Version
	k.DataType = key.DataType
	k.SubKey = key.SubKey
	k.Trailer = key.Trailer
}

func (k InternalKey) MakeUserKey() []byte {
	buf := make([]byte, SubKeyHeaderLength+len(k.SubKey))
	copy(buf[SubKeyVersionOffset:SubKeyDataTypeOffset], k.Version)
	buf[SubKeyDataTypeOffset] = k.DataType
	copy(buf[SubKeyDataOffset:], k.SubKey)
	return buf
}

func (k InternalKey) MakeUserKeyByBuf(buf []byte) []byte {
	buf = buf[:0]
	buf = append(buf, k.Version...)
	buf = append(buf, k.DataType)
	buf = append(buf, k.SubKey...)
	return buf
}

func (k InternalKey) Size() int {
	return len(k.SubKey) + 8 + SubKeyHeaderLength
}

func (k InternalKey) SeqNum() uint64 {
	return k.Trailer >> 8
}

func (k InternalKey) Kind() InternalKeyKind {
	return InternalKeyKind(k.Trailer & 0xff)
}

func (k InternalKey) Valid() bool {
	return k.Kind() <= base.InternalKeyKindMax
}

func (k InternalKey) String() string {
	return fmt.Sprintf("%d@%d@%v@%d#%d,%d", GetKeyVersion(k.Version), k.DataType, k.SubKey, k.Trailer, k.SeqNum(), k.Kind())
}
