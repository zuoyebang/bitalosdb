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
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

type InternalKeyKind uint8

const (
	InternalKeyKindDelete  InternalKeyKind = 0
	InternalKeyKindSet     InternalKeyKind = 1
	InternalKeyKindMax     InternalKeyKind = 18
	InternalKeyKindInvalid InternalKeyKind = 255

	InternalKeySeqNumMax = uint64(1<<56 - 1)
)

var internalKeyKindNames = []string{
	InternalKeyKindDelete:  "DEL",
	InternalKeyKindSet:     "SET",
	InternalKeyKindInvalid: "INVALID",
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

// MakeInternalKey constructs an internal key from a specified user key,
// sequence number and kind.
func MakeInternalKey(userKey []byte, seqNum uint64, kind InternalKeyKind) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (seqNum << 8) | uint64(kind),
	}
}

func MakeMinKey(userKey []byte) InternalKey {
	return MakeInternalKey(userKey, 1, InternalKeyKindSet)
}

// MakeSearchKey constructs an internal key that is appropriate for searching
// for a the specified user key. The search key contain the maximual sequence
// number and kind ensuring that it sorts before any other internal keys for
// the same user key.
func MakeSearchKey(userKey []byte) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (InternalKeySeqNumMax << 8) | uint64(InternalKeyKindMax),
	}
}

var kindsMap = map[string]InternalKeyKind{
	"DEL":     InternalKeyKindDelete,
	"SET":     InternalKeyKindSet,
	"INVALID": InternalKeyKindInvalid,
}

// ParseInternalKey parses the string representation of an internal key. The
// format is <user-key>.<kind>.<seq-num>. If the seq-num starts with a "b" it
// is marked as a batch-seq-num (i.e. the InternalKeySeqNumBatch bit is set).
func ParseInternalKey(s string) InternalKey {
	x := strings.Split(s, ".")
	ukey := x[0]
	kind, ok := kindsMap[x[1]]
	if !ok {
		panic(fmt.Sprintf("unknown kind: %q", x[1]))
	}
	j := 0
	if x[2][0] == 'b' {
		j = 1
	}
	seqNum, _ := strconv.ParseUint(x[2][j:], 10, 64)

	return MakeInternalKey([]byte(ukey), seqNum, kind)
}

// DecodeInternalKey decodes an encoded internal key. See InternalKey.Encode().
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

// InternalCompare compares two internal keys using the specified comparison
// function. For equal user keys, internal keys compare in descending sequence
// number order. For equal user keys and sequence numbers, internal keys
// compare in descending kind order (though this should never happen in
// practice).
func InternalCompare(a, b InternalKey) int {
	if x := bytes.Compare(a.UserKey, b.UserKey); x != 0 {
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

// Encode encodes the receiver into the buffer. The buffer must be large enough
// to hold the encoded data. See InternalKey.Size().
func (k InternalKey) Encode(buf []byte) {
	i := copy(buf, k.UserKey)
	binary.LittleEndian.PutUint64(buf[i:], k.Trailer)
}

// EncodeTrailer returns the trailer encoded to an 8-byte array.
func (k InternalKey) EncodeTrailer() [8]byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], k.Trailer)
	return buf
}

// Size returns the encoded size of the key.
func (k InternalKey) Size() int {
	return len(k.UserKey) + 8
}

// SetSeqNum sets the sequence number component of the key.
func (k *InternalKey) SetSeqNum(seqNum uint64) {
	k.Trailer = (seqNum << 8) | (k.Trailer & 0xff)
}

// SeqNum returns the sequence number component of the key.
func (k InternalKey) SeqNum() uint64 {
	return k.Trailer >> 8
}

// Visible returns true if the key is visible at the specified snapshot
// sequence number.
func (k InternalKey) Visible(snapshot uint64) bool {
	seqNum := k.SeqNum()
	return seqNum < snapshot
}

// SetKind sets the kind component of the key.
func (k *InternalKey) SetKind(kind InternalKeyKind) {
	k.Trailer = (k.Trailer &^ 0xff) | uint64(kind)
}

// Kind returns the kind compoment of the key.
func (k InternalKey) Kind() InternalKeyKind {
	return InternalKeyKind(k.Trailer & 0xff)
}

// Valid returns true if the key has a valid kind.
func (k InternalKey) Valid() bool {
	return k.Kind() <= InternalKeyKindMax
}

// Clone clones the storage for the UserKey component of the key.
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
	return fmt.Sprintf("%s#%d,%d", string(k.UserKey), k.SeqNum(), k.Kind())
}
