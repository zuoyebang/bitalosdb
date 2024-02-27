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

package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func (k InternalKey) encodedString() string {
	buf := make([]byte, k.Size())
	k.Encode(buf)
	return string(buf)
}

func TestInternalKey(t *testing.T) {
	k := MakeInternalKey([]byte("foo"), 0x08070605040302, 1)
	if got, want := k.encodedString(), "foo\x01\x02\x03\x04\x05\x06\x07\x08"; got != want {
		t.Fatalf("k = %q want %q", got, want)
	}
	if !k.Valid() {
		t.Fatalf("invalid key")
	}
	if got, want := string(k.UserKey), "foo"; got != want {
		t.Errorf("ukey = %q want %q", got, want)
	}
	if got, want := k.Kind(), InternalKeyKind(1); got != want {
		t.Errorf("kind = %d want %d", got, want)
	}
	if got, want := k.SeqNum(), uint64(0x08070605040302); got != want {
		t.Errorf("seqNum = %d want %d", got, want)
	}
}

func TestInvalidInternalKey(t *testing.T) {
	testCases := []string{
		"",
		"\x01\x02\x03\x04\x05\x06\x07",
		"foo",
		"foo\x08\x07\x06\x05\x04\x03\x02",
		"foo\x13\x07\x06\x05\x04\x03\x02\x01",
	}
	for _, tc := range testCases {
		k := DecodeInternalKey([]byte(tc))
		if k.Valid() {
			t.Errorf("%q is a valid key, want invalid", tc)
		}

		if k.Kind() == InternalKeyKindInvalid && k.UserKey != nil {
			t.Errorf("expected nil UserKey after decoding encodedKey=%q", tc)
		}
	}
}

func TestInternalKeyComparer(t *testing.T) {
	keys := []string{
		"" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"" + "\x00\xff\xff\xff\xff\xff\xff\xff",
		"" + "\x01\x01\x00\x00\x00\x00\x00\x00",
		"" + "\x00\x01\x00\x00\x00\x00\x00\x00",
		"",
		"" + "\x01\x00\x00\x00\x00\x00\x00\x00",
		"" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\x00" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\x00blue" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"bl\x00ue" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"blue" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"blue\x00" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"green" + "\xff\x11\x00\x00\x00\x00\x00\x00",
		"green" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"green" + "\x01\x00\x00\x00\x00\x00\x00\x00",
		"red" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"red" + "\x01\x72\x73\x74\x75\x76\x77\x78",
		"red" + "\x01\x00\x00\x00\x00\x00\x00\x11",
		"red" + "\x01\x00\x00\x00\x00\x00\x11\x00",
		"red" + "\x01\x00\x00\x00\x00\x11\x00\x00",
		"red" + "\x01\x00\x00\x00\x11\x00\x00\x00",
		"red" + "\x01\x00\x00\x11\x00\x00\x00\x00",
		"red" + "\x01\x00\x11\x00\x00\x00\x00\x00",
		"red" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"red" + "\x00\x11\x00\x00\x00\x00\x00\x00",
		"red" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xfe" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"\xfe" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xff" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"\xff" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xff\x40" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"\xff\x40" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xff\xff" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"\xff\xff" + "\x00\x00\x00\x00\x00\x00\x00\x00",
	}
	c := DefaultComparer.Compare
	for i := range keys {
		for j := range keys {
			ik := DecodeInternalKey([]byte(keys[i]))
			jk := DecodeInternalKey([]byte(keys[j]))
			got := InternalCompare(c, ik, jk)
			want := 0
			if i < j {
				want = -1
			} else if i > j {
				want = +1
			}
			if got != want {
				t.Errorf("i=%d, j=%d, keys[i]=%q, keys[j]=%q: got %d, want %d",
					i, j, keys[i], keys[j], got, want)
			}
		}
	}
}

func TestMakeInternalValue(t *testing.T) {
	value := []byte("test_value")
	seqNum := uint64(1)
	kind := InternalKeyKindSet
	iValue := MakeInternalValue(value, seqNum, kind)
	require.Equal(t, iValue.SeqNum(), seqNum)
	require.Equal(t, iValue.Kind(), kind)
}

func TestEncodeInternalValue(t *testing.T) {
	value := []byte("test_value")
	seqNum := uint64(10)
	kind := InternalKeyKindSet
	encodeValue, closer := EncodeInternalValue(value, seqNum, kind)
	defer func() {
		if closer != nil {
			closer()
		}
	}()
	iv := DecodeInternalValue(encodeValue)
	require.Equal(t, iv.SeqNum(), seqNum)
	require.Equal(t, iv.Kind(), kind)
}
