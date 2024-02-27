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
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"unicode/utf8"
)

type Compare func(a, b []byte) int

type Equal func(a, b []byte) bool

type AbbreviatedKey func(key []byte) uint64

type FormatKey func(key []byte) fmt.Formatter

type FormatValue func(key, value []byte) fmt.Formatter

type Separator func(dst, a, b []byte) []byte

type Successor func(dst, a []byte) []byte

type Split func(a []byte) int

type Comparer struct {
	Compare        Compare
	Equal          Equal
	AbbreviatedKey AbbreviatedKey
	FormatKey      FormatKey
	FormatValue    FormatValue
	Separator      Separator
	Split          Split
	Successor      Successor
	Name           string
}

var DefaultFormatter = func(key []byte) fmt.Formatter {
	return FormatBytes(key)
}

var DefaultComparer = &Comparer{
	Compare: bytes.Compare,
	Equal:   bytes.Equal,

	AbbreviatedKey: func(key []byte) uint64 {
		if len(key) >= 8 {
			return binary.BigEndian.Uint64(key)
		}
		var v uint64
		for _, b := range key {
			v <<= 8
			v |= uint64(b)
		}
		return v << uint(8*(8-len(key)))
	},

	FormatKey: DefaultFormatter,

	Separator: func(dst, a, b []byte) []byte {
		i, n := SharedPrefixLen(a, b), len(dst)
		dst = append(dst, a...)

		min := len(a)
		if min > len(b) {
			min = len(b)
		}
		if i >= min {
			return dst
		}

		if a[i] >= b[i] {
			return dst
		}

		if i < len(b)-1 || a[i]+1 < b[i] {
			i += n
			dst[i]++
			return dst[:i+1]
		}

		i += n + 1
		for ; i < len(dst); i++ {
			if dst[i] != 0xff {
				dst[i]++
				return dst[:i+1]
			}
		}
		return dst
	},

	Successor: func(dst, a []byte) (ret []byte) {
		for i := 0; i < len(a); i++ {
			if a[i] != 0xff {
				dst = append(dst, a[:i+1]...)
				dst[len(dst)-1]++
				return dst
			}
		}
		return append(dst, a...)
	},

	Name: "bitalosdb.BytewiseComparator",
}

func SharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	asUint64 := func(c []byte, i int) uint64 {
		return binary.LittleEndian.Uint64(c[i:])
	}
	for i < n-7 && asUint64(a, i) == asUint64(b, i) {
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

type FormatBytes []byte

const lowerhex = "0123456789abcdef"

func (p FormatBytes) Format(s fmt.State, c rune) {
	buf := make([]byte, 0, len(p))
	for _, b := range p {
		if b < utf8.RuneSelf && strconv.IsPrint(rune(b)) {
			buf = append(buf, b)
			continue
		}
		buf = append(buf, `\x`...)
		buf = append(buf, lowerhex[b>>4])
		buf = append(buf, lowerhex[b&0xF])
	}
	s.Write(buf)
}
