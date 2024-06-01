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

package compress

import (
	"github.com/golang/snappy"
)

const (
	CompressTypeNo int = 0 + iota
	CompressTypeSnappy
)

type Compressor interface {
	Encode(dst, src []byte) []byte
	Decode(dst, src []byte) ([]byte, error)
	Type() int
}

var (
	NoCompressor     noCompressor
	SnappyCompressor snappyCompressor
)

func SetCompressor(t int) Compressor {
	switch t {
	case CompressTypeSnappy:
		return SnappyCompressor
	default:
		return NoCompressor
	}
}

type noCompressor struct{}

func (c noCompressor) Encode(dst, src []byte) []byte {
	return src
}

func (c noCompressor) Decode(dst, src []byte) ([]byte, error) {
	return src, nil
}

func (c noCompressor) Type() int {
	return CompressTypeNo
}

type snappyCompressor struct{}

func (sc snappyCompressor) Encode(dst, src []byte) []byte {
	return snappy.Encode(dst, src)
}

func (sc snappyCompressor) Decode(dst, src []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}

func (sc snappyCompressor) Type() int {
	return CompressTypeSnappy
}
