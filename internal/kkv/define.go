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
	"math"

	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

const (
	SlotIdLength           = 2
	KeyMd5Length           = 16
	KeyMaxUpperBoundLength = 8

	SubKeyHeaderLength     = 9
	SubKeyVersionLength    = 8
	SubKeyVersionOffset    = 0
	SubKeyDataTypeOffset   = SubKeyVersionOffset + SubKeyVersionLength
	SubKeyDataOffset       = SubKeyHeaderLength
	SubKeyUpperBoundLength = SubKeyHeaderLength + KeyMaxUpperBoundLength
	SubKeyListIndexLength  = 8
	SubKeyListLength       = SubKeyHeaderLength + SubKeyListIndexLength
	SubKeyZsetLength       = SubKeyHeaderLength + KeyMd5Length

	SiKeyLength           = 8
	SiKeyHeaderLength     = SubKeyHeaderLength + SiKeyLength
	SiKeyDataOffset       = SubKeyHeaderLength
	SiKeySubKeyDataOffset = SiKeyDataOffset + SiKeyLength
	SiKeyLowerBoundLength = SubKeyHeaderLength + SiKeyLength
	SiKeyUpperBoundLength = SiKeyLowerBoundLength + KeyMaxUpperBoundLength

	InitalLeftIndex  uint64 = math.MaxUint64 / 2
	InitalRightIndex        = InitalLeftIndex + 1
	ListReadMax             = 10000

	ExpireKeyLength = 19
)

const (
	DataTypeNone uint8 = 0 + iota
	DataTypeString
	DataTypeBitmap
	DataTypeHash
	DataTypeList
	DataTypeSet
	DataTypeZset
	DataTypeZsetIndex
	DataTypeExpireKey
	DataTypeDKHash
	DataTypeDKSet
)

var (
	MaxScoreBound = utils.Float64ToByteSort(math.MaxFloat64, nil)
	MaxUpperBound = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	ScanEndCurosr = []byte("0")
)
