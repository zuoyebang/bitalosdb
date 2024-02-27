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

package utils

import (
	"encoding/binary"
	"math"
)

func Float64ToByteSort(float float64, buf []byte) []byte {
	bits := math.Float64bits(float)
	if buf == nil {
		buf = make([]byte, 8, 8)
	}
	_ = buf[7]
	binary.BigEndian.PutUint64(buf, bits)
	if buf[0] < 1<<7 {
		buf[0] ^= 1 << 7
	} else {
		buf[0] ^= 0xff
		buf[1] ^= 0xff
		buf[2] ^= 0xff
		buf[3] ^= 0xff
		buf[4] ^= 0xff
		buf[5] ^= 0xff
		buf[6] ^= 0xff
		buf[7] ^= 0xff
	}
	return buf
}
