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

package utils

import (
	"encoding/binary"
	"math"
)

func Float64ToByteSort(float float64, buf []byte) []byte {
	bits := math.Float64bits(float)
	if buf == nil {
		buf = make([]byte, 8)
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

func ByteSortToFloat64(buf []byte) float64 {
	_ = buf[7]
	var tmpbuf [8]byte
	if buf[0] >= 1<<7 {
		tmpbuf[0] = buf[0] ^ (1 << 7)
		tmpbuf[1] = buf[1]
		tmpbuf[2] = buf[2]
		tmpbuf[3] = buf[3]
		tmpbuf[4] = buf[4]
		tmpbuf[5] = buf[5]
		tmpbuf[6] = buf[6]
		tmpbuf[7] = buf[7]
	} else {
		tmpbuf[0] = buf[0] ^ 0xff
		tmpbuf[1] = buf[1] ^ 0xff
		tmpbuf[2] = buf[2] ^ 0xff
		tmpbuf[3] = buf[3] ^ 0xff
		tmpbuf[4] = buf[4] ^ 0xff
		tmpbuf[5] = buf[5] ^ 0xff
		tmpbuf[6] = buf[6] ^ 0xff
		tmpbuf[7] = buf[7] ^ 0xff
	}
	bits := binary.BigEndian.Uint64(tmpbuf[:])
	return math.Float64frombits(bits)
}
