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

package simd

const (
	h1Mask32  uint32 = 0xffff_ff80
	h2Mask32  uint32 = 0x0000_007f
	h1Mask64  uint64 = 0xffff_ffff_ffff_ff80
	h2Mask64  uint64 = 0x0000_0000_0000_007f
	Empty     int8   = -128 // 0b1000_0000
	Tombstone int8   = -2   // 0b1111_1110
)

type H1_32 uint32
type H1_64 uint64
type H2 int8

type Metadata [GroupSize]int8

func NewEmptyMetadata() (meta Metadata) {
	for i := range meta {
		meta[i] = Empty
	}
	return
}

func fastMod64(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

func fastMod32(x, n uint32) uint32 {
	return x % n
}

func SplitHash32(h uint32) (H1_32, H2) {
	return H1_32((h & h1Mask32) >> 7), H2(h & h2Mask32)
}

func ProbeStart32(hi H1_32, groups int) uint32 {
	return fastMod32(uint32(hi), uint32(groups))
}

func SplitHash64(h uint64) (H1_64, H2) {
	return H1_64((h & h1Mask64) >> 7), H2(h & h2Mask64)
}

func ProbeStart64(hi H1_64, groups int) uint32 {
	return fastMod64(uint32(hi), uint32(groups))
}

func NumGroups(n uint32) (groups uint32) {
	groups = (n + MaxAvgGroupLoad - 1) / MaxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}
