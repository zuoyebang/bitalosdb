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

package bitpage

import (
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/v2/bitpage/vectorindex64"
)

type vatOffsetIndex struct {
	num  int
	data []byte
}

func (i *vatOffsetIndex) setData(buf []byte) {
	i.data = buf
	i.num = len(i.data) / vatOffsetLength
}

func (i *vatOffsetIndex) getSize() int {
	return i.num
}

func (i *vatOffsetIndex) getOffset(pos int) uint32 {
	if pos >= i.num {
		return 0
	}
	return binary.BigEndian.Uint32(i.data[pos*vatOffsetLength:])
}

type vatZsetIndex struct {
	versionIndex *vectorindex64.VectorIndexV96
	offsetIndex  vatOffsetIndex
}

func (i *vatZsetIndex) getZindexVersionIndex(keyVersion []byte) (int, int, bool) {
	v, err := i.versionIndex.Get(keyVersion)
	if err != nil {
		return 0, 0, false
	}

	sp := int(binary.BigEndian.Uint32(v[0:4]))
	ep := int(binary.BigEndian.Uint32(v[4:8]))
	return sp, ep, true
}

func (i *vatZsetIndex) setZindexVersionIndex(version []byte, sp, ep uint32) {
	v, _ := i.versionIndex.GetInMem(version)
	binary.BigEndian.PutUint32(v[0:4], sp)
	binary.BigEndian.PutUint32(v[4:8], ep)
	i.versionIndex.Set(version, v)
}

func (i *vatZsetIndex) getZdataVersionIndex(keyVersion []byte) (uint32, bool) {
	v, err := i.versionIndex.Get(keyVersion)
	if err != nil {
		return 0, false
	}

	offset := binary.BigEndian.Uint32(v[8:12])
	if offset == 0 {
		return 0, false
	}

	return offset, true
}

func (i *vatZsetIndex) setZdataVersionIndex(version []byte, offset uint32) {
	var v [12]byte
	binary.BigEndian.PutUint32(v[8:12], offset)
	i.versionIndex.Set(version, v)
}

func (i *vatZsetIndex) setVersionIndexReader(buf []byte) {
	i.versionIndex.SetReader(buf)
}

func (i *vatZsetIndex) setOffsetIndexReader(buf []byte) {
	i.offsetIndex.setData(buf)
}

func (i *vatZsetIndex) getOffset(pos int) uint32 {
	return i.offsetIndex.getOffset(pos)
}

func (i *vatZsetIndex) getOffsetSize() int {
	return i.offsetIndex.getSize()
}

func (a *vectorArrayTable) getHashSetListVersionIndex(keyVersion []byte) (uint32, uint32, bool) {
	value, err := a.hashSetListVersionIndex.Get(keyVersion)
	if err != nil {
		return 0, 0, false
	}

	h := uint32(value >> 32)
	l := uint32(value & vatVersionIndex64ValueMask)
	return h, l, true
}

func (a *vectorArrayTable) setHashSetListVersionIndex(version []byte, h, l uint32) {
	value := uint64(h)<<32 + uint64(l)
	a.hashSetListVersionIndex.Set(version, value)
}
