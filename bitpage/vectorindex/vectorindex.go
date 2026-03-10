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

package vectorindex

import (
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

const (
	sizeHeader = int(unsafe.Sizeof(header{}))
	maxGroups  = 1 << 31

	offsetMask   uint32 = 0x7fff_ffff
	conflictMask uint32 = 0x8000_0000
)

type group [simd.GroupSize][8]byte

var emptySli = [64]byte{
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
}

type header struct {
	groupNum uint32
}

type VectorIndexOptions struct {
	CompareKeyFunc func(key []byte, off uint32) (v []byte, c int)
	CompareVerFunc func(key []byte, off uint32) (c int)
	Logger         base.Logger
}

type index struct {
	cmpKey func(key []byte, off uint32) (v []byte, c int)
	cmpVer func(key []byte, off uint32) (c int)
	*header
	buf    []byte
	ctrl   []simd.Metadata
	groups []group
	data   []byte
}

func newIndex(groups uint32,
	cmpKey func(key []byte, off uint32) (v []byte, c int),
	cmpVer func(key []byte, off uint32) (c int)) (i *index) {
	//groups = align4Groups(groups)
	i = &index{
		cmpKey: cmpKey,
		cmpVer: cmpVer,
	}
	i.header = &header{
		groupNum: groups,
	}

	i.buf = manual.New(int(groups*simd.GroupSize)*9 + sizeHeader)
	simd.InitMem128(unsafe.Pointer(&i.buf[sizeHeader]), unsafe.Pointer(&emptySli[0]), int(groups))
	i.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&i.buf[sizeHeader])))[:groups]
	i.groups = (*(*[maxGroups]group)(unsafe.Pointer(&i.buf[sizeHeader+int(groups*simd.GroupSize)])))[:groups]
	i.data = make([]byte, 0, 256<<10)
	i.marshalHeader()
	return i
}

func (i *index) Free() {
	manual.Free(i.buf)
	i.ctrl = nil
	i.groups = nil
}

func (i *index) marshalHeader() {
	binary.LittleEndian.PutUint32(i.buf[0:], i.groupNum)
}

func (i *index) unmarshalHeader() {
	i.header = &header{}
	i.groupNum = binary.LittleEndian.Uint32(i.buf[0:])
}

func (i *index) Add(h uint32, offs []uint32) {
	hi, lo := simd.SplitHash32(h)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchEmpty(&i.ctrl[g])
		if matches != 0 {
			s := simd.NextMatch(&matches)
			l := len(offs)
			if l > 1 {
				off := uint32(len(i.data))
				i.appendData(offs)
				i.marshalItemMeta(i.groups[g][s][:], h, off, l)
			} else if l == 1 {
				i.marshalItemMeta(i.groups[g][s][:], h, offs[0], l)
			} else {
				return
			}
			i.ctrl[g][s] = int8(lo)
			return
		}
		g += 1
		if g >= uint32(len(i.groups)) {
			g = 0
		}
	}
}

func (i *index) GetValue(key []byte, h uint32) ([]byte, error) {
	hi, lo := simd.SplitHash32(h)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchH2(&i.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hash, offset, conflict := i.unmarshalItemMeta(i.groups[g][s])
			if hash == h {
				if !conflict {
					if v, c := i.cmpKey(key, offset); c == 0 {
						return v, nil
					}
				} else {
					pos := int(offset)
					m, n := binary.Uvarint(i.data[pos:])
					pos += n
					l := int(m)
					for j := 0; j < l; j++ {
						of, n1 := binary.Uvarint(i.data[pos:])
						if v, c := i.cmpKey(key, uint32(of)); c == 0 {
							return v, nil
						}
						pos += n1
					}
				}
			}
		}

		matches = simd.MetaMatchEmpty(&i.ctrl[g])
		if matches != 0 {
			return nil, base.ErrNotFound
		}
		g += 1
		if g >= uint32(len(i.groups)) {
			g = 0
		}
	}
}

func (i *index) GetOffset(key []byte, h uint32) (uint32, error) {
	hi, lo := simd.SplitHash32(h)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchH2(&i.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hash, offset, conflict := i.unmarshalItemMeta(i.groups[g][s])
			if hash == h {
				if !conflict {
					if c := i.cmpVer(key, offset); c == 0 {
						return offset, nil
					}
				} else {
					pos := int(offset)
					m, n := binary.Uvarint(i.data[pos:])
					pos += n
					l := int(m)
					for j := 0; j < l; j++ {
						off1, n1 := binary.Uvarint(i.data[pos:])
						off := uint32(off1)
						if c := i.cmpVer(key, off); c == 0 {
							return off, nil
						}
						pos += n1
					}
				}
			}
		}

		matches = simd.MetaMatchEmpty(&i.ctrl[g])
		if matches != 0 {
			return 0, base.ErrNotFound
		}
		g += 1
		if g >= uint32(len(i.groups)) {
			g = 0
		}
	}
}

func (i *index) appendData(offs []uint32) {
	var buf [binary.MaxVarintLen32]byte
	num := len(offs)
	n := binary.PutUvarint(buf[:], uint64(num))
	i.data = append(i.data, buf[:n]...)
	for _, of := range offs {
		n = binary.PutUvarint(buf[:], uint64(of))
		i.data = append(i.data, buf[:n]...)
	}
}

func (i *index) unmarshalItemMeta(buf [8]byte) (hash uint32, offset uint32, conflict bool) {
	hash = binary.LittleEndian.Uint32(buf[:4])
	off := binary.LittleEndian.Uint32(buf[4:])
	offset = off & offsetMask
	conflict = (off & conflictMask) > 0
	return
}

func (i *index) marshalItemMeta(buf []byte, hash uint32, offset uint32, conflictNum int) {
	binary.LittleEndian.PutUint32(buf[:4], hash)
	if conflictNum > 1 {
		binary.LittleEndian.PutUint32(buf[4:], offset+conflictMask)
	} else {
		binary.LittleEndian.PutUint32(buf[4:], offset)
	}
	return
}

type VectorIndex struct {
	op  *VectorIndexOptions
	idx *index
	m   map[uint32][]uint32
}

func NewVectorIndex(op *VectorIndexOptions) *VectorIndex {
	v := &VectorIndex{
		op: op,
		m:  nil,
	}
	return v
}

func (v *VectorIndex) Add(h32, off uint32) {
	if v.m == nil {
		v.m = make(map[uint32][]uint32, 1<<10)
	}
	v.m[h32] = append(v.m[h32], off)
}

func (v *VectorIndex) GetValue(k []byte, h32 uint32) (val []byte, err error) {
	if v.idx == nil {
		return nil, base.ErrNotFound
	}
	return v.idx.GetValue(k, h32)
}

func (v *VectorIndex) GetOffset(k []byte, h32 uint32) (off uint32, err error) {
	return v.idx.GetOffset(k, h32)
}

func (v *VectorIndex) Serialize(w io.Writer) (int, error) {
	if len(v.m) == 0 {
		v.m = nil
		return 0, nil
	}

	v.idx = newIndex(simd.NumGroups(uint32(len(v.m))),
		v.op.CompareKeyFunc,
		v.op.CompareVerFunc)

	defer func() {
		v.m = nil
		v.idx.Free()
	}()

	for h, bs := range v.m {
		v.idx.Add(h, bs)
	}

	if n, err := w.Write(v.idx.buf); err != nil {
		return n, err
	} else {
		m, err := w.Write(v.idx.data)
		return n + m, err
	}
}

func (v *VectorIndex) SetReader(buf []byte) {
	if v.idx == nil {
		v.idx = &index{
			cmpKey: v.op.CompareKeyFunc,
			cmpVer: v.op.CompareVerFunc,
		}
	}
	v.idx.buf = buf
	v.idx.unmarshalHeader()
	v.idx.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&buf[sizeHeader])))[:v.idx.groupNum]
	v.idx.groups = (*(*[maxGroups]group)(unsafe.Pointer(&buf[sizeHeader+int(v.idx.groupNum*simd.GroupSize)])))[:v.idx.groupNum]
	v.idx.data = buf[int(v.idx.groupNum*simd.GroupSize)*9+sizeHeader:]
}
