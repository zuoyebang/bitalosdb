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

package vectorindex64

import (
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

const (
	sizeHeader = int(unsafe.Sizeof(header{}))
	maxGroups  = 1 << 31
)

type group32 [simd.GroupSize][12]byte

type group64 [simd.GroupSize][16]byte

type group96 [simd.GroupSize][20]byte
type group128 [simd.GroupSize][24]byte

func marshal32(buf []byte, key []byte, offset uint32) {
	copy(buf[:8], key)
	binary.LittleEndian.PutUint32(buf[8:12], offset)
}
func unmarshal32(buf []byte) (k []byte, offset uint32) {
	k = buf[:8]
	offset = binary.LittleEndian.Uint32(buf[8:12])
	return k, offset
}

func marshal64(buf []byte, k []byte, offset uint64) {
	copy(buf[:8], k)
	binary.LittleEndian.PutUint64(buf[8:16], offset)
}
func unmarshal64(buf []byte) (k []byte, offset uint64) {
	k = buf[:8]
	offset = binary.LittleEndian.Uint64(buf[8:16])
	return
}

func marshal96(buf []byte, k []byte, v []byte) {
	copy(buf[:8], k)
	copy(buf[8:20], v)
}
func unmarshal96(buf []byte) (k []byte, v []byte) {
	k = buf[:8]
	v = buf[8:20]
	return
}

func marshal128(buf []byte, k []byte, v []byte) {
	copy(buf[:8], k)
	copy(buf[8:24], v)
}
func unmarshal128(buf []byte) (k []byte, v []byte) {
	k = buf[:8]
	v = buf[8:24]
	return
}

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
	Logger base.Logger
}

type indexV32 struct {
	*header
	buf    []byte
	ctrl   []simd.Metadata
	groups []group32
}

func newIndexV32(groupNum uint32) (i *indexV32) {
	i = &indexV32{}
	i.header = &header{
		groupNum: groupNum,
	}

	i.buf = manual.New(int(groupNum*simd.GroupSize)*13 + sizeHeader)
	simd.InitMem128(unsafe.Pointer(&i.buf[sizeHeader]), unsafe.Pointer(&emptySli[0]), int(groupNum))
	i.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&i.buf[sizeHeader])))[:groupNum]
	i.groups = (*(*[maxGroups]group32)(unsafe.Pointer(&i.buf[sizeHeader+int(groupNum*simd.GroupSize)])))[:groupNum]

	i.marshalHeader()
	return i
}

func (i *indexV32) Free() {
	i.ctrl = nil
	i.groups = nil
	if i.buf != nil {
		manual.Free(i.buf)
		i.buf = nil
	}
}

func (i *indexV32) marshalHeader() {
	binary.LittleEndian.PutUint32(i.buf[0:], i.groupNum)
}

func (i *indexV32) unmarshalHeader() {
	i.header = &header{}
	i.groupNum = binary.LittleEndian.Uint32(i.buf[0:])
}

func (i *indexV32) Add(k []byte, off uint32) {
	hs := hash.Fnv32(k)
	hi, lo := simd.SplitHash32(hs)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchEmpty(&i.ctrl[g])
		if matches != 0 {
			s := simd.NextMatch(&matches)
			marshal32(i.groups[g][s][:], k, off)
			i.ctrl[g][s] = int8(lo)
			return
		}
		g += 1
		if g >= uint32(len(i.groups)) {
			g = 0
		}
	}
}

func (i *indexV32) Get(k []byte) (uint32, error) {
	hs := hash.Fnv32(k)
	hi, lo := simd.SplitHash32(hs)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchH2(&i.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hash, offset := unmarshal32(i.groups[g][s][:])
			if string(hash) == string(k) {
				return offset, nil
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

type VectorIndexV32 struct {
	op  *VectorIndexOptions
	idx *indexV32
	m   map[uint64]uint32
}

func NewVectorIndexV32(op *VectorIndexOptions) *VectorIndexV32 {
	v := &VectorIndexV32{
		op: op,
		m:  nil,
	}
	return v
}

func (v *VectorIndexV32) Set(k []byte, off uint32) {
	if v.m == nil {
		v.m = make(map[uint64]uint32, 1<<10)
	}
	v.m[*(*uint64)(unsafe.Pointer(&k[0]))] = off
}

func (v *VectorIndexV32) Get(k []byte) (off uint32, err error) {
	if v.idx == nil {
		return 0, base.ErrNotFound
	}
	return v.idx.Get(k)
}

func (v *VectorIndexV32) Serialize(w io.Writer) (int, error) {
	if len(v.m) == 0 {
		v.m = nil
		return 0, nil
	}

	v.idx = newIndexV32(simd.NumGroups(uint32(len(v.m))))

	defer func() {
		v.m = nil
		v.idx.Free()
	}()

	for key, off := range v.m {
		v.idx.Add(unsafe.Slice((*byte)(unsafe.Pointer(&key)), 8), off)
	}

	return w.Write(v.idx.buf)
}

func (v *VectorIndexV32) SetReader(buf []byte) {
	if v.idx == nil {
		v.idx = &indexV32{}
	}
	v.idx.buf = buf
	v.idx.unmarshalHeader()
	v.idx.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&buf[sizeHeader])))[:v.idx.groupNum]
	v.idx.groups = (*(*[maxGroups]group32)(unsafe.Pointer(&buf[sizeHeader+int(v.idx.groupNum*simd.GroupSize)])))[:v.idx.groupNum]
}

type indexV64 struct {
	*header
	buf    []byte
	ctrl   []simd.Metadata
	groups []group64
}

func newIndexV64(groupNum uint32) (i *indexV64) {
	i = &indexV64{}
	i.header = &header{
		groupNum: groupNum,
	}

	i.buf = manual.New(int(groupNum*simd.GroupSize)*17 + sizeHeader)
	simd.InitMem128(unsafe.Pointer(&i.buf[sizeHeader]), unsafe.Pointer(&emptySli[0]), int(groupNum))
	i.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&i.buf[sizeHeader])))[:groupNum]
	i.groups = (*(*[maxGroups]group64)(unsafe.Pointer(&i.buf[sizeHeader+int(groupNum*simd.GroupSize)])))[:groupNum]

	i.marshalHeader()
	return i
}

func (i *indexV64) Free() {
	i.ctrl = nil
	i.groups = nil
	manual.Free(i.buf)
}

func (i *indexV64) marshalHeader() {
	binary.LittleEndian.PutUint32(i.buf[0:], i.groupNum)
}

func (i *indexV64) unmarshalHeader() {
	i.header = &header{}
	i.groupNum = binary.LittleEndian.Uint32(i.buf[0:])
}

func (i *indexV64) Add(k []byte, off uint64) {
	hs := hash.Fnv32(k)
	hi, lo := simd.SplitHash32(hs)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchEmpty(&i.ctrl[g])
		if matches != 0 {
			s := simd.NextMatch(&matches)
			marshal64(i.groups[g][s][:], k, off)
			i.ctrl[g][s] = int8(lo)
			return
		}
		g += 1
		if g >= uint32(len(i.groups)) {
			g = 0
		}
	}
}

func (i *indexV64) Get(k []byte) (uint64, error) {
	hs := hash.Fnv32(k)
	hi, lo := simd.SplitHash32(hs)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchH2(&i.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hash, offset := unmarshal64(i.groups[g][s][:])
			if string(hash) == string(k) {
				return offset, nil
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

type VectorIndexV64 struct {
	op  *VectorIndexOptions
	idx *indexV64
	m   map[uint64]uint64
}

func NewVectorIndexV64(op *VectorIndexOptions) *VectorIndexV64 {
	v := &VectorIndexV64{
		op: op,
		m:  nil,
	}
	return v
}

func (v *VectorIndexV64) Size() int {
	if v.m == nil {
		return 0
	}
	return len(v.m)
}

func (v *VectorIndexV64) Set(k []byte, off uint64) {
	if v.m == nil {
		v.m = make(map[uint64]uint64, 1<<10)
	}
	v.m[*(*uint64)(unsafe.Pointer(&k[0]))] = off
}

func (v *VectorIndexV64) Get(k []byte) (uint64, error) {
	if v.idx == nil {
		return 0, base.ErrNotFound
	}
	return v.idx.Get(k)
}

func (v *VectorIndexV64) GetInMem(k []byte) (uint64, bool) {
	if v.m == nil {
		return 0, false
	}
	if val, ok := v.m[*(*uint64)(unsafe.Pointer(&k[0]))]; !ok {
		return 0, false
	} else {
		return val, true
	}
}

func (v *VectorIndexV64) Serialize(w io.Writer) (int, error) {
	if len(v.m) == 0 {
		v.m = nil
		return 0, nil
	}

	v.idx = newIndexV64(simd.NumGroups(uint32(len(v.m))))

	defer func() {
		v.m = nil
		v.idx.Free()
	}()

	for key, off := range v.m {
		v.idx.Add(unsafe.Slice((*byte)(unsafe.Pointer(&key)), 8), off)
	}

	return w.Write(v.idx.buf)
}

func (v *VectorIndexV64) SetReader(buf []byte) {
	if v.idx == nil {
		v.idx = &indexV64{}
	}
	v.idx.buf = buf
	v.idx.unmarshalHeader()
	v.idx.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&buf[sizeHeader])))[:v.idx.groupNum]
	v.idx.groups = (*(*[maxGroups]group64)(unsafe.Pointer(&buf[sizeHeader+int(v.idx.groupNum*simd.GroupSize)])))[:v.idx.groupNum]
}

type indexV96 struct {
	*header
	buf    []byte
	ctrl   []simd.Metadata
	groups []group96
}

func newIndexV96(groupNum uint32) (i *indexV96) {
	i = &indexV96{}
	i.header = &header{
		groupNum: groupNum,
	}

	i.buf = manual.New(int(groupNum*simd.GroupSize)*21 + sizeHeader)
	simd.InitMem128(unsafe.Pointer(&i.buf[sizeHeader]), unsafe.Pointer(&emptySli[0]), int(groupNum))
	i.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&i.buf[sizeHeader])))[:groupNum]
	i.groups = (*(*[maxGroups]group96)(unsafe.Pointer(&i.buf[sizeHeader+int(groupNum*simd.GroupSize)])))[:groupNum]

	i.marshalHeader()
	return i
}

func (i *indexV96) Free() {
	i.ctrl = nil
	i.groups = nil
	manual.Free(i.buf)
}

func (i *indexV96) marshalHeader() {
	binary.LittleEndian.PutUint32(i.buf[0:], i.groupNum)
}

func (i *indexV96) unmarshalHeader() {
	i.header = &header{}
	i.groupNum = binary.LittleEndian.Uint32(i.buf[0:])
}

func (i *indexV96) Add(k []byte, v []byte) {
	hs := hash.Fnv32(k)
	hi, lo := simd.SplitHash32(hs)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchEmpty(&i.ctrl[g])
		if matches != 0 {
			s := simd.NextMatch(&matches)
			marshal96(i.groups[g][s][:], k, v)
			i.ctrl[g][s] = int8(lo)
			return
		}
		g += 1
		if g >= uint32(len(i.groups)) {
			g = 0
		}
	}
}

func (i *indexV96) Get(k []byte) ([]byte, error) {
	hs := hash.Fnv32(k)
	hi, lo := simd.SplitHash32(hs)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchH2(&i.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hash, v := unmarshal96(i.groups[g][s][:])
			if string(hash) == string(k) {
				return v, nil
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

type VectorIndexV96 struct {
	op  *VectorIndexOptions
	idx *indexV96
	m   map[uint64][12]byte
}

func NewVectorIndexV96(op *VectorIndexOptions) *VectorIndexV96 {
	v := &VectorIndexV96{
		op: op,
		m:  nil,
	}
	return v
}

func (v *VectorIndexV96) Size() int {
	if v.m == nil {
		return 0
	}
	return len(v.m)
}

func (v *VectorIndexV96) Set(k []byte, val [12]byte) {
	if v.m == nil {
		v.m = make(map[uint64][12]byte, 1<<10)
	}
	v.m[*(*uint64)(unsafe.Pointer(&k[0]))] = val
}

func (v *VectorIndexV96) GetInMem(k []byte) ([12]byte, bool) {
	if v.m == nil {
		return [12]byte{}, false
	}
	if val, ok := v.m[*(*uint64)(unsafe.Pointer(&k[0]))]; !ok {
		return [12]byte{}, false
	} else {
		return val, true
	}
}

func (v *VectorIndexV96) Get(k []byte) ([]byte, error) {
	if v.idx == nil {
		return nil, base.ErrNotFound
	}
	return v.idx.Get(k)
}

func (v *VectorIndexV96) Serialize(w io.Writer) (int, error) {
	if len(v.m) == 0 {
		v.m = nil
		return 0, nil
	}

	v.idx = newIndexV96(simd.NumGroups(uint32(len(v.m))))

	defer func() {
		v.m = nil
		v.idx.Free()
	}()

	for key, val := range v.m {
		v.idx.Add(unsafe.Slice((*byte)(unsafe.Pointer(&key)), 8), val[:])
	}

	return w.Write(v.idx.buf)
}

func (v *VectorIndexV96) SetReader(buf []byte) {
	if v.idx == nil {
		v.idx = &indexV96{}
	}
	v.idx.buf = buf
	v.idx.unmarshalHeader()
	v.idx.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&buf[sizeHeader])))[:v.idx.groupNum]
	v.idx.groups = (*(*[maxGroups]group96)(unsafe.Pointer(&buf[sizeHeader+int(v.idx.groupNum*simd.GroupSize)])))[:v.idx.groupNum]
}

type indexV128 struct {
	*header
	buf    []byte
	ctrl   []simd.Metadata
	groups []group128
}

func newIndexV128(groupNum uint32) (i *indexV128) {
	i = &indexV128{}
	i.header = &header{
		groupNum: groupNum,
	}

	i.buf = manual.New(int(groupNum*simd.GroupSize)*25 + sizeHeader)
	simd.InitMem128(unsafe.Pointer(&i.buf[sizeHeader]), unsafe.Pointer(&emptySli[0]), int(groupNum))
	i.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&i.buf[sizeHeader])))[:groupNum]
	i.groups = (*(*[maxGroups]group128)(unsafe.Pointer(&i.buf[sizeHeader+int(groupNum*simd.GroupSize)])))[:groupNum]

	i.marshalHeader()
	return i
}

func (i *indexV128) Free() {
	i.ctrl = nil
	i.groups = nil
	manual.Free(i.buf)
}

func (i *indexV128) marshalHeader() {
	binary.LittleEndian.PutUint32(i.buf[0:], i.groupNum)
}

func (i *indexV128) unmarshalHeader() {
	i.header = &header{}
	i.groupNum = binary.LittleEndian.Uint32(i.buf[0:])
}

func (i *indexV128) Add(k []byte, v []byte) {
	hs := hash.Fnv32(k)
	hi, lo := simd.SplitHash32(hs)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchEmpty(&i.ctrl[g])
		if matches != 0 {
			s := simd.NextMatch(&matches)
			marshal128(i.groups[g][s][:], k, v)
			i.ctrl[g][s] = int8(lo)
			return
		}
		g += 1
		if g >= uint32(len(i.groups)) {
			g = 0
		}
	}
}

func (i *indexV128) Get(k []byte) ([]byte, error) {
	hs := hash.Fnv32(k)
	hi, lo := simd.SplitHash32(hs)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchH2(&i.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hash, v := unmarshal128(i.groups[g][s][:])
			if string(hash) == string(k) {
				return v, nil
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

type VectorIndexV128 struct {
	op  *VectorIndexOptions
	idx *indexV128
	m   map[uint64][16]byte
}

func NewVectorIndexV128(op *VectorIndexOptions) *VectorIndexV128 {
	v := &VectorIndexV128{
		op: op,
		m:  nil,
	}
	return v
}

func (v *VectorIndexV128) Size() int {
	if v.m == nil {
		return 0
	}
	return len(v.m)
}

func (v *VectorIndexV128) Set(k []byte, val [16]byte) {
	if v.m == nil {
		v.m = make(map[uint64][16]byte, 1<<10)
	}
	v.m[*(*uint64)(unsafe.Pointer(&k[0]))] = val
}

func (v *VectorIndexV128) GetInMem(k []byte) ([16]byte, bool) {
	if v.m == nil {
		return [16]byte{}, false
	}
	if val, ok := v.m[*(*uint64)(unsafe.Pointer(&k[0]))]; !ok {
		return [16]byte{}, false
	} else {
		return val, true
	}
}

func (v *VectorIndexV128) Get(k []byte) ([]byte, error) {
	if v.idx == nil {
		return nil, base.ErrNotFound
	}
	return v.idx.Get(k)
}

func (v *VectorIndexV128) Serialize(w io.Writer) (int, error) {
	if len(v.m) == 0 {
		v.m = nil
		return 0, nil
	}

	v.idx = newIndexV128(simd.NumGroups(uint32(len(v.m))))

	defer func() {
		v.m = nil
		v.idx.Free()
	}()

	for key, val := range v.m {
		v.idx.Add(unsafe.Slice((*byte)(unsafe.Pointer(&key)), 8), val[:])
	}

	return w.Write(v.idx.buf)
}

func (v *VectorIndexV128) SetReader(buf []byte) {
	if v.idx == nil {
		v.idx = &indexV128{}
	}
	v.idx.buf = buf
	v.idx.unmarshalHeader()
	v.idx.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&buf[sizeHeader])))[:v.idx.groupNum]
	v.idx.groups = (*(*[maxGroups]group128)(unsafe.Pointer(&buf[sizeHeader+int(v.idx.groupNum*simd.GroupSize)])))[:v.idx.groupNum]
}
