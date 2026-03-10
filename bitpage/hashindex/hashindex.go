// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this table except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashindex

import (
	"encoding/binary"
	"sync"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

const (
	sizeHeader = int(unsafe.Sizeof(header{}))
	maxGroups  = 1 << 31

	offsetMask   uint32 = 0x7fff_ffff
	conflictMask uint32 = 0x8000_0000
)

type group [simd.GroupSize][8]byte

var emptySli = [16]byte{
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
}

type header struct {
	slots    uint32
	resident uint32
}

type VectorIndexOptions struct {
	Logger   base.Logger
	FilePath string
	HashSize uint32
}

type index struct {
	*header
	limit  uint32
	data   []byte
	ctrl   []simd.Metadata
	groups []group
	rwLock sync.RWMutex
}

func newIndex(slots uint32, data []byte) (i *index, err error) {
	groups := slots / simd.GroupSize
	i = &index{
		limit: simd.MaxAvgGroupLoad * groups,
		data:  data,
	}

	i.header = &header{
		slots: slots,
	}

	s := binary.LittleEndian.Uint32(i.data[0:])
	if s == 0 {
		simd.InitMem128(unsafe.Pointer(&i.data[sizeHeader]), unsafe.Pointer(&emptySli[0]), int(groups))
	} else if slots != s {
		return nil, errors.Errorf("slot err: input:%d file:%d", slots, s)
	} else {
		i.resident = binary.LittleEndian.Uint32(i.data[4:])
	}

	i.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&i.data[sizeHeader])))[:groups]
	i.groups = (*(*[maxGroups]group)(unsafe.Pointer(&i.data[sizeHeader+int(i.slots)])))[:groups]
	return i, nil
}

func (i *index) reloadData(data []byte) (sz int, err error) {
	i.data = data
	i.unmarshalHeader()
	groups := i.slots / simd.GroupSize
	i.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&i.data[sizeHeader])))[:groups]
	i.groups = (*(*[maxGroups]group)(unsafe.Pointer(&i.data[sizeHeader+int(i.slots)])))[:groups]
	sz = sizeHeader + int(i.slots*9)
	return
}

func (i *index) marshalHeader() {
	binary.LittleEndian.PutUint32(i.data[0:], i.slots)
	binary.LittleEndian.PutUint32(i.data[4:], i.resident)
}

func (i *index) unmarshalHeader() {
	i.header = &header{}
	i.slots = binary.LittleEndian.Uint32(i.data[0:])
	i.resident = binary.LittleEndian.Uint32(i.data[4:])
}

func (i *index) Add(fnv uint32, off uint32) (err error) {
	hi, lo := simd.SplitHash32(fnv)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchH2(&i.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hash := binary.LittleEndian.Uint32(i.groups[g][s][:4])
			if hash == fnv {
				i.rwLock.Lock()
				binary.LittleEndian.PutUint32(i.groups[g][s][4:], off)
				i.rwLock.Unlock()
				return
			}
		}

		matches = simd.MetaMatchEmpty(&i.ctrl[g])
		if matches != 0 {
			if i.resident < i.limit {
				s := simd.NextMatch(&matches)
				i.resident++
				binary.LittleEndian.PutUint32(i.groups[g][s][:4], fnv)
				binary.LittleEndian.PutUint32(i.groups[g][s][4:], off)
				i.ctrl[g][s] = int8(lo)
			} else {
				err = base.ErrTableFull
			}
			return
		}
		g += 1
		if g >= uint32(len(i.groups)) {
			g = 0
		}
	}

}

func (i *index) Get(fnv uint32) (uint32, error) {
	hi, lo := simd.SplitHash32(fnv)
	g := simd.ProbeStart32(hi, len(i.ctrl))
	for {
		matches := simd.MetaMatchH2(&i.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hash := binary.LittleEndian.Uint32(i.groups[g][s][:4])
			if hash == fnv {
				i.rwLock.RLock()
				r := binary.LittleEndian.Uint32(i.groups[g][s][4:])
				i.rwLock.RUnlock()
				return r, nil
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
	l := len(offs)*4 + 2
	bs, c := bytepools.ReaderBytePools.GetBytePool(l)
	binary.LittleEndian.PutUint16(bs, uint16(len(offs)))
	off := 2
	for _, of := range offs {
		binary.LittleEndian.PutUint32(bs[off:], of)
		off += 4
	}
	c()
}

func (i *index) close() {
	i.marshalHeader()
	i.ctrl = nil
	i.groups = nil
	return
}

func (i *index) unmarshalItemMeta(buf []byte) (hash uint32, offset uint32) {
	hash = binary.LittleEndian.Uint32(buf[:4])
	offset = binary.LittleEndian.Uint32(buf[4:])
	return
}

func (i *index) marshalItemMeta(buf []byte, hash uint32, offset uint32) {
	binary.LittleEndian.PutUint32(buf[:4], hash)
	binary.LittleEndian.PutUint32(buf[4:], offset)
	return
}

type VectorIndex struct {
	idxFileSize int
	op          *VectorIndexOptions
	tbl         *Table
	idx         []*index
	lock        sync.RWMutex
}

func NewVectorIndex(op *VectorIndexOptions) (*VectorIndex, error) {
	groups := simd.NumGroups(op.HashSize)
	slots := groups * simd.GroupSize

	idxFileSize := int(slots)*9 + sizeHeader

	// TODO 调整table初始化
	tbl, isNew, err := OpenTable(op.FilePath, idxFileSize)
	if err != nil {
		return nil, err
	}

	v := &VectorIndex{
		idxFileSize: idxFileSize,
		op:          op,
		tbl:         tbl,
	}
	var nextIdxOff int
	if isNew {
		for {
			idx, err := newIndex(slots, tbl.data[nextIdxOff:nextIdxOff+idxFileSize])
			if err != nil {
				return nil, err
			}

			v.idx = append(v.idx, idx)

			nextIdxOff += idxFileSize
			if nextIdxOff >= tbl.filesz {
				break
			}
		}
	} else {
		for {
			idx := new(index)
			sz, err := idx.reloadData(tbl.data[nextIdxOff:])
			if err != nil {
				return nil, err
			}

			v.idx = append(v.idx, idx)

			nextIdxOff += sz
			if nextIdxOff >= tbl.filesz {
				break
			}
		}
	}

	return v, nil
}

func (v *VectorIndex) Add(fnv32, off uint32) {
	for _, idx := range v.idx {
		if err := idx.Add(fnv32, off); err == nil {
			return
		}
	}

	lIdx := len(v.idx)
	for _, idx := range v.idx {
		idx.marshalHeader()
	}

	slots := v.idx[0].slots / 2
	eSize := sizeHeader + int(slots*9)

	v.lock.Lock()
	if err := v.tbl.ExpandSize(eSize); err != nil {
		v.lock.Unlock()
		v.op.Logger.Errorf("hashindex expandMmapSize error: %s", err.Error())
		return
	}

	of := 0
	for i := 0; i < lIdx; i++ {
		sz, err := v.idx[i].reloadData(v.tbl.data[of:])
		if err != nil {
			v.lock.Unlock()
			v.op.Logger.Errorf("hashindex reloadData error: %s", err.Error())
			return
		}
		of += sz
	}
	v.lock.Unlock()
	idx, err := newIndex(slots, v.tbl.data[of:])
	if err != nil {
		v.op.Logger.Errorf("hashindex newIndex error: %s", err.Error())
		return
	}

	_ = idx.Add(fnv32, off)

	v.idx = append(v.idx, idx)
}

func (v *VectorIndex) Get(fnv32 uint32) (off uint32, err error) {
	v.lock.RLock()
	defer v.lock.RUnlock()
	for _, idx := range v.idx {
		if off, err = idx.Get(fnv32); err == nil {
			return
		}
	}
	return 0, base.ErrNotFound
}

func (v *VectorIndex) Sync() error {
	v.lock.Lock()
	defer v.lock.Unlock()
	for _, idx := range v.idx {
		idx.marshalHeader()
	}

	return v.tbl.Sync()
}

func (v *VectorIndex) Close() error {
	v.lock.Lock()
	defer v.lock.Unlock()
	for _, idx := range v.idx {
		idx.close()
	}
	v.idx = nil

	return v.tbl.Close()
}
