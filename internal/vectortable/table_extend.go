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

package vectortable

import (
	"bytes"
	"encoding/binary"
	"sync"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/mmap"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
	"golang.org/x/sys/unix"
)

const (
	bytesMeta40  = 40 // metaSizeStNkStr metaSizeNkStr metaSizeNkKKV
	bytesMeta44  = 44 // metaSizeStNkKKV metaSizeStrWithKey
	bytesMeta48  = 48 // metaSizeKKVWithKey metaSizeStKKVWithKey metaSizeStStrWithKey
	bytesMeta56  = 56 // metaSizeNkList
	bytesMeta60  = 60 // metaSizeStNkList
	bytesMeta64  = 64 // metaSizeListWithKey metaSizeStListWithKey
	maxReuseSize = 64

	sizeMetaHeaderVer = 4
	sizeMetaReuseSlot = 4

	byteMetaMagic byte = 0xff
)

var magicBytes = [16]byte{
	0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff,
}

var VerMap = map[uint16]map[byte]uint32{
	0: nil,
	1: {
		bytesMeta40: sizeMetaHeaderVer,
		bytesMeta44: sizeMetaHeaderVer + 4,
		bytesMeta48: sizeMetaHeaderVer + 8,
		bytesMeta56: sizeMetaHeaderVer + 12,
		bytesMeta60: sizeMetaHeaderVer + 16,
		bytesMeta64: sizeMetaHeaderVer + 20,
	},
}

type tableExtend struct {
	*table
	upperOffset uint64
	reuseMap    map[byte]uint32
	reuseBufSz  uint32
	dataLock    sync.RWMutex
}

func openTableExtend(path string, off *OffsetKeeper, extendSize uint32, ver uint16, logger base.Logger) (*tableExtend, error) {
	opts := &tableOptions{
		openType:  tableWriteBlockMmap,
		logger:    logger,
		blockSize: extendSize,
	}
	t, err := openTable(path, off, opts)
	if err != nil {
		return nil, err
	}

	tf := &tableExtend{
		table:      t,
		reuseMap:   VerMap[ver],
		reuseBufSz: uint32(sizeMetaReuseSlot * len(VerMap[ver])),
	}

	filesz := t.filesz
	tf.upperOffset = t.offset.GetOffset()
	if tf.upperOffset < uint64(t.filesz) {
		t.filesz = int(tf.upperOffset)
	}
	if err = tf.mmapWriteNext(); err != nil {
		return nil, err
	}

	if filesz == 0 {
		offset, _ := tf.allocSize(sizeMetaHeaderVer)
		if offset != 0 {
			tf.close()
			return nil, errors.Errorf("openTableExtend alloc version fail file:%s", path)
		}
		binary.LittleEndian.PutUint16(t.data[offset:], ver)
		l := uint64(tf.reuseBufSz * 2)
		reuseOff, err := tf.allocSize(l)
		if err != nil {
			return nil, err
		}

		for i := uint64(0); i < l; i++ {
			tf.data[reuseOff+i] = 0
		}
	} else {
		ver = binary.LittleEndian.Uint16(t.data)
		tf.reuseMap = VerMap[ver]
		tf.reuseBufSz = uint32(sizeMetaReuseSlot * len(VerMap[ver]))

		if tf.reuseBufSz > 0 {
			offBuf := sizeMetaHeaderVer + tf.reuseBufSz
			for i := uint32(sizeMetaHeaderVer); i < offBuf; i += sizeMetaReuseSlot {
				iBuf := i + tf.reuseBufSz
				if bytes.Equal(t.data[i:i+sizeMetaReuseSlot], t.data[iBuf:iBuf+sizeMetaReuseSlot]) {
					continue
				}
				t.logger.Errorf("panic table reload offset not equal: headerOff:%d oriOff:%d offset:%d",
					i,
					binary.LittleEndian.Uint32(t.data[i:]),
					binary.LittleEndian.Uint32(t.data[iBuf:]),
				)
				offset := uint64(binary.LittleEndian.Uint32(t.data[iBuf:])) << 2
				if offset+metaSizeNkStr > off.GetOffset() {
					copy(t.data[iBuf:iBuf+sizeMetaReuseSlot], t.data[i:i+sizeMetaReuseSlot])
					continue
				}
				offMagic := offset + offsetMetaMagic
				offMagicTail := offMagic + 16
				if bytes.Equal(t.data[offMagic:offMagicTail], magicBytes[:]) {
					copy(t.data[i:i+sizeMetaReuseSlot], t.data[iBuf:iBuf+sizeMetaReuseSlot])
				} else {
					copy(t.data[iBuf:iBuf+sizeMetaReuseSlot], t.data[i:i+sizeMetaReuseSlot])
				}
			}
		}
	}

	return tf, nil
}

func (t *tableExtend) allocSize(size uint64) (uint64, error) {
	if t.offset.GetOffset()+size > t.upperOffset {
		if err := t.mmapWriteNext(); err != nil {
			return 0, err
		}
	}

	offset := t.offset.AllocSpace(size)
	if t.offset.GetOffset() > maxOffset {
		t.offset.Init(offset)
		return 0, base.ErrFileFull
	}

	return offset, nil
}

func (t *tableExtend) mmapWriteNext() error {
	newSize := int(t.upperOffset + t.mmapBlockSize)
	if newSize > t.filesz {
		if err := t.fileTruncate(newSize, false); err != nil {
			return err
		}
	}

	_ = t.msync()
	b, err := mmapFile(t.file, newSize, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	t.dataLock.Lock()
	oldData := t.data
	t.data = nil
	t.data = b
	t.upperOffset = uint64(newSize)
	t.dataLock.Unlock()

	if oldData != nil {
		_ = unix.Munmap(oldData)
		oldData = nil
	}

	return nil
}

func (t *tableExtend) ReuseAlloc(size uint64) (offset uint64, err error) {
	if len(t.reuseMap) == 0 || size > maxReuseSize {
		offset, err = t.allocSize(size)
		return
	}
	var s = byte(size)
	if hOff, ok := t.reuseMap[s]; ok {
		offset = uint64(binary.LittleEndian.Uint32(t.data[hOff:])) << 2
		if offset == 0 {
			offset, err = t.allocSize(size)
			return
		}
		offMagic := offset + offsetMetaMagic
		offMagicTail := offMagic + 16

		if offMagicTail >= t.offset.GetOffset() {
			offset, err = t.allocSize(size)
			t.logger.Errorf("panic table reuse alloc size error offset:%d len:%d", offset, t.offset.GetOffset())
			return
		}

		if !simd.Equal128(unsafe.Pointer(&t.data[offMagic]), unsafe.Pointer(&magicBytes[0])) {
			oriOff := offset
			offset = uint64(binary.LittleEndian.Uint32(t.data[hOff+t.reuseBufSz:])) << 2
			if offset == 0 {
				offset, err = t.allocSize(size)
				return
			} else if oriOff == offset {
				binary.LittleEndian.PutUint32(t.data[hOff+t.reuseBufSz:], 0)
				binary.LittleEndian.PutUint32(t.data[hOff:], 0)
				offset, err = t.allocSize(size)
				t.logger.Errorf("panic table reuse alloc size error oriOff:%d offset:%d len:%d", oriOff, offset, t.offset.GetOffset())
				return
			}

			if offset >= t.offset.GetOffset() {
				offset, err = t.allocSize(size)
				t.logger.Errorf("panic table reuse alloc size error offset:%d len:%d", offset, t.offset.GetOffset())
				return
			}
			offMagic = offset + offsetMetaMagic
			if !bytes.Equal(t.data[offMagic:offMagic+16], magicBytes[:]) {
				binary.LittleEndian.PutUint32(t.data[hOff+t.reuseBufSz:], 0)
				binary.LittleEndian.PutUint32(t.data[hOff:], 0)
				offset, err = t.allocSize(size)
				t.logger.Errorf("panic table reuse alloc size error oriOff:%d offset:%d len:%d", oriOff, offset, t.offset.GetOffset())
				return
			}
		}

		nOff := binary.LittleEndian.Uint32(t.data[offset:])
		binary.LittleEndian.PutUint32(t.data[hOff+t.reuseBufSz:], nOff)
		binary.LittleEndian.PutUint32(t.data[hOff:], nOff)
		return
	} else {
		offset, err = t.allocSize(size)
		return
	}
}

func (t *tableExtend) ReuseFree(offset uint64, size uint32) {
	if offset > t.offset.GetOffset() {
		t.logger.Errorf("panic table reuse free size error offset:%d len:%d", offset, t.offset.GetOffset())
		return
	}

	if hOff, ok := t.reuseMap[byte(size)]; ok {
		binary.LittleEndian.PutUint32(t.data[offset:], binary.LittleEndian.Uint32(t.data[hOff:]))
		nOff := uint32(offset >> 2)
		binary.LittleEndian.PutUint32(t.data[hOff+t.reuseBufSz:], nOff)
		simd.InitMem128(unsafe.Pointer(&t.data[offset+offsetMetaMagic]), unsafe.Pointer(&magicBytes[0]), 1)
		binary.LittleEndian.PutUint32(t.data[hOff:], nOff)
	}
}

func (t *tableExtend) ReuseCap(size uint32) int {
	var count int
	if hOff, ok := t.reuseMap[byte(size)]; ok {
		offset := uint64(hOff)
		for {
			offset = uint64(binary.LittleEndian.Uint32(t.data[offset:])) << 2
			if offset == 0 {
				break
			}
			count++
		}
	}
	return count
}

func (t *tableExtend) ReuseAllocPanic(c int, size uint64) (offset uint64, err error) {
	if len(t.reuseMap) == 0 || size > maxReuseSize {
		offset, err = t.allocSize(size)
		return
	}
	var s = byte(size)
	if hOff, ok := t.reuseMap[s]; ok {
		offset = uint64(binary.LittleEndian.Uint32(t.data[hOff:])) << 2
		if offset == 0 {
			offset, err = t.allocSize(size)
			return
		}
		nOff := binary.LittleEndian.Uint32(t.data[offset:])
		binary.LittleEndian.PutUint32(t.data[hOff+t.reuseBufSz:], nOff)
		if c == 1 {
			panic(c)
		}
		binary.LittleEndian.PutUint32(t.data[hOff:], nOff)
		return
	} else {
		offset, err = t.allocSize(size)
		return
	}
}

func (t *tableExtend) ReuseFreePanic(c int, offset uint64, size uint32) {
	if hOff, ok := t.reuseMap[byte(size)]; ok {
		binary.LittleEndian.PutUint32(t.data[offset:], binary.LittleEndian.Uint32(t.data[hOff:]))
		nOff := uint32(offset >> 2)
		binary.LittleEndian.PutUint32(t.data[hOff+t.reuseBufSz:], nOff)
		if c == 3 {
			panic(c)
		}
		t.data[offset+offsetMetaMagic0] = byteMetaMagic
		t.data[offset+offsetMetaMagic1] = byteMetaMagic
		t.data[offset+offsetMetaMagic2] = byteMetaMagic
		t.data[offset+offsetMetaMagic3] = byteMetaMagic
		if c == 4 {
			panic(c)
		}
		binary.LittleEndian.PutUint32(t.data[hOff:], nOff)
	}
}
