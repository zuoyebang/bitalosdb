// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitalosdb

import (
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
)

func (d *DB) NewBitupleIter() *BitupleIterator {
	iter := &BitupleIterator{
		db:      d,
		curSlot: -1,
	}
	return iter
}

type BitupleIterator struct {
	db            *DB
	curSlot       int
	curTuplePos   uint16
	curTupleCount uint16
	curBituple    *Bituple
	curVtIter     base.VectorTableIterator
	iterKey       []byte
}

func (i *BitupleIterator) findNextVtIter() bool {
	if i.curBituple != nil {
		if i.curVtIter != nil {
			i.curVtIter.Close()
			i.curVtIter = nil
		}

		i.curTuplePos++
		if i.curTuplePos < i.curTupleCount {
			i.curVtIter = i.curBituple.tupleList[i.curTuplePos].vt.NewIterator()
			return true
		}

		i.curBituple = nil
	}

	for {
		i.curSlot++
		if i.curSlot >= metaSlotNum {
			return false
		}

		bt := i.db.getBitupleSafe(i.curSlot)
		if bt == nil {
			continue
		}

		i.curBituple = bt
		i.curTuplePos = 0
		i.curTupleCount = bt.tupleCount
		i.curVtIter = bt.tupleList[i.curTuplePos].vt.NewIterator()
		return true
	}
}

func (i *BitupleIterator) findNextKey() (key []byte, dataType uint8, ts uint64) {
	final := true
	for final {
		if !i.findNextVtIter() {
			i.iterKey = nil
			return nil, 0, 0
		}
		key, _, _, _, dataType, ts, _, _, _, _, _, _, final = i.curVtIter.Next()
	}
	i.iterKey = key
	return
}

func (i *BitupleIterator) First() ([]byte, uint8, uint64) {
	i.curBituple = nil
	i.curSlot = -1
	return i.findNextKey()
}

func (i *BitupleIterator) Next() ([]byte, uint8, uint64) {
	if i.iterKey == nil {
		return nil, 0, 0
	}

	key, _, _, _, dataType, ts, _, _, _, _, _, _, final := i.curVtIter.Next()
	if final {
		return i.findNextKey()
	}
	i.iterKey = key
	return key, dataType, ts
}

func (i *BitupleIterator) Seek(cursor []byte) ([]byte, uint8, uint64) {
	if len(cursor) != 12 {
		return nil, 0, 0
	}

	i.curSlot = int(binary.BigEndian.Uint16(cursor[0:2]))
	i.curTuplePos = binary.BigEndian.Uint16(cursor[2:4])

	for {
		if i.curSlot >= metaSlotNum {
			return nil, 0, 0
		}

		bt := i.db.getBitupleSafe(i.curSlot)
		if bt == nil || i.curTuplePos >= bt.tupleCount || len(bt.tupleList) == 0 {
			i.curSlot++
			i.curTuplePos = 0
			continue
		}

		i.curBituple = bt
		i.curTupleCount = bt.tupleCount
		if i.curVtIter != nil {
			i.curVtIter.Close()
		}
		i.curVtIter = bt.tupleList[i.curTuplePos].vt.NewIterator()
		break
	}

	g := binary.BigEndian.Uint32(cursor[4:8])
	s := binary.BigEndian.Uint32(cursor[8:12])
	key, _, _, _, dataType, ts, _, _, _, _, _, _, final := i.curVtIter.SeekCursor(g, s)
	if final {
		return i.findNextKey()
	}
	i.iterKey = key
	return key, dataType, ts
}

func (i *BitupleIterator) GetCursor() []byte {
	if i.curVtIter == nil {
		return nil
	}

	g, s, ok := i.curVtIter.GetCursor()
	if !ok {
		if i.curTuplePos < i.curTupleCount-1 {
			i.curTuplePos++
		} else {
			if i.curSlot >= metaSlotNum-1 {
				return nil
			}
			i.curSlot++
			i.curTuplePos = 0
		}
		g = 0
		s = 0
	}

	cursor := make([]byte, 12)
	binary.BigEndian.PutUint16(cursor[0:2], uint16(i.curSlot))
	binary.BigEndian.PutUint16(cursor[2:4], i.curTuplePos)
	binary.BigEndian.PutUint32(cursor[4:8], g)
	binary.BigEndian.PutUint32(cursor[8:12], s)
	return cursor
}

func (i *BitupleIterator) Close() error {
	if i.curVtIter != nil {
		i.curVtIter.Close()
	}
	return nil
}

type TupleIterator struct {
	bt          *Bituple
	curTuplePos int
	tupleCount  int
	tupleList   []*Tuple
	curVtIter   base.VectorTableIterator
	final       bool
}

func (b *Bituple) NewBitupleIter() *TupleIterator {
	iter := &TupleIterator{
		bt:          b,
		curTuplePos: -1,
		tupleCount:  int(b.tupleCount),
		tupleList:   b.tupleList,
	}
	return iter
}

func (i *TupleIterator) findNextVtIter() bool {
	if i.curVtIter != nil {
		i.curVtIter.Close()
		i.curVtIter = nil
	}

	i.curTuplePos++
	if i.curTuplePos < i.tupleCount {
		i.curVtIter = i.tupleList[i.curTuplePos].vt.NewIterator()
		return true
	}
	return false
}

func (i *TupleIterator) findNextKey() (
	key []byte, h, l, seqNum uint64, dataType uint8,
	timestamp uint64, version uint64, slotId uint16, size uint32,
	pre, next uint64, value []byte, final bool,
) {
	final = true
	for final {
		if !i.findNextVtIter() {
			i.final = true
			return
		}
		key, h, l, seqNum, dataType, timestamp, version, slotId, size, pre, next, value, final = i.curVtIter.Next()
	}
	i.final = false
	return
}

func (i *TupleIterator) First() (
	key []byte, h, l, seqNum uint64, dataType uint8,
	timestamp uint64, version uint64, slotId uint16, size uint32,
	pre, next uint64, value []byte, final bool,
) {
	return i.findNextKey()
}

func (i *TupleIterator) Next() (
	key []byte, h, l, seqNum uint64, dataType uint8,
	timestamp uint64, version uint64, slotId uint16, size uint32,
	pre, next uint64, value []byte, final bool,
) {
	if !i.final {
		key, h, l, seqNum, dataType, timestamp, version, slotId, size, pre, next, value, final = i.curVtIter.Next()
		if final {
			return i.findNextKey()
		}
		i.final = false
	}
	return
}

func (i *TupleIterator) Close() error {
	if i.curVtIter != nil {
		i.curVtIter.Close()
	}
	return nil
}
