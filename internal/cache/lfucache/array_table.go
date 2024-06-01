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

package lfucache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/zuoyebang/bitalosdb/internal/cache/lfucache/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/manual"
)

const (
	itemHeaderLen = 2
)

type arrayTable struct {
	id       int64
	num      int
	index    []uint32
	arenaBuf []byte
	arena    *arenaskl.Arena
}

func checkArrayTable(obj interface{}) {
	at := obj.(*arrayTable)
	if at.arenaBuf != nil {
		fmt.Fprintf(os.Stderr, "%p: arrayTable(%d) buffer was not freed\n", at.arenaBuf, at.id)
		os.Exit(1)
	}
}

func newArrayTable(id int64, size int) *arrayTable {
	arenaBuf := manual.New(size + 8)
	at := &arrayTable{
		id:       id,
		num:      0,
		arenaBuf: arenaBuf,
		arena:    arenaskl.NewArena(arenaBuf),
	}

	return at
}

func arrayTableEntrySize(keyBytes, valueBytes int) int {
	return keyBytes + valueBytes + itemHeaderLen
}

func (t *arrayTable) add(key []byte, value []byte) error {
	keySize := len(key)
	valueSize := len(value)
	itemSize := arrayTableEntrySize(keySize, valueSize)
	itemOffset, err := t.arena.AllocNoAlign(uint32(itemSize))
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint16(t.arena.GetBytes(itemOffset, itemHeaderLen), uint16(keySize))
	copy(t.arena.GetBytes(itemOffset+itemHeaderLen, uint32(keySize)), key)
	copy(t.arena.GetBytes(itemOffset+itemHeaderLen+uint32(keySize), uint32(valueSize)), value)

	t.index = append(t.index, itemOffset)
	t.num++

	return nil
}

func (t *arrayTable) validIndexPos(i int) bool {
	return i >= 0 && i < t.num
}

func (t *arrayTable) get(key []byte) ([]byte, bool, internalKeyKind) {
	indexPos := sort.Search(t.num, func(i int) bool {
		return bytes.Compare(t.getKey(i), key) != -1
	})

	k, v := t.getKV(indexPos)
	if k == nil || bytes.Compare(k, key) != 0 {
		return nil, false, internalKeyKindInvalid
	}

	return v, true, internalKeyKindSet
}

func (t *arrayTable) getKey(i int) []byte {
	if !t.validIndexPos(i) {
		return nil
	}

	itemOffset := t.index[i]
	keySize := uint32(binary.BigEndian.Uint16(t.arena.GetBytes(itemOffset, itemHeaderLen)))
	key := t.arena.GetBytes(itemOffset+itemHeaderLen, keySize)
	return key
}

func (t *arrayTable) getKV(i int) ([]byte, []byte) {
	if !t.validIndexPos(i) {
		return nil, nil
	}

	var itemSize uint32
	itemOffset := t.index[i]
	if i == t.num-1 {
		itemSize = t.arena.Size() - itemOffset
	} else {
		itemSize = t.index[i+1] - itemOffset
	}

	keySize := uint32(binary.BigEndian.Uint16(t.arena.GetBytes(itemOffset, itemHeaderLen)))
	key := t.arena.GetBytes(itemOffset+itemHeaderLen, keySize)
	valueSize := itemSize - keySize - itemHeaderLen
	value := t.arena.GetBytes(itemOffset+itemHeaderLen+keySize, valueSize)

	return key, value
}

func (t *arrayTable) seek(key []byte) (*internalKey, []byte) {
	iter := t.newIter(nil)
	defer iter.Close()

	ik, value := iter.SeekGE(key)
	if ik == nil || !bytes.Equal(ik.UserKey, key) {
		return nil, nil
	}
	return ik, value
}

func (t *arrayTable) newIter(o *iterOptions) internalIterator {
	iter := iterPool.Get().(*arrayTableIterator)
	*iter = arrayTableIterator{
		at:        t,
		indexPos:  0,
		iterKey:   new(internalKey),
		iterValue: nil,
	}
	return iter
}

func (t *arrayTable) newFlushIter(o *iterOptions, bytesFlushed *uint64) internalIterator {
	iter := &arrayTableFlushIterator{
		arrayTableIterator: arrayTableIterator{at: t, iterKey: new(internalKey)},
		bytesIterated:      bytesFlushed,
	}
	return iter
}

func (t *arrayTable) inuseBytes() uint64 {
	return uint64(t.arena.Size())
}

func (t *arrayTable) totalBytes() uint64 {
	return uint64(t.arena.Capacity())
}

func (t *arrayTable) close() error {
	return nil
}

func (t *arrayTable) readyForFlush() bool {
	return true
}

func (t *arrayTable) getID() int64 {
	return t.id
}

func (t *arrayTable) count() int {
	return t.num
}
