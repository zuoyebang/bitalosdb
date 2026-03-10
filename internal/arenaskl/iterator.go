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

package arenaskl

import (
	"encoding/binary"
	"sync"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

type InternalKKVKey = kkv.InternalKey

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

type Iterator struct {
	list  *Skiplist
	nd    *node
	key   base.InternalKey
	lower []byte
	upper []byte
}

var _ kkv.InternalIterator = (*Iterator)(nil)

var iterPool = sync.Pool{
	New: func() interface{} {
		return &Iterator{}
	},
}

func (it *Iterator) Close() error {
	it.list = nil
	it.nd = nil
	it.lower = nil
	it.upper = nil
	iterPool.Put(it)
	return nil
}

func (it *Iterator) String() string {
	return "memtable"
}

func (it *Iterator) Error() error {
	return nil
}

func (it *Iterator) getKKVKey(key base.InternalKey) *InternalKKVKey {
	kkvKey := kkv.MakeInternalKey(key)
	return &kkvKey
}

func (it *Iterator) SeekGE(key []byte) (*InternalKKVKey, []byte) {
	_, it.nd, _ = it.list.seekForBaseSplice(key)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil, nil
	}
	return it.getKKVKey(it.key), it.value()
}

func (it *Iterator) SeekLT(key []byte) (*InternalKKVKey, []byte) {
	it.nd, _, _ = it.list.seekForBaseSplice(key)
	if it.nd == it.list.head {
		return nil, nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.cmp(it.lower, it.key.UserKey) > 0 {
		it.nd = it.list.head
		return nil, nil
	}
	return it.getKKVKey(it.key), it.value()
}

func (it *Iterator) First() (*InternalKKVKey, []byte) {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil, nil
	}
	return it.getKKVKey(it.key), it.value()
}

func (it *Iterator) Last() (*InternalKKVKey, []byte) {
	it.nd = it.list.getPrev(it.list.tail, 0)
	if it.nd == it.list.head {
		return nil, nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.cmp(it.lower, it.key.UserKey) > 0 {
		it.nd = it.list.head
		return nil, nil
	}
	return it.getKKVKey(it.key), it.value()
}

func (it *Iterator) Next() (*InternalKKVKey, []byte) {
	it.nd = it.list.getSkipNext(it.nd)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil, nil
	}
	return it.getKKVKey(it.key), it.value()
}

func (it *Iterator) Prev() (*InternalKKVKey, []byte) {
	it.nd = it.list.getSkipPrev(it.nd)
	if it.nd == it.list.head {
		return nil, nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.cmp(it.lower, it.key.UserKey) > 0 {
		it.nd = it.list.head
		return nil, nil
	}
	return it.getKKVKey(it.key), it.value()
}

func (it *Iterator) value() []byte {
	return it.nd.getValue(it.list.arena)
}

func (it *Iterator) Head() bool {
	return it.nd == it.list.head
}

func (it *Iterator) Tail() bool {
	return it.nd == it.list.tail
}

func (it *Iterator) SetBounds(lower, upper []byte) {
	it.lower = lower
	it.upper = upper
}

func (it *Iterator) decodeKey() {
	b := it.list.arena.getBytes(it.nd.keyOffset, it.nd.keySize)
	l := len(b) - 8
	if l >= 0 {
		it.key.Trailer = binary.LittleEndian.Uint64(b[l:])
		it.key.UserKey = b[:l:l]
	} else {
		it.key.Trailer = uint64(base.InternalKeyKindInvalid)
		it.key.UserKey = nil
	}
}
