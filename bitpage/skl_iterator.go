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

	"github.com/zuoyebang/bitalosdb/internal/base"
)

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

type sklIterator struct {
	list *skl
	nd   *node
	key  internalKey
}

var _ base.InternalIterator = (*sklIterator)(nil)

func (it *sklIterator) Close() error {
	it.list = nil
	it.nd = nil
	return nil
}

func (it *sklIterator) String() string {
	return "sklIterator"
}

func (it *sklIterator) Error() error {
	return nil
}

func (it *sklIterator) SeekGE(key []byte) (*internalKey, []byte) {
	_, it.nd, _ = it.list.seekForBaseSplice(key)
	if it.nd == it.list.tail {
		return nil, nil
	}

	it.decodeKey()
	return &it.key, it.value()
}

func (it *sklIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*internalKey, []byte) {
	if trySeekUsingNext {
		if it.nd == it.list.tail {
			return nil, nil
		}
		less := it.list.cmp(it.key.UserKey, key) < 0
		const numNexts = 5
		for i := 0; less && i < numNexts; i++ {
			k, _ := it.Next()
			if k == nil {
				return nil, nil
			}
			less = it.list.cmp(it.key.UserKey, key) < 0
		}
		if !less {
			return &it.key, it.value()
		}
	}
	return it.SeekGE(key)
}

func (it *sklIterator) SeekLT(key []byte) (*internalKey, []byte) {
	it.nd, _, _ = it.list.seekForBaseSplice(key)
	if it.nd == it.list.head {
		return nil, nil
	}
	it.decodeKey()
	return &it.key, it.value()
}

func (it *sklIterator) First() (*internalKey, []byte) {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	return &it.key, it.value()
}

func (it *sklIterator) Last() (*internalKey, []byte) {
	it.nd = it.list.getPrev(it.list.tail, 0)
	if it.nd == it.list.head {
		return nil, nil
	}
	it.decodeKey()
	return &it.key, it.value()
}

func (it *sklIterator) Next() (*internalKey, []byte) {
	it.nd = it.list.getSkipNext(it.nd)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	return &it.key, it.value()
}

func (it *sklIterator) Prev() (*internalKey, []byte) {
	it.nd = it.list.getSkipPrev(it.nd)
	if it.nd == it.list.head {
		return nil, nil
	}
	it.decodeKey()
	return &it.key, it.value()
}

func (it *sklIterator) value() []byte {
	return it.nd.getValue(it.list.tbl)
}

func (it *sklIterator) Head() bool {
	return it.nd == it.list.head
}

func (it *sklIterator) Tail() bool {
	return it.nd == it.list.tail
}

func (it *sklIterator) SetBounds(lower, upper []byte) {
}

func (it *sklIterator) decodeKey() {
	b := it.list.tbl.getBytes(it.nd.keyOffset, it.nd.keySize)
	l := len(b) - 8
	if l >= 0 {
		it.key.Trailer = binary.LittleEndian.Uint64(b[l:])
		it.key.UserKey = b[:l:l]
	} else {
		it.key.Trailer = uint64(internalKeyKindInvalid)
		it.key.UserKey = nil
	}
}
