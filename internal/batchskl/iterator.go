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

package batchskl

import "github.com/zuoyebang/bitalosdb/internal/base"

type splice struct {
	prev uint32
	next uint32
}

type Iterator struct {
	list  *Skiplist
	nd    uint32
	key   base.InternalKey
	lower []byte
	upper []byte
}

func (it *Iterator) Close() error {
	it.list = nil
	it.nd = 0
	return nil
}

func (it *Iterator) SeekGE(key []byte) *base.InternalKey {
	_, it.nd = it.seekForBaseSplice(key, it.list.abbreviatedKey(key))
	if it.nd == it.list.tail {
		return nil
	}
	nodeKey := it.list.getKey(it.nd)
	if it.upper != nil && it.list.cmp(it.upper, nodeKey.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil
	}
	it.key = nodeKey
	return &it.key
}

func (it *Iterator) SeekLT(key []byte) *base.InternalKey {
	it.nd, _ = it.seekForBaseSplice(key, it.list.abbreviatedKey(key))
	if it.nd == it.list.head {
		return nil
	}
	nodeKey := it.list.getKey(it.nd)
	if it.lower != nil && it.list.cmp(it.lower, nodeKey.UserKey) > 0 {
		it.nd = it.list.head
		return nil
	}
	it.key = nodeKey
	return &it.key
}

func (it *Iterator) First() *base.InternalKey {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail {
		return nil
	}
	nodeKey := it.list.getKey(it.nd)
	if it.upper != nil && it.list.cmp(it.upper, nodeKey.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil
	}
	it.key = nodeKey
	return &it.key
}

func (it *Iterator) Last() *base.InternalKey {
	it.nd = it.list.getPrev(it.list.tail, 0)
	if it.nd == it.list.head {
		return nil
	}
	nodeKey := it.list.getKey(it.nd)
	if it.lower != nil && it.list.cmp(it.lower, nodeKey.UserKey) > 0 {
		it.nd = it.list.head
		return nil
	}
	it.key = nodeKey
	return &it.key
}

func (it *Iterator) Next() *base.InternalKey {
	it.nd = it.list.getNext(it.nd, 0)
	if it.nd == it.list.tail {
		return nil
	}
	nodeKey := it.list.getKey(it.nd)
	if it.upper != nil && it.list.cmp(it.upper, nodeKey.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil
	}
	it.key = nodeKey
	return &it.key
}

func (it *Iterator) Prev() *base.InternalKey {
	it.nd = it.list.getPrev(it.nd, 0)
	if it.nd == it.list.head {
		return nil
	}
	nodeKey := it.list.getKey(it.nd)
	if it.lower != nil && it.list.cmp(it.lower, nodeKey.UserKey) > 0 {
		it.nd = it.list.head
		return nil
	}
	it.key = nodeKey
	return &it.key
}

func (it *Iterator) Key() *base.InternalKey {
	return &it.key
}

func (it *Iterator) KeyInfo() (offset, keyStart, keyEnd uint32) {
	n := it.list.node(it.nd)
	return n.offset, n.keyStart, n.keyEnd
}

func (it *Iterator) Head() bool {
	return it.nd == it.list.head
}

func (it *Iterator) Tail() bool {
	return it.nd == it.list.tail
}

func (it *Iterator) Valid() bool {
	return it.list != nil && it.nd != it.list.head && it.nd != it.list.tail
}

func (it *Iterator) String() string {
	return "batch"
}

func (it *Iterator) SetBounds(lower, upper []byte) {
	it.lower = lower
	it.upper = upper
}

func (it *Iterator) seekForBaseSplice(key []byte, abbreviatedKey uint64) (prev, next uint32) {
	prev = it.list.head
	for level := it.list.height - 1; ; level-- {
		prev, next = it.list.findSpliceForLevel(key, abbreviatedKey, level, prev)
		if level == 0 {
			break
		}
	}

	return
}
