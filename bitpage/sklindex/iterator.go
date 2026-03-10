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

package sklindex

import (
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
	start int32
	end   int32
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
	it.list.Unref()
	it.list = nil
	it.nd = nil
	it.start = -1
	it.end = -1
	it.lower = nil
	it.upper = nil
	iterPool.Put(it)
	return nil
}

func (it *Iterator) String() string {
	return "sklindexIterator"
}

func (it *Iterator) Error() error {
	return nil
}

func (it *Iterator) getKKVKey(key base.InternalKey) *InternalKKVKey {
	kkvKey := kkv.MakeInternalKey(key)
	return &kkvKey
}

func (it *Iterator) SeekGE(key []byte) (*InternalKKVKey, []byte) {
	var arriOffset int32
	_, it.nd, arriOffset = it.list.seekForIterator(key)
	if it.nd == it.list.tail && arriOffset == -1 {
		it.start = -1
		return nil, nil
	}

	arrin := int32(it.list.arrayIndexLen)
	it.start = arriOffset
	it.end = arrin

	var offset uint32
	if it.nd != it.list.tail {
		ndKey := it.list.fetchIKeyCallback(it.nd.itemOffset)
		if arriOffset != -1 && arriOffset < arrin {
			arriKey := it.list.fetchIKeyCallback(it.list.arrayIndex[arriOffset])
			if it.list.cmp(ndKey.UserKey, arriKey.UserKey) <= 0 {
				offset = it.nd.itemOffset
				it.nd = it.list.getNext(it.nd, 0)
				it.key = ndKey
			} else {
				offset = it.list.arrayIndex[arriOffset]
				it.key = arriKey
				it.start++
			}
		} else {
			offset = it.nd.itemOffset
			it.nd = it.list.getNext(it.nd, 0)
			it.key = ndKey
		}
	} else {
		if arriOffset < arrin {
			offset = it.list.arrayIndex[arriOffset]
			it.key = it.list.fetchIKeyCallback(offset)
			it.start++
		} else {
			it.start = -1
			return nil, nil
		}
	}

	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil, nil
	}

	return it.getKKVKey(it.key), it.list.fetchValueCallback(offset)
}

func (it *Iterator) SeekLT(key []byte) (*InternalKKVKey, []byte) {
	var arriOffset int32
	it.nd, _, arriOffset = it.list.seekForIterator(key)
	if it.nd == it.list.head && arriOffset == -1 {
		it.end = -1
		return nil, nil
	}

	it.start = 0
	it.end = arriOffset - 1

	var offset uint32
	if it.nd != it.list.head {
		ndKey := it.list.fetchIKeyCallback(it.nd.itemOffset)
		if it.end >= 0 {
			arriKey := it.list.fetchIKeyCallback(it.list.arrayIndex[it.end])
			if it.list.cmp(ndKey.UserKey, arriKey.UserKey) > 0 {
				offset = it.nd.itemOffset
				it.nd = it.list.getPrev(it.nd, 0)
				it.key = ndKey
			} else {
				offset = it.list.arrayIndex[it.end]
				it.key = arriKey
				it.end--
			}
		} else {
			offset = it.nd.itemOffset
			it.nd = it.list.getPrev(it.nd, 0)
			it.key = ndKey
		}
	} else {
		if it.end >= 0 {
			offset = it.list.arrayIndex[it.end]
			it.key = it.list.fetchIKeyCallback(offset)
			it.end--
		} else {
			it.end = -1
			return nil, nil
		}
	}

	if it.lower != nil && it.list.cmp(it.lower, it.key.UserKey) > 0 {
		it.nd = it.list.head
		return nil, nil
	}

	return it.getKKVKey(it.key), it.list.fetchValueCallback(offset)
}

func (it *Iterator) First() (*InternalKKVKey, []byte) {
	offset := it.FirstOffset()
	if offset == 0 {
		return nil, nil
	}
	it.key = it.list.fetchIKeyCallback(offset)
	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil, nil
	}
	return it.getKKVKey(it.key), it.list.fetchValueCallback(offset)
}

func (it *Iterator) Last() (*InternalKKVKey, []byte) {
	offset := it.LastOffset()
	if offset == 0 {
		return nil, nil
	}
	it.key = it.list.fetchIKeyCallback(offset)
	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.head
		return nil, nil
	}
	return it.getKKVKey(it.key), it.list.fetchValueCallback(offset)
}

func (it *Iterator) Next() (*InternalKKVKey, []byte) {
	offset := it.NextOffset()
	if offset == 0 {
		return nil, nil
	}
	it.key = it.list.fetchIKeyCallback(offset)
	if it.lower != nil && it.list.cmp(it.lower, it.key.UserKey) > 0 {
		it.nd = it.list.tail
		return nil, nil
	}
	return it.getKKVKey(it.key), it.list.fetchValueCallback(offset)
}

func (it *Iterator) Prev() (*InternalKKVKey, []byte) {
	offset := it.PrevOffset()
	if offset == 0 {
		return nil, nil
	}
	it.key = it.list.fetchIKeyCallback(offset)
	if it.lower != nil && it.list.cmp(it.lower, it.key.UserKey) > 0 {
		it.nd = it.list.head
		return nil, nil
	}
	return it.getKKVKey(it.key), it.list.fetchValueCallback(offset)
}

func (it *Iterator) FirstOffset() uint32 {
	var offset uint32
	arrin := int32(it.list.arrayIndexLen)
	it.end = arrin

	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail {
		if arrin > 0 {
			it.start = 1
			offset = it.list.arrayIndex[0]
		}
		return offset
	}

	if it.nd.arriOffset > 0 {
		it.start = 1
		offset = it.list.arrayIndex[0]
	} else if it.nd.arriOffset == 0 {
		it.start = 0
		offset = it.nd.itemOffset
		it.nd = it.list.getNext(it.nd, 0)
	} else {
		it.start = -1
		offset = it.nd.itemOffset
		it.nd = it.list.getNext(it.nd, 0)
	}

	return offset
}

func (it *Iterator) NextOffset() uint32 {
	var offset uint32

	if it.start < 0 {
		if it.nd == it.list.tail {
			return 0
		}
		offset = it.nd.itemOffset
		it.nd = it.list.getNext(it.nd, 0)
		return offset
	}

	if it.start <= it.end {
		if it.nd.arriOffset <= it.start {
			if it.nd == it.list.tail {
				if it.start == it.end {
					return 0
				}
				offset = it.list.arrayIndex[it.start]
				it.start++
				return offset
			}
			offset = it.nd.itemOffset
			it.nd = it.list.getNext(it.nd, 0)
			return offset
		}

		offset = it.list.arrayIndex[it.start]
		it.start++
		return offset
	}

	return 0
}

func (it *Iterator) LastOffset() uint32 {
	var offset uint32
	arrin := int32(it.list.arrayIndexLen)
	it.start = 0

	it.nd = it.list.getPrev(it.list.tail, 0)
	if it.nd == it.list.head {
		if arrin > 0 {
			it.end = arrin - 1
			offset = it.list.arrayIndex[it.end]
			it.end--
			return offset
		}
		return 0
	}

	if it.nd.arriOffset >= arrin {
		it.end = arrin - 1
		offset = it.nd.itemOffset
		it.nd = it.list.getPrev(it.nd, 0)
	} else if it.nd.arriOffset > 0 {
		it.end = arrin - 1
		offset = it.list.arrayIndex[it.end]
		it.end--
	} else {
		it.end = -1
		offset = it.nd.itemOffset
		it.nd = it.list.getPrev(it.nd, 0)
	}

	return offset
}

func (it *Iterator) PrevOffset() uint32 {
	var offset uint32

	if it.end < 0 {
		if it.nd == it.list.head {
			return 0
		}
		offset = it.nd.itemOffset
		it.nd = it.list.getPrev(it.nd, 0)
		return offset
	}

	if it.start <= it.end {
		if it.nd.arriOffset > it.end {
			offset = it.nd.itemOffset
			it.nd = it.list.getPrev(it.nd, 0)
			return offset
		}

		offset = it.list.arrayIndex[it.end]
		it.end--
		return offset
	}

	return 0
}

func (it *Iterator) ArrayIndexPos() int32 {
	return it.nd.arriOffset
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
