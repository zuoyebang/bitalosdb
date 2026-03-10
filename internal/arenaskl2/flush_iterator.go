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

package arenaskl2

import (
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
)

type flushIterator struct {
	Iterator
	bytesIterated *uint64
}

var _ base.InternalIterator = (*flushIterator)(nil)

func (it *flushIterator) String() string {
	return "memtable"
}

func (it *flushIterator) SeekGE(key []byte) (*base.InternalKey, []byte) {
	panic("bitalosdb: SeekGE unimplemented")
}

func (it *flushIterator) SeekLT(key []byte) (*base.InternalKey, []byte) {
	panic("bitalosdb: SeekLT unimplemented")
}

func (it *flushIterator) Last() (*base.InternalKey, []byte) {
	panic("bitalosdb: Last unimplemented")
}

func (it *flushIterator) getValue() []byte {
	return it.nd.getValue(it.list.arena, false)
}

func (it *flushIterator) First() (*base.InternalKey, []byte) {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil, nil
	}
	*it.bytesIterated += uint64(it.nd.allocSize)
	return &it.key, it.getValue()
}

func (it *flushIterator) Next() (*base.InternalKey, []byte) {
	it.nd = it.list.getSkipNext(it.nd)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	*it.bytesIterated += uint64(it.nd.allocSize)
	return &it.key, it.getValue()
}

func (it *flushIterator) Prev() (*base.InternalKey, []byte) {
	panic("bitalosdb: Prev unimplemented")
}
