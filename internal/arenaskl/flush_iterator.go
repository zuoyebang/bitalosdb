// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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

import "github.com/zuoyebang/bitalosdb/internal/base"

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

func (it *flushIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	panic("bitalosdb: SeekPrefixGE unimplemented")
}

func (it *flushIterator) SeekLT(key []byte) (*base.InternalKey, []byte) {
	panic("bitalosdb: SeekLT unimplemented")
}

func (it *flushIterator) First() (*base.InternalKey, []byte) {
	key, val := it.Iterator.First()
	if key == nil {
		return nil, nil
	}
	*it.bytesIterated += uint64(it.nd.allocSize)
	return key, val
}

func (it *flushIterator) Next() (*base.InternalKey, []byte) {
	it.nd = it.list.getSkipNext(it.nd)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	*it.bytesIterated += uint64(it.nd.allocSize)
	return &it.key, it.value()
}

func (it *flushIterator) Prev() (*base.InternalKey, []byte) {
	panic("bitalosdb: Prev unimplemented")
}
