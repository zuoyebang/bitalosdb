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
	"sort"
	"sync"

	"github.com/zuoyebang/bitalosdb/internal/cache/lfucache/internal/base"
)

var _ base.InternalIterator = (*arrayTableFlushIterator)(nil)
var _ base.InternalIterator = (*arrayTableIterator)(nil)

type arrayTableIterator struct {
	at        *arrayTable
	indexPos  int
	iterKey   *internalKey
	iterValue []byte
}

var iterPool = sync.Pool{
	New: func() interface{} {
		return &arrayTableIterator{}
	},
}

func (ai *arrayTableIterator) findItem() (*internalKey, []byte) {
	key, value := ai.at.getKV(ai.indexPos)
	if key == nil {
		return nil, nil
	}

	*(ai.iterKey) = base.MakeMinKey(key)
	ai.iterValue = value
	return ai.iterKey, ai.iterValue
}

func (ai *arrayTableIterator) First() (*internalKey, []byte) {
	ai.indexPos = 0
	return ai.findItem()
}

func (ai *arrayTableIterator) Next() (*internalKey, []byte) {
	ai.indexPos++
	return ai.findItem()
}

func (ai *arrayTableIterator) SeekGE(key []byte) (*internalKey, []byte) {
	ai.indexPos = sort.Search(ai.at.num, func(i int) bool {
		return bytes.Compare(ai.at.getKey(i), key) != -1
	})

	return ai.findItem()
}

func (ai *arrayTableIterator) Prev() (*internalKey, []byte) {
	panic("arrayTableIterator: Prev unimplemented")
}

func (ai *arrayTableIterator) Last() (*internalKey, []byte) {
	panic("arrayTableIterator: Last unimplemented")
}

func (ai *arrayTableIterator) SeekLT(seek []byte) (*internalKey, []byte) {
	panic("arrayTableIterator: SeekLT unimplemented")
}

func (ai *arrayTableIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *internalKey, value []byte) {
	panic("arrayTableIterator: SeekPrefixGE unimplemented")
}

func (ai *arrayTableIterator) SetBounds(lower, upper []byte) {
}

func (ai *arrayTableIterator) Error() error {
	return nil
}

func (ai *arrayTableIterator) Close() error {
	iterPool.Put(ai)
	return nil
}

func (ai *arrayTableIterator) String() string {
	return "arrayTableIterator"
}

type arrayTableFlushIterator struct {
	arrayTableIterator
	bytesIterated *uint64
}

func (ai *arrayTableFlushIterator) First() (*internalKey, []byte) {
	ai.indexPos = 0
	return ai.findItem()
}

func (ai *arrayTableFlushIterator) Next() (*internalKey, []byte) {
	ai.indexPos++
	return ai.findItem()
}

func (ai *arrayTableFlushIterator) SeekGE(seek []byte) (*internalKey, []byte) {
	panic("arrayTableFlushIterator: SeekGE unimplemented")
}

func (ai *arrayTableFlushIterator) Prev() (*internalKey, []byte) {
	panic("arrayTableFlushIterator: Prev unimplemented")
}

func (ai *arrayTableFlushIterator) SeekLT(seek []byte) (*internalKey, []byte) {
	panic("arrayTableFlushIterator: SeekLT unimplemented")
}

func (ai *arrayTableFlushIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *internalKey, value []byte) {
	panic("arrayTableFlushIterator: SeekPrefixGE unimplemented")
}

func (ai *arrayTableFlushIterator) String() string {
	return "arrayTableFlushIterator"
}
