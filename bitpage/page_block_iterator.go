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

package bitpage

import (
	"bytes"
	"sync"

	"github.com/zuoyebang/bitalosdb/internal/base"
)

var _ base.InternalIterator = (*pageBlockIterator)(nil)

type pageBlockIterator struct {
	pb          *pageBlock
	intIndexPos int
	iterKey     internalKey
	keyBuf      []byte
	iterValue   []byte
	sharedCache *sharedInfo
}

var pbIterPool = sync.Pool{
	New: func() interface{} {
		return &pageBlockIterator{}
	},
}

func (pi *pageBlockIterator) findItem() (*internalKey, []byte) {
	sharedKey1, sharedKey2, value := pi.pb.getSharedKV(pi.intIndexPos, pi.sharedCache)
	if sharedKey1 == nil {
		return nil, nil
	}

	var key []byte
	if sharedKey2 == nil {
		key = sharedKey1
	} else {
		pi.keyBuf = append(pi.keyBuf[:0], sharedKey1...)
		key = append(pi.keyBuf, sharedKey2...)
	}

	pi.iterKey = base.MakeInternalSetKey(key)
	pi.iterValue = value
	return &pi.iterKey, pi.iterValue
}

func (pi *pageBlockIterator) First() (*internalKey, []byte) {
	pi.intIndexPos = 0
	return pi.findItem()
}

func (pi *pageBlockIterator) Next() (*internalKey, []byte) {
	pi.intIndexPos++
	return pi.findItem()
}

func (pi *pageBlockIterator) Prev() (*internalKey, []byte) {
	pi.intIndexPos--
	return pi.findItem()
}

func (pi *pageBlockIterator) Last() (*internalKey, []byte) {
	pi.intIndexPos = pi.pb.num - 1
	return pi.findItem()
}

func (pi *pageBlockIterator) SeekGE(key []byte) (*internalKey, []byte) {
	pi.intIndexPos = pi.pb.findKeyByIntIndex(key)
	return pi.findItem()
}

func (pi *pageBlockIterator) SeekLT(key []byte) (*internalKey, []byte) {
	pi.intIndexPos = pi.pb.findKeyByIntIndex(key)
	poskey, _ := pi.findItem()
	if poskey != nil {
		return pi.Prev()
	}

	lastKey, lastValue := pi.Last()
	if lastKey != nil && bytes.Compare(lastKey.UserKey, key) < 0 {
		return lastKey, lastValue
	}

	return nil, nil
}

func (pi *pageBlockIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *internalKey, value []byte) {
	return pi.SeekGE(key)
}

func (pi *pageBlockIterator) SetBounds(lower, upper []byte) {
}

func (pi *pageBlockIterator) Error() error {
	return nil
}

func (pi *pageBlockIterator) Close() error {
	pi.pb = nil
	pi.intIndexPos = 0
	pi.iterValue = nil
	if pi.keyBuf != nil {
		pi.keyBuf = pi.keyBuf[:0]
	}
	if pi.sharedCache != nil {
		pi.sharedCache.idx = -1
		pi.sharedCache.key = nil
	}
	pbIterPool.Put(pi)
	return nil
}

func (pi *pageBlockIterator) String() string {
	return "pageBlockIterator"
}
