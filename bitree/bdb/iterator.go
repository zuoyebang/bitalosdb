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

package bdb

import (
	"github.com/zuoyebang/bitalosdb/internal/base"
)

type BdbIterator struct {
	iter      *Cursor
	iterKey   []byte
	iterValue []byte
	ikey      *base.InternalKey
	value     []byte
	rTx       *ReadTx
}

func (i *BdbIterator) saveKV() {
	if i.ikey == nil {
		i.ikey = new(base.InternalKey)
	}
	*(i.ikey) = base.MakeInternalSetKey(i.iterKey)
	i.value = i.iterValue
}

func (i *BdbIterator) First() (*base.InternalKey, []byte) {
	i.iterKey, i.iterValue = i.iter.First()
	if i.iterKey == nil {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BdbIterator) Last() (*base.InternalKey, []byte) {
	i.iterKey, i.iterValue = i.iter.Last()
	if i.iterKey == nil {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BdbIterator) Next() (*base.InternalKey, []byte) {
	i.iterKey, i.iterValue = i.iter.Next()
	if i.iterKey == nil {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BdbIterator) Prev() (*base.InternalKey, []byte) {
	i.iterKey, i.iterValue = i.iter.Prev()
	if i.iterKey == nil {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BdbIterator) SeekGE(key []byte) (*base.InternalKey, []byte) {
	i.iterKey, i.iterValue = i.iter.Seek(key)
	if i.iterKey == nil {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BdbIterator) SeekLT(key []byte) (*base.InternalKey, []byte) {
	return nil, nil
}

func (i *BdbIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *base.InternalKey, value []byte) {
	return nil, nil
}

func (i *BdbIterator) SetBounds(lower, upper []byte) {
}

func (i *BdbIterator) Error() error {
	return nil
}

func (i *BdbIterator) Close() error {
	return i.rTx.Unref(true)
}

func (i *BdbIterator) Ikey() *base.InternalKey {
	return i.ikey
}

func (i *BdbIterator) Value() []byte {
	return i.value
}

func (i *BdbIterator) String() string {
	return "BdbIterator"
}
