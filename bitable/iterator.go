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

package bitable

import (
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalostable"
)

type BitableIterator struct {
	btable    *Bitable
	iter      *bitalostable.Iterator
	iterKey   []byte
	iterValue []byte
	ikey      *base.InternalKey
	value     []byte
	putPools  []func()
}

func (i *BitableIterator) saveKV() {
	i.iterKey = i.iter.Key()
	i.iterValue = i.iter.Value()

	iv := base.DecodeInternalValue(i.iterValue)
	if iv.Kind() == base.InternalKeyKindSetBithash {
		if !base.CheckValueValidByKeySetBithash(iv.UserValue) {
			i.ikey = nil
			i.value = nil
			return
		}

		fn := binary.LittleEndian.Uint32(iv.UserValue)
		value, putPool, err := i.btable.bithashGet(i.iterKey, fn)
		if err != nil {
			i.ikey = nil
			i.value = nil
			return
		}

		i.value = value
		iv.SetKind(base.InternalKeyKindSet)
		if putPool != nil {
			i.putPools = append(i.putPools, putPool)
		}
	} else {
		i.value = iv.UserValue
	}

	if i.ikey == nil {
		i.ikey = new(base.InternalKey)
	}

	*(i.ikey) = base.MakeInternalKey2(i.iterKey, iv.Header)
}

func (i *BitableIterator) First() (*base.InternalKey, []byte) {
	if !i.iter.First() {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BitableIterator) Last() (*base.InternalKey, []byte) {
	if !i.iter.Last() {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BitableIterator) Next() (*base.InternalKey, []byte) {
	if !i.iter.Next() {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BitableIterator) Prev() (*base.InternalKey, []byte) {
	if !i.iter.Prev() {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BitableIterator) SeekGE(seek []byte) (*base.InternalKey, []byte) {
	if !i.iter.SeekGE(seek) {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BitableIterator) SeekLT(seek []byte) (*base.InternalKey, []byte) {
	if !i.iter.SeekLT(seek) {
		return nil, nil
	}

	i.saveKV()
	return i.ikey, i.value
}

func (i *BitableIterator) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	return i.SeekGE(key)
}

func (i *BitableIterator) Error() error {
	return i.iter.Error()
}

func (i *BitableIterator) Close() error {
	if len(i.putPools) > 0 {
		for _, f := range i.putPools {
			f()
		}
	}
	return i.iter.Close()
}

func (i *BitableIterator) Valid() bool {
	return i.iter.Valid()
}

func (i *BitableIterator) SetBounds(lower, upper []byte) {
}

func (i *BitableIterator) String() string {
	return "BitableIterator"
}
