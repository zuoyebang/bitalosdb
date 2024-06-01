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

package bitree

import (
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

type BitreeIterator struct {
	btree      *Bitree
	ops        *options.IterOptions
	cmp        base.Compare
	compact    bool
	err        error
	iterKey    *base.InternalKey
	iterValue  []byte
	ikey       *base.InternalKey
	value      []byte
	putPools   []func()
	lower      []byte
	upper      []byte
	bdbIter    *bdb.BdbIterator
	bpageIter  *bitpage.PageIterator
	bpageIters map[bitpage.PageNum]*bitpage.PageIterator
}

func (i *BitreeIterator) getKV() (*base.InternalKey, []byte) {
	if i.iterKey == nil {
		return nil, nil
	}

	if i.compact {
		return i.iterKey, i.iterValue
	}

	switch i.iterKey.Kind() {
	case base.InternalKeyKindDelete, base.InternalKeyKindPrefixDelete:
		return i.iterKey, i.iterValue
	}

	iv := base.DecodeInternalValue(i.iterValue)
	if iv.Kind() == base.InternalKeyKindSetBithash {
		if !base.CheckValueValidByKeySetBithash(iv.UserValue) {
			return nil, nil
		}
		fn := binary.LittleEndian.Uint32(iv.UserValue)
		value, putPool, err := i.btree.bithashGet(i.iterKey.UserKey, fn)
		if err != nil {
			return nil, nil
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
	*(i.ikey) = base.MakeInternalKey2(i.iterKey.UserKey, iv.Header)

	return i.ikey, i.value
}

func (i *BitreeIterator) setBitpageIter(v []byte) bool {
	pn := bitpage.PageNum(utils.BytesToUint32(v))
	pageIter, ok := i.bpageIters[pn]
	if !ok {
		pageIter = i.btree.newPageIter(pn, i.ops)
		if pageIter == nil {
			return false
		}
		i.bpageIters[pn] = pageIter
	}

	i.bpageIter = pageIter
	return true
}

func (i *BitreeIterator) findBdbFirst() bool {
	bdbKey, bdbValue := i.bdbIter.First()
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) findBdbLast() bool {
	bdbKey, bdbValue := i.bdbIter.Last()
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) findBdbNext() bool {
	bdbKey, bdbValue := i.bdbIter.Next()
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) findBdbPrev() bool {
	bdbKey, bdbValue := i.bdbIter.Prev()
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) findBdbSeekGE(key []byte) bool {
	bdbKey, bdbValue := i.bdbIter.SeekGE(key)
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) First() (*base.InternalKey, []byte) {
	if !i.findBdbFirst() {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.bpageIter.First()
	for i.iterKey == nil {
		if !i.findBdbNext() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.bpageIter.First()
	}

	if i.upper != nil && i.cmp(i.upper, i.iterKey.UserKey) <= 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Last() (*base.InternalKey, []byte) {
	if !i.findBdbLast() {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.bpageIter.Last()
	for i.iterKey == nil {
		if !i.findBdbPrev() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.bpageIter.Last()
	}

	if i.lower != nil && i.cmp(i.lower, i.iterKey.UserKey) > 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Next() (*base.InternalKey, []byte) {
	if i.iterKey == nil {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.bpageIter.Next()
	for i.iterKey == nil {
		if !i.findBdbNext() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.bpageIter.First()
	}

	if i.upper != nil && i.cmp(i.upper, i.iterKey.UserKey) <= 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Prev() (*base.InternalKey, []byte) {
	if i.iterKey == nil {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.bpageIter.Prev()
	for i.iterKey == nil {
		if !i.findBdbPrev() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.bpageIter.Last()
	}

	if i.lower != nil && i.cmp(i.lower, i.iterKey.UserKey) > 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) SeekGE(key []byte) (*base.InternalKey, []byte) {
	if !i.findBdbSeekGE(key) {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.bpageIter.SeekGE(key)
	for i.iterKey == nil {
		if !i.findBdbNext() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.bpageIter.SeekGE(key)
	}

	if i.upper != nil && i.cmp(i.upper, i.iterKey.UserKey) <= 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) SeekLT(key []byte) (*base.InternalKey, []byte) {
	if !i.findBdbSeekGE(key) {
		return nil, nil
	}

	i.iterKey, i.iterValue = i.bpageIter.SeekLT(key)
	for i.iterKey == nil {
		if !i.findBdbPrev() {
			return nil, nil
		}
		i.iterKey, i.iterValue = i.bpageIter.SeekLT(key)
	}

	if i.lower != nil && i.cmp(i.lower, i.iterKey.UserKey) > 0 {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	return i.SeekGE(key)
}

func (i *BitreeIterator) Close() error {
	if len(i.putPools) > 0 {
		for _, f := range i.putPools {
			f()
		}
	}

	for _, pageIter := range i.bpageIters {
		if err := pageIter.Close(); err != nil && i.err == nil {
			i.err = err
		}
	}

	if err := i.bdbIter.Close(); err != nil && i.err == nil {
		i.err = err
	}

	return i.err
}

func (i *BitreeIterator) Error() error {
	return nil
}

func (i *BitreeIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *BitreeIterator) SetCompact() {
	i.compact = true
}

func (i *BitreeIterator) String() string {
	return "BitreeIterator"
}
