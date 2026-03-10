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
	"github.com/zuoyebang/bitalosdb/v2/bitpage"
	"github.com/zuoyebang/bitalosdb/v2/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

type BitreeIterator struct {
	btree      *Bitree
	opts       *iterOptions
	cmp        base.Compare
	compact    bool
	err        error
	iterKey    *InternalKKVKey
	iterValue  []byte
	key        InternalKKVKey
	value      []byte
	putPools   []func()
	lower      []byte
	upper      []byte
	bdbCursor  *bdb.Cursor
	bdbTx      *bdb.ReadTx
	bpageIter  *bitpage.PageIterator
	bpageIters map[bitpage.PageNum]*bitpage.PageIterator
}

func (i *BitreeIterator) getKV() (*InternalKKVKey, []byte) {
	if i.iterKey == nil {
		return nil, nil
	}

	if i.compact {
		return i.iterKey, i.iterValue
	}

	switch i.iterKey.Kind() {
	case internalKeyKindDelete, internalKeyKindPrefixDelete:
		return i.iterKey, i.iterValue
	default:
		isSeparate, value, iv := base.DecodeInternalValue(i.iterValue)
		if !isSeparate {
			return i.iterKey, value
		}

		if base.CheckValueBithashValid(iv.UserValue) {
			i.value = iv.UserValue
		} else {
			iv.SetKind(base.InternalKeyKindDelete)
			i.value = nil
		}

		i.iterKey.SetTrailer(iv.Header)
		i.key.Copy(i.iterKey)
		return &i.key, i.value
	}
}

func (i *BitreeIterator) setBitpageIter(v []byte) bool {
	pn := bitpage.PageNum(utils.BytesToUint32(v))
	pageIter, ok := i.bpageIters[pn]
	if !ok {
		pageIter = i.btree.newPageIter(pn, i.opts)
		if pageIter == nil {
			return false
		}
		i.bpageIters[pn] = pageIter
	}

	i.bpageIter = pageIter
	return true
}

func (i *BitreeIterator) findBdbFirst() bool {
	bdbKey, bdbValue := i.bdbCursor.First()
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) findBdbLast() bool {
	bdbKey, bdbValue := i.bdbCursor.Last()
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) findBdbNext() bool {
	bdbKey, bdbValue := i.bdbCursor.Next()
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) findBdbPrev() bool {
	bdbKey, bdbValue := i.bdbCursor.Prev()
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) findBdbSeekGE(key []byte) bool {
	bdbKey, bdbValue := i.bdbCursor.Seek(key)
	if bdbKey == nil {
		return false
	}

	return i.setBitpageIter(bdbValue)
}

func (i *BitreeIterator) First() (*InternalKKVKey, []byte) {
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

	if kkv.IsUserKeyGEUpperBound(i.iterKey, i.upper) {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Last() (*InternalKKVKey, []byte) {
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

	if kkv.IsUserKeyLTLowerBound(i.iterKey, i.lower) {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Next() (*InternalKKVKey, []byte) {
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

	if kkv.IsUserKeyGEUpperBound(i.iterKey, i.upper) {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) Prev() (*InternalKKVKey, []byte) {
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

	if kkv.IsUserKeyLTLowerBound(i.iterKey, i.lower) {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) SeekGE(key []byte) (*InternalKKVKey, []byte) {
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

	if kkv.IsUserKeyGEUpperBound(i.iterKey, i.upper) {
		return nil, nil
	}

	return i.getKV()
}

func (i *BitreeIterator) SeekLT(key []byte) (*InternalKKVKey, []byte) {
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

	if kkv.IsUserKeyLTLowerBound(i.iterKey, i.lower) {
		return nil, nil
	}

	return i.getKV()
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

	if err := i.bdbTx.Unref(true); err != nil && i.err == nil {
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
