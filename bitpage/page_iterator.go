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

package bitpage

import (
	"bytes"
	"sync"

	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/iterator"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

type iterPos int8

const (
	iterPosCurForward iterPos = 0
	iterPosNext       iterPos = 1
	iterPosPrev       iterPos = -1
	iterPosCurReverse iterPos = -2
)

type pageIterAlloc struct {
	dbi     PageIterator
	key     InternalKKVKey
	merging iterator.KKVMergingIter
	mLevels [3]iterator.KKVMergingIterLevel
}

var pageIterAllocPool = sync.Pool{
	New: func() interface{} {
		return &pageIterAlloc{}
	},
}

type PageIterator struct {
	opts              iterOptions
	iter              InternalKKVIterator
	closer            func()
	err               error
	key               InternalKKVKey
	keyBuf            []byte
	value             []byte
	iterKey           *InternalKKVKey
	iterValue         []byte
	alloc             *pageIterAlloc
	iterValidityState IterValidityState
	pos               iterPos
}

type IterValidityState int8

const (
	IterExhausted IterValidityState = iota
	IterValid
)

func (i *PageIterator) getKV() (*InternalKKVKey, []byte) {
	if i.iterValidityState == IterValid {
		return &i.key, i.value
	}

	return nil, nil
}

func (i *PageIterator) findNextEntry() {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurForward

	for i.iterKey != nil {
		kind := i.iterKey.Kind()
		switch kind {
		case internalKeyKindSet:
			i.key.Copy(i.iterKey)
			i.value = i.iterValue
			i.iterValidityState = IterValid
			return
		case internalKeyKindDelete, internalKeyKindPrefixDelete:
			i.key.Copy(i.iterKey)
			i.value = nil
			i.iterValidityState = IterValid
			return
		default:
			i.opts.Logger.Errorf("bitpage: PageIterator findNextEntry invalid internal key kind %d", kind)
			i.nextUserKey()
			continue
		}
	}
}

func (i *PageIterator) nextUserKey() {
	if i.iterKey == nil {
		return
	}

	if i.iterValidityState != IterValid {
		i.key.Copy(i.iterKey)
	}

	for {
		i.iterKey, i.iterValue = i.iter.Next()
		if i.iterKey == nil || !kkv.UserKeyEqual(&i.key, i.iterKey) {
			break
		}
	}
}

func (i *PageIterator) findPrevEntry() {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurReverse

	for i.iterKey != nil {
		if i.iterValidityState == IterValid {
			if !kkv.UserKeyEqual(i.iterKey, &i.key) {
				i.pos = iterPosPrev
				if i.err != nil {
					i.iterValidityState = IterExhausted
				}
				return
			}
		}

		i.key.Copy(i.iterKey)
		if i.iterKey.Kind() == internalKeyKindSet {
			i.value = i.iterValue
		} else {
			i.value = nil
		}
		i.iterValidityState = IterValid
		i.iterKey, i.iterValue = i.iter.Prev()
	}

	if i.iterValidityState == IterValid {
		i.pos = iterPosPrev
		if i.err != nil {
			i.iterValidityState = IterExhausted
		}
	}
}

func (i *PageIterator) prevUserKey() {
	if i.iterKey == nil {
		return
	}
	if i.iterValidityState != IterValid {
		i.key.Copy(i.iterKey)
	}
	for {
		i.iterKey, i.iterValue = i.iter.Prev()
		if i.iterKey == nil {
			break
		}
		if !kkv.UserKeyEqual(&i.key, i.iterKey) {
			break
		}
	}
}

func (i *PageIterator) SeekGE(key []byte) (*InternalKKVKey, []byte) {
	i.err = nil
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && bytes.Compare(key, lowerBound) < 0 {
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && bytes.Compare(key, upperBound) > 0 {
		key = upperBound
	}

	i.iterKey, i.iterValue = i.iter.SeekGE(key)
	i.findNextEntry()
	return i.getKV()
}

func (i *PageIterator) SeekLT(key []byte) (*InternalKKVKey, []byte) {
	i.err = nil
	if upperBound := i.opts.GetUpperBound(); upperBound != nil && bytes.Compare(key, upperBound) > 0 {
		key = upperBound
	} else if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && bytes.Compare(key, lowerBound) < 0 {
		key = lowerBound
	}

	i.iterKey, i.iterValue = i.iter.SeekLT(key)
	i.findPrevEntry()
	return i.getKV()
}

func (i *PageIterator) First() (*InternalKKVKey, []byte) {
	i.err = nil
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
	} else {
		i.iterKey, i.iterValue = i.iter.First()
	}
	i.findNextEntry()
	return i.getKV()
}

func (i *PageIterator) Last() (*InternalKKVKey, []byte) {
	i.err = nil
	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
	}
	i.findPrevEntry()
	return i.getKV()
}

func (i *PageIterator) Next() (*InternalKKVKey, []byte) {
	if i.err != nil {
		return i.getKV()
	}
	switch i.pos {
	case iterPosCurForward:
		i.nextUserKey()
	case iterPosCurReverse:
		if i.iterKey != nil {
			i.err = errors.New("switching from reverse to forward but iter is not at prev")
			i.iterValidityState = IterExhausted
			break
		}
		if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
			i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
		} else {
			i.iterKey, i.iterValue = i.iter.First()
		}
	case iterPosPrev:
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
			} else {
				i.iterKey, i.iterValue = i.iter.First()
			}
		} else {
			i.nextUserKey()
		}
		i.nextUserKey()
	}
	i.findNextEntry()
	return i.getKV()
}

func (i *PageIterator) Prev() (*InternalKKVKey, []byte) {
	if i.err != nil {
		return i.getKV()
	}
	switch i.pos {
	case iterPosCurReverse:
		i.prevUserKey()
	case iterPosCurForward:
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			if upperBound := i.opts.GetUpperBound(); upperBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
			} else {
				i.iterKey, i.iterValue = i.iter.Last()
			}
		} else {
			i.prevUserKey()
		}
	}

	i.findPrevEntry()
	return i.getKV()
}

func (i *PageIterator) String() string {
	return "PageIterator"
}

func (i *PageIterator) Error() error {
	err := i.err
	if i.iter != nil {
		err = utils.FirstError(i.err, i.iter.Error())
	}
	return err
}

func (i *PageIterator) Close() error {
	if i.iter != nil {
		i.err = utils.FirstError(i.err, i.iter.Close())
	}
	err := i.err

	if i.closer != nil {
		i.closer()
		i.closer = nil
	}

	if alloc := i.alloc; alloc != nil {
		*i = PageIterator{}
		pageIterAllocPool.Put(alloc)
	}
	return err
}

func (i *PageIterator) SetBounds(lower, upper []byte) {
	i.iterKey = nil
	i.iterValue = nil

	switch i.pos {
	case iterPosCurForward:
		i.pos = iterPosCurForward
	case iterPosCurReverse, iterPosPrev:
		i.pos = iterPosCurReverse
	}
	i.iterValidityState = IterExhausted

	i.opts.LowerBound = lower
	i.opts.UpperBound = upper
	i.iter.SetBounds(lower, upper)
}
