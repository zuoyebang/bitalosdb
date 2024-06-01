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
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

type iterPos int8

const (
	iterPosCurForward iterPos = 0
	iterPosNext       iterPos = 1
	iterPosPrev       iterPos = -1
	iterPosCurReverse iterPos = -2
)

const maxKeyBufCacheSize = 4 << 10

type pageIterAlloc struct {
	dbi                 PageIterator
	key                 internalKey
	keyBuf              []byte
	prefixOrFullSeekKey []byte
	merging             mergingIter
	mlevels             [3]mergingIterLevel
}

var pageIterAllocPool = sync.Pool{
	New: func() interface{} {
		return &pageIterAlloc{}
	},
}

type PageIterator struct {
	opts                iterOptions
	cmp                 base.Compare
	equal               base.Equal
	iter                internalIterator
	readState           *readState
	readStateCloser     func()
	err                 error
	key                 *internalKey
	keyBuf              []byte
	value               []byte
	iterKey             *internalKey
	iterValue           []byte
	alloc               *pageIterAlloc
	prefixOrFullSeekKey []byte
	iterValidityState   IterValidityState
	pos                 iterPos
	lastPositioningOp   lastPositioningOpKind
}

type lastPositioningOpKind int8

const (
	unknownLastPositionOp lastPositioningOpKind = iota
	seekGELastPositioningOp
	seekLTLastPositioningOp
)

type IterValidityState int8

const (
	IterExhausted IterValidityState = iota
	IterValid
)

func (i *PageIterator) getKV() (*base.InternalKey, []byte) {
	if i.iterValidityState == IterValid {
		return i.key, i.value
	}

	return nil, nil
}

func (i *PageIterator) findNextEntry() {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurForward

	for i.iterKey != nil {
		key := *i.iterKey

		switch key.Kind() {
		case internalKeyKindSet:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			*i.key = base.MakeInternalKey2(i.keyBuf, key.Trailer)
			i.value = i.iterValue
			i.iterValidityState = IterValid
			return

		case internalKeyKindDelete, internalKeyKindPrefixDelete:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			*i.key = base.MakeInternalKey2(i.keyBuf, key.Trailer)
			i.value = nil
			i.iterValidityState = IterValid
			return

		default:
			i.err = errors.Errorf("bitpage: invalid internal key kind %s", key.Kind())
			i.iterValidityState = IterExhausted
			return
		}
	}
}

func (i *PageIterator) nextUserKey() {
	if i.iterKey == nil {
		return
	}

	if i.iterValidityState != IterValid {
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		*i.key = base.MakeInternalKey2(i.keyBuf, i.iterKey.Trailer)
	}

	for {
		i.iterKey, i.iterValue = i.iter.Next()
		if i.iterKey == nil || !i.equal(i.key.UserKey, i.iterKey.UserKey) {
			break
		}
	}
}

func (i *PageIterator) findPrevEntry() {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurReverse

	for i.iterKey != nil {
		key := *i.iterKey

		if i.iterValidityState == IterValid {
			if !i.equal(key.UserKey, i.key.UserKey) {
				i.pos = iterPosPrev
				if i.err != nil {
					i.iterValidityState = IterExhausted
				}
				return
			}
		}

		switch key.Kind() {
		case internalKeyKindSet:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			*i.key = base.MakeInternalKey2(i.keyBuf, key.Trailer)
			i.value = i.iterValue
			i.iterValidityState = IterValid
			i.iterKey, i.iterValue = i.iter.Prev()
		case internalKeyKindDelete, internalKeyKindPrefixDelete:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			*i.key = base.MakeInternalKey2(i.keyBuf, key.Trailer)
			i.value = nil
			i.iterValidityState = IterValid
			i.iterKey, i.iterValue = i.iter.Prev()
		default:
			i.err = errors.Errorf("bitpage: invalid internal key kind %s", key.Kind())
			i.iterValidityState = IterExhausted
			return
		}
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
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		*i.key = base.MakeInternalKey2(i.keyBuf, i.iterKey.Trailer)
	}
	for {
		i.iterKey, i.iterValue = i.iter.Prev()
		if i.iterKey == nil {
			break
		}
		if !i.equal(i.key.UserKey, i.iterKey.UserKey) {
			break
		}
	}
}

func (i *PageIterator) SeekGE(key []byte) (*internalKey, []byte) {
	lastPositioningOp := i.lastPositioningOp

	i.lastPositioningOp = unknownLastPositionOp
	i.err = nil
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	}

	if lastPositioningOp == seekGELastPositioningOp {
		if i.cmp(i.prefixOrFullSeekKey, key) <= 0 {
			if i.iterValidityState == IterExhausted || (i.iterValidityState == IterValid && i.cmp(key, i.key.UserKey) <= 0) {
				return i.getKV()
			}
		}
	}

	i.iterKey, i.iterValue = i.iter.SeekGE(key)
	i.findNextEntry()
	if i.Error() == nil {
		i.prefixOrFullSeekKey = append(i.prefixOrFullSeekKey[:0], key...)
		i.lastPositioningOp = seekGELastPositioningOp
	}
	return i.getKV()
}

func (i *PageIterator) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*internalKey, []byte) {
	return nil, nil
}

func (i *PageIterator) SeekLT(key []byte) (*internalKey, []byte) {
	lastPositioningOp := i.lastPositioningOp
	i.lastPositioningOp = unknownLastPositionOp
	i.err = nil
	if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	} else if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	}

	if lastPositioningOp == seekLTLastPositioningOp {
		if i.cmp(key, i.prefixOrFullSeekKey) <= 0 {
			if i.iterValidityState == IterExhausted || (i.iterValidityState == IterValid && i.cmp(i.key.UserKey, key) < 0) {
				return i.getKV()
			}
		}
	}

	i.iterKey, i.iterValue = i.iter.SeekLT(key)
	i.findPrevEntry()
	if i.Error() == nil {
		i.prefixOrFullSeekKey = append(i.prefixOrFullSeekKey[:0], key...)
		i.lastPositioningOp = seekLTLastPositioningOp
	}
	return i.getKV()
}

func (i *PageIterator) First() (*internalKey, []byte) {
	i.err = nil
	i.lastPositioningOp = unknownLastPositionOp
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
	} else {
		i.iterKey, i.iterValue = i.iter.First()
	}
	i.findNextEntry()
	return i.getKV()
}

func (i *PageIterator) Last() (*internalKey, []byte) {
	i.err = nil
	i.lastPositioningOp = unknownLastPositionOp
	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
	}
	i.findPrevEntry()
	return i.getKV()
}

func (i *PageIterator) Next() (*internalKey, []byte) {
	if i.err != nil {
		return i.getKV()
	}
	i.lastPositioningOp = unknownLastPositionOp
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

func (i *PageIterator) Prev() (*internalKey, []byte) {
	if i.err != nil {
		return i.getKV()
	}
	i.lastPositioningOp = unknownLastPositionOp
	switch i.pos {
	case iterPosCurForward:
	case iterPosCurReverse:
		i.prevUserKey()
	case iterPosPrev:
	}
	if i.pos == iterPosCurForward {
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

	if i.readStateCloser != nil {
		i.readStateCloser()
		i.readStateCloser = nil
	}

	if alloc := i.alloc; alloc != nil {
		if cap(i.keyBuf) >= maxKeyBufCacheSize {
			alloc.keyBuf = nil
		} else {
			alloc.keyBuf = i.keyBuf
		}
		if cap(i.prefixOrFullSeekKey) >= maxKeyBufCacheSize {
			alloc.prefixOrFullSeekKey = nil
		} else {
			alloc.prefixOrFullSeekKey = i.prefixOrFullSeekKey
		}
		*i = PageIterator{}
		pageIterAllocPool.Put(alloc)
	}
	return err
}

func (i *PageIterator) SetBounds(lower, upper []byte) {
	i.lastPositioningOp = unknownLastPositionOp
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
