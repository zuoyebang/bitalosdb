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

package bitalosdb

import (
	"sync"

	"github.com/zuoyebang/bitalosdb/v2/bitree"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/iterator"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

func (d *DB) NewKKVIterator(o *IterOptions) *KKVIterator {
	slotId := uint16(o.SlotId)
	ms := d.getMemShard(slotId)
	rs := ms.loadReadState()
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = KKVIterator{
		d:         d,
		alloc:     buf,
		cmp:       d.cmp,
		equal:     d.equal,
		iter:      &buf.merging,
		keyBuf:    buf.keyBuf,
		readState: rs,
		memShard:  ms,
		btree:     nil,
		closers:   buf.closers,
		closerNum: 0,
	}
	dbi.opts = *o
	dbi.opts.Logger = d.opts.Logger

	mlevels := buf.mlevels[:0]
	numMergingLevels := len(rs.memtables) + 1
	if numMergingLevels > cap(mlevels) {
		mlevels = make([]iterator.KKVMergingIterLevel, 0, numMergingLevels)
	}

	for i := len(rs.memtables) - 1; i >= 0; i-- {
		memIter := rs.memtables[i].newIter(&dbi.opts)
		mlevels = append(mlevels, iterator.NewKKVMergingIterLevel(memIter))
	}

	btree := d.getBitreeRead(slotId)
	if btree != nil {
		kkvIter := btree.NewKKVIter(o)
		mlevels = append(mlevels, iterator.NewKKVMergingIterLevel(kkvIter))
		dbi.btree = btree
	}

	buf.merging.Init(&dbi.opts, mlevels...)
	return dbi
}

type iterPos int8

const (
	iterPosCurForward       iterPos = 0
	iterPosNext             iterPos = 1
	iterPosPrev             iterPos = -1
	iterPosCurReverse       iterPos = -2
	iterPosCurForwardPaused iterPos = 2
	iterPosCurReversePaused iterPos = -3
)

const maxKeyBufCacheSize = 4 << 10
const maxCloserCacheSize = 100

type IteratorStatsKind int8

const (
	InterfaceCall IteratorStatsKind = iota
	InternalIterCall
	NumStatsKind
)

type IteratorStats struct {
	ForwardSeekCount [NumStatsKind]int
	ReverseSeekCount [NumStatsKind]int
	ForwardStepCount [NumStatsKind]int
	ReverseStepCount [NumStatsKind]int
}

type iterAlloc struct {
	dbi     KKVIterator
	keyBuf  []byte
	merging iterator.KKVMergingIter
	mlevels [3]iterator.KKVMergingIterLevel
	closers []func()
}

var iterAllocPool = sync.Pool{
	New: func() interface{} {
		return &iterAlloc{}
	},
}

type KKVIterator struct {
	d                 *DB
	opts              IterOptions
	cmp               Compare
	equal             Equal
	iter              InternalKKVIterator
	err               error
	key               InternalKKVKey
	keyBuf            []byte
	value             []byte
	valueBuf          []byte
	iterKey           *InternalKKVKey
	iterValue         []byte
	alloc             *iterAlloc
	stats             IteratorStats
	iterValidityState IterValidityState
	pos               iterPos
	readState         *memReadState
	memShard          *memTableShard
	btree             *bitree.Bitree
	closers           []func()
	closerNum         int
}

type IterValidityState int8

const (
	IterExhausted IterValidityState = iota
	IterValid
)

func (i *KKVIterator) getBithashValue() ([]byte, error) {
	value, valueCloser, err := i.btree.BithashGetEncode(i.getIterKey(), i.iterValue)
	if err != nil {
		return nil, err
	}

	if valueCloser != nil {
		i.closers = append(i.closers, valueCloser)
		i.closerNum++
	}

	return value, nil
}

func (i *KKVIterator) findNextEntry() {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurForward

	for i.iterKey != nil {
		key := *i.iterKey

		switch key.Kind() {
		case InternalKeyKindDelete, InternalKeyKindPrefixDelete:
			i.nextUserKey()
			continue
		case InternalKeyKindSet:
			i.key.Copy(i.iterKey)
			i.value = i.iterValue
			i.iterValidityState = IterValid
			return
		case InternalKeyKindSetBithash:
			value, err := i.getBithashValue()
			if err != nil {
				i.nextUserKey()
				continue
			}
			i.key.Copy(i.iterKey)
			i.value = value
			i.iterValidityState = IterValid
			return
		default:
			i.opts.Logger.Errorf("bitalosdb: KKVIterator findNextEntry invalid internal key kind %d", key.Kind())
			i.nextUserKey()
			continue
		}
	}
}

func (i *KKVIterator) nextUserKey() {
	if i.iterKey == nil {
		return
	}
	done := i.iterKey.SeqNum() == 0
	if i.iterValidityState != IterValid {
		i.key.Copy(i.iterKey)
	}
	for {
		i.iterKey, i.iterValue = i.iter.Next()
		i.stats.ForwardStepCount[InternalIterCall]++
		if done || i.iterKey == nil {
			break
		}
		if !kkv.UserKeyEqual(&i.key, i.iterKey) {
			break
		}
		done = i.iterKey.SeqNum() == 0
	}
}

func (i *KKVIterator) findPrevEntry() {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurReverse

	for i.iterKey != nil {
		key := *i.iterKey

		if i.iterValidityState == IterValid {
			if !kkv.UserKeyEqual(&i.key, i.iterKey) {
				i.pos = iterPosPrev
				if i.err != nil {
					i.iterValidityState = IterExhausted
				}
				return
			}
		}

		switch key.Kind() {
		case InternalKeyKindDelete, InternalKeyKindPrefixDelete:
			i.iterValidityState = IterExhausted
		case InternalKeyKindSet:
			i.key.Copy(i.iterKey)
			i.value = i.iterValue
			i.iterValidityState = IterValid
		case InternalKeyKindSetBithash:
			value, err := i.getBithashValue()
			if err == nil {
				i.key.Copy(i.iterKey)
				i.value = value
				i.iterValidityState = IterValid
			} else {
				i.iterValidityState = IterExhausted
			}
		default:
			i.iterValidityState = IterExhausted
			i.opts.Logger.Errorf("bitalosdb: KKVIterator findPrevEntry invalid internal key kind %d", key.Kind())
		}

		i.iterKey, i.iterValue = i.iter.Prev()
		i.stats.ReverseStepCount[InternalIterCall]++
	}

	if i.iterValidityState == IterValid {
		i.pos = iterPosPrev
		if i.err != nil {
			i.iterValidityState = IterExhausted
		}
	}
}

func (i *KKVIterator) prevUserKey() {
	if i.iterKey == nil {
		return
	}
	if i.iterValidityState != IterValid {
		i.key.Copy(i.iterKey)
	}
	for {
		i.iterKey, i.iterValue = i.iter.Prev()
		i.stats.ReverseStepCount[InternalIterCall]++
		if i.iterKey == nil {
			break
		}
		if !kkv.UserKeyEqual(&i.key, i.iterKey) {
			break
		}
	}
}

func (i *KKVIterator) SeekGE(key []byte) bool {
	return i.SeekGEWithLimit(key) == IterValid
}

func (i *KKVIterator) SeekGEWithLimit(key []byte) IterValidityState {
	i.err = nil
	i.stats.ForwardSeekCount[InterfaceCall]++
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	}
	i.iterKey, i.iterValue = i.iter.SeekGE(key)
	i.stats.ForwardSeekCount[InternalIterCall]++
	i.findNextEntry()
	return i.iterValidityState
}

func (i *KKVIterator) SeekLT(key []byte) bool {
	i.err = nil
	i.stats.ReverseSeekCount[InterfaceCall]++
	if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	} else if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	}
	i.iterKey, i.iterValue = i.iter.SeekLT(key)
	i.stats.ReverseSeekCount[InternalIterCall]++
	i.findPrevEntry()
	return i.iterValidityState == IterValid
}

func (i *KKVIterator) First() bool {
	i.err = nil
	i.stats.ForwardSeekCount[InterfaceCall]++
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
	} else {
		i.iterKey, i.iterValue = i.iter.First()
	}
	i.stats.ForwardSeekCount[InternalIterCall]++
	i.findNextEntry()
	return i.iterValidityState == IterValid
}

func (i *KKVIterator) Last() bool {
	i.err = nil
	i.stats.ReverseSeekCount[InterfaceCall]++
	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
	}
	i.stats.ReverseSeekCount[InternalIterCall]++
	i.findPrevEntry()
	return i.iterValidityState == IterValid
}

func (i *KKVIterator) Next() bool {
	return i.NextWithLimit() == IterValid
}

func (i *KKVIterator) NextWithLimit() IterValidityState {
	i.stats.ForwardStepCount[InterfaceCall]++
	if i.err != nil {
		return i.iterValidityState
	}

	switch i.pos {
	case iterPosCurForward:
		i.nextUserKey()
	case iterPosCurReverse:
		if i.iterKey != nil {
			i.err = errors.New("bitalosdb: switching from reverse to forward but iter is not at prev")
			i.iterValidityState = IterExhausted
			return i.iterValidityState
		}

		if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
			i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
		} else {
			i.iterKey, i.iterValue = i.iter.First()
		}
		i.stats.ForwardSeekCount[InternalIterCall]++
	case iterPosCurReversePaused:
		if i.iterKey == nil {
			i.err = errors.New("bitalosdb: switching paused from reverse to forward but iter is exhausted")
			i.iterValidityState = IterExhausted
			return i.iterValidityState
		}
		i.nextUserKey()
	case iterPosPrev:
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
			} else {
				i.iterKey, i.iterValue = i.iter.First()
			}
			i.stats.ForwardSeekCount[InternalIterCall]++
		} else {
			i.nextUserKey()
		}
		i.nextUserKey()
	}
	i.findNextEntry()
	return i.iterValidityState
}

func (i *KKVIterator) Prev() bool {
	return i.PrevWithLimit() == IterValid
}

func (i *KKVIterator) PrevWithLimit() IterValidityState {
	i.stats.ReverseStepCount[InterfaceCall]++
	if i.err != nil {
		return i.iterValidityState
	}

	if i.pos == iterPosCurReverse {
		i.prevUserKey()
	}
	if i.pos == iterPosCurForward || i.pos == iterPosNext || i.pos == iterPosCurForwardPaused {
		stepAgain := i.pos == iterPosNext
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			if upperBound := i.opts.GetUpperBound(); upperBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
			} else {
				i.iterKey, i.iterValue = i.iter.Last()
			}
			i.stats.ReverseSeekCount[InternalIterCall]++
		} else {
			i.prevUserKey()
		}
		if stepAgain {
			i.prevUserKey()
		}
	}
	i.findPrevEntry()
	return i.iterValidityState
}

func (i *KKVIterator) Error() error {
	err := i.err
	if i.iter != nil {
		err = utils.FirstError(i.err, i.iter.Error())
	}
	return err
}

func (i *KKVIterator) Stats() IteratorStats {
	return i.stats
}

func (i *KKVIterator) getInternalStepCount() int {
	return i.stats.ForwardStepCount[1] + i.stats.ReverseStepCount[1]
}

func (i *KKVIterator) getInterfaceStepCount() int {
	return i.stats.ForwardStepCount[0] + i.stats.ReverseStepCount[0]
}

func (i *KKVIterator) getInternalSeekCount() int {
	return i.stats.ForwardSeekCount[1] + i.stats.ForwardSeekCount[1]
}

func (i *KKVIterator) getInterfaceSeekCount() int {
	return i.stats.ForwardSeekCount[0] + i.stats.ForwardSeekCount[0]
}

func (i *KKVIterator) Close() error {
	internalStepCount := i.getInternalStepCount()
	if internalStepCount > consts.IterSlowCountThreshold {
		i.memShard.iterSlowCount.Add(1)
	}

	closerNum := len(i.closers)
	if closerNum != i.closerNum {
		i.opts.Logger.Errorf("KKVIterator closer num not eq exp(%d) act(%d)", i.closerNum, closerNum)
	}

	if closerNum > 0 {
		for _, f := range i.closers {
			f()
		}
	}

	if i.iter != nil {
		i.err = utils.FirstError(i.err, i.iter.Close())
	}
	err := i.err

	if i.readState != nil {
		i.readState.unref()
		i.readState = nil
	}

	i.btree = nil

	if alloc := i.alloc; alloc != nil {
		if cap(i.keyBuf) >= maxKeyBufCacheSize {
			alloc.keyBuf = nil
		} else {
			alloc.keyBuf = i.keyBuf
		}
		if cap(i.closers) >= maxCloserCacheSize {
			alloc.closers = nil
		} else {
			alloc.closers = i.closers[:0]
		}
		*i = KKVIterator{}
		iterAllocPool.Put(alloc)
	}
	return err
}

func (i *KKVIterator) SetBounds(lower, upper []byte) {
	i.iterKey = nil
	i.iterValue = nil

	switch i.pos {
	case iterPosCurForward, iterPosNext, iterPosCurForwardPaused:
		i.pos = iterPosCurForward
	case iterPosCurReverse, iterPosPrev, iterPosCurReversePaused:
		i.pos = iterPosCurReverse
	}
	i.iterValidityState = IterExhausted

	i.opts.LowerBound = lower
	i.opts.UpperBound = upper
	i.iter.SetBounds(lower, upper)
}

func (i *KKVIterator) Valid() bool {
	return i.iterValidityState == IterValid
}

func (i *KKVIterator) Key() []byte {
	i.keyBuf = i.key.MakeUserKeyByBuf(i.keyBuf)
	return i.keyBuf
}

func (i *KKVIterator) getIterKey() []byte {
	i.keyBuf = i.iterKey.MakeUserKeyByBuf(i.keyBuf)
	return i.keyBuf
}

func (i *KKVIterator) Value() []byte {
	if len(i.value) == 0 {
		return []byte{}
	}
	return i.value
}

func (i *KKVIterator) GetSubKey() []byte {
	if len(i.key.SubKey) == 0 {
		return []byte{}
	}
	return i.key.SubKey
}

func (i *KKVIterator) GetZsetKey() (float64, []byte) {
	if len(i.key.SubKey) < kkv.SiKeyLength {
		return 0, []byte{}
	}
	return kkv.DecodeZsetScore(i.key.SubKey), i.GetZsetMember()
}

func (i *KKVIterator) GetZsetMember() []byte {
	if len(i.key.SubKey) <= kkv.SiKeyLength {
		return nil
	}
	return i.key.SubKey[kkv.SiKeyLength:]
}

func (i *KKVIterator) GetZsetScore() (float64, bool) {
	if len(i.key.SubKey) < kkv.SiKeyLength {
		return 0, false
	}
	return kkv.DecodeZsetScore(i.key.SubKey), true
}

func (i *KKVIterator) GetListIndex() uint64 {
	return kkv.DecodeListKeyIndex(i.key.SubKey)
}
