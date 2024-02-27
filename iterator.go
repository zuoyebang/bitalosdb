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

package bitalosdb

import (
	"bytes"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/utils"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

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

var errReversePrefixIteration = errors.New("bitalosdb: unsupported reverse prefix iteration")

type IteratorMetrics struct {
	ReadAmp int
}

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

var _ redact.SafeFormatter = &IteratorStats{}

type Iterator struct {
	db                  *DB
	opts                IterOptions
	cmp                 Compare
	equal               Equal
	split               Split
	iter                internalIterator
	readState           *readState
	err                 error
	key                 []byte
	keyBuf              []byte
	value               []byte
	valueBuf            []byte
	iterKey             *InternalKey
	iterValue           []byte
	alloc               *iterAlloc
	prefixOrFullSeekKey []byte
	stats               IteratorStats
	batch               *Batch
	seqNum              uint64
	iterValidityState   IterValidityState
	pos                 iterPos
	hasPrefix           bool
	lastPositioningOp   lastPositioningOpKind
}

type lastPositioningOpKind int8

const (
	unknownLastPositionOp lastPositioningOpKind = iota
	seekPrefixGELastPositioningOp
	seekGELastPositioningOp
	seekLTLastPositioningOp
)

type IterValidityState int8

const (
	IterExhausted IterValidityState = iota
	IterValid
	IterAtLimit
)

func (i *Iterator) findNextEntry() {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurForward

	for i.iterKey != nil {
		key := *i.iterKey

		if i.hasPrefix {
			if n := i.split(key.UserKey); !bytes.Equal(i.prefixOrFullSeekKey, key.UserKey[:n]) {
				return
			}
		}

		switch key.Kind() {
		case InternalKeyKindDelete:
			i.nextUserKey()
			continue

		case InternalKeyKindSet:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = i.iterValue
			i.iterValidityState = IterValid
			return

		default:
			i.err = errors.Errorf("bitalosdb: invalid internal key kind %d", key.Kind())
			i.iterValidityState = IterExhausted
			return
		}
	}
}

func (i *Iterator) nextUserKey() {
	if i.iterKey == nil {
		return
	}
	done := i.iterKey.SeqNum() == 0
	if i.iterValidityState != IterValid {
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		i.key = i.keyBuf
	}
	for {
		i.iterKey, i.iterValue = i.iter.Next()
		i.stats.ForwardStepCount[InternalIterCall]++
		if done || i.iterKey == nil {
			break
		}
		if !i.equal(i.key, i.iterKey.UserKey) {
			break
		}
		done = i.iterKey.SeqNum() == 0
	}
}

func (i *Iterator) findPrevEntry() {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurReverse

	for i.iterKey != nil {
		key := *i.iterKey

		if i.iterValidityState == IterValid {
			if !i.equal(key.UserKey, i.key) {
				i.pos = iterPosPrev
				if i.err != nil {
					i.iterValidityState = IterExhausted
				}
				return
			}
		}

		switch key.Kind() {
		case InternalKeyKindDelete:
			i.value = nil
			i.iterValidityState = IterExhausted
			i.iterKey, i.iterValue = i.iter.Prev()
			i.stats.ReverseStepCount[InternalIterCall]++
		case InternalKeyKindSet:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = i.iterValue
			i.iterValidityState = IterValid
			i.iterKey, i.iterValue = i.iter.Prev()
			i.stats.ReverseStepCount[InternalIterCall]++
		default:
			i.err = errors.Errorf("bitalosdb: invalid internal key kind %d", key.Kind())
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

func (i *Iterator) prevUserKey() {
	if i.iterKey == nil {
		return
	}
	if i.iterValidityState != IterValid {
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		i.key = i.keyBuf
	}
	for {
		i.iterKey, i.iterValue = i.iter.Prev()
		i.stats.ReverseStepCount[InternalIterCall]++
		if i.iterKey == nil {
			break
		}
		if !i.equal(i.key, i.iterKey.UserKey) {
			break
		}
	}
}

func (i *Iterator) SeekGE(key []byte) bool {
	return i.SeekGEWithLimit(key) == IterValid
}

func (i *Iterator) SeekGEWithLimit(key []byte) IterValidityState {
	lastPositioningOp := i.lastPositioningOp
	i.lastPositioningOp = unknownLastPositionOp

	i.err = nil
	i.hasPrefix = false
	i.stats.ForwardSeekCount[InterfaceCall]++
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	}
	seekInternalIter := true
	if lastPositioningOp == seekGELastPositioningOp && i.batch == nil {
		cmp := i.cmp(i.prefixOrFullSeekKey, key)
		if cmp <= 0 {
			if i.iterValidityState == IterExhausted ||
				(i.iterValidityState == IterValid && i.cmp(key, i.key) <= 0) {
				if !invariants.Enabled || !disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
					i.lastPositioningOp = seekGELastPositioningOp
					return i.iterValidityState
				}
			}
			if i.pos == iterPosCurForwardPaused && i.cmp(key, i.iterKey.UserKey) <= 0 {
				seekInternalIter = false
			}
		}
	}
	if seekInternalIter {
		i.iterKey, i.iterValue = i.iter.SeekGE(key)
		i.stats.ForwardSeekCount[InternalIterCall]++
	}
	i.findNextEntry()
	if i.Error() == nil && i.batch == nil {
		i.prefixOrFullSeekKey = append(i.prefixOrFullSeekKey[:0], key...)
		i.lastPositioningOp = seekGELastPositioningOp
	}
	return i.iterValidityState
}

func (i *Iterator) SeekPrefixGE(key []byte) bool {
	lastPositioningOp := i.lastPositioningOp
	i.lastPositioningOp = unknownLastPositionOp
	i.err = nil
	i.stats.ForwardSeekCount[InterfaceCall]++

	prefixLen := i.split(key)
	keyPrefix := key[:prefixLen]
	trySeekUsingNext := false
	if lastPositioningOp == seekPrefixGELastPositioningOp {
		cmp := i.cmp(i.prefixOrFullSeekKey, keyPrefix)
		trySeekUsingNext = cmp < 0
		if invariants.Enabled && trySeekUsingNext && disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
			trySeekUsingNext = false
		}
	}
	if cap(i.prefixOrFullSeekKey) < prefixLen {
		i.prefixOrFullSeekKey = make([]byte, prefixLen)
	} else {
		i.prefixOrFullSeekKey = i.prefixOrFullSeekKey[:prefixLen]
	}
	i.hasPrefix = true
	copy(i.prefixOrFullSeekKey, keyPrefix)

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		if n := i.split(lowerBound); !bytes.Equal(i.prefixOrFullSeekKey, lowerBound[:n]) {
			i.err = errors.New("bitalosdb: SeekPrefixGE supplied with key outside of lower bound")
			return false
		}
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		if n := i.split(upperBound); !bytes.Equal(i.prefixOrFullSeekKey, upperBound[:n]) {
			i.err = errors.New("bitalosdb: SeekPrefixGE supplied with key outside of upper bound")
			return false
		}
		key = upperBound
	}

	i.iterKey, i.iterValue = i.iter.SeekPrefixGE(i.prefixOrFullSeekKey, key, trySeekUsingNext)
	i.stats.ForwardSeekCount[InternalIterCall]++
	i.findNextEntry()
	if i.Error() == nil {
		i.lastPositioningOp = seekPrefixGELastPositioningOp
	}
	return i.iterValidityState == IterValid
}

func disableSeekOpt(key []byte, ptr uintptr) bool {
	simpleHash := (11400714819323198485 * uint64(ptr)) >> 63
	return key != nil && key[0]&byte(1) == 0 && simpleHash == 0
}

func (i *Iterator) SeekLT(key []byte) bool {
	return i.SeekLTWithLimit(key) == IterValid
}

func (i *Iterator) SeekLTWithLimit(key []byte) IterValidityState {
	lastPositioningOp := i.lastPositioningOp
	i.lastPositioningOp = unknownLastPositionOp
	i.err = nil
	i.hasPrefix = false
	i.stats.ReverseSeekCount[InterfaceCall]++
	if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	} else if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	}
	seekInternalIter := true
	if lastPositioningOp == seekLTLastPositioningOp && i.batch == nil {
		cmp := i.cmp(key, i.prefixOrFullSeekKey)
		if cmp <= 0 {
			if i.iterValidityState == IterExhausted ||
				(i.iterValidityState == IterValid && i.cmp(i.key, key) < 0) {
				if !invariants.Enabled || !disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
					i.lastPositioningOp = seekLTLastPositioningOp
					return i.iterValidityState
				}
			}
			if i.pos == iterPosCurReversePaused && i.cmp(i.iterKey.UserKey, key) < 0 {
				seekInternalIter = false
			}
		}
	}
	if seekInternalIter {
		i.iterKey, i.iterValue = i.iter.SeekLT(key)
		i.stats.ReverseSeekCount[InternalIterCall]++
	}
	i.findPrevEntry()
	if i.Error() == nil && i.batch == nil {
		i.prefixOrFullSeekKey = append(i.prefixOrFullSeekKey[:0], key...)
		i.lastPositioningOp = seekLTLastPositioningOp
	}
	return i.iterValidityState
}

func (i *Iterator) First() bool {
	i.err = nil
	i.hasPrefix = false
	i.lastPositioningOp = unknownLastPositionOp
	i.stats.ForwardSeekCount[InterfaceCall]++
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
		i.stats.ForwardSeekCount[InternalIterCall]++
	} else {
		i.iterKey, i.iterValue = i.iter.First()
		i.stats.ForwardSeekCount[InternalIterCall]++
	}
	i.findNextEntry()
	return i.iterValidityState == IterValid
}

func (i *Iterator) Last() bool {
	i.err = nil
	i.hasPrefix = false
	i.lastPositioningOp = unknownLastPositionOp
	i.stats.ReverseSeekCount[InterfaceCall]++
	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
		i.stats.ReverseSeekCount[InternalIterCall]++
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
		i.stats.ReverseSeekCount[InternalIterCall]++
	}
	i.findPrevEntry()
	return i.iterValidityState == IterValid
}

func (i *Iterator) Next() bool {
	return i.NextWithLimit() == IterValid
}

func (i *Iterator) NextWithLimit() IterValidityState {
	i.stats.ForwardStepCount[InterfaceCall]++

	if i.err != nil {
		return i.iterValidityState
	}
	i.lastPositioningOp = unknownLastPositionOp
	switch i.pos {
	case iterPosCurForward:
		i.nextUserKey()
	case iterPosCurForwardPaused:
	case iterPosCurReverse:
		if i.iterKey != nil {
			i.err = errors.New("bitalosdb: switching from reverse to forward but iter is not at prev")
			i.iterValidityState = IterExhausted
			return i.iterValidityState
		}

		if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
			i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
			i.stats.ForwardSeekCount[InternalIterCall]++
		} else {
			i.iterKey, i.iterValue = i.iter.First()
			i.stats.ForwardSeekCount[InternalIterCall]++
		}
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
				i.stats.ForwardSeekCount[InternalIterCall]++
			} else {
				i.iterKey, i.iterValue = i.iter.First()
				i.stats.ForwardSeekCount[InternalIterCall]++
			}
		} else {
			i.nextUserKey()
		}
		i.nextUserKey()
	case iterPosNext:
	}
	i.findNextEntry()
	return i.iterValidityState
}

func (i *Iterator) Prev() bool {
	return i.PrevWithLimit() == IterValid
}

func (i *Iterator) PrevWithLimit() IterValidityState {
	i.stats.ReverseStepCount[InterfaceCall]++
	if i.err != nil {
		return i.iterValidityState
	}
	i.lastPositioningOp = unknownLastPositionOp
	if i.hasPrefix {
		i.err = errReversePrefixIteration
		i.iterValidityState = IterExhausted
		return i.iterValidityState
	}
	switch i.pos {
	case iterPosCurForward:
	case iterPosCurForwardPaused:
	case iterPosCurReverse:
		i.prevUserKey()
	case iterPosCurReversePaused:
	case iterPosNext:
	case iterPosPrev:
	}
	if i.pos == iterPosCurForward || i.pos == iterPosNext || i.pos == iterPosCurForwardPaused {
		stepAgain := i.pos == iterPosNext
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			if upperBound := i.opts.GetUpperBound(); upperBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
				i.stats.ReverseSeekCount[InternalIterCall]++
			} else {
				i.iterKey, i.iterValue = i.iter.Last()
				i.stats.ReverseSeekCount[InternalIterCall]++
			}
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

func (i *Iterator) Key() []byte {
	return i.key
}

func (i *Iterator) Value() []byte {
	return i.value
}

func (i *Iterator) Valid() bool {
	return i.iterValidityState == IterValid
}

func (i *Iterator) Error() error {
	err := i.err
	if i.iter != nil {
		err = utils.FirstError(i.err, i.iter.Error())
	}
	return err
}

func (i *Iterator) Close() error {
	if i.db != nil {
		stepCount := i.stats.ForwardStepCount[1] + i.stats.ReverseStepCount[1]
		if stepCount > consts.IterSlowCountThreshold {
			i.db.iterSlowCount.Add(1)
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
		*i = Iterator{}
		iterAllocPool.Put(alloc)
	}
	return err
}

func (i *Iterator) SetBounds(lower, upper []byte) {
	i.lastPositioningOp = unknownLastPositionOp
	i.hasPrefix = false
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

func (i *Iterator) Metrics() IteratorMetrics {
	m := IteratorMetrics{
		ReadAmp: 1,
	}
	if mi, ok := i.iter.(*mergingIter); ok {
		m.ReadAmp = len(mi.levels)
	}
	return m
}

func (i *Iterator) ResetStats() {
	i.stats = IteratorStats{}
}

func (i *Iterator) Stats() IteratorStats {
	return i.stats
}

func (i *Iterator) Clone() (*Iterator, error) {
	readState := i.readState
	if readState == nil {
		return nil, errors.New("bitalosdb: cannot Clone a closed Iterator")
	}

	readState.ref()

	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		db:                  i.db,
		opts:                i.opts,
		alloc:               buf,
		cmp:                 i.cmp,
		equal:               i.equal,
		iter:                &buf.merging,
		split:               i.split,
		readState:           readState,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		batch:               i.batch,
		seqNum:              i.seqNum,
	}
	return finishInitializingIter(i.db, buf), nil
}

func (stats *IteratorStats) String() string {
	return redact.StringWithoutMarkers(stats)
}

func (stats *IteratorStats) SafeFormat(s redact.SafePrinter, verb rune) {
	for i := range stats.ForwardStepCount {
		switch IteratorStatsKind(i) {
		case InterfaceCall:
			s.SafeString("(interface (dir, seek, step): ")
		case InternalIterCall:
			s.SafeString(", (internal (dir, seek, step): ")
		}
		s.Printf("(fwd, %d, %d), (rev, %d, %d))",
			stats.ForwardSeekCount[i], stats.ForwardStepCount[i],
			stats.ReverseSeekCount[i], stats.ReverseStepCount[i])
	}
}
