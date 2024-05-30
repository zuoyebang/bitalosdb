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
	"encoding/binary"
	"errors"
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

type compactionIter struct {
	iter      internalIterator
	err       error
	key       internalKey
	value     []byte
	keyBuf    []byte
	valid     bool
	iterKey   *internalKey
	iterValue []byte
	skip      bool
	pos       iterPos
}

func newCompactionIter(iter internalIterator) *compactionIter {
	i := &compactionIter{
		iter: iter,
	}
	return i
}

func (i *compactionIter) First() (*internalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	i.iterKey, i.iterValue = i.iter.First()
	i.pos = iterPosNext
	return i.Next()
}

func (i *compactionIter) Next() (*internalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if i.pos == iterPosCurForward {
		if i.skip {
			i.skipInStripe()
		} else {
			i.nextInStripe()
		}
	}

	i.pos = iterPosCurForward
	i.valid = false
	for i.iterKey != nil {
		switch i.iterKey.Kind() {
		case internalKeyKindDelete, internalKeyKindSet:
			i.saveKey()
			i.value = i.iterValue
			i.valid = true
			i.skip = true
			return &i.key, i.value

		default:
			i.err = errors.New("invalid internal key kind")
			i.valid = false
			return nil, nil
		}
	}

	return nil, nil
}

func (i *compactionIter) skipInStripe() {
	i.skip = true
	var change stripeChangeType
	for {
		change = i.nextInStripe()
		if change == sameStripeNonSkippable || change == newStripe {
			break
		}
	}

	if change == newStripe {
		i.skip = false
	}
}

func (i *compactionIter) iterNext() bool {
	i.iterKey, i.iterValue = i.iter.Next()
	return i.iterKey != nil
}

type stripeChangeType int

const (
	newStripe stripeChangeType = iota
	sameStripeSkippable
	sameStripeNonSkippable
)

func (i *compactionIter) nextInStripe() stripeChangeType {
	if !i.iterNext() {
		return newStripe
	}

	key := i.iterKey
	if bytes.Compare(i.key.UserKey, key.UserKey) != 0 {
		return newStripe
	}

	if key.Kind() == internalKeyKindInvalid {
		return sameStripeNonSkippable
	}

	if i.key.Kind() == internalKeyKindSet && key.Kind() == internalKeyKindSet {
		valueLen := len(i.value)
		iterValueLen := len(i.iterValue)

		curFreq := binary.BigEndian.Uint16(i.value[valueLen-LFU_FREQ_LENGTH:])
		nextFreq := binary.BigEndian.Uint16(i.iterValue[iterValueLen-LFU_FREQ_LENGTH:])

		if LFU_FREQ_MAX-curFreq >= nextFreq {
			curFreq += nextFreq
		} else {
			curFreq = LFU_FREQ_MAX
		}
		binary.BigEndian.PutUint16(i.value[valueLen-LFU_FREQ_LENGTH:], curFreq)
		nextFreq = 0
		binary.BigEndian.PutUint16(i.iterValue[iterValueLen-LFU_FREQ_LENGTH:], nextFreq)
	}

	return sameStripeSkippable
}

func (i *compactionIter) saveKey() {
	i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
	i.key.UserKey = i.keyBuf
	i.key.Trailer = i.iterKey.Trailer
}

func (i *compactionIter) Key() internalKey {
	return i.key
}

func (i *compactionIter) Value() []byte {
	return i.value
}

func (i *compactionIter) Valid() bool {
	return i.valid
}

func (i *compactionIter) Error() error {
	return i.err
}

func (i *compactionIter) Close() error {
	err := i.iter.Close()
	if i.err == nil {
		i.err = err
	}

	return i.err
}
