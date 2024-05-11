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
	"errors"
)

type compactionIter struct {
	bp        *Bitpage
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

func newCompactionIter(bp *Bitpage, iter internalIterator) *compactionIter {
	i := &compactionIter{
		bp:   bp,
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
		case internalKeyKindSet, internalKeyKindDelete, internalKeyKindPrefixDelete:
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
	if !bytes.Equal(i.key.UserKey, key.UserKey) {
		return newStripe
	}

	if key.Kind() == internalKeyKindInvalid {
		return sameStripeNonSkippable
	}

	if i.bp != nil && key.SeqNum() == 1 {
		i.bp.deleteBithashKey(i.iterValue)
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
