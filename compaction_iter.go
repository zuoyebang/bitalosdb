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
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/errors"
)

type compactionIter struct {
	cmp       Compare
	iter      internalIterator
	err       error
	key       InternalKey
	value     []byte
	keyBuf    []byte
	valid     bool
	iterKey   *InternalKey
	iterValue []byte
	skip      bool
	pos       iterPos
	first     bool
	end       bool
}

func (i *compactionIter) IsVisitFirst() bool {
	return i.first == true
}

func (i *compactionIter) SetVisitEnd() {
	i.end = true
}

func (i *compactionIter) IsVisitEnd() bool {
	return i.end == true
}

func (i *compactionIter) First() (*InternalKey, []byte) {
	i.first = true
	if i.err != nil {
		return nil, nil
	}
	i.iterKey, i.iterValue = i.iter.First()
	i.pos = iterPosNext
	return i.Next()
}

func (i *compactionIter) Next() (*InternalKey, []byte) {
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
		case InternalKeyKindSet, InternalKeyKindDelete, InternalKeyKindPrefixDelete:
			i.saveKey()
			i.value = i.iterValue
			i.valid = true
			i.skip = true
			return &i.key, i.value
		default:
			i.err = errors.Errorf("bitalosdb: invalid internal key kind %d", i.iterKey.Kind())
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
	if i.cmp(i.key.UserKey, key.UserKey) != 0 {
		return newStripe
	}

	if key.Kind() == base.InternalKeyKindInvalid {
		return sameStripeNonSkippable
	}
	return sameStripeSkippable
}

func (i *compactionIter) saveKey() {
	i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
	i.key.UserKey = i.keyBuf
	i.key.Trailer = i.iterKey.Trailer
}

func (i *compactionIter) Key() InternalKey {
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
