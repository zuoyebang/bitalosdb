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
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

type stripeChangeType int

const (
	newStripe stripeChangeType = iota
	sameStripeSkippable
	sameStripeNonSkippable
)

type compactionIter struct {
	bp        *Bitpage
	iter      InternalKKVIterator
	err       error
	key       InternalKKVKey
	valid     bool
	iterKey   *InternalKKVKey
	iterValue []byte
	skip      bool
	pos       iterPos
	isPreCalc bool
}

func (i *compactionIter) First() (*InternalKKVKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	i.iterKey, i.iterValue = i.iter.First()
	i.pos = iterPosNext
	return i.Next()
}

func (i *compactionIter) Next() (*InternalKKVKey, []byte) {
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
	if i.iterKey == nil {
		return nil, nil
	}
	i.key.Copy(i.iterKey)
	i.valid = true
	i.skip = true
	return &i.key, i.iterValue
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

func (i *compactionIter) nextInStripe() stripeChangeType {
	i.iterKey, i.iterValue = i.iter.Next()
	if i.iterKey == nil || !kkv.UserKeyEqual(&i.key, i.iterKey) {
		return newStripe
	}

	if i.iterKey.Kind() == internalKeyKindInvalid {
		return sameStripeNonSkippable
	}

	if !i.isPreCalc && i.bp != nil && i.iterKey.SeqNum() == 1 {
		i.bp.deleteBithashKey(i.iterValue)
	}

	return sameStripeSkippable
}

func (i *compactionIter) Key() []byte {
	return i.key.MakeUserKey()
}

func (i *compactionIter) Close() error {
	err := i.iter.Close()
	if i.err == nil {
		i.err = err
	}

	return i.err
}
