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
)

type superTableIterator struct {
	st        *superTable
	indexes   stIndexes
	indexPos  int
	iterKey   internalKey
	iterValue []byte
}

func (i *superTableIterator) findItem() (*internalKey, []byte) {
	if i.indexPos < 0 || i.indexPos >= len(i.indexes) {
		return nil, nil
	}

	ikey, value := i.st.getItem(i.indexes[i.indexPos])
	if ikey.UserKey == nil {
		return nil, nil
	}

	i.iterKey = ikey
	i.iterValue = value
	return &i.iterKey, i.iterValue
}

func (i *superTableIterator) First() (*internalKey, []byte) {
	i.indexPos = 0
	return i.findItem()
}

func (i *superTableIterator) Next() (*internalKey, []byte) {
	i.indexPos++
	return i.findItem()
}

func (i *superTableIterator) Prev() (*internalKey, []byte) {
	i.indexPos--
	return i.findItem()
}

func (i *superTableIterator) Last() (*internalKey, []byte) {
	i.indexPos = len(i.indexes) - 1
	return i.findItem()
}

func (i *superTableIterator) SeekGE(key []byte) (*internalKey, []byte) {
	i.indexPos = i.st.findKeyIndexPos(i.indexes, key)
	return i.findItem()
}

func (i *superTableIterator) SeekLT(key []byte) (*internalKey, []byte) {
	i.indexPos = i.st.findKeyIndexPos(i.indexes, key)
	poskey, _ := i.findItem()
	if poskey != nil {
		return i.Prev()
	}

	lastKey, lastValue := i.Last()
	if lastKey != nil && bytes.Compare(lastKey.UserKey, key) < 0 {
		return lastKey, lastValue
	}
	return nil, nil

}

func (i *superTableIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *internalKey, value []byte) {
	return i.SeekGE(key)
}

func (i *superTableIterator) SetBounds(lower, upper []byte) {
}

func (i *superTableIterator) Error() error {
	return nil
}

func (i *superTableIterator) Close() error {
	return nil
}

func (i *superTableIterator) String() string {
	return "superTableIterator"
}
