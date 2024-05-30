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

type internalIterAdapter struct {
	internalIterator
	key *InternalKey
	val []byte
}

func newInternalIterAdapter(iter internalIterator) *internalIterAdapter {
	return &internalIterAdapter{
		internalIterator: iter,
	}
}

func (i *internalIterAdapter) update(key *InternalKey, val []byte) bool {
	i.key = key
	i.val = val
	return i.key != nil
}

func (i *internalIterAdapter) String() string {
	return "internal-iter-adapter"
}

func (i *internalIterAdapter) SeekGE(key []byte) bool {
	return i.update(i.internalIterator.SeekGE(key))
}

func (i *internalIterAdapter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) bool {
	return i.update(i.internalIterator.SeekPrefixGE(prefix, key, trySeekUsingNext))
}

func (i *internalIterAdapter) SeekLT(key []byte) bool {
	return i.update(i.internalIterator.SeekLT(key))
}

func (i *internalIterAdapter) First() bool {
	return i.update(i.internalIterator.First())
}

func (i *internalIterAdapter) Last() bool {
	return i.update(i.internalIterator.Last())
}

func (i *internalIterAdapter) Next() bool {
	return i.update(i.internalIterator.Next())
}

func (i *internalIterAdapter) Prev() bool {
	return i.update(i.internalIterator.Prev())
}

func (i *internalIterAdapter) Key() *InternalKey {
	return i.key
}

func (i *internalIterAdapter) Value() []byte {
	return i.val
}

func (i *internalIterAdapter) Valid() bool {
	return i.key != nil
}
