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

	"github.com/zuoyebang/bitalosdb/internal/consts"
)

type Batch struct {
	db       *DB
	bitowers [consts.DefaultBitowerNum]*BatchBitower
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return &Batch{}
	},
}

func newBatch(db *DB) *Batch {
	b := batchPool.Get().(*Batch)
	b.db = db
	for i := range b.bitowers {
		b.bitowers[i] = nil
		b.bitowers[i] = newBatchBitowerByIndex(db, i)
	}
	return b
}

func (b *Batch) getBatchBitower(key []byte) *BatchBitower {
	index := b.db.getBitowerIndexByKey(key)
	if b.bitowers[index] == nil {
		b.bitowers[index] = newBatchBitowerByIndex(b.db, index)
	}
	return b.bitowers[index]
}

func (b *Batch) Set(key, value []byte, opts *WriteOptions) error {
	return b.getBatchBitower(key).Set(key, value, opts)
}

func (b *Batch) SetMultiValue(key []byte, values ...[]byte) error {
	return b.getBatchBitower(key).SetMultiValue(key, values...)
}

func (b *Batch) PrefixDeleteKeySet(key []byte, opts *WriteOptions) error {
	return b.getBatchBitower(key).PrefixDeleteKeySet(key, opts)
}

func (b *Batch) Delete(key []byte, opts *WriteOptions) error {
	return b.getBatchBitower(key).Delete(key, opts)
}

func (b *Batch) Commit(o *WriteOptions) error {
	return b.db.Apply(b, o)
}

func (b *Batch) Close() error {
	for i := range b.bitowers {
		if b.bitowers[i] != nil {
			b.bitowers[i].Close()
			b.bitowers[i] = nil
		}
	}
	b.db = nil
	return nil
}

func (b *Batch) Reset() {
	for i := range b.bitowers {
		if b.bitowers[i] != nil {
			b.bitowers[i].Reset()
		}
	}
}

func (b *Batch) Empty() bool {
	for i := range b.bitowers {
		if b.bitowers[i] != nil && !b.bitowers[i].Empty() {
			return false
		}
	}
	return true
}

func (b *Batch) Count() uint32 {
	var count uint32
	for i := range b.bitowers {
		if b.bitowers[i] != nil {
			count += b.bitowers[i].Count()
		}
	}
	return count
}
