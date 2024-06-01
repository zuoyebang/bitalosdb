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

package bitable

import (
	bt "github.com/zuoyebang/bitalostable"
)

type BitableBatch struct {
	batch *bt.Batch
	wo    *bt.WriteOptions
}

func (b *Bitable) NewBatch() *BitableBatch {
	batch := &BitableBatch{
		batch: b.db.NewBatch(),
		wo:    b.wo,
	}
	return batch
}

func (b *Bitable) NewFlushBatch(n int) *BitableBatch {
	batch := &BitableBatch{
		batch: b.db.NewFlushBatch(n),
		wo:    b.wo,
	}
	return batch
}

func (b *BitableBatch) Commit() error {
	return b.batch.Commit(b.wo)
}

func (b *BitableBatch) Set(key []byte, val []byte) error {
	return b.batch.Set(key, val, b.wo)
}

func (b *BitableBatch) Delete(key []byte) error {
	return b.batch.Delete(key, b.wo)
}

func (b *BitableBatch) Size() int {
	return b.batch.Len()
}

func (b *BitableBatch) Empty() bool {
	return b.batch.Empty()
}

func (b *BitableBatch) Reset() {
	b.batch.Reset()
}

func (b *BitableBatch) AllocFree() {
	b.batch.AllocFree()
}

func (b *BitableBatch) Close() error {
	err := b.batch.Close()
	b.batch = nil
	return err
}
