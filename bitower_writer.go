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
	"github.com/zuoyebang/bitalosdb/bitree"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bitask"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

type flushBitowerWriter struct {
	s      *Bitower
	writer *bitree.BitreeWriter
	err    error
}

func (w *flushBitowerWriter) Set(key base.InternalKey, val []byte) error {
	uk := key.UserKey
	if len(uk) <= 0 {
		return nil
	}

	if w.s.db.cache != nil {
		cacheKeyHash := w.s.db.cache.GetKeyHash(key.UserKey)
		switch key.Kind() {
		case base.InternalKeyKindSet:
			_ = w.s.db.cache.Set(key.UserKey, val, cacheKeyHash)
		case base.InternalKeyKindDelete:
			_ = w.s.db.cache.ExistAndDelete(key.UserKey, cacheKeyHash)
		}
	}

	if err := w.writer.Apply(key, val); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *flushBitowerWriter) Finish() error {
	if w.err != nil {
		w.writer.UnrefBdbTx()
		return w.err
	}
	return w.writer.Finish()
}

type flushWriter struct {
	db      *DB
	writers [consts.DefaultBitowerNum]*flushBitowerWriter
}

func (w *flushWriter) Set(key base.InternalKey, val []byte) error {
	uk := key.UserKey
	if len(uk) <= 0 {
		return nil
	}

	index := w.db.getBitowerIndexByKey(uk)
	return w.writers[index].Set(key, val)
}

func (w *flushWriter) Finish() error {
	var err error

	for i := range w.writers {
		err = utils.FirstError(err, w.writers[i].Finish())
	}

	return err
}

func (d *DB) newFlushWriter() (*flushWriter, error) {
	var err error
	w := &flushWriter{
		db: d,
	}

	for i, bitower := range d.bitowers {
		w.writers[i], err = bitower.newFlushWriter()
		if err != nil {
			return nil, err
		}
	}

	return w, nil
}

func (d *DB) doMemFlushTask(task *bitask.MemFlushTaskData) {
	d.bitowers[task.Index].flush(task.NeedReport, true)
}

func (d *DB) doBitpageTask(task *bitask.BitpageTaskData) {
	d.bitowers[task.Index].btree.DoBitpageTask(task)
}
