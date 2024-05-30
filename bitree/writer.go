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

package bitree

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/bithash"
	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bitask"
)

type BitreeWriter struct {
	btree       *Bitree
	bdbReadTx   *bdb.ReadTx
	bdbCursor   *bdb.Cursor
	bhashWriter *bithash.BithashWriter
	bpageWriter map[bitpage.PageNum]*bitpage.PageWriter
}

func (w *BitreeWriter) getBitpageWriter(pn bitpage.PageNum, sentinel []byte) *bitpage.PageWriter {
	pageWriter, ok := w.bpageWriter[pn]
	if ok {
		return pageWriter
	}

	writer := w.btree.bpage.GetPageWriter(pn, sentinel)
	if writer == nil {
		return nil
	}

	w.bpageWriter[pn] = writer
	return writer
}

func (w *BitreeWriter) set(key base.InternalKey, value []byte, pn bitpage.PageNum, sentinel []byte) (err error) {
	pageWriter := w.getBitpageWriter(pn, sentinel)
	if pageWriter == nil {
		return errors.Errorf("bitree: getBitpageWriter nil index:%d pageNum:%s", w.btree.index, pn)
	}

	switch key.Kind() {
	case base.InternalKeyKindSet:
		keySeqNum := key.SeqNum()
		if w.btree.IsKvSeparate(len(value)) {
			keyFileNum, err := w.bhashWriter.Add(key, value)
			if err != nil {
				return err
			}

			isExistTm, tm := w.btree.opts.KvTimestampFunc(value, 1)
			if isExistTm {
				var ev [20]byte
				binary.LittleEndian.PutUint64(ev[0:8], (keySeqNum<<8)|uint64(base.InternalKeyKindSetBithash))
				binary.LittleEndian.PutUint32(ev[8:12], uint32(keyFileNum))
				binary.LittleEndian.PutUint64(ev[12:20], tm)
				err = pageWriter.Set(key, ev[:])
			} else {
				var ev [12]byte
				binary.LittleEndian.PutUint64(ev[0:8], (keySeqNum<<8)|uint64(base.InternalKeyKindSetBithash))
				binary.LittleEndian.PutUint32(ev[8:12], uint32(keyFileNum))
				err = pageWriter.Set(key, ev[:])
			}
		} else {
			ev, evCloser := base.EncodeInternalValue(value, keySeqNum, base.InternalKeyKindSet)
			err = pageWriter.Set(key, ev)
			evCloser()
		}
	case base.InternalKeyKindDelete, base.InternalKeyKindPrefixDelete:
		err = pageWriter.Set(key, nil)
	}

	return err
}

func (w *BitreeWriter) Apply(key base.InternalKey, value []byte) error {
	kind := key.Kind()
	if kind != base.InternalKeyKindPrefixDelete {
		pn, sentinel := w.btree.findKeyPageNum(key.UserKey, w.bdbCursor)
		if pn == nilPageNum {
			return errors.New("findKeyPageNum nil")
		}

		if err := w.set(key, value, pn, sentinel); err != nil {
			return err
		}
	} else {
		pns, sentinels := w.btree.findPrefixDeleteKeyPageNums(key.UserKey, w.bdbCursor)
		if len(pns) == 0 || len(sentinels) == 0 {
			return errors.New("findPrefixDeleteKeyPageNums nil")
		}

		for j := range pns {
			if err := w.set(key, value, pns[j], sentinels[j]); err != nil {
				continue
			}
		}
	}

	return nil
}

func (w *BitreeWriter) RefBdbTx() {
	if w.bdbReadTx == nil {
		w.bdbReadTx = w.btree.txPool.Load()
		w.bdbCursor = w.bdbReadTx.Bucket().Cursor()
	}
}

func (w *BitreeWriter) UnrefBdbTx() {
	if w.bdbReadTx != nil {
		w.bdbReadTx.Unref(true)
		w.bdbReadTx = nil
	}
}

func (w *BitreeWriter) Finish() error {
	w.UnrefBdbTx()

	if w.bhashWriter != nil {
		if err := w.btree.bhash.FlushFinish(w.bhashWriter); err != nil {
			return err
		}
		w.bhashWriter = nil
	}

	flushSize := w.btree.opts.BitpageFlushSize

	for pn, pageWriter := range w.bpageWriter {
		if err := pageWriter.FlushFinish(); err != nil {
			w.btree.opts.Logger.Errorf("bitree pageWriter FlushFinish fail pn:%d err:%s", pn, err)
			continue
		}
		pageWriter.UpdateMetaTimestamp()
		if pageWriter.MaybePageFlush(flushSize) {
			w.btree.opts.BitpageTaskPushFunc(&bitask.BitpageTaskData{
				Index:    w.btree.index,
				Event:    bitask.BitpageEventFlush,
				Pn:       uint32(pn),
				Sentinel: pageWriter.Sentinel,
			})
		}
	}

	w.bpageWriter = nil
	return nil
}

func (t *Bitree) NewBitreeWriter() (*BitreeWriter, error) {
	writer := &BitreeWriter{
		btree: t,
	}

	if t.opts.UseBithash {
		w, err := t.bhash.FlushStart()
		if err != nil {
			return nil, err
		}
		writer.bhashWriter = w
	}

	writer.RefBdbTx()
	writer.bpageWriter = make(map[bitpage.PageNum]*bitpage.PageWriter, 16)

	return writer, nil
}
