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

	"github.com/zuoyebang/bitalosdb/v2/bithash"
	"github.com/zuoyebang/bitalosdb/v2/bitpage"
	"github.com/zuoyebang/bitalosdb/v2/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
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
	if writer != nil {
		w.bpageWriter[pn] = writer
	}
	return writer
}

func (w *BitreeWriter) Set(key internalKey, value []byte) error {
	switch key.Kind() {
	case internalKeyKindPrefixDelete:
		pns, sentinels := w.btree.findPrefixDeleteKeyPageNums(key.UserKey, w.bdbCursor)
		if len(pns) == 0 || len(sentinels) == 0 {
			return errors.New("findPrefixDeleteKeyPageNums nil")
		}

		for j := range pns {
			if err := w.setBitpage(key, value, pns[j], sentinels[j]); err != nil {
				continue
			}
		}
	default:
		pn, sentinel := w.btree.findKeyPageNum(key.UserKey, w.bdbCursor)
		if pn == nilPageNum {
			return errors.New("findKeyPageNum nil")
		}

		if err := w.setBitpage(key, value, pn, sentinel); err != nil {
			return err
		}
	}
	return nil
}

func (w *BitreeWriter) setBitpage(key internalKey, value []byte, pn bitpage.PageNum, sentinel []byte) error {
	pageWriter := w.getBitpageWriter(pn, sentinel)
	if pageWriter == nil {
		return errors.Errorf("bitree: getBitpageWriter nil index:%d pageNum:%s", w.btree.index, pn)
	}

	switch key.Kind() {
	case internalKeyKindSet:
		valueSize := len(value)
		if w.btree.IsKvSeparate(key, valueSize) {
			keyFileNum, err := w.setBithash(key, value)
			if err != nil {
				return err
			}
			var ev [13]byte
			keySeqNum := key.SeqNum()
			ev[0] = base.InternalValueKindBithash
			binary.LittleEndian.PutUint64(ev[1:9], (keySeqNum<<8)|uint64(internalKeyKindSetBithash))
			binary.LittleEndian.PutUint32(ev[9:13], uint32(keyFileNum))
			return pageWriter.Set(key, ev[:])
		} else if valueSize >= base.InternalValueBithashSize {
			return pageWriter.Set(key, []byte{base.InternalValueKindSet}, value)
		} else {
			return pageWriter.Set(key, value)
		}
	case internalKeyKindDelete, internalKeyKindPrefixDelete:
		return pageWriter.Set(key, nil)
	}

	return nil
}

func (w *BitreeWriter) setBithash(key internalKey, value []byte) (bithash.FileNum, error) {
	if w.bhashWriter == nil {
		if w.btree.bhash == nil {
			if err := w.btree.bithashOpen(); err != nil {
				return 0, err
			}
		}

		writer, err := w.btree.bhash.FlushStart()
		if err != nil {
			return 0, err
		}
		w.bhashWriter = writer
	}

	ikey := key.Clone()
	return w.bhashWriter.Add(ikey, value)
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

func (w *BitreeWriter) Finish(checkFlush, isForce bool) error {
	w.UnrefBdbTx()

	if w.bhashWriter != nil {
		if err := w.btree.bhash.FlushFinish(w.bhashWriter); err != nil {
			return err
		}
		w.bhashWriter = nil
	}

	for _, pageWriter := range w.bpageWriter {
		if err := pageWriter.FlushFinish(); err != nil {
			return err
		}

		if checkFlush {
			pageWriter.FlushCheckAndPush(isForce)
		}
	}

	w.bpageWriter = nil
	return nil
}
