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

package bitree

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/bithash"
	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/internal/base"
)

type BitreeWriter struct {
	btree       *Bitree
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
	case base.InternalKeyKindDelete:
		err = pageWriter.Set(key, nil)
	}

	return err
}
