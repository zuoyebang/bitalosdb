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
	"io"
	"time"

	"github.com/zuoyebang/bitalosdb/bitable"
	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

func (t *Bitree) newBitableIter(o *options.IterOptions) *bitable.BitableIterator {
	if t.btable == nil {
		return nil
	}

	return t.btable.NewIter(o)
}

func (t *Bitree) bitableExist(key []byte) bool {
	_, closer, err := t.bitableGet(key)
	if closer != nil {
		closer.Close()
	}

	return err != base.ErrNotFound
}

func (t *Bitree) bitableGet(key []byte) ([]byte, io.Closer, error) {
	if t.btable == nil || !t.opts.IsFlushedBitableCB() {
		return nil, nil, base.ErrNotFound
	}

	return t.btable.Get(key)
}

func (t *Bitree) bitableDelete(key []byte) error {
	if t.btable == nil || !t.opts.IsFlushedBitableCB() {
		return nil
	}

	val, closer, err := t.btable.Get(key)
	if closer != nil {
		_ = closer.Close()
	}
	if err == base.ErrNotFound {
		return nil
	}

	t.DeleteBithashKey(val)

	return t.btable.Delete(key)
}

func (t *Bitree) ManualCompact() error {
	if t.btable == nil {
		return nil
	}

	err := t.btable.Compact(nil, []byte{0xff, 0xff}, false)
	return err
}

func (t *Bitree) CompactBitreeToBitable() (pn bitpage.PageNum) {
	if t.btable == nil {
		return
	}

	t.dbState.WaitBitowerHighPriority(t.index)
	t.dbState.LockBitowerWrite(t.index)
	defer t.dbState.UnlockBitowerWrite(t.index)

	start := time.Now()

	deleteBitable := func(batch *bitable.BitableBatch, key []byte) {
		btVal, closer, btErr := t.bitableGet(key)
		defer func() {
			if closer != nil {
				closer.Close()
			}
		}()
		if btErr != base.ErrNotFound {
			t.DeleteBithashKey(btVal)
			_ = batch.Delete(key)
		}
	}

	flushToBitable := func(ciMaxSize int) error {
		t.btable.CloseAutomaticCompactions()
		defer t.btable.OpenAutomaticCompactions()

		iter := t.newBitreeIter(nil)
		defer iter.Close()

		iter.SetCompact()
		ciFlushSize := ciMaxSize - 6<<20
		var batch *bitable.BitableBatch
		var err error

		for k, v := iter.First(); k != nil; k, v = iter.Next() {
			if batch == nil {
				batch = t.btable.NewFlushBatch(ciMaxSize)
			}

			switch k.Kind() {
			case base.InternalKeyKindSet:
				if t.opts.BitpageOpts.CheckExpireCB(k.UserKey, v) {
					deleteBitable(batch, k.UserKey)
				} else {
					_ = batch.Set(k.UserKey, v)
				}
			case base.InternalKeyKindDelete:
				deleteBitable(batch, k.UserKey)
			}

			if batch.Size() > ciFlushSize {
				if err = batch.Commit(); err != nil {
					return err
				}
				_ = batch.Close()
				batch = nil
			}

			if err != nil {
				t.opts.Logger.Errorf("[COMPACTBITABLE %d] flushToBitable write fail err:%s", t.index, err)
				err = nil
			}
		}

		if batch == nil {
			return nil
		}

		err = batch.Commit()
		_ = batch.Close()
		batch = nil
		return err
	}

	if err := flushToBitable(consts.CompactToBitableCiMaxSize); err != nil {
		t.opts.Logger.Errorf("[COMPACTBITABLE %d] flushToBitable fail err:%s", t.index, err)
		return
	}

	resetBitree := func() error {
		tx, err := t.bdb.Begin(true)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		bkt := tx.Bucket(consts.BdbBucketName)
		if bkt == nil {
			err = ErrBucketNotExist
			return err
		}

		it := t.NewBdbIter()
		defer it.Close()

		var pns []bitpage.PageNum
		for k, v := it.First(); k != nil; k, v = it.Next() {
			if err = bkt.Delete(k.UserKey); err != nil {
				return err
			}
			pns = append(pns, bitpage.PageNum(utils.BytesToUint32(v)))
		}

		pn, err = t.bpage.NewPage()
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = t.bpage.FreePage(pn, false)
			}
		}()

		if err = bkt.Put(consts.BdbMaxKey, pn.ToByte()); err != nil {
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}

		t.bpage.MarkFreePages(pns)
		return nil
	}

	if err := resetBitree(); err != nil {
		pn = nilPageNum
		t.opts.Logger.Errorf("[COMPACTBITABLE %d] bdbReset fail err:%s", t.index, err)
	}

	if !t.BdbUpdate() {
		t.opts.Logger.Errorf("[COMPACTBITABLE %d] bdb txPool swaptx fail", t.index)
	}

	t.bpage.ResetStats()
	t.opts.Logger.Infof("[COMPACTBITABLE %d] compact finish cost:%.3fs", t.index, time.Since(start).Seconds())

	return
}

func (t *Bitree) BitableDebugInfo(dataType string) string {
	if t.btable == nil || !t.opts.IsFlushedBitableCB() {
		return ""
	}
	return t.btable.DebugInfo(dataType)
}
