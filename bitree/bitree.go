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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path"

	"github.com/zuoyebang/bitalosdb/bitable"
	"github.com/zuoyebang/bitalosdb/bithash"
	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type FS vfs.FS
type File vfs.File

var (
	ErrBdbNotExist    = errors.New("bitree bdb not exist")
	ErrBucketNotExist = errors.New("bitree bucket not exist")
	ErrBitreeIterNil  = errors.New("bitree newIter nil")
)

type Bitree struct {
	opts           *base.BitreeOptions
	closed         bool
	kvSeparateSize int
	index          int
	bdb            *bdb.DB
	txPool         *TxPool
	bpage          *bitpage.Bitpage
	bhash          *bithash.Bithash
	btable         *bitable.Bitable
	writer         *BitreeWriter
	bpageTask      *bitpageTask
	dbState        *statemachine.DbStateMachine

	lastCompactBithashTime uint64
}

func NewBitree(path string, opts *base.BitreeOptions) (*Bitree, error) {
	var err error

	t := &Bitree{
		opts:           opts,
		index:          opts.Index,
		kvSeparateSize: opts.KvSeparateSize,
		dbState:        opts.DbState,
	}

	defer func() {
		if err != nil {
			if t.bpage != nil {
				_ = t.bpage.Close()
			}
			if t.bhash != nil {
				_ = t.bhash.Close()
			}
			if t.btable != nil {
				_ = t.btable.Close()
			}
			if t.txPool != nil {
				_ = t.txPool.Close()
			}
			if t.bdb != nil {
				_ = t.bdb.Close()
			}
		}
	}()

	opts.BitpageOpts.CheckExpireCB = t.KvCheckExpire
	opts.BitpageOpts.BithashDeleteCB = t.bithashDelete
	opts.BitpageOpts.BitableDeleteCB = t.bitableDelete
	opts.BitpageOpts.PushTaskCB = t.pushTaskData
	opts.BitpageOpts.Index = t.index
	bitpagePath := consts.MakeBitpagePath(path, t.index)
	t.bpage, err = bitpage.Open(bitpagePath, opts.BitpageOpts)
	if err != nil {
		return nil, err
	}

	opts.BdbOpts.PushTaskCB = t.pushTaskData
	opts.BdbOpts.CheckPageSplitted = t.checkPageSplitted
	opts.BdbOpts.Index = t.index
	bitreePath := consts.MakeBitreeFilePath(path, t.index)
	if err = t.openBdb(bitreePath, opts.BdbOpts); err != nil {
		return nil, err
	}
	if err = t.openTxPool(); err != nil {
		return nil, err
	}

	if opts.UseBithash {
		opts.BithashOpts.Index = t.index
		bithashPath := consts.MakeBithashPath(path, t.index)
		t.kvSeparateSize = opts.KvSeparateSize
		t.bhash, err = bithash.Open(bithashPath, opts.BithashOpts)
		if err != nil {
			return nil, err
		}
	}

	if opts.UseBitable {
		opts.BitableOpts.Index = t.index
		opts.BitableOpts.CheckExpireCB = t.KvCheckExpire
		bitablePath := consts.MakeBitablePath(path, t.index)
		t.btable, err = bitable.Open(t.bhash, bitablePath, opts.BitableOpts)
		if err != nil {
			return nil, err
		}
	}

	t.runBitpageTask()

	return t, nil
}

func (t *Bitree) CloseTask() {
	if t.bpage != nil {
		t.bpageTask.close()
	}
}

func (t *Bitree) Close() error {
	if t.closed {
		return nil
	}

	t.closed = true

	if t.bpage != nil {
		t.bpageTask.wg.Wait()
		_ = t.bpage.Close()
		t.bpage = nil
	}

	if t.bhash != nil {
		_ = t.bhash.Close()
		t.bhash = nil
	}

	if t.btable != nil {
		_ = t.btable.Close()
		t.btable = nil
	}

	if t.txPool != nil {
		_ = t.txPool.Close()
		t.txPool = nil
	}

	if t.bdb != nil {
		_ = t.bdb.Close()
		t.bdb = nil
	}

	return nil
}

func (t *Bitree) getInternal(key []byte, khash uint32) ([]byte, bool, func()) {
	pvalue, pexist, pcloser, pkind := t.bitpageGet(key, khash)
	if pexist || pkind == base.InternalKeyKindDelete {
		return pvalue, pexist, pcloser
	}

	tvalue, tcloser, terr := t.bitableGet(key)
	if terr == base.ErrNotFound {
		return nil, false, nil
	}
	return tvalue, true, func() {
		_ = tcloser.Close()
	}
}

func (t *Bitree) Get(key []byte, khash uint32) ([]byte, bool, func()) {
	var err error
	var value []byte
	var putPool func() = nil

	ivalue, iexist, icloser := t.getInternal(key, khash)
	if !iexist {
		return nil, false, nil
	}

	iv := base.DecodeInternalValue(ivalue)
	if iv.Kind() == base.InternalKeyKindSetBithash {
		if !base.CheckValueValidByKeySetBithash(iv.UserValue) {
			icloser()
			return nil, false, nil
		}

		fn := binary.LittleEndian.Uint32(iv.UserValue)
		value, putPool, err = t.bithashGetByHash(key, khash, fn)
		if err != nil {
			icloser()
			return nil, false, nil
		}
	} else {
		value = iv.UserValue
	}

	if putPool != nil {
		return value, true, func() {
			icloser()
			putPool()
		}
	} else {
		return value, true, icloser
	}
}

func (t *Bitree) Exist(key []byte, khash uint32) bool {
	_, iexist, icloser := t.getInternal(key, khash)
	if icloser != nil {
		icloser()
	}

	return iexist
}

func (t *Bitree) newBitreeIter(o *base.IterOptions) *BitreeIterator {
	iter := &BitreeIterator{
		btree:      t,
		cmp:        t.opts.Cmp,
		ops:        o,
		bdbIter:    t.NewBdbIter(),
		bpageIters: make(map[bitpage.PageNum]*bitpage.PageIterator, 1<<2),
	}
	iter.SetBounds(o.GetLowerBound(), o.GetUpperBound())
	return iter
}

func (t *Bitree) NewIters(o *base.IterOptions) (iters []base.InternalIterator) {
	it := t.newBitreeIter(o)
	if it != nil {
		iters = append(iters, it)
	}

	if t.btable != nil && t.opts.IsFlushedBitableCB() {
		itBt := t.newBitableIter(o)
		if itBt != nil {
			iters = append(iters, itBt)
		}
	}

	return iters
}

func (t *Bitree) IsKvSeparate(len int) bool {
	if !t.opts.UseBithash {
		return false
	}

	return len > t.kvSeparateSize
}

func (t *Bitree) MemFlushStart() error {
	bw := &BitreeWriter{
		btree: t,
	}

	if t.opts.UseBithash {
		w, err := t.bhash.FlushStart()
		if err != nil {
			return err
		}
		bw.bhashWriter = w
	}

	bw.bpageWriter = make(map[bitpage.PageNum]*bitpage.PageWriter, 1<<4)
	t.writer = bw
	return nil
}

func (t *Bitree) MemFlushFinish() error {
	if t.writer == nil {
		return nil
	}

	if t.writer.bhashWriter != nil {
		if err := t.bhash.FlushFinish(t.writer.bhashWriter); err != nil {
			return err
		}
		t.writer.bhashWriter = nil
	}

	for pn, pageWriter := range t.writer.bpageWriter {
		if err := pageWriter.FlushFinish(); err != nil {
			t.opts.Logger.Errorf("bitree pageWriter FlushFinish fail pn:%d err:%s", pn, err.Error())
			continue
		}
		pageWriter.UpdateMetaTimestamp()
		if pageWriter.MaybePageFlush(t.opts.BitpageFlushSize) {
			task := &base.BitpageTaskData{
				Event:    base.BitpageEventFlush,
				Pn:       uint32(pn),
				Sentinel: pageWriter.Sentinel,
			}
			t.pushTaskData(task)
		}
	}

	t.writer.bpageWriter = nil
	t.writer = nil
	return nil
}

func (t *Bitree) BatchUpdate(keys []base.InternalKey, values [][]byte) error {
	if len(keys) != len(values) || len(keys) == 0 {
		return nil
	}

	rtx := t.txPool.Load()
	defer func() {
		rtx.Unref(true)
	}()

	bkt := rtx.Bucket()
	if bkt == nil {
		return ErrBucketNotExist
	}

	cursor := bkt.Cursor()
	for i := 0; i < len(keys); i++ {
		pn, sentinel := t.findKeyPageNumByCursor(keys[i].UserKey, cursor)
		if pn == nilPageNum {
			t.opts.Logger.Errorf("bitree BatchUpdate findKeyPageNum nil index:%d key:%s", t.index, keys[i].String())
			continue
		}

		if err := t.writer.set(keys[i], values[i], pn, sentinel); err != nil {
			t.opts.Logger.Errorf("bitree BatchUpdate set err index:%d key:%s err:%s", t.index, keys[i].String(), err)
			continue
		}
	}

	return nil
}

func (t *Bitree) Checkpoint(destDir, dbDir string) error {
	bitpageDest := path.Join(destDir, t.opts.FS.PathBase(consts.MakeBitpagePath(dbDir, t.index)))
	if err := t.bpage.Checkpoint(bitpageDest); err != nil {
		return err
	}

	if t.opts.UseBithash {
		bithashDest := path.Join(destDir, t.opts.FS.PathBase(consts.MakeBithashPath(dbDir, t.index)))
		if err := t.bhash.Checkpoint(bithashDest); err != nil {
			return err
		}
	}

	if t.btable != nil && t.opts.IsFlushedBitableCB() {
		bitableDest := path.Join(destDir, t.opts.FS.PathBase(consts.MakeBitablePath(dbDir, t.index)))
		if err := t.btable.Checkpoint(bitableDest); err != nil {
			return err
		}
	}

	return nil
}

func (t *Bitree) BitreeDebugInfo(dataType string) string {
	if t.bdb == nil || t.bpage == nil {
		return ""
	}

	buf := new(bytes.Buffer)

	bdbIter := t.NewBdbIter()
	defer bdbIter.Close()

	for bdbKey, bdbValue := bdbIter.First(); bdbKey != nil; bdbKey, bdbValue = bdbIter.Next() {
		pn := bitpage.PageNum(utils.BytesToUint32(bdbValue))
		pageInfo := t.bpage.GetPageDebugInfo(pn)
		fmt.Fprintf(buf, "%s-bitpage%d-%s: pn:%d ct:%s ut:%s split:%d\n",
			dataType, t.index, bdbKey.String(), pn,
			utils.FmtUnixMillTime(pageInfo.Ct),
			utils.FmtUnixMillTime(pageInfo.Ut),
			pageInfo.SplitState,
		)
	}

	if t.opts.UseBlockCompress {
		fmt.Fprintf(buf, "%s-bitpage%d-blockCache: %s\n", dataType, t.index, t.bpage.GetCacheMetrics())
	}

	return buf.String()
}

func (t *Bitree) DeleteBithashKey(v []byte) {
	if v == nil {
		return
	}

	dv := base.DecodeInternalValue(v)
	if dv.Kind() == base.InternalKeyKindSetBithash && base.CheckValueValidByKeySetBithash(dv.UserValue) {
		fn := binary.LittleEndian.Uint32(dv.UserValue)
		_ = t.bithashDelete(fn)
	}
}

func (t *Bitree) KvCheckExpire(key, value []byte) bool {
	dv := base.DecodeInternalValue(value)
	if dv.Kind() == base.InternalKeyKindSetBithash {
		isExpire := false
		if len(dv.UserValue) == 12 {
			tm := binary.LittleEndian.Uint64(dv.UserValue[4:12])
			if tm > 0 {
				_, nowTime := t.opts.KvTimestampFunc(nil, 2)
				if tm <= nowTime {
					isExpire = true
				}
			}
		}
		if !isExpire {
			isExpire = t.opts.KvCheckExpire(key, nil)
		}
		if isExpire {
			fn := binary.LittleEndian.Uint32(dv.UserValue)
			_ = t.bithashDelete(fn)
		}
		return isExpire
	}

	return t.opts.KvCheckExpire(key, dv.UserValue)
}
