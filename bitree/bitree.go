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
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type FS vfs.FS
type File vfs.File

var (
	ErrBdbNotExist    = errors.New("bitree bdb not exist")
	ErrBucketNotExist = errors.New("bitree bucket not exist")
)

type Bitree struct {
	opts           *options.BitreeOptions
	closed         bool
	kvSeparateSize int
	index          int
	bdb            *bdb.DB
	txPool         *TxPool
	bpage          *bitpage.Bitpage
	bhash          *bithash.Bithash
	btable         *bitable.Bitable
	dbState        *statemachine.DbStateMachine
}

func NewBitree(path string, opts *options.BitreeOptions) (*Bitree, error) {
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
	opts.BitpageOpts.Index = t.index
	bitpagePath := base.MakeBitpagepath(path, t.index)
	t.bpage, err = bitpage.Open(bitpagePath, opts.BitpageOpts)
	if err != nil {
		return nil, err
	}

	opts.BdbOpts.CheckPageSplitted = t.checkPageSplitted
	opts.BdbOpts.Index = t.index
	bitreePath := base.MakeBitreeFilepath(path, t.index)
	if err = t.openBdb(bitreePath, opts.BdbOpts); err != nil {
		return nil, err
	}
	if err = t.openTxPool(); err != nil {
		return nil, err
	}

	if opts.UseBithash {
		opts.BithashOpts.Index = t.index
		bithashPath := base.MakeBithashpath(path, t.index)
		t.kvSeparateSize = opts.KvSeparateSize
		t.bhash, err = bithash.Open(bithashPath, opts.BithashOpts)
		if err != nil {
			return nil, err
		}
	}

	if opts.UseBitable {
		opts.BitableOpts.Index = t.index
		opts.BitableOpts.CheckExpireCB = t.KvCheckExpire
		bitablePath := base.MakeBitablepath(path, t.index)
		t.btable, err = bitable.Open(t.bhash, bitablePath, opts.BitableOpts)
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

func (t *Bitree) Index() int {
	return t.index
}

func (t *Bitree) Close() error {
	if t.closed {
		return nil
	}

	t.closed = true

	if t.bpage != nil {
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

func (t *Bitree) newBitreeIter(o *options.IterOptions) *BitreeIterator {
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

func (t *Bitree) NewIters(o *options.IterOptions) []base.InternalIterator {
	iters := make([]base.InternalIterator, 1)
	iters[0] = t.newBitreeIter(o)

	if t.btable != nil && t.opts.IsFlushedBitableCB() {
		bitableIter := t.newBitableIter(o)
		if bitableIter != nil {
			iters = append(iters, bitableIter)
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

func (t *Bitree) Checkpoint(fs vfs.FS, destDir, dbDir string) error {
	bitpageDest := path.Join(destDir, fs.PathBase(base.MakeBitpagepath(dbDir, t.index)))
	if err := t.bpage.Checkpoint(fs, bitpageDest); err != nil {
		return err
	}

	if t.opts.UseBithash {
		bithashDest := fs.PathJoin(destDir, fs.PathBase(base.MakeBithashpath(dbDir, t.index)))
		if err := t.bhash.Checkpoint(fs, bithashDest); err != nil {
			return err
		}
	}

	if t.btable != nil && t.opts.IsFlushedBitableCB() {
		bitableDest := fs.PathJoin(destDir, fs.PathBase(base.MakeBitablepath(dbDir, t.index)))
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

func (t *Bitree) GetBitpageCount() int {
	return t.bpage.GetPageCount()
}

func (t *Bitree) ManualFlushBitpage() {
	t.MaybeScheduleFlush(true)
}
