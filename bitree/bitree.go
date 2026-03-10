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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/zuoyebang/bitalosdb/v2/bithash"
	"github.com/zuoyebang/bitalosdb/v2/bitpage"
	"github.com/zuoyebang/bitalosdb/v2/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

type FS vfs.FS
type File vfs.File

var (
	ErrBdbNotExist    = errors.New("bitree bdb not exist")
	ErrBucketNotExist = errors.New("bitree bucket not exist")
)

type Bitree struct {
	opts                    *options.BitreeOptions
	mu                      sync.Mutex
	closed                  bool
	kvSeparateSize          int
	index                   int
	bdb                     *bdb.DB
	txPool                  *TxPool
	bdbPath                 string
	bpagePath               string
	bpage                   *bitpage.Bitpage
	bhashPath               string
	bhashOpen               sync.Once
	bhash                   *bithash.Bithash
	bithashCompactLowerSize int64
	bithashCompactUpperSize int64
	bithashCompactMiniSize  int64
}

func NewBitree(path string, opts *options.BitreeOptions) (*Bitree, error) {
	var err error

	t := &Bitree{
		opts:  opts,
		index: opts.Index,
	}

	defer func() {
		if err != nil {
			if t.bpage != nil {
				_ = t.bpage.Close()
			}
			if t.bhash != nil {
				_ = t.bhash.Close()
			}
			if t.txPool != nil {
				_ = t.txPool.Close()
			}
			if t.bdb != nil {
				_ = t.bdb.Close()
			}
		}
	}()

	opts.BitpageOpts.Index = t.index
	opts.BitpageOpts.BithashDeleteCB = t.bithashDelete
	t.bithashCompactLowerSize = int64(consts.BithashTableSize)
	t.bithashCompactUpperSize = t.bithashCompactLowerSize * 2
	t.bithashCompactMiniSize = t.bithashCompactLowerSize - 1<<20

	if opts.UseBithash {
		t.bhashPath = base.MakeBithashpath(path, t.index)
		t.kvSeparateSize = opts.KvSeparateSize
		if os2.IsExist(t.bhashPath) {
			opts.BithashOpts.Index = t.index
			t.bhash, err = bithash.Open(t.bhashPath, opts.BithashOpts)
			if err != nil {
				return nil, err
			}
		}
	}

	t.bpagePath = base.MakeBitpagepath(path, t.index)
	t.bpage, err = bitpage.Open(t.bpagePath, opts.BitpageOpts)
	if err != nil {
		return nil, err
	}

	opts.BdbOpts.CheckPageFreed = t.checkPageFreed
	opts.BdbOpts.Index = t.index
	t.bdbPath = base.MakeBitreeFilepath(path, t.index)
	if err = t.openBdb(t.bdbPath, opts.BdbOpts); err != nil {
		return nil, err
	}
	if err = t.openTxPool(); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Bitree) Index() int {
	return t.index
}

func (t *Bitree) getInternal(key []byte, khash uint32) ([]byte, bool, func()) {
	pvalue, pexist, pcloser, pkind := t.bitpageGet(key, khash)
	if pexist || pkind == internalKeyKindDelete {
		return pvalue, pexist, pcloser
	}

	return nil, false, nil
}

func (t *Bitree) Get(key []byte, khash uint32) ([]byte, bool, func()) {
	ivalue, iexist, icloser := t.getInternal(key, khash)
	if !iexist {
		return nil, false, nil
	}

	isSeparate, v, iv := base.DecodeInternalValue(ivalue)
	if !isSeparate {
		return v, true, icloser
	}

	if !base.CheckValueBithashValid(iv.UserValue) {
		icloser()
		return nil, false, nil
	}

	fn := binary.LittleEndian.Uint32(iv.UserValue)
	value, putPool, err := t.bithashGetByHash(key, khash, fn)
	if err != nil {
		icloser()
		return nil, false, nil
	}

	return value, true, func() {
		putPool()
		icloser()
	}
}

func (t *Bitree) Exist(key []byte, khash uint32) bool {
	return t.bitpageExist(key, khash)
}

func (t *Bitree) Set(key internalKey, value []byte) error {
	pn, sentinel, pcloser := t.FindKeyPageNum(key.UserKey)
	defer pcloser()
	pageWriter := t.bpage.GetPageWriter(pn, sentinel)
	return pageWriter.Set(key, value)
}

func (t *Bitree) NewBitreeWriter() (*BitreeWriter, error) {
	writer := &BitreeWriter{
		btree:       t,
		bpageWriter: make(map[bitpage.PageNum]*bitpage.PageWriter, 8),
	}

	writer.RefBdbTx()
	return writer, nil
}

func (t *Bitree) NewIter(o *iterOptions) *BitreeIterator {
	iter := &BitreeIterator{
		btree:      t,
		cmp:        t.opts.Cmp,
		opts:       o,
		bpageIters: make(map[bitpage.PageNum]*bitpage.PageIterator, 4),
	}
	rtx := t.txPool.Load()
	iter.bdbCursor = rtx.Bucket().Cursor()
	iter.bdbTx = rtx
	iter.SetBounds(o.GetLowerBound(), o.GetUpperBound())
	return iter
}

func (t *Bitree) NewIters(o *iterOptions) []InternalKKVIterator {
	iters := make([]InternalKKVIterator, 1)
	iters[0] = t.NewIter(o)

	return iters
}

func (t *Bitree) NewKKVIter(o *iterOptions) InternalKKVIterator {
	if o == nil {
		o = &iterOptions{}
	}
	return t.NewIter(o)
}

func (t *Bitree) IsKvSeparate(key internalKey, len int) bool {
	if !t.opts.UseBithash {
		return false
	}

	dataType := kkv.GetKeyDataType(key.UserKey)
	return kkv.IsUseBithash(dataType) && len > t.kvSeparateSize
}

func (t *Bitree) Checkpoint(fs vfs.FS, destDir string) error {
	if err := vfs.Copy(fs, t.bdbPath, fs.PathJoin(destDir, fs.PathBase(t.bdbPath))); err != nil {
		return err
	}

	bitpageDest := path.Join(destDir, fs.PathBase(t.bpagePath))
	if err := t.bpage.Checkpoint(fs, bitpageDest); err != nil {
		return err
	}

	if t.opts.UseBithash && t.bhash != nil {
		bithashDest := fs.PathJoin(destDir, fs.PathBase(t.bhashPath))
		if err := t.bhash.Checkpoint(fs, bithashDest); err != nil {
			return err
		}
	}

	return nil
}

func (t *Bitree) BitreeDebugInfo() string {
	if t.bdb == nil || t.bpage == nil {
		return ""
	}

	buf := new(bytes.Buffer)
	rtx := t.txPool.Load()
	bdbIter := rtx.Bucket().Cursor()
	defer rtx.Unref(true)

	for bdbKey, bdbValue := bdbIter.First(); bdbKey != nil; bdbKey, bdbValue = bdbIter.Next() {
		pn := bitpage.PageNum(utils.BytesToUint32(bdbValue))
		pageInfo := t.bpage.GetPageDebugInfo(pn)
		fmt.Fprintf(buf, "bitpage%d-%s: pn:%d ct:%s ut:%s state:%s\n",
			t.index, bdbKey, pn,
			utils.FmtUnixMillTime(pageInfo.Ct),
			utils.FmtUnixMillTime(pageInfo.Ut),
			pageInfo.State,
		)
	}

	return buf.String()
}

func (t *Bitree) BitreeDirDiskInfo(buf *bytes.Buffer) {
	if t.bpage == nil {
		return
	}

	dirname := t.bpage.GetDirname()
	fmt.Fprintf(buf, "dir-bitpage%d: dirSize=%s\n", t.index, utils.FmtSize(os2.GetDirSize(dirname)))
}

func (t *Bitree) BithashDiskInfo() uint64 {
	if t.bhash == nil {
		return 0
	}

	dirname := t.bhash.GetDirname()
	return uint64(os2.GetDirSize(dirname))
}

func (t *Bitree) BitreeDiskInfo() uint64 {
	if t.bpage == nil {
		return 0
	}

	dirname := t.bpage.GetDirname()
	return uint64(os2.GetDirSize(dirname))
}

func (t *Bitree) GetBitpageCount() int {
	return t.bpage.GetPageCount()
}

func (t *Bitree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

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

func (t *Bitree) RemoveDir() error {
	if err := os2.RemoveDir(t.bpagePath); err != nil {
		return err
	}
	if err := os2.RemoveDir(t.bhashPath); err != nil {
		return err
	}
	if os2.IsExist(t.bdbPath) {
		if err := os.Remove(t.bdbPath); err != nil {
			return err
		}
	}
	return nil
}
