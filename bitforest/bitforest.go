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

package bitforest

import (
	"path"
	"runtime/debug"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/bitree"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/cache"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/manifest"
	"github.com/zuoyebang/bitalosdb/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type Bitforest struct {
	treeNum        int
	trees          []*bitree.Bitree
	opts           *base.BitforestOptions
	dirname        string
	meta           *manifest.Metadata
	writer         *Writer
	flushedBitable atomic.Bool
	flushBufSize   int
	flushStat      *FlushStat
	cache          cache.ICache
	closed         atomic.Bool
	dbState        *statemachine.DbStateMachine
}

func NewBitforest(
	dirname string, meta *manifest.Metadata, optsPool *base.OptionsPool,
) (*Bitforest, error) {
	opts := optsPool.Clone(base.BitforestOptionsType).(*base.BitforestOptions)

	bf := &Bitforest{
		dirname: dirname,
		meta:    meta,
		opts:    opts,
		cache:   opts.Cache,
		dbState: opts.DbState,
	}

	bf.treeNum = consts.BitforestDefaultTreeNum
	bf.flushBufSize = consts.BitforestDefaultFlushBufSize
	bf.InitFlushedBitable()

	bf.trees = make([]*bitree.Bitree, bf.treeNum)
	for i := 0; i < bf.treeNum; i++ {
		tree, err := bf.openBitree(i, optsPool)
		if err != nil {
			return nil, err
		}

		bf.trees[i] = tree
	}

	bf.writer = NewWriter(bf)
	bf.flushStat = NewFlushStat()

	return bf, nil
}

func (bf *Bitforest) openBitree(index int, optsPool *base.OptionsPool) (*bitree.Bitree, error) {
	brOpts := optsPool.Clone(base.BitreeOptionsType).(*base.BitreeOptions)
	brOpts.IsFlushedBitableCB = bf.IsFlushedBitable
	brOpts.Index = index
	btree, err := bitree.NewBitree(bf.dirname, brOpts)
	if err != nil {
		return nil, err
	}

	bf.opts.Logger.Infof("open bitree success tree:%d", index)
	return btree, nil
}

func (bf *Bitforest) getKeyBitreeIndex(key []byte) int {
	h := bf.opts.KeyHashFunc(key)
	return bf.getBitreeIndex(h)
}

func (bf *Bitforest) getBitreeIndex(h int) int {
	return h % bf.treeNum
}

func (bf *Bitforest) getBitree(index int) *bitree.Bitree {
	return bf.trees[index]
}

func (bf *Bitforest) Get(key []byte) ([]byte, bool, func()) {
	defer func() {
		if r := recover(); r != any(nil) {
			bf.opts.Logger.Errorf("bitforest: Get panic key:%s err:%v stack:%s", key, r, string(debug.Stack()))
		}
	}()

	khash := hash.Crc32(key)

	useCache := false
	if bf.cache != nil && !bf.dbState.IsMemFlushing() {
		useCache = true
		ivCache, ivCloser, ivExist := bf.cache.Get(key, khash)
		if ivExist {
			return ivCache, true, ivCloser
		}
	}

	index := bf.getKeyBitreeIndex(key)
	v, vexist, vcloser := bf.getBitree(index).Get(key, khash)
	if vexist {
		if useCache && v != nil {
			bf.cache.Set(key, v, khash)
		}

		return v, true, vcloser
	}
	return nil, false, nil
}

func (bf *Bitforest) Exist(key []byte) bool {
	defer func() {
		if r := recover(); r != any(nil) {
			bf.opts.Logger.Errorf("bitforest: Exist panic key:%s err:%v stack:%s", key, r, string(debug.Stack()))
		}
	}()

	khash := hash.Crc32(key)

	if bf.cache != nil && !bf.dbState.IsMemFlushing() {
		_, ivCloser, ivExist := bf.cache.Get(key, khash)
		if ivCloser != nil {
			ivCloser()
		}
		return ivExist
	}

	index := bf.getKeyBitreeIndex(key)
	return bf.getBitree(index).Exist(key, khash)
}

func (bf *Bitforest) Close() (err error) {
	for i := 0; i < bf.treeNum; i++ {
		close(bf.writer.commitClosedCh[i])
		bf.trees[i].CloseTask()
	}
	bf.writer.commits.Wait()

	for i := 0; i < bf.treeNum; i++ {
		if err = bf.trees[i].Close(); err != nil {
			bf.opts.Logger.Infof("bitree close fail index:%d err:%s", i, err)
		}
	}

	return err
}

func (bf *Bitforest) SetClosed() {
	bf.closed.Store(true)
}

func (bf *Bitforest) IsClosed() bool {
	return bf.closed.Load()
}

func (bf *Bitforest) GetWriter() *Writer {
	return bf.writer
}

func (bf *Bitforest) Checkpoint(destDir string) error {
	for i := 0; i < bf.treeNum; i++ {
		srcPath := consts.MakeBitreeFilePath(bf.dirname, i)
		destPath := path.Join(destDir, bf.opts.FS.PathBase(srcPath))
		if err := vfs.Copy(bf.opts.FS, srcPath, destPath); err != nil {
			return err
		}

		if err := bf.getBitree(i).Checkpoint(destDir, bf.dirname); err != nil {
			return err
		}
	}
	return nil
}

func (bf *Bitforest) IsFlushedBitable() bool {
	if !bf.opts.UseBitable {
		return false
	}
	return bf.flushedBitable.Load()
}

func (bf *Bitforest) InitFlushedBitable() {
	if !bf.opts.UseBitable {
		return
	}
	flushedBitable := bf.meta.GetFieldFlushedBitable()
	if flushedBitable == 1 {
		bf.flushedBitable.Store(true)
	} else {
		bf.flushedBitable.Store(false)
	}
}

func (bf *Bitforest) SetFlushedBitable() {
	if !bf.opts.UseBitable {
		return
	}
	bf.meta.SetFieldFlushedBitable()
	bf.flushedBitable.Store(true)
}
