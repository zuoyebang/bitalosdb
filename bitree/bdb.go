// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitree

import (
	"bytes"
	"sync"

	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

func (t *Bitree) openBdb(path string, opts *options.BdbOptions) error {
	db, err := bdb.Open(path, opts)
	if err != nil {
		return err
	}

	var tx *bdb.Tx
	tx, err = db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	t.bdb = db
	bucket := tx.Bucket(consts.BdbBucketName)
	if bucket != nil {
		_ = tx.Rollback()
		return nil
	}

	bucket, err = tx.CreateBucket(consts.BdbBucketName)
	if err != nil {
		return err
	}

	var pn bitpage.PageNum
	pn, err = t.bpage.NewPage()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = t.bpage.FreePage(pn, false)
		}
	}()

	if err = bucket.Put(consts.BdbMaxKey, pn.ToByte()); err != nil {
		return err
	}

	err = tx.Commit()
	return err
}

func (t *Bitree) bdbSet(key, value []byte) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bucket := tx.Bucket(consts.BdbBucketName)
		if bucket == nil {
			return bdb.ErrBucketNotFound
		}
		return bucket.Put(key, value)
	})
	return err
}

func (t *Bitree) bdbDelete(key []byte) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bucket := tx.Bucket(consts.BdbBucketName)
		if bucket == nil {
			return bdb.ErrBucketNotFound
		}
		return bucket.Delete(key)
	})
	return err
}

func (t *Bitree) NewBdbIter() *bdb.BdbIterator {
	rtx := t.txPool.Load()
	return t.bdb.NewIter(rtx)
}

func (t *Bitree) FindKeyPageNum(key []byte) (bitpage.PageNum, []byte, func()) {
	rtx := t.txPool.Load()
	rtxCloser := func() {
		rtx.Unref(true)
	}

	bkt := rtx.Bucket()
	if bkt == nil {
		t.opts.Logger.Errorf("FindKeyPageNum Bucket is nil index:%d", t.index)
		return nilPageNum, nil, rtxCloser
	}

	pn, sentinel := t.findKeyPageNum(key, bkt.Cursor())
	return pn, sentinel, rtxCloser
}

func (t *Bitree) findKeyPageNum(key []byte, cursor *bdb.Cursor) (bitpage.PageNum, []byte) {
	sentinel, v := cursor.Seek(key)
	if v == nil {
		return nilPageNum, nil
	}
	return bitpage.PageNum(utils.BytesToUint32(v)), sentinel
}

func (t *Bitree) findPrefixDeleteKeyPageNums(
	key []byte, cursor *bdb.Cursor,
) (pns []bitpage.PageNum, sentinels [][]byte) {
	sentinel, v := cursor.Seek(key)
	if v == nil {
		return
	}

	sentinels = append(sentinels, sentinel)
	pns = append(pns, bitpage.PageNum(utils.BytesToUint32(v)))

	if bytes.Equal(sentinel, consts.BdbMaxKey) {
		return
	}

	keyPrefixDelete := t.opts.KeyPrefixDeleteFunc(key)
	if keyPrefixDelete != t.opts.KeyPrefixDeleteFunc(sentinel) {
		return
	}

	for {
		nk, nv := cursor.Next()
		if nk == nil || nv == nil {
			break
		}

		sentinels = append(sentinels, nk)
		pns = append(pns, bitpage.PageNum(utils.BytesToUint32(nv)))

		if bytes.Equal(sentinel, consts.BdbMaxKey) || keyPrefixDelete != t.opts.KeyPrefixDeleteFunc(nk) {
			break
		}
	}

	return
}

func (t *Bitree) BdbDiskSize() int64 {
	return t.bdb.DiskSize()
}

func (t *Bitree) BdbPath() string {
	return t.bdb.Path()
}

func (t *Bitree) BdbUpdate() bool {
	_ = t.bdb.Update(func(tx *bdb.Tx) error { return nil })
	return t.txPool.Update()
}

type TxPool struct {
	lock sync.RWMutex
	rTx  *bdb.ReadTx
	bdb  *bdb.DB
}

func (t *Bitree) openTxPool() error {
	if t.bdb == nil {
		return ErrBdbNotExist
	}

	tx, err := t.bdb.Begin(false)
	if err != nil {
		return err
	}

	bkt := tx.Bucket(consts.BdbBucketName)
	if bkt == nil {
		return ErrBucketNotExist
	}

	rt := &bdb.ReadTx{}
	rt.Init(tx, bkt, t.bdb)
	t.txPool = &TxPool{
		rTx: rt,
		bdb: t.bdb,
	}

	return nil
}

func (tp *TxPool) Load() *bdb.ReadTx {
	tp.lock.RLock()
	rTx := tp.rTx
	rTx.Ref()
	tp.lock.RUnlock()
	return rTx
}

func (tp *TxPool) Update() bool {
	tx, err := tp.bdb.Begin(false)
	if err != nil {
		return false
	}

	bkt := tx.Bucket(consts.BdbBucketName)
	if bkt == nil {
		return false
	}

	rt := &bdb.ReadTx{}
	rt.Init(tx, bkt, tp.bdb)

	tp.lock.Lock()
	prev := tp.rTx
	tp.rTx = rt
	tp.lock.Unlock()

	if prev != nil {
		prev.Unref(true)
	}

	return true
}

func (tp *TxPool) Close() error {
	tp.lock.RLock()
	rTx := tp.rTx
	tp.lock.RUnlock()
	return rTx.Unref(false)
}
