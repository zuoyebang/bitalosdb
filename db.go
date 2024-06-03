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
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bitask"
	"github.com/zuoyebang/bitalosdb/internal/cache"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotFound            = base.ErrNotFound
	ErrClosed              = errors.New("bitalosdb: closed")
	errMemExceedDelPercent = errors.New("memtable exceed delpercent")
)

type Reader interface {
	Get(key []byte) (value []byte, closer func(), err error)
	NewIter(o *IterOptions) *Iterator
	Close() error
}

type Writer interface {
	Apply(batch *Batch, o *WriteOptions) error
	ApplyBitower(batch *BatchBitower, o *WriteOptions) error
	Delete(key []byte, o *WriteOptions) error
	LogData(data []byte, index int, opts *WriteOptions) error
	Set(key, value []byte, o *WriteOptions) error
	PrefixDeleteKeySet(key []byte, o *WriteOptions) error
}

type DB struct {
	dirname        string
	walDirname     string
	opts           *Options
	optspool       *options.OptionsPool
	cmp            Compare
	equal          Equal
	split          Split
	timeNow        func() time.Time
	compressor     compress.Compressor
	fileLock       io.Closer
	dataDir        vfs.File
	closed         atomic.Bool
	dbState        *statemachine.DbStateMachine
	taskWg         sync.WaitGroup
	taskClosed     chan struct{}
	memFlushTask   *bitask.MemFlushTask
	bpageTask      *bitask.BitpageTask
	bitowers       [consts.DefaultBitowerNum]*Bitower
	cache          cache.ICache
	meta           *metaSet
	flushedBitable atomic.Bool
	flushMemTime   atomic.Int64
}

var _ Reader = (*DB)(nil)
var _ Writer = (*DB)(nil)

func (d *DB) getBitowerIndexByKey(key []byte) int {
	if len(key) > 0 {
		slot := d.opts.KeyHashFunc(key)
		return base.GetBitowerIndex(slot)
	}
	return 0
}

func (d *DB) getBitowerByKey(key []byte) *Bitower {
	index := d.getBitowerIndexByKey(key)
	return d.bitowers[index]
}

func (d *DB) checkBitowerIndex(index int) bool {
	return index >= 0 && index < consts.DefaultBitowerNum
}

func (d *DB) Get(key []byte) ([]byte, func(), error) {
	if d.IsClosed() {
		return nil, nil, ErrClosed
	}

	index := d.getBitowerIndexByKey(key)
	return d.bitowers[index].Get(key)
}

func (d *DB) Exist(key []byte) (bool, error) {
	if d.IsClosed() {
		return false, ErrClosed
	}

	index := d.getBitowerIndexByKey(key)
	if exist := d.bitowers[index].Exist(key); !exist {
		return false, ErrNotFound
	}
	return true, nil
}

func (d *DB) CheckKeySize(key []byte) error {
	if len(key) > consts.MaxKeySize || len(key) == 0 {
		return errors.New("invalid key size")
	}

	return nil
}

func (d *DB) Set(key, value []byte, opts *WriteOptions) error {
	if err := d.CheckKeySize(key); err != nil {
		return err
	}

	b := newBatchBitower(d)
	_ = b.Set(key, value, opts)
	if err := d.ApplyBitower(b, opts); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) PrefixDeleteKeySet(key []byte, opts *WriteOptions) error {
	if err := d.CheckKeySize(key); err != nil {
		return err
	}

	b := newBatchBitower(d)
	_ = b.PrefixDeleteKeySet(key, opts)
	if err := d.ApplyBitower(b, opts); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) Delete(key []byte, opts *WriteOptions) error {
	b := newBatchBitower(d)
	_ = b.Delete(key, opts)
	if err := d.ApplyBitower(b, opts); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) LogData(data []byte, index int, opts *WriteOptions) error {
	b := newBatchBitowerByIndex(d, index)
	_ = b.LogData(data, opts)
	if err := d.ApplyBitower(b, opts); err != nil {
		return err
	}

	b.release()
	return nil
}

func (d *DB) ApplyBitower(batch *BatchBitower, opts *WriteOptions) error {
	if d.IsClosed() {
		return ErrClosed
	}

	if !batch.indexValid {
		return errors.New("index invalid")
	}

	if atomic.LoadUint32(&batch.applied) != 0 {
		return errors.New("batchBitower already applied")
	}

	if batch.db != nil && batch.db != d {
		return errors.Errorf("batchBitower db mismatch: %p != %p", batch.db, d)
	}

	isSync := opts.GetSync()
	if isSync && d.opts.DisableWAL {
		return errors.New("WAL disabled sync unusable")
	}

	if batch.db == nil {
		batch.refreshMemTableSize()
	}

	index := batch.index
	if err := d.bitowers[index].commit.Commit(batch, isSync); err != nil {
		return errors.Errorf("batch apply commit fail index:%d err:%s", index, err)
	}

	return nil
}

func (d *DB) Apply(batch *Batch, opts *WriteOptions) error {
	if d.IsClosed() {
		return ErrClosed
	}

	isSync := opts.GetSync()
	if isSync && d.opts.DisableWAL {
		return errors.New("WAL disabled sync unusable")
	}

	if batch.db != nil && batch.db != d {
		return errors.Errorf("batch db mismatch: %p != %p", batch.db, d)
	}

	for i, tower := range batch.bitowers {
		if tower == nil || atomic.LoadUint32(&tower.applied) != 0 {
			continue
		}

		if tower.db == nil {
			tower.refreshMemTableSize()
		}

		if err := d.bitowers[i].commit.Commit(tower, isSync); err != nil {
			return errors.Errorf("batch apply commit fail index:%d err:%s", i, err)
		}
	}

	return nil
}

type iterAlloc struct {
	dbi                 Iterator
	keyBuf              []byte
	prefixOrFullSeekKey []byte
	merging             mergingIter
	mlevels             [2]mergingIterLevel
}

var iterAllocPool = sync.Pool{
	New: func() interface{} {
		return &iterAlloc{}
	},
}

func (d *DB) newBitowerIter(o *IterOptions) *Iterator {
	if d.IsClosed() {
		return nil
	}

	index := base.GetBitowerIndex(int(o.SlotId))
	bitower := d.bitowers[index]
	rs := bitower.loadReadState()
	seqNum := atomic.LoadUint64(&d.meta.atomic.visibleSeqNum)
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		alloc:               buf,
		cmp:                 d.cmp,
		equal:               d.equal,
		iter:                &buf.merging,
		split:               d.split,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		seqNum:              seqNum,
		index:               index,
		bitower:             bitower,
		readState:           rs,
		readStates:          nil,
	}
	if o != nil {
		dbi.opts = *o
	}
	dbi.opts.Logger = d.opts.Logger

	mlevels := buf.mlevels[:0]
	numMergingLevels := 1

	for i := len(rs.memtables) - 1; i >= 0; i-- {
		mem := rs.memtables[i]
		if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
			continue
		}
		numMergingLevels++
	}

	btreeIters := bitower.newBitreeIter(&dbi.opts)
	numMergingLevels += len(btreeIters)

	if numMergingLevels > cap(mlevels) {
		mlevels = make([]mergingIterLevel, 0, numMergingLevels)
	}

	for i := len(rs.memtables) - 1; i >= 0; i-- {
		mem := rs.memtables[i]
		if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
			continue
		}
		mlevels = append(mlevels, mergingIterLevel{
			iter: mem.newIter(&dbi.opts),
		})
	}

	for i := len(btreeIters) - 1; i >= 0; i-- {
		mlevels = append(mlevels, mergingIterLevel{
			iter: btreeIters[i],
		})
	}

	buf.merging.init(&dbi.opts, dbi.cmp, mlevels...)
	buf.merging.snapshot = seqNum
	return dbi
}

func (d *DB) newAllBitowerIter(o *IterOptions) *Iterator {
	if d.IsClosed() {
		return nil
	}

	var mlevels []mergingIterLevel
	seqNum := atomic.LoadUint64(&d.meta.atomic.visibleSeqNum)
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		alloc:               buf,
		cmp:                 d.cmp,
		equal:               d.equal,
		iter:                &buf.merging,
		split:               d.split,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		seqNum:              seqNum,
		index:               iterAllFlag,
		bitower:             nil,
		readState:           nil,
	}
	if o != nil {
		dbi.opts = *o
	} else {
		dbi.opts = IterAll
	}
	dbi.opts.Logger = d.opts.Logger
	dbi.readStates = make([]*readState, len(d.bitowers))
	for i := range d.bitowers {
		dbi.readStates[i] = d.bitowers[i].loadReadState()
		for j := len(dbi.readStates[i].memtables) - 1; j >= 0; j-- {
			mem := dbi.readStates[i].memtables[j]
			if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
				continue
			}
			mlevels = append(mlevels, mergingIterLevel{
				iter: mem.newIter(&dbi.opts),
			})
		}

		btreeIters := d.bitowers[i].newBitreeIter(&dbi.opts)
		for k := len(btreeIters) - 1; k >= 0; k-- {
			mlevels = append(mlevels, mergingIterLevel{
				iter: btreeIters[k],
			})
		}
	}

	buf.merging.init(&dbi.opts, dbi.cmp, mlevels...)
	buf.merging.snapshot = seqNum
	return dbi
}

func (d *DB) NewIter(o *IterOptions) *Iterator {
	if !o.IsGetAll() {
		return d.newBitowerIter(o)
	} else {
		return d.newAllBitowerIter(o)
	}
}

func (d *DB) NewBatch() *Batch {
	return newBatch(d)
}

func (d *DB) NewBatchBitower() *BatchBitower {
	return newBatchBitower(d)
}

func (d *DB) IsClosed() bool {
	return d.closed.Load()
}

func (d *DB) closeTask() {
	close(d.taskClosed)
	if d.memFlushTask != nil {
		d.memFlushTask.Close()
	}
	if d.bpageTask != nil {
		d.bpageTask.Close()
	}
	d.taskWg.Wait()
}

func (d *DB) Close() (err error) {
	if d.IsClosed() {
		return ErrClosed
	}

	d.opts.Logger.Infof("bitalosdb close start")

	d.closed.Store(true)

	defer func() {
		if d.cache != nil {
			d.cache.Close()
		}
		d.optspool.Close()
		d.opts.Logger.Infof("bitalosdb close finish")
	}()

	d.closeTask()

	for i := range d.bitowers {
		err = utils.FirstError(err, d.bitowers[i].Close())
	}

	err = utils.FirstError(err, d.meta.close())
	err = utils.FirstError(err, d.fileLock.Close())
	err = utils.FirstError(err, d.dataDir.Close())

	return err
}

func (d *DB) Flush() error {
	g, _ := errgroup.WithContext(context.Background())
	for i := range d.bitowers {
		index := i
		g.Go(func() error {
			return d.bitowers[index].Flush()
		})
	}
	return g.Wait()
}

func (d *DB) AsyncFlush() (<-chan struct{}, error) {
	if d.IsClosed() {
		return nil, ErrClosed
	}

	flushed := make(chan struct{})
	flusheds := make([]<-chan struct{}, 0, len(d.bitowers))

	for i := range d.bitowers {
		ch, err := d.bitowers[i].AsyncFlush()
		if err != nil {
			return nil, err
		}
		if ch != nil {
			flusheds = append(flusheds, ch)
		}
	}

	if len(flusheds) == 0 {
		return nil, nil
	}

	go func() {
		for _, ch := range flusheds {
			<-ch
		}
		close(flushed)
	}()

	return flushed, nil
}

func (d *DB) LockTask() {
	d.dbState.LockTask()
}

func (d *DB) UnlockTask() {
	d.dbState.UnlockTask()
}

func (d *DB) Id() int {
	return d.opts.Id
}

func (d *DB) isFlushedBitable() bool {
	if !d.opts.UseBitable {
		return false
	}
	return d.flushedBitable.Load()
}

func (d *DB) setFlushedBitable() {
	if !d.opts.UseBitable {
		return
	}
	d.meta.meta.SetFieldFlushedBitable()
	d.flushedBitable.Store(true)
}

func (d *DB) initFlushedBitable() {
	if !d.opts.UseBitable {
		return
	}
	flushedBitable := d.meta.meta.GetFieldFlushedBitable()
	if flushedBitable == 1 {
		d.flushedBitable.Store(true)
	} else {
		d.flushedBitable.Store(false)
	}
}
