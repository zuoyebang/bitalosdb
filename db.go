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
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/v2/bitree"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/list2"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/zuoyebang/bitalosdb/v2/internal/vectormap"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

var (
	ErrNotFound      = base.ErrNotFound
	ErrWrongType     = base.ErrWrongType
	ErrClosed        = errors.New("bitalosdb: closed")
	ErrNoSlotId      = errors.New("bitalosdb: not set slotId")
	ErrSubKeySize    = errors.New("invalid subKey size")
	ErrKeySize       = errors.New("invalid key size")
	ErrSiKeySize     = errors.New("invalid siKey size")
	ErrValueSize     = errors.New("invalid value size")
	ErrZsetMemberNil = errors.New("zset member is nil")
	ErrDKExist       = errors.New("bitalosdb: distributed key exist")
	ErrDKShardNum    = errors.New("invalid shard num")
	ErrBitUnmarshal  = errors.New("ERR bitmap unmarshal fail")
	ErrBitMarshal    = errors.New("ERR bitmap marshal fail")
	ErrBitupleNil    = errors.New("bituple is nil")
)

const (
	slotBitupleNotCreated uint8 = 0
	slotBitupleCreated    uint8 = 1
)

type DB struct {
	dirname      string
	opts         *Options
	optspool     *options.OptionsPool
	cmp          Compare
	equal        Equal
	compressor   compress.Compressor
	fileLock     io.Closer
	dataDir      vfs.File
	bpageTask    *bitask.BitpageTask
	memFlushTask *bitask.MemTableFlushTask
	vmFlushTask  *bitask.VmTableFlushTask
	taskWg       sync.WaitGroup
	taskClosed   chan struct{}
	closed       atomic.Bool
	ckpStatus    atomic.Int32
	dbState      *statemachine.DbStateMachine
	meta         *metadata
	eliTask      *eliminateTask
	memFlusher   *memFlushWriter
	bituples     [metaSlotNum + 1]*slotBituple
	memShard     [consts.MemIndexShardNum + 1]*memTableShard
	vmShard      [consts.MemIndexShardNum]*vmTableShard
	vmPool       *vmapPool
	flushStats   *dbFlushStats
}

type slotBituple struct {
	d       *DB
	mu      sync.Mutex
	created bool
	btSafe  atomic.Pointer[Bituple]
}

func newSlotBituple(d *DB) *slotBituple {
	return &slotBituple{
		d: d,
	}
}

func (sb *slotBituple) createBituple(i int, status uint8) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if sb.created {
		return nil
	}

	if err := sb.d.newBituple(i, status); err != nil {
		return err
	}

	sb.created = true
	return nil
}

func (sb *slotBituple) free(i int) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if sb.created {
		sb.d.bituples[i].btSafe.Store(nil)
		sb.d.meta.writeSlotBituple(i, slotBitupleNotCreated)
		sb.created = false
	}
}

func (d *DB) getBitupleSafe(i int) *Bituple {
	return d.bituples[i].btSafe.Load()
}

func (d *DB) getBitupleRead(i uint16) (*Bituple, error) {
	t := d.bituples[i].btSafe.Load()
	if t != nil {
		return t, nil
	}
	return nil, ErrNotFound
}

func (d *DB) getBitupleWrite(i int) (*Bituple, error) {
	bt := d.getBitupleSafe(i)
	if bt != nil {
		return bt, nil
	}

	if err := d.bituples[i].createBituple(i, slotBitupleNotCreated); err != nil {
		return nil, err
	}

	bt = d.getBitupleSafe(i)
	if bt != nil {
		return bt, nil
	}

	return nil, ErrBitupleNil
}

func (d *DB) getBitreeRead(i uint16) *bitree.Bitree {
	bt := d.getBitupleSafe(int(i))
	if bt != nil {
		return bt.kkv
	}
	return nil
}

func (d *DB) newBituple(idx int, status uint8) (err error) {
	opts := d.optspool.CloneBitupleOptions()
	opts.Index = idx
	opts.DisableStoreKey = d.opts.DisableStoreKey
	opts.VectorTableHashSize = d.opts.VectorTableHashSize
	opts.VectorTableGCThreshold = d.opts.CompactInfo.VtGCThreshold
	opts.TupleCount = d.opts.VectorTableCount
	opts.GetNextSeqNum = d.meta.getNextSeqNum
	opts.CheckExpireFunc = d.checkExpireInVmTable
	b := &Bituple{
		db:         d,
		opts:       opts,
		index:      opts.Index,
		dbPath:     d.dirname,
		tupleCount: d.meta.tupleCount,
	}
	btreeOpts := b.opts.BitreeOpts
	btreeOpts.Index = b.index
	btreeOpts.BitpageOpts.Options = btreeOpts.BitpageOpts.Options.Clone()
	if b.isEliminate() {
		btreeOpts.BitpageOpts.UseVi = false
	} else {
		btreeOpts.BitpageOpts.UseVi = true
		b.dirname = base.MakeBituplepath(b.dbPath, opts.Index)
		if err = opts.FS.MkdirAll(b.dirname, 0755); err != nil {
			return err
		}
		if err = b.newTuples(); err != nil {
			return err
		}
	}

	b.kkv, err = bitree.NewBitree(b.dbPath, btreeOpts)
	if err != nil {
		return err
	}

	if status == slotBitupleNotCreated {
		d.meta.writeSlotBituple(idx, slotBitupleCreated)
	}

	d.bituples[idx].btSafe.Store(b)

	d.opts.Logger.Infof("newBituple(%d) success", idx)
	return nil
}

func (d *DB) newVmTable() {
	poolSize := consts.VmTablePoolSize
	vmSize := d.opts.VmTableSize
	vmHashSize := uint32(vmSize / 64)

	d.vmPool = &vmapPool{
		size:   poolSize,
		vmSize: vmSize,
		logger: d.opts.Logger,
		recvCh: make(chan *vectormap.VectorMap, poolSize),
		queue:  list2.NewQueue(),
		opts: &vectormap.VectorMapOptions{
			MaxMem:   vmSize,
			HashSize: vmHashSize,
			StoreKey: !d.opts.DisableStoreKey,
			Shards:   consts.VmTableShardNum,
			Logger:   d.opts.Logger,
		},
	}

	for i := 0; i < d.vmPool.size; i++ {
		vm := d.vmPool.newVectorMap()
		d.vmPool.queue.Push(vm)
	}

	d.vmPool.worker()

	for i := range d.vmShard {
		d.vmShard[i] = newVmTableShard(d, i)
	}

	d.opts.Logger.Infof("vmTable new success vmSize:%d vmHashSize:%d poolSize:%d", vmSize, vmHashSize, poolSize)
}

func (d *DB) newMemTable() {
	for i := range d.memShard {
		d.memShard[i] = newMemTableShard(d, i)
	}
}

func (d *DB) getMemShard(slotId uint16) *memTableShard {
	index := consts.KKVSlotToShard(int(slotId))
	return d.memShard[index]
}

func (d *DB) getVmShard(slotId uint16) *vmTableShard {
	index := consts.KVSlotToShard(int(slotId))
	return d.vmShard[index]
}

func (d *DB) Close() (err error) {
	if d.IsClosed() {
		return ErrClosed
	}

	d.opts.Logger.Infof("bitalosdb close start")
	d.closed.Store(true)

	close(d.taskClosed)
	if d.eliTask != nil {
		d.eliTask.close()
	}
	if d.vmFlushTask != nil {
		d.vmFlushTask.Close()
	}
	if d.memFlushTask != nil {
		d.memFlushTask.Close()
	}
	if d.bpageTask != nil {
		d.bpageTask.Close()
	}
	d.taskWg.Wait()

	for i := range d.vmShard {
		d.vmShard[i].close()
	}
	d.vmPool.close()
	for i := range d.memShard {
		d.memShard[i].close()
	}

	var wg sync.WaitGroup
	for i := range d.bituples {
		bt := d.getBitupleSafe(i)
		if bt != nil {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if err = bt.Close(); err != nil {
					d.opts.Logger.Errorf("bitalosdb close bituple(%d) err:%s", idx, err)
				}
			}(i)
		}
	}
	wg.Wait()

	err = utils.FirstError(err, d.meta.close())
	err = utils.FirstError(err, d.fileLock.Close())
	err = utils.FirstError(err, d.dataDir.Close())

	d.optspool.Close()
	d.opts.Logger.Infof("bitalosdb close finish")

	return err
}

func (d *DB) IsClosed() bool {
	return d.closed.Load()
}

func (d *DB) SetAutoCompact(newVal bool) {
	d.opts.AutoCompact = newVal
}

func (d *DB) doBitreeTask(task *bitask.BitpageTaskData) {
	btree := d.getBitreeRead(uint16(task.Index))
	if btree != nil {
		btree.DoBitpageTask(task)
	}
}

func (d *DB) doMemTableFlushTask(task *bitask.MemTableFlushTaskData) {
	d.memShard[task.Index].flush(true, task.IsForce)
}

func (d *DB) doVmTableFlushTask(task *bitask.VmTableFlushTaskData) {
	d.vmShard[task.Index].flush(true)
}

func (d *DB) RemoveSlot(slotId uint16) error {
	if slotId >= metaSlotNum {
		return nil
	}

	i := int(slotId)
	bt := d.getBitupleSafe(i)
	if bt == nil {
		return nil
	}

	vmShard := d.getVmShard(slotId)
	memShard := d.getMemShard(slotId)
	vmFlushed, _ := vmShard.asyncFlush()
	memFlushed, _ := memShard.asyncFlush(false)
	if vmFlushed != nil {
		<-vmFlushed
	}
	if memFlushed != nil {
		<-memFlushed
	}

	if err := bt.Close(); err != nil {
		return err
	}

	if err := bt.RemoveDir(); err != nil {
		return err
	}

	d.bituples[i].free(i)

	d.opts.Logger.Infof("bitalosdb remove slot(%d) success", slotId)
	return nil
}

func (d *DB) Flush(flushForce bool) error {
	vmFlushed, _ := d.AsyncVmFlush()
	memFlushed, _ := d.AsyncMemFlush(flushForce)
	if vmFlushed != nil {
		<-vmFlushed
	}
	if memFlushed != nil {
		<-memFlushed
	}
	return nil
}

func (d *DB) FlushVm() error {
	flushed, err := d.AsyncVmFlush()
	if err != nil {
		return err
	}
	if flushed != nil {
		<-flushed
	}
	return nil
}

func (d *DB) AsyncVmFlush() (<-chan struct{}, error) {
	if d.IsClosed() {
		return nil, ErrClosed
	}

	flushed := make(chan struct{})
	flusheds := make([]<-chan struct{}, 0, len(d.vmShard))

	for i := range d.vmShard {
		ch, err := d.vmShard[i].asyncFlush()
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

func (d *DB) FlushMemtable() error {
	flushed, err := d.AsyncMemFlush(false)
	if err != nil {
		return err
	}
	if flushed != nil {
		<-flushed
	}
	return nil
}

func (d *DB) AsyncMemFlush(flushForce bool) (<-chan struct{}, error) {
	if d.IsClosed() {
		return nil, ErrClosed
	}

	flushed := make(chan struct{})
	flusheds := make([]<-chan struct{}, 0, len(d.memShard))

	for i := range d.memShard {
		ch, err := d.memShard[i].asyncFlush(flushForce)
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

func (d *DB) FlushBitpage() {
	for i := range d.bituples {
		bt := d.getBitupleSafe(i)
		if bt != nil {
			bt.kkv.MaybeScheduleFlush()
		}
	}
}

func (d *DB) DebugInfo() string {
	info := new(bytes.Buffer)
	for i := range d.bituples {
		bt := d.getBitupleSafe(i)
		if bt != nil {
			bitreeInfo := bt.kkv.BitreeDebugInfo()
			if bitreeInfo != "" {
				info.WriteString(bitreeInfo)
			}

			bhInfo := bt.kkv.BithashDebugInfo()
			if bhInfo != "" {
				info.WriteString(bhInfo)
			}
		}
	}
	return info.String()
}

func (d *DB) DirDiskInfo() string {
	info := new(bytes.Buffer)

	info.WriteString(d.meta.info())

	for i := range d.bituples {
		bt := d.getBitupleSafe(i)
		if bt != nil {
			info.WriteString(bt.GetInfo())
		}
	}

	return info.String()
}

func (d *DB) DiskInfo() (bitupleDisk, bitpageDisk, bithashDisk uint64) {
	for i := range d.bituples {
		bt := d.getBitupleSafe(i)
		if bt != nil {
			u, p, h := bt.GetDiskInfo()
			bitupleDisk += u
			bitpageDisk += p
			bithashDisk += h
		}
	}
	return
}
