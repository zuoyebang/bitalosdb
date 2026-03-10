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
	"github.com/zuoyebang/bitalosdb/v2/bitree"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
)

func Open(dirname string, opts *Options) (db *DB, err error) {
	var optsPool *options.OptionsPool
	opts = opts.Clone().EnsureDefaults()
	opts.Logger.Infof("open bitalosdb start")
	if opts.private.optspool == nil {
		optsPool = opts.ensureOptionsPool(nil)
	} else {
		optsPool = opts.private.optspool
	}

	d := &DB{
		dirname:    dirname,
		opts:       opts,
		optspool:   optsPool,
		cmp:        opts.Comparer.Compare,
		equal:      opts.Comparer.Equal,
		compressor: compress.SetCompressor(opts.CompressionType),
		dbState:    optsPool.DbState,
		meta:       &metadata{},
		taskClosed: make(chan struct{}),
		flushStats: NewFlushStats(),
	}

	for i := range d.bituples {
		d.bituples[i] = newSlotBituple(d)
	}

	d.memFlusher = &memFlushWriter{
		db:      d,
		writers: make(map[uint16]*bitree.BitreeWriter, metaSlotNum),
	}

	if err = opts.FS.MkdirAll(dirname, 0755); err != nil {
		return nil, err
	}
	d.dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	fileLock, err := opts.FS.Lock(base.MakeFilepath(opts.FS, dirname, fileTypeLock, 0))
	if err != nil {
		d.dataDir.Close()
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

	if d.meta, err = openMetadata(dirname, opts); err != nil {
		return nil, err
	}

	d.vmFlushTask = bitask.NewVmTableFlushTask(&bitask.VmTableFlushTaskOptions{
		Size:      consts.VmTableTaskSize,
		WorkerNum: consts.VmTbaleTaskWorkerNum,
		Logger:    d.opts.Logger,
		DoFunc:    d.doVmTableFlushTask,
		TaskWg:    &d.taskWg,
	})

	d.memFlushTask = bitask.NewMemTableFlushTask(&bitask.MemTableFlushTaskOptions{
		Size:   consts.MemTableTaskSize,
		Logger: d.opts.Logger,
		DoFunc: d.doMemTableFlushTask,
		TaskWg: &d.taskWg,
	})

	d.bpageTask = bitask.NewBitpageTask(&bitask.BitpageTaskOptions{
		Size:      consts.BitpageTaskSize,
		WorkerNum: d.opts.BitpageTaskWorkerNum,
		Logger:    opts.Logger,
		DoFunc:    d.doBitreeTask,
		TaskWg:    &d.taskWg,
	})
	d.optspool.BaseOptions.BitpageTaskPushFunc = d.bpageTask.PushTask

	d.newVmTable()
	d.newMemTable()

	for idx, status := range d.meta.slotsStatus {
		if status == slotBitupleCreated {
			if err = d.bituples[idx].createBituple(idx, slotBitupleCreated); err != nil {
				return nil, err
			}
		}
	}

	d.expandBitupleShard()

	if err = d.newEliminateTask(); err != nil {
		return nil, err
	}

	d.runVtGCTask()
	d.runBithashGCTask()

	d.fileLock, fileLock = fileLock, nil
	d.opts.Logger.Infof("open bitalosdb success opts:%+v", d.opts)

	return d, nil
}

func (d *DB) expandBitupleShard() {
	if d.meta.tupleCount == d.opts.VectorTableCount {
		return
	} else if d.opts.VectorTableCount < d.meta.tupleCount {
		d.opts.Logger.Infof("expandBitupleShard not support shrink from %d to %d", d.meta.tupleCount, d.opts.VectorTableCount)
		return
	}

	var oldTupleList, newTupleList []*Tuple
	expandTupleCount := d.opts.VectorTableCount
	oldTupleCount := d.meta.tupleCount
	tsr := d.meta.splitTupleShardRanges(expandTupleCount)
	d.opts.Logger.Infof("expandBitupleShard from %d to %d start", oldTupleCount, expandTupleCount)

	for i := range d.bituples {
		bt := d.getBitupleSafe(i)
		if bt == nil || bt.isEliminate() {
			continue
		}

		tupleList := make([]*Tuple, expandTupleCount)
		newTupleId := d.meta.tupleNextId
		for j := uint16(0); j < expandTupleCount; j++ {
			tp, err := newTuple(bt, newTupleId, nil)
			if err != nil {
				d.opts.Logger.Errorf("expandBitupleShard tuple(%d-%d) new panic err:%s", bt.index, newTupleId, err)
				for _, ntp := range newTupleList {
					ntp.close(true)
				}
				return
			}

			newTupleList = append(newTupleList, tp)
			tupleList[j] = tp
			newTupleId++
			d.opts.Logger.Infof("expandBitupleShard tuple(%d-%d) new success", tp.index, tp.tn)
		}

		for j := uint16(0); j < expandTupleCount; j++ {
			for k := tsr[j].start; k <= tsr[j].end; k++ {
				bt.tupleShards[k] = tupleList[j]
			}
		}

		oldTupleList = append(oldTupleList, bt.tupleList...)
		iter := bt.NewBitupleIter()
		bt.tupleList = tupleList
		bt.tupleCount = expandTupleCount
		key, hi, lo, seqNum, dataType, ts, version, slotId, size, pre, next, value, final := iter.First()
		for ; !final; key, hi, lo, seqNum, dataType, ts, version, slotId, size, pre, next, value, final = iter.Next() {
			tuple := bt.getHashTuple(lo)
			if err := tuple.vt.Set(key, hi, lo, seqNum, dataType, ts, slotId, version, size, pre, next, value); err != nil {
				d.opts.Logger.Errorf("expandBitupleShard set (%d-%d) fail key:%s err:%s",
					bt.index, tuple.index, string(key), err)
			}
		}
		iter.Close()

		for _, tp := range bt.tupleList {
			tp.vt.MSync()
		}

		d.opts.Logger.Infof("expandBitupleShard bituple(%d) success", bt.index)
	}

	d.meta.writeTupleShardRanges(tsr)

	for _, tp := range oldTupleList {
		tp.close(true)
		d.opts.Logger.Infof("expandBitupleShard tuple(%d-%d) free success", tp.index, tp.tn)
	}

	oldTupleList = nil
	newTupleList = nil

	d.opts.Logger.Infof("expandBitupleShard from %d to %d finish", oldTupleCount, expandTupleCount)
	return
}
