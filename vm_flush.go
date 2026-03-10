// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitalosdb

import (
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
	"github.com/zuoyebang/bitalosdb/v2/internal/humanize"
)

type vttable interface {
	getValue(h, l uint64) (value []byte, seqNum uint64, dataType uint8, slotId uint16, timestamp uint64, kind InternalKeyKind, closer func(), err error)
	getMeta(h, l uint64) (dataType uint8, slotId uint16, kind InternalKeyKind, seqNum, timestamp, version, lindex, rindex uint64, size uint32, err error)
	getTimestamp(h, l uint64) (seqNum, timestamp uint64, dataType uint8, kind InternalKeyKind, exist bool)
	exist(h, l uint64) (seqNum uint64, kind InternalKeyKind, exist bool)
	set(key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, slotId uint16, version uint64, size uint32, lindex, rindex uint64, value []byte) error
	setTimestamp(h, l, seqNum uint64, timestamp uint64, dataType uint8) error
	newIter() base.VectorTableIterator
	getId() int
	getSize() uint32
	close() error
	empty() bool
	isVtable() bool
}

type vmFlushableEntry struct {
	vttable
	flushed    chan struct{}
	obsolete   bool
	readerRefs atomic.Int32
	release    func()
}

func (e *vmFlushableEntry) readerRef() {
	e.readerRefs.Add(1)
}

func (e *vmFlushableEntry) readerUnref() {
	if e.readerRefs.Add(-1) == 0 {
		if e.release != nil {
			e.release()
			e.release = nil
		}
	}
}

type vmFlushableList []*vmFlushableEntry

func (e *vmFlushableEntry) setObsolete() {
	e.obsolete = true
}

func newVmFlushableEntry(v vttable) *vmFlushableEntry {
	entry := &vmFlushableEntry{
		vttable: v,
		flushed: make(chan struct{}),
	}
	entry.readerRefs.Store(1)
	return entry
}

func (vs *vmTableShard) maybeScheduleFlush(isTask bool) {
	if vs.vm.compact.flushing || vs.db.IsClosed() || len(vs.vm.queue) <= 1 {
		return
	}

	vs.vm.compact.flushing = true

	if isTask {
		vs.db.vmFlushTask.PushTask(&bitask.VmTableFlushTaskData{
			Index: vs.index,
		})
	} else {
		go vs.flush(false)
	}
}

func (vs *vmTableShard) flush(isTask bool) {
	vs.vm.Lock()
	defer vs.vm.Unlock()

	defer func() {
		vs.vm.compact.flushing = false
		vs.maybeScheduleFlush(isTask)
		vs.vm.compact.cond.Broadcast()
	}()

	n := len(vs.vm.queue) - 1
	if n == 0 {
		return
	}

	var flushed vmFlushableList
	err := vs.runFlush(vs.vm.queue[:n])
	if err == nil {
		flushed = vs.vm.queue[:n]
		vs.vm.queue = vs.vm.queue[n:]
		vs.updateReadState()
	}

	vs.vm.Unlock()
	defer vs.vm.Lock()

	for i := range flushed {
		flushed[i].readerUnref()
		close(flushed[i].flushed)
	}
}

func (vs *vmTableShard) runFlush(flushing vmFlushableList) error {
	defer func() {
		if r := recover(); r != any(nil) {
			vs.db.opts.Logger.Errorf("%s runFlush panic error:%v stack:%s", vs.logTag, r, string(debug.Stack()))
		}
	}()

	vs.vm.Unlock()
	defer vs.vm.Lock()

	d := vs.db
	flushNum := len(flushing)
	startTime := time.Now()
	d.opts.Logger.Infof("%s flushing %d vmtable to bituple start", vs.logTag, flushNum)
	d.dbState.SetVmTableHighPriority(vs.index, true)
	d.dbState.RLockTask()
	defer func() {
		d.dbState.SetVmTableHighPriority(vs.index, false)
		d.dbState.RUnlockTask()
		d.opts.Logger.Infof("%s flushed %d vmtable to bituple done cost:%.3fs", vs.logTag, flushNum, time.Since(startTime).Seconds())
	}()

	flushNum = 0
	for i := range flushing {
		if flushing[i].empty() {
			continue
		}

		if err := vs.flushVm(flushing[i]); err != nil {
			return err
		}

		flushNum++
	}

	for k, t := range vs.tupleFlusher {
		_ = t.vt.MSync()
		delete(vs.tupleFlusher, k)
	}

	return nil
}

func (vs *vmTableShard) flushVm(vm *vmFlushableEntry) error {
	d := vs.db
	keyNum := 0
	written := 0
	expireNum := 0
	entryId := vm.getId()
	entrySize := int(vm.getSize())
	isVt := vm.isVtable()
	iter := vm.newIter()
	defer iter.Close()

	startFlushTime := time.Now()
	key, hi, lo, sn, dataType, ts, version, slotId, size, lindex, rindex, value, final := iter.First()
	for ; !final; key, hi, lo, sn, dataType, ts, version, slotId, size, lindex, rindex, value, final = iter.Next() {
		written += len(key) + len(value) + 24
		seqNum, keyKind := base.DecodeTrailer(sn)
		bt, err := vs.db.getBitupleWrite(int(slotId))
		if err != nil {
			d.opts.Logger.Errorf("%s flush vmtable(%d) getBituple(%d) err:%s", vs.logTag, entryId, slotId, err)
			return err
		}

		tuple := bt.getHashTuple(lo)

		keyNum++
		switch keyKind {
		case InternalKeyKindSet:
			if d.isTimestampAlive(ts) {
				if err = tuple.vt.Set(key, hi, lo, seqNum, dataType, ts, slotId, version, size, lindex, rindex, value); err != nil {
					d.opts.Logger.Errorf("%s flush vmtable(%d) set key:%s err:%s", vs.logTag, entryId, string(key), err)
				}
			} else {
				tuple.vt.Delete(hi, lo, seqNum)
				expireNum++
			}
		case InternalKeyKindExpireAt:
			if d.isTimestampAlive(ts) {
				if err = tuple.vt.SetTimestamp(hi, lo, seqNum, ts, dataType); err != nil {
					d.opts.Logger.Errorf("%s flush vmtable(%d) setTimestamp key:%s err:%s", vs.logTag, entryId, string(key), err)
				}
			} else {
				tuple.vt.Delete(hi, lo, seqNum)
				expireNum++
			}
		case InternalKeyKindDelete:
			tuple.vt.Delete(hi, lo, seqNum)
		default:
			d.opts.Logger.Errorf("%s flush vmtable(%d) not support kind key:%s keyKind:%d", vs.logTag, entryId, string(key), keyKind)
			continue
		}

		tk := bt.index*1000 + int(tuple.tn)
		if _, ok := vs.tupleFlusher[tk]; !ok {
			vs.tupleFlusher[tk] = tuple
		}
	}

	cost := time.Since(startFlushTime)
	d.opts.Logger.Infof("%s flushed vmtable(%d) finish written(%s) keys(%d) vmKeys(%d) expireKeys(%d) isVt(%v) cost:%.3fs",
		vs.logTag, entryId, humanize.Int64(int64(written)), keyNum, entrySize, expireNum, isVt, cost.Seconds())
	if entrySize != keyNum {
		d.opts.Logger.Errorf("%s flush vmtable(%d) panic keys not equal exp:%d act:%d", vs.logTag, entryId, entrySize, keyNum)
	}
	d.flushStats.setFlushVmTableTime(cost.Nanoseconds())
	return nil
}
