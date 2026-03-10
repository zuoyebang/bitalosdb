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
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/vectormap"
	"github.com/zuoyebang/bitalosdb/v2/internal/vectortable"
)

type vmTableShard struct {
	db           *DB
	index        int
	tid          int
	logTag       string
	tupleFlusher map[int]*Tuple

	readState struct {
		sync.RWMutex
		val *vmReadState
	}

	vm struct {
		sync.RWMutex
		id      int
		mutable *vmTable
		queue   vmFlushableList

		compact struct {
			cond     sync.Cond
			flushing bool
		}
	}
}

func newVmTableShard(d *DB, index int) *vmTableShard {
	var vmEntry *vmFlushableEntry

	vs := &vmTableShard{
		db:           d,
		index:        index,
		tupleFlusher: make(map[int]*Tuple, metaSlotNum*d.opts.VectorTableCount),
	}

	vs.vm.compact.cond.L = &vs.vm.RWMutex
	vs.logTag = fmt.Sprintf("[VMSHARD %d]", vs.index)

	dirname := base.MakeVmVtablepath(d.dirname, vs.index)
	os2.RemoveDir(dirname)
	d.opts.FS.MkdirAll(dirname, 0755)

	vs.vm.Lock()
	defer vs.vm.Unlock()

	vs.vm.mutable, vmEntry = vs.newVmTable(false)
	vs.vm.queue = append(vs.vm.queue, vmEntry)
	vs.updateReadState()

	return vs
}

func (vs *vmTableShard) newVmTable(isVt bool) (*vmTable, *vmFlushableEntry) {
	d := vs.db
	vs.tid++
	vmt := &vmTable{
		id:   vs.tid,
		isVt: isVt,
	}

	if !vmt.isVt {
		vid := fmt.Sprintf("%d-%d", vs.index, vmt.id)
		vmt.vm = d.vmPool.get(vid)
	} else {
		vmt.vm = vs.newVmVtable()
	}

	entry := newVmFlushableEntry(vmt)
	entry.release = func() {
		if !vmt.isVt {
			d.vmPool.put(vmt.vm.(*vectormap.VectorMap))
		}
		if err := entry.close(); err != nil {
			d.opts.Logger.Infof("%s vmtable(%d) release fail err:%s", vs.logTag, entry.getId(), err)
		} else {
			d.opts.Logger.Infof("%s vmtable(%d) release success", vs.logTag, entry.getId())
		}
	}
	d.opts.Logger.Infof("%s vmtable(%d) isVt:%v new success", vs.logTag, vmt.id, vmt.isVt)
	return vmt, entry
}

func (vs *vmTableShard) loadReadState() *vmReadState {
	vs.readState.RLock()
	state := vs.readState.val
	state.ref()
	vs.readState.RUnlock()
	return state
}

func (vs *vmTableShard) updateReadState() {
	rs := &vmReadState{
		memtables: vs.vm.queue,
	}
	rs.refcnt.Store(1)

	for _, mem := range rs.memtables {
		mem.readerRef()
	}

	vs.readState.Lock()
	old := vs.readState.val
	vs.readState.val = rs
	vs.readState.Unlock()

	if old != nil {
		old.unref()
	}
}

func (vs *vmTableShard) makeRoomForWrite(isVt, isFlush bool) {
	newVm, entry := vs.newVmTable(isVt)
	vs.vm.mutable = newVm
	vs.vm.queue = append(vs.vm.queue, entry)
	vs.updateReadState()
	if isFlush {
		vs.maybeScheduleFlush(true)
	}
}

func (vs *vmTableShard) switchMutable() {
	vs.vm.Lock()
	defer vs.vm.Unlock()

	if vs.vm.mutable.isWriteFull() {
		isVt := false
		if vs.db.IsCheckpointSyncing() && len(vs.vm.queue) >= vs.db.opts.VmTableStopWritesThreshold {
			isVt = true
		}
		vs.makeRoomForWrite(isVt, true)
	}
}

func (vs *vmTableShard) newVmVtable() *vectortable.VectorTable {
	d := vs.db
	dirname := base.MakeVmVtablepath(d.dirname, vs.index)
	opts := &options.VectorTableOptions{
		Options:       d.optspool.BaseOptions,
		Index:         vs.index,
		Dirname:       dirname,
		Filename:      makeFilename(fileTypeVectorTable, 0),
		HashSize:      consts.VectorMapTableHashSizeDefault,
		MmapBlockSize: d.optspool.BaseOptions.TableMmapBlockSize,
		StoreKey:      !d.opts.DisableStoreKey,
		WithSlot:      true,
		WriteBuffer:   false,
		ReleaseFunc: func(fns []string) {
			for _, fn := range fns {
				if e := os.Remove(fn); e != nil {
					d.opts.Logger.Errorf("vmVtable remove panic file:%s error:%s", fn, e)
				} else {
					d.opts.Logger.Infof("vmVtable remove success file:%s", fn)
				}
			}
		},
	}
	vt, err := vectortable.NewVectorTable(opts)
	if err != nil {
		d.opts.Logger.Errorf("panic vmVtable new fail error:%s", err)
		return nil
	}

	return vt
}

func (vs *vmTableShard) switchVmVtable() {
	vs.vm.Lock()
	defer vs.vm.Unlock()

	if vs.vm.mutable.isVt {
		vs.vm.mutable.msync()
		vmId := vs.vm.mutable.id
		vs.makeRoomForWrite(false, true)
		vs.db.opts.Logger.Infof("%s vmtable(%d) vtable switch success", vs.logTag, vmId)
	}
}

func (vs *vmTableShard) close() {
	vs.vm.Lock()
	defer vs.vm.Unlock()

	for vs.vm.compact.flushing {
		vs.vm.compact.cond.Wait()
	}

	if err := vs.runFlush(vs.vm.queue); err != nil {
		vs.db.opts.Logger.Errorf("%s close runFlush err:%s", vs.logTag, err)
	}

	vs.readState.val.unref()
	for _, vm := range vs.vm.queue {
		vm.readerUnref()
	}
}

func (vs *vmTableShard) asyncFlush() (<-chan struct{}, error) {
	if vs.db.IsClosed() {
		return nil, ErrClosed
	}

	vs.vm.Lock()
	defer vs.vm.Unlock()
	empty := true
	for i := range vs.vm.queue {
		if !vs.vm.queue[i].isVtable() {
			empty = false
			break
		}
	}
	if empty {
		return nil, nil
	}
	flushed := vs.vm.queue[len(vs.vm.queue)-1].flushed
	vs.makeRoomForWrite(false, true)
	return flushed, nil
}

type vmTable struct {
	id     int
	vm     base.VectorMemTable
	isVt   bool
	isFull atomic.Bool
}

func (m *vmTable) getId() int {
	return m.id
}

func (m *vmTable) setWriteFull() {
	m.isFull.Store(true)
}

func (m *vmTable) isWriteFull() bool {
	return m.isFull.Load()
}

func (m *vmTable) getValue(h, l uint64) (
	value []byte, seqNum uint64, dataType uint8, slotId uint16,
	timestamp uint64, kind InternalKeyKind, closer func(), err error,
) {
	var sn uint64
	value, sn, dataType, timestamp, slotId, closer, err = m.vm.GetValue(h, l)
	if err == nil {
		seqNum, kind = base.DecodeTrailer(sn)
	}
	return
}

func (m *vmTable) getMeta(h, l uint64) (
	dataType uint8, slotId uint16, kind InternalKeyKind,
	seqNum, timestamp, version, lindex, rindex uint64,
	size uint32, err error,
) {
	var sn uint64
	sn, dataType, timestamp, slotId, version, size, lindex, rindex, err = m.vm.GetMeta(h, l)
	if err == nil {
		seqNum, kind = base.DecodeTrailer(sn)
	}
	return
}

func (m *vmTable) exist(h, l uint64) (seqNum uint64, kind InternalKeyKind, exist bool) {
	var sn uint64
	sn, exist = m.vm.Has(h, l)
	if !exist {
		return
	}
	seqNum, kind = base.DecodeTrailer(sn)
	return
}

func (m *vmTable) getTimestamp(h, l uint64) (seqNum, timestamp uint64, dataType uint8, kind InternalKeyKind, exist bool) {
	var err error
	var sn uint64
	sn, timestamp, dataType, err = m.vm.GetTimestamp(h, l)
	if err == nil {
		exist = true
		seqNum, kind = base.DecodeTrailer(sn)
	}
	return
}

func (m *vmTable) set(
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64,
	slotId uint16, version uint64, size uint32, lindex, rindex uint64,
	value []byte,
) error {
	return m.vm.Set(key, h, l, seqNum, dataType, timestamp, slotId, version, size, lindex, rindex, value)
}

func (m *vmTable) setTimestamp(h, l, seqNum uint64, timestamp uint64, dataType uint8) error {
	return m.vm.SetTimestamp(h, l, seqNum, timestamp, dataType)
}

func (m *vmTable) newIter() base.VectorTableIterator {
	return m.vm.NewIterator()
}

func (m *vmTable) close() error {
	if m.isVt {
		return m.vm.Close(true)
	}

	return nil
}

func (m *vmTable) msync() error {
	if m.isVt {
		return m.vm.MSync()
	}
	return nil
}

func (m *vmTable) empty() bool {
	return m.getSize() == 0
}

func (m *vmTable) getSize() uint32 {
	return m.vm.GetKeySize()
}

func (m *vmTable) isVtable() bool {
	return m.isVt
}
