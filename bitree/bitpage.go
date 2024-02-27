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
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

const nilPageNum bitpage.PageNum = 0

func (t *Bitree) BitpageStats() *bitpage.Stats {
	return t.bpage.Stats()
}

func (t *Bitree) newPageIter(pn bitpage.PageNum, o *base.IterOptions) *bitpage.PageIterator {
	return t.bpage.NewIter(pn, o)
}

func (t *Bitree) bitpageGet(key []byte, khash uint32) ([]byte, bool, func(), base.InternalKeyKind) {
	pn, _, rtxCloser := t.FindKeyPageNum(key)
	defer rtxCloser()
	if pn == nilPageNum {
		return nil, false, nil, base.InternalKeyKindInvalid
	}

	return t.bpage.Get(pn, key, khash)
}

func (t *Bitree) bitpageFlush(task *base.BitpageTaskData) error {
	t.dbState.LockDbWrite()
	defer t.dbState.UnlockDbWrite()

	if t.bpageTask.isClosed() {
		return nil
	}

	logTag := fmt.Sprintf("[BITPAGEFLUSH] index:%d taskId:%d pn:%d", t.index, task.Id, task.Pn)

	if err := t.bpage.PageFlush(bitpage.PageNum(task.Pn), task.Sentinel, logTag); err != nil {
		t.opts.Logger.Errorf("%s bitpageFlush fail err:%s", logTag, err)
		return err
	}

	t.dbState.AddBitpageFlushCount()
	return nil
}

func (t *Bitree) bitpageSplit(task *base.BitpageTaskData) {
	t.dbState.LockDbWrite()
	defer t.dbState.UnlockDbWrite()

	if t.bpageTask.isClosed() {
		return
	}

	logTag := fmt.Sprintf("[BITPAGESPLIT] index:%d taskId:%d pn:%d", t.index, task.Id, task.Pn)
	if task.Sentinel == nil {
		t.opts.Logger.Errorf("%s split fail sentinel key is nil", logTag)
		return
	}

	sps, err := t.bpage.PageSplitStart(bitpage.PageNum(task.Pn), logTag)
	if err == nil {
		err = t.bdb.Update(func(tx *bdb.Tx) error {
			bkt := tx.Bucket(consts.BdbBucketName)
			if bkt == nil {
				return bdb.ErrBucketNotFound
			}

			last := len(sps) - 1
			for i := 0; i < last; i++ {
				if err = bkt.Put(sps[i].Sentinel, sps[i].Pn.ToByte()); err != nil {
					return err
				}
			}
			if err = bkt.Put(task.Sentinel, sps[last].Pn.ToByte()); err != nil {
				return err
			}
			return nil
		})
	}
	t.bpage.PageSplitEnd(bitpage.PageNum(task.Pn), sps, err)

	if err != nil {
		t.opts.Logger.Errorf("%s split fail err:%s", logTag, err.Error())
		return
	}

	t.dbState.AddBitpageSplitCount()

	if !t.BdbUpdate() {
		t.opts.Logger.Errorf("%s bdb txPool swaptx fail", logTag)
	}

	t.opts.Logger.Infof("%s split success spsNum:%d", logTag, len(sps))
}

func (t *Bitree) bitpageFreePage(task *base.BitpageTaskData) {
	if len(task.Pns) == 0 {
		return
	}

	for _, pn := range task.Pns {
		logTag := fmt.Sprintf("[BITPAGEFREE] index:%d taskId:%d pn:%d", t.index, task.Id, pn)
		t.opts.Logger.Infof("%s start", logTag)
		if err := t.bpage.FreePage(bitpage.PageNum(pn), true); err != nil {
			if err == bitpage.ErrPageNotSplitted {
				t.opts.Logger.Errorf("%s skip not splitted", logTag)
			} else {
				t.opts.Logger.Errorf("%s fail err:%s", logTag, err)
			}
		} else {
			t.opts.Logger.Infof("%s end", logTag)
		}
	}
}

func (t *Bitree) checkPageSplitted(pn uint32) bool {
	return t.bpage.PageSplitted2(bitpage.PageNum(pn))
}

func (t *Bitree) pushTaskData(task *base.BitpageTaskData) {
	if t.bpageTask.isClosed() {
		return
	}

	task.Id = t.bpageTask.taskId.Add(1)
	task.SendTime = time.Now()
	t.bpageTask.taskChs <- task
}

func (t *Bitree) runBitpageTask() {
	t.bpageTask = &bitpageTask{
		btree:   t,
		taskChs: make(chan *base.BitpageTaskData, 2<<14),
	}

	t.bpageTask.closed.Store(false)
	t.bpageTask.taskId.Store(0)
	t.bpageTask.runTask()
	t.manualScheduleFlush()
}

func (t *Bitree) manualScheduleFlush() {
	pns := t.bpage.GetNeedFlushPageNums()
	for i := range pns {
		task := &base.BitpageTaskData{
			Event:    base.BitpageEventFlush,
			Pn:       uint32(pns[i]),
			Sentinel: nil,
		}
		t.pushTaskData(task)
	}
	t.opts.Logger.Infof("bitree manualScheduleFlush index:%d pns:%v", t.index, pns)
}

type bitpageTask struct {
	btree   *Bitree
	taskId  atomic.Uint64
	wg      sync.WaitGroup
	closed  atomic.Bool
	taskChs chan *base.BitpageTaskData
}

func (t *bitpageTask) runTask() {
	logTag := fmt.Sprintf("[BITPAGETASK] index:%d", t.btree.index)

	t.wg.Add(1)
	go func() {
		defer func() {
			t.wg.Done()
			if r := recover(); r != nil {
				t.btree.opts.Logger.Errorf("%s runTask panic err:%v stack:%s", logTag, r, string(debug.Stack()))
				t.runTask()
			}
		}()

		for {
			task, ok := <-t.taskChs
			if !ok || t.isClosed() || task == nil {
				return
			}

			duration := time.Since(task.SendTime)
			t.btree.opts.Logger.Infof("%s recvTask taskId:%d event:%d pn:%d pns:%v wait:%.3fs",
				logTag, task.Id, task.Event, task.Pn, task.Pns, duration.Seconds())

			t.btree.dbState.WaitHighPriority()

			switch task.Event {
			case base.BitpageEventFlush:
				_ = t.btree.bitpageFlush(task)
			case base.BitpageEventSplit:
				t.btree.bitpageSplit(task)
			case base.BitpageEventFreePage:
				t.btree.bitpageFreePage(task)
			}
		}
	}()
}

func (t *bitpageTask) isClosed() bool {
	return t.closed.Load()
}

func (t *bitpageTask) close() {
	t.closed.Store(true)
	t.taskChs <- nil
}
