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
	"fmt"

	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bitask"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

const nilPageNum bitpage.PageNum = 0

func (t *Bitree) BitpageStats() *bitpage.Stats {
	return t.bpage.Stats()
}

func (t *Bitree) newPageIter(pn bitpage.PageNum, o *options.IterOptions) *bitpage.PageIterator {
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

func (t *Bitree) DoBitpageTask(task *bitask.BitpageTaskData) {
	switch task.Event {
	case bitask.BitpageEventFlush:
		t.bitpageTaskFlush(task)
	case bitask.BitpageEventSplit:
		t.bitpageTaskSplit(task)
	case bitask.BitpageEventFreePage:
		t.bitpageTaskFreePage(task)
	}

	watiCh := task.WaitDuration.Seconds()
	eventName := bitask.GetBitpageEventName(task.Event)

	if len(task.Pns) == 0 {
		t.opts.Logger.Infof("[BITPAGE %d] doTask finish event:%s pn:%d waitCh:%.3fs",
			t.index, eventName, task.Pn, watiCh)
	} else {
		t.opts.Logger.Infof("[BITPAGE %d] doTask finish event:%s pns:%v waitCh:%.3fs",
			t.index, eventName, task.Pns, watiCh)
	}
}

func (t *Bitree) bitpageTaskFlush(task *bitask.BitpageTaskData) {
	logTag := fmt.Sprintf("[BITPAGE %d] flush page(%d)", t.index, task.Pn)

	t.dbState.LockBitowerWrite(t.index)
	defer t.dbState.UnlockBitowerWrite(t.index)

	if err := t.bpage.PageFlush(bitpage.PageNum(task.Pn), task.Sentinel, logTag); err != nil {
		t.opts.Logger.Errorf("%s flush fail err:%s", logTag, err.Error())
		return
	}

	t.dbState.AddBitpageFlushCount()
}

func (t *Bitree) bitpageTaskSplit(task *bitask.BitpageTaskData) {
	logTag := fmt.Sprintf("[BITPAGE %d] split page(%d)", t.index, task.Pn)

	t.dbState.LockBitowerWrite(t.index)
	defer t.dbState.UnlockBitowerWrite(t.index)

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

func (t *Bitree) bitpageTaskFreePage(task *bitask.BitpageTaskData) {
	if len(task.Pns) == 0 {
		return
	}

	for _, pn := range task.Pns {
		if err := t.bpage.FreePage(bitpage.PageNum(pn), true); err != nil {
			if err == bitpage.ErrPageNotSplitted {
				t.opts.Logger.Errorf("[BITPAGE %d] free page(%d) skip not splitted", t.index, pn)
			} else {
				t.opts.Logger.Errorf("[BITPAGE %d] free page(%d) fail err:%s", t.index, pn, err)
			}
		} else {
			t.opts.Logger.Infof("[BITPAGE %d] free page(%d) done", t.index, pn)
		}
	}
}

func (t *Bitree) checkPageSplitted(pn uint32) bool {
	return t.bpage.PageSplitted2(bitpage.PageNum(pn))
}

func (t *Bitree) MaybeScheduleFlush(isForce bool) {
	pns := t.bpage.GetNeedFlushPageNums(isForce)
	if len(pns) == 0 {
		return
	}

	for i := range pns {
		t.opts.BitpageTaskPushFunc(&bitask.BitpageTaskData{
			Index:    t.index,
			Event:    bitask.BitpageEventFlush,
			Pn:       uint32(pns[i]),
			Sentinel: nil,
		})
	}

	t.opts.Logger.Infof("[BITPAGE %d] manualScheduleFlush isForce:%v pns:%v", t.index, isForce, pns)
}
