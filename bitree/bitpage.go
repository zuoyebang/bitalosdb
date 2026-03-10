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
	"fmt"

	"github.com/zuoyebang/bitalosdb/v2/bitpage"
	"github.com/zuoyebang/bitalosdb/v2/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

const nilPageNum bitpage.PageNum = 0

func (t *Bitree) BitpageStats() *bitpage.Stats {
	return t.bpage.Stats()
}

func (t *Bitree) newPageIter(pn bitpage.PageNum, o *options.IterOptions) *bitpage.PageIterator {
	return t.bpage.NewIter(pn, o)
}

func (t *Bitree) bitpageGet(key []byte, khash uint32) ([]byte, bool, func(), internalKeyKind) {
	pn, _, rtxCloser := t.FindKeyPageNum(key)
	defer rtxCloser()
	if pn == nilPageNum {
		return nil, false, nil, base.InternalKeyKindInvalid
	}
	return t.bpage.Get(pn, key, khash)
}

func (t *Bitree) bitpageExist(key []byte, khash uint32) bool {
	pn, _, rtxCloser := t.FindKeyPageNum(key)
	defer rtxCloser()
	if pn == nilPageNum {
		return false
	}
	return t.bpage.Exist(pn, key, khash)
}

func (t *Bitree) DoBitpageTask(task *bitask.BitpageTaskData) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

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
	var err error
	logTag := fmt.Sprintf("[BITPAGE %d] flush page(%d)", t.index, task.Pn)
	t.opts.DbState.WaitMemTableHighPriority(t.index)
	t.opts.DbState.LockKKVWrite(t.index)
	defer func() {
		t.opts.DbState.UnlockKKVWrite(t.index)
		if err == nil {
			t.opts.DbState.AddBitpageFlushCount()
		}
	}()

	pn := bitpage.PageNum(task.Pn)
	err = t.bpage.PageFlush(pn, task.Sentinel)
	if err == nil {
		return
	} else if err != base.ErrPageFlushEmpty {
		t.opts.Logger.Errorf("%s flush fail err:%s", logTag, err)
		return
	}

	if task.Sentinel == nil {
		t.opts.Logger.Infof("%s empty skip free by sentinel nil", logTag)
	} else if bytes.Equal(task.Sentinel, consts.BdbMaxKey) {
		t.opts.Logger.Infof("%s empty skip free by max sentinel", logTag)
	} else {
		if err = t.bdb.Update(func(tx *bdb.Tx) error {
			bkt := tx.Bucket(consts.BdbBucketName)
			if bkt == nil {
				return bdb.ErrBucketNotFound
			}
			return bkt.Delete(task.Sentinel)
		}); err != nil {
			t.opts.Logger.Errorf("%s flush page empty bdb update err:%s", logTag, err)
			return
		}

		t.bpage.SetPageFreed(pn)

		if !t.bdbUpdate() {
			t.opts.Logger.Errorf("%s bdbUpdate fail", logTag)
		}

		t.opts.Logger.Infof("%s empty mark free page done", logTag)
	}
}

func (t *Bitree) bitpageTaskSplit(task *bitask.BitpageTaskData) {
	logTag := fmt.Sprintf("[BITPAGE %d] split page(%d)", t.index, task.Pn)

	t.opts.DbState.LockKKVWrite(t.index)
	defer t.opts.DbState.UnlockKKVWrite(t.index)

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
			return bkt.Put(task.Sentinel, sps[last].Pn.ToByte())
		})
	}
	t.bpage.PageSplitEnd(bitpage.PageNum(task.Pn), sps, err)

	if err != nil {
		t.opts.Logger.Errorf("%s split fail err:%s", logTag, err)
		return
	}

	t.opts.DbState.AddBitpageSplitCount()

	if !t.bdbUpdate() {
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
			if err == base.ErrPageNotFreed {
				t.opts.Logger.Errorf("[BITPAGE %d] free page(%d) skip not freed", t.index, pn)
			} else {
				t.opts.Logger.Errorf("[BITPAGE %d] free page(%d) fail err:%s", t.index, pn, err)
			}
		}
	}
}

func (t *Bitree) checkPageFreed(pn uint32) bool {
	return t.bpage.IsPageFreed(bitpage.PageNum(pn))
}

func (t *Bitree) MaybeScheduleFlush() {
	sentinels := make(map[uint32][]byte)
	rtx := t.txPool.Load()
	defer rtx.Unref(false)
	cursor := rtx.Bucket().Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		pn := utils.BytesToUint32(v)
		sentinels[pn] = k
	}

	t.bpage.MaybeScheduleFlush(func(pn uint32) []byte {
		return sentinels[pn]
	})
}

type PageSize struct {
	Index     int
	Pn        uint32
	InuseSize uint64
}
type SortedPageSizeList []PageSize

func (x SortedPageSizeList) Len() int { return len(x) }
func (x SortedPageSizeList) Less(i, j int) bool {
	return x[i].InuseSize < x[j].InuseSize
}
func (x SortedPageSizeList) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (t *Bitree) GetBitpageInuseSize() []PageSize {
	res := t.bpage.GetPageInuseSize()
	if len(res) == 0 {
		return nil
	}

	bi := t.Index()
	num := len(res) / 2
	ps := make([]PageSize, num)
	j := 0
	for i := 0; i < num; i++ {
		ps[i] = PageSize{
			Index:     bi,
			Pn:        uint32(res[j]),
			InuseSize: res[j+1],
		}
		j += 2
	}

	return ps
}
