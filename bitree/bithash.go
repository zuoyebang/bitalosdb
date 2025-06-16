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
	"time"

	"github.com/zuoyebang/bitalosdb/bithash"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

var (
	bithashCompactLowerSize = int64(consts.BithashTableMaxSize)
	bithashCompactUpperSize = bithashCompactLowerSize * 2
	bithashCompactMiniSize  = int64(consts.BithashTableMaxSize - 1<<20)
)

func (t *Bitree) bithashGetByHash(key []byte, khash uint32, fn uint32) ([]byte, func(), error) {
	return t.bhash.Get(key, khash, bithash.FileNum(fn))
}

func (t *Bitree) bithashGet(key []byte, fn uint32) ([]byte, func(), error) {
	khash := hash.Crc32(key)
	return t.bhash.Get(key, khash, bithash.FileNum(fn))
}

func (t *Bitree) bithashDelete(fn uint32) error {
	if !t.opts.UseBithash {
		return nil
	}

	return t.bhash.Delete(bithash.FileNum(fn))
}

func (t *Bitree) BithashStats() *bithash.Stats {
	if t.bhash == nil {
		return nil
	}
	return t.bhash.Stats()
}

func (t *Bitree) BithashDebugInfo(dataType string) string {
	if t.bhash == nil {
		return ""
	}
	return t.bhash.DebugInfo(dataType)
}

func (t *Bitree) CompactBithash(deletePercent float64) {
	if t.bhash == nil {
		return
	}

	var (
		remainPercent   float64
		reserveSize     int64
		compactFileNums []bithash.FileNum
	)

	logTag := fmt.Sprintf("[COMPACTBITHASH %d] compact bithash", t.index)

	compactBithash := func(fns []bithash.FileNum, size int64) {
		if len(fns) == 0 {
			return
		}
		t.opts.Logger.Infof("%s start fns:%v reserveSize:%s", logTag, fns, utils.FmtSize(size))
		if err := t.compactBithashFiles(fns, logTag); err != nil {
			t.opts.Logger.Errorf("%s fail err:%s", logTag, err)
		}
	}

	delFiles := t.bhash.CheckFilesDelPercent(deletePercent)
	if len(delFiles) > 0 {
		for _, file := range delFiles {
			if file.DelPercent >= 1 {
				remainPercent = 1
			} else {
				remainPercent = 1 - file.DelPercent
			}
			fileReserveSize := int64(float64(file.Size) * remainPercent)
			reserveSize += fileReserveSize
			if reserveSize < bithashCompactLowerSize {
				compactFileNums = append(compactFileNums, file.FileNum)
			} else if reserveSize < bithashCompactUpperSize {
				compactFileNums = append(compactFileNums, file.FileNum)
				compactBithash(compactFileNums, reserveSize)
				reserveSize = 0
				compactFileNums = compactFileNums[:0]
			} else {
				compactBithash(compactFileNums, reserveSize-fileReserveSize)
				reserveSize = fileReserveSize
				compactFileNums = compactFileNums[:0]
				compactFileNums = append(compactFileNums, file.FileNum)
			}
		}

		if len(compactFileNums) > 0 {
			compactBithash(compactFileNums, reserveSize)
		}
	}

	miniFiles := t.bhash.CheckFilesMiniSize()
	miniFilesNum := len(miniFiles)
	if miniFilesNum > 1 {
		reserveSize = 0
		compactFileNums = compactFileNums[:0]
		for i, file := range miniFiles {
			if file.Size > 0 {
				reserveSize += file.Size
			}
			compactFileNums = append(compactFileNums, file.FileNum)
			if reserveSize > bithashCompactMiniSize || (i == miniFilesNum-1 && len(compactFileNums) > 1) {
				compactBithash(compactFileNums, reserveSize)
				reserveSize = 0
				compactFileNums = compactFileNums[:0]
			}
		}
	}
}

func (t *Bitree) compactBithashFiles(fileNums []bithash.FileNum, logTag string) error {
	t.dbState.LockTask()
	defer t.dbState.UnlockTask()

	bw, err := t.bhash.NewBithashWriter(true)
	if err != nil {
		return err
	}

	dstFn := bw.GetFileNum()

	defer func() {
		if err != nil {
			if err1 := bw.Remove(); err1 != nil {
				t.opts.Logger.Errorf("%s remove BithashWriter fail err:%s", logTag, err1)
			}
		}
	}()

	var obsoleteFileNums []bithash.FileNum
	var delKeyTotal int
	mgFnMap := make(map[bithash.FileNum]bool, 1<<8)
	delFnMap := make(map[bithash.FileNum]bool, 1<<8)

	compactFile := func(srcFn bithash.FileNum) (e error) {
		var iter *bithash.TableIterator
		var iterKeyNum, mgKeyNum, delKeyNum, expireKeyNum int

		start := time.Now()

		iter, e = t.bhash.NewTableIter(srcFn)
		if e != nil {
			return
		}
		defer func() {
			if err2 := iter.Close(); err2 != nil {
				t.opts.Logger.Errorf("%s close iter panic err:%s", logTag, err2)
			}

			if e == nil {
				if delKeyNum > 0 {
					delKeyTotal += delKeyNum
				}
				t.opts.Logger.Infof("%s file %d to %d done iterKeyNum:%d mgKeyNum:%d delKeyNum:%d expireKeyNum:%d cost:%.4f",
					logTag, srcFn, dstFn, iterKeyNum, mgKeyNum, delKeyNum, expireKeyNum, time.Since(start).Seconds())
			}
		}()

		findKey := func(iterKey *base.InternalKey, khash uint32) bool {
			v, vexist, vcloser := t.getInternal(iterKey.UserKey, khash)
			if !vexist {
				return false
			}
			defer vcloser()

			dv := base.DecodeInternalValue(v)
			return iterKey.SeqNum() == dv.SeqNum()
		}

		for ik, v, fn := iter.First(); iter.Valid(); ik, v, fn = iter.Next() {
			iterKeyNum++

			if _, ok := delFnMap[fn]; !ok {
				delFnMap[fn] = true
			}

			if t.opts.KvCheckExpire(ik.UserKey, v) {
				delKeyNum++
				expireKeyNum++
				continue
			}

			khash := hash.Crc32(ik.UserKey)
			if !findKey(ik, khash) {
				delKeyNum++
				continue
			}

			if e = bw.AddIkey(ik, v, khash, fn); e != nil {
				return
			}

			if _, ok := mgFnMap[fn]; !ok {
				mgFnMap[fn] = true
			}

			mgKeyNum++
		}

		if e = iter.Error(); e != nil {
			return
		}

		for k := range delFnMap {
			if _, ok := mgFnMap[k]; ok {
				delete(delFnMap, k)
			}
		}

		return
	}

	for _, compactFn := range fileNums {
		if err = compactFile(compactFn); err != nil {
			t.opts.Logger.Errorf("%s file %d to %d fail err:%s", logTag, compactFn, dstFn, err)
			return err
		}

		obsoleteFileNums = append(obsoleteFileNums, compactFn)
	}

	if err = bw.Finish(); err != nil {
		return err
	}

	for mgFn := range mgFnMap {
		t.bhash.SetFileNumMap(dstFn, mgFn)
	}
	for delFn := range delFnMap {
		t.bhash.DeleteFileNumMap(delFn)
	}

	if len(obsoleteFileNums) > 0 {
		t.bhash.RemoveTableFiles(obsoleteFileNums)
	}

	delKeyTotalOld := int(t.bhash.StatsGetDelKeyTotal())
	if delKeyTotal > 0 {
		t.bhash.StatsSubKeyTotal(delKeyTotal)
		if delKeyTotalOld >= delKeyTotal {
			t.bhash.StatsSubDelKeyTotal(delKeyTotal)
		} else {
			t.bhash.StatsSetDelKeyTotal(0)
		}
	}

	t.opts.Logger.Infof("%s files %v to %d success delKey:%d delKeyTotalOld:%d delKeyTotalNew:%d",
		logTag, obsoleteFileNums, dstFn, delKeyTotal, delKeyTotalOld, int(t.bhash.StatsGetDelKeyTotal()))

	return nil
}
