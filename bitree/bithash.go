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
	"time"

	"github.com/zuoyebang/bitalosdb/bithash"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/utils"
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

	logTag := fmt.Sprintf("[COMPACTBITHASH %d]", t.index)

	delFiles := t.bhash.CheckFilesDelPercent(deletePercent)
	delFilesNum := len(delFiles)
	if delFilesNum > 1 {
		var remainPercent float64
		var delFileNums []bithash.FileNum
		for i, file := range delFiles {
			if file.DelPercent < 1 {
				remainPercent += 1 - file.DelPercent
			}
			delFileNums = append(delFileNums, file.FileNum)
			if remainPercent > 0.95 || (i == delFilesNum-1 && len(delFileNums) > 1) {
				t.opts.Logger.Infof("%s compact bithash delPercent files start compactFn:%v remainPercent:%.4f",
					logTag, delFileNums, remainPercent)
				err := t.compactBithashFiles(delFileNums, logTag)
				if err != nil {
					t.opts.Logger.Errorf("%s compact bithash delPercent files fail err:%s", logTag, err)
				}
				remainPercent = 0
				delFileNums = delFileNums[:0]
			}
		}
	}

	miniFiles := t.bhash.CheckFilesMiniSize()
	miniFilesNum := len(miniFiles)
	if miniFilesNum > 1 {
		var remainSize int64
		var miniFileNums []bithash.FileNum
		fileMaxSize := t.bhash.TableMaxSize() - (1 << 20)
		for i, file := range miniFiles {
			if file.Size > 0 {
				remainSize += file.Size
			}
			miniFileNums = append(miniFileNums, file.FileNum)
			if remainSize > fileMaxSize || (i == miniFilesNum-1 && len(miniFileNums) > 1) {
				t.opts.Logger.Infof("%s compact bithash miniSize files start compactFn:%v remainSize:%s",
					logTag, miniFileNums, utils.FmtSize(uint64(remainSize)))
				err := t.compactBithashFiles(miniFileNums, logTag)
				if err != nil {
					t.opts.Logger.Errorf("%s compact bithash miniSize files fail err:%s", logTag, err)
				}
				remainSize = 0
				miniFileNums = miniFileNums[:0]
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
		var mgKeyNum, delKeyNum, expireKeyNum int

		start := time.Now()

		iter, e = t.bhash.NewTableIter(srcFn)
		if e != nil {
			return
		}
		defer func() {
			if iter != nil {
				e = iter.Close()
			}

			cost := time.Since(start).Seconds()
			if e == nil {
				if delKeyNum > 0 {
					delKeyTotal += delKeyNum
				}
				t.opts.Logger.Infof("%s compactFile %d to %d done mgKeyNum:%d delKeyNum:%d expireKeyNum:%d cost:%.4f",
					logTag, srcFn, dstFn, mgKeyNum, delKeyNum, expireKeyNum, cost)
			} else {
				t.opts.Logger.Errorf("%s compactFile %d to %d fail err:%s", logTag, srcFn, dstFn, e)
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
				t.opts.Logger.Errorf("%s compactFile AddIkey fail ikey:%s keyFn:%d dstFn:%d err:%s", logTag, ik.String(), fn, dstFn, e)
				return
			}

			if _, ok := mgFnMap[fn]; !ok {
				mgFnMap[fn] = true
			}
			mgKeyNum++
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
			t.opts.Logger.Errorf("%s compactFile fail fn:%d err:%s", logTag, compactFn, err)
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

	t.opts.Logger.Infof("%s compact %v to %d success delKey:%d delKeyTotalOld:%d delKeyTotalNew:%d",
		logTag, obsoleteFileNums, dstFn, delKeyTotal, delKeyTotalOld, int(t.bhash.StatsGetDelKeyTotal()))

	return nil
}
