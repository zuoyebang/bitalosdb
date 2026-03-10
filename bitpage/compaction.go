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

package bitpage

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/humanize"
	"github.com/zuoyebang/bitalosdb/v2/internal/iterator"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

func (p *page) isNeedSplitPage(size uint64) bool {
	return size >= p.bp.opts.BitpageSplitSize
}

func (p *page) makeVectorArrayTable() (*vectorArrayTable, *flushableEntry, error) {
	fn := p.bp.meta.getNextAtFileNum(p.pn)
	path := p.bp.makeFilePath(fileTypeVectorArrayTable, p.pn, fn)
	return p.newVectorArrayTable(path, fn, false)
}

func (p *page) newMergingIter(flushing flushableList) *iterator.KKVMergingIter {
	var its []InternalKKVIterator
	for i := range flushing {
		iter := flushing[i].newIter(nil)
		if iter != nil {
			its = append(its, iter)
		}
	}
	return iterator.NewKKVMergingIter(p.bp.opts.Logger, its...)
}

func (p *page) flush(sentinel []byte) (err error) {
	logTag := fmt.Sprintf("[BITPAGE %d] flush page(%d)", p.bp.index, p.pn)

	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%s flush panic err:%v stack:%s", logTag, r, string(debug.Stack()))
		}
	}()

	p.setFlushState(pageFlushStateStart)

	var oldSize uint64
	stNum := len(p.stQueue)
	flushing := make(flushableList, 0, stNum+1)
	for i := 0; i < stNum; i++ {
		flushing = append(flushing, p.stQueue[i])
		oldSize += p.stQueue[i].dataBytes()
	}

	if len(flushing) > 0 && p.arrtable != nil {
		flushing = append(flushing, p.arrtable)
		oldSize += p.arrtable.dataBytes()
	}

	if len(flushing) == 0 {
		return nil
	}

	immediateSplit := p.preFlush(flushing, oldSize, logTag)
	if immediateSplit {
		p.setFlushState(pageFlushStateFinish)
		p.pushSplitTask(sentinel)
		p.bp.opts.Logger.Infof("%s push immediate split task", logTag)
		return nil
	}

	if err = p.makeMutableForWrite(); err != nil {
		return err
	}

	var atEntry *flushableEntry
	atEntry, err = p.runFlush(flushing, oldSize, logTag)
	if err != nil {
		return err
	}

	n := len(p.stQueue) - 1
	p.bp.meta.setMinUnflushedStFileNum(p.pn, p.stQueue[n].fileNum)
	p.stQueue = p.stQueue[n:]
	p.arrtable = atEntry
	p.updateReadState()
	p.setFlushState(pageFlushStateFinish)

	for i := range flushing {
		flushing[i].setObsolete()
		flushing[i].readerUnref()
	}

	if atEntry == nil {
		return base.ErrPageFlushEmpty
	}

	if sentinel != nil && p.bp.pageNoneSplit(p.pn) {
		newSize := atEntry.inuseBytes()
		if p.isNeedSplitPage(newSize) {
			p.pushSplitTask(sentinel)
			p.bp.opts.Logger.Infof("%s push split task", logTag)
		}
	}

	return nil
}

func (p *page) preFlush(flushing flushableList, oldSize uint64, logTag string) bool {
	if !p.isNeedSplitPage(oldSize) {
		return false
	}

	var keyPrefixDeleteKind, estimateSize int
	var lastPrefixDelete []byte

	checkKeyPrefixDelete := func(ik *InternalKKVKey, iv []byte) bool {
		if lastPrefixDelete == nil {
			return false
		}

		if bytes.Equal(lastPrefixDelete, ik.Version) {
			p.bp.deleteBithashKey(iv)
			return true
		} else {
			lastPrefixDelete = nil
			return false
		}
	}

	iter := &compactionIter{
		bp:        p.bp,
		iter:      p.newMergingIter(flushing),
		isPreCalc: true,
	}
	defer iter.Close()

	startTime := time.Now()
	for iterKey, iterValue := iter.First(); iterKey != nil; iterKey, iterValue = iter.Next() {
		switch iterKey.Kind() {
		case internalKeyKindSet:
			if checkKeyPrefixDelete(iterKey, iterValue) {
				continue
			}
			estimateSize += estimateVatItemSize(iterKey.DataType, iterKey.SubKey, iterValue)
		case internalKeyKindPrefixDelete:
			lastPrefixDelete = iterKey.Version
			keyPrefixDeleteKind++
		}
	}

	isNeedSplit := p.isNeedSplitPage(uint64(estimateSize))
	p.bp.opts.Logger.Infof("%s preFlush isNeedSplit:%v oldSize:%s estimateSize:%s cost:%.3fs",
		logTag,
		isNeedSplit,
		utils.FmtSize(int64(oldSize)),
		utils.FmtSize(int64(estimateSize)),
		time.Since(startTime).Seconds())

	return isNeedSplit
}

func (p *page) runFlush(
	flushing flushableList, oldSize uint64, logTag string,
) (atEntry *flushableEntry, retErr error) {
	var at *vectorArrayTable
	var keyPrefixDeleteKind, prefixDeleteNum, deleteNum int
	var lastKeyVersion uint64

	iter := &compactionIter{
		bp:   p.bp,
		iter: p.newMergingIter(flushing),
	}
	defer iter.Close()

	defer func() {
		if retErr != nil && atEntry != nil {
			atEntry.setObsolete()
			atEntry.readerUnref()
		}
	}()

	checkKeyPrefixDelete := func(keyVersion uint64, iv []byte) bool {
		if lastKeyVersion == 0 {
			return false
		} else if keyVersion == lastKeyVersion {
			p.bp.deleteBithashKey(iv)
			return true
		} else {
			lastKeyVersion = 0
			return false
		}
	}

	p.bp.opts.Logger.Infof("%s runFlush start", logTag)

	startTime := time.Now()
	iterNum := 0
	for iterKey, iterValue := iter.First(); iterKey != nil; iterKey, iterValue = iter.Next() {
		iterNum++
		switch iterKey.Kind() {
		case internalKeyKindSet:
			if checkKeyPrefixDelete(kkv.DecodeKeyVersion(iterKey.Version), iterValue) {
				prefixDeleteNum++
				continue
			}
			if at == nil {
				at, atEntry, retErr = p.makeVectorArrayTable()
				if retErr != nil {
					return nil, retErr
				}
			}
			if _, err := at.writeItemByHash(iterKey, iterValue); err != nil {
				p.bp.opts.Logger.Errorf("%s writeItem fail err:%s", logTag, err)
			}
		case internalKeyKindPrefixDelete:
			lastKeyVersion = kkv.DecodeKeyVersion(iterKey.Version)
			keyPrefixDeleteKind++
		case internalKeyKindDelete:
			deleteNum++
		}
	}

	if at == nil {
		p.bp.opts.Logger.Infof("%s runFlush finish atNil oldSize:%s iterNum:%d deleteNum:%d pdKindKeys:%d pdDelKeys:%d in %.3fs",
			logTag, utils.FmtSize(int64(oldSize)),
			iterNum,
			deleteNum,
			keyPrefixDeleteKind,
			prefixDeleteNum,
			time.Since(startTime).Seconds())
		return nil, nil
	}

	if retErr = at.writeFinish(iter.Key()); retErr != nil {
		return nil, retErr
	}

	p.bp.meta.setNextArrayTableFileNum(p.pn)

	p.bp.opts.Logger.Infof("%s runFlush finish at:%s flushNum:%d oldSize:%s newSize:%s iterNum:%d deleteNum:%d pdKindKeys:%d pdDelKeys:%d %s in %.3fs",
		logTag,
		at.getId(),
		len(flushing),
		utils.FmtSize(int64(oldSize)),
		utils.FmtSize(int64(at.inuseBytes())),
		iterNum,
		deleteNum,
		keyPrefixDeleteKind,
		prefixDeleteNum,
		at.headerInfo(),
		time.Since(startTime).Seconds())

	return atEntry, nil
}

func (p *page) split(logTag string, pages []*page) error {
	splitNum := len(pages)
	p.bp.opts.Logger.Infof("%s start splitNum:%d splitSize:%s", logTag, splitNum, humanize.Uint64(p.bp.opts.BitpageSplitSize))
	startTime := time.Now()
	defer func() {
		if r := recover(); r != any(nil) {
			p.bp.opts.Logger.Errorf("%s split panic err:%v stack:%s", logTag, r, string(debug.Stack()))
		}
	}()

	var flushing flushableList
	var oldSize uint64
	var current int
	var writeBytes uint64
	var atCurrent *vectorArrayTable
	var lastPage, finished bool

	flushing = append(flushing, p.stQueue...)
	if p.arrtable != nil {
		flushing = append(flushing, p.arrtable)
	}
	for i := range flushing {
		oldSize += flushing[i].dataBytes()
	}

	iiter := p.newMergingIter(flushing)
	iter := &compactionIter{
		bp:   p.bp,
		iter: iiter,
	}
	defer iter.Close()

	ats := make([]*vectorArrayTable, splitNum)
	atEntrys := make([]*flushableEntry, splitNum)

	for i := 0; i < splitNum; i++ {
		at, atEntry, err := pages[i].makeVectorArrayTable()
		if err != nil {
			return err
		}

		ats[i] = at
		atEntrys[i] = atEntry
	}

	atCurrent = ats[current]
	splitSize := oldSize / uint64(splitNum)
	for iterKey, iterValue := iter.First(); iterKey != nil; iterKey, iterValue = iter.Next() {
		if iterKey.Kind() != internalKeyKindSet {
			continue
		}

		wn, err := atCurrent.writeItemByHash(iterKey, iterValue)
		if err != nil {
			return err
		}

		if finished {
			finished = false
		}

		writeBytes += uint64(wn)
		if !lastPage && writeBytes >= splitSize {
			current++
			if current == splitNum {
				lastPage = true
			} else {
				if err = atCurrent.writeFinish(iter.Key()); err != nil {
					return err
				}
				atCurrent = ats[current]
				writeBytes = 0
				finished = true
			}
		}
	}
	if !finished {
		if err := atCurrent.writeFinish(iter.Key()); err != nil {
			return err
		}
	}

	var newPageInfo strings.Builder
	for i := 0; i < splitNum; i++ {
		if !ats[i].empty() {
			pages[i].maxKey = utils.CloneBytes(ats[i].getMaxKey())
			pages[i].arrtable = atEntrys[i]
			pages[i].updateReadState()
			newPageInfo.WriteString(fmt.Sprintf("%s ", utils.FmtSize(int64(ats[i].dataBytes()))))
		} else {
			_ = p.bp.FreePage(pages[i].pn, false)
			pages[i] = nil
		}
	}

	p.bp.opts.Logger.Infof("%s finish flushing(%d) splitSize(%s) oldSize(%s) newSize(%s), cost:%.3fs",
		logTag,
		len(flushing),
		utils.FmtSize(int64(splitSize)),
		utils.FmtSize(int64(oldSize)),
		newPageInfo.String(),
		time.Since(startTime).Seconds(),
	)
	return nil
}

func (p *page) pushSplitTask(sentinel []byte) {
	p.bp.opts.BitpageTaskPushFunc(&bitask.BitpageTaskData{
		Index:    p.bp.index,
		Event:    bitask.BitpageEventSplit,
		Pn:       uint32(p.pn),
		Sentinel: sentinel,
	})
	p.bp.meta.setPageState(p.pn, pageStateSplitSendTask)
}
