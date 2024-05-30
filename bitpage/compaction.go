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
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/internal/bitask"

	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/humanize"
)

func (p *page) makeNewArrayTable() (*arrayTable, *flushableEntry, error) {
	fn := p.bp.meta.getNextAtFileNum(p.pn)
	path := p.bp.makeFilePath(fileTypeArrayTable, p.pn, fn)
	return p.newArrayTable(path, fn, false)
}

func (p *page) flush(sentinel []byte, logTag string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%s flush panic err:%v stack:%s", logTag, r, string(debug.Stack()))
		}
	}()

	var n int
	var flushing flushableList
	var minUnflushedStFileNum FileNum
	var oldSize uint64

	prepareFlushing := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		if err := p.makeMutableForWrite(false); err != nil {
			return errors.Wrap(err, "bitpage: makeMutableForWrite fail")
		}

		n = len(p.mu.stQueue) - 1
		minUnflushedStFileNum = p.mu.stQueue[n].fileNum

		for i := 0; i < n; i++ {
			logNum := p.mu.stQueue[i].fileNum
			if logNum >= minUnflushedStFileNum {
				return errors.New("bitpage: flush next file number is unset")
			}
			flushing = append(flushing, p.mu.stQueue[i])
			oldSize += p.mu.stQueue[i].inuseBytes()
		}

		if n > 0 && p.mu.arrtable != nil {
			flushing = append(flushing, p.mu.arrtable)
			oldSize += p.mu.arrtable.inuseBytes()
		}
		return nil
	}

	p.bp.opts.Logger.Infof("%s flush start", logTag)
	p.setFlushState(pageFlushStateStart)

	if err = prepareFlushing(); err != nil {
		return errors.Wrapf(err, "bitpage: %s flush prepareFlushing fail", logTag)
	}

	if len(flushing) == 0 {
		return nil
	}

	var atEntry *flushableEntry
	atEntry, err = p.runFlush(flushing, oldSize, logTag)
	if err != nil {
		return errors.Wrapf(err, "bitpage: %s flush fail", logTag)
	}

	p.mu.Lock()
	p.bp.meta.setMinUnflushedStFileNum(p.pn, minUnflushedStFileNum)
	p.mu.stQueue = p.mu.stQueue[n:]
	p.mu.arrtable = atEntry
	p.updateReadState()
	for i := range flushing {
		flushing[i].setObsolete()
		flushing[i].readerUnref()
	}
	p.mu.Unlock()

	p.setFlushState(pageFlushStateFinish)

	if atEntry == nil {
		return nil
	}

	if sentinel != nil && p.bp.pageNoneSplit(p.pn) {
		newSize := atEntry.inuseBytes()
		if newSize > p.bp.opts.BitpageSplitSize {
			p.bp.opts.BitpageTaskPushFunc(&bitask.BitpageTaskData{
				Index:    p.bp.index,
				Event:    bitask.BitpageEventSplit,
				Pn:       uint32(p.pn),
				Sentinel: sentinel,
			})
			p.bp.meta.setSplitState(p.pn, pageSplitStateSendTask)
			p.bp.opts.Logger.Infof("%s push split task", logTag)
		}
	}

	return nil
}

func (p *page) runFlush(flushing flushableList, oldSize uint64, logTag string) (atEntry *flushableEntry, retErr error) {
	var iiter internalIterator
	var at *arrayTable
	var keyPrefixDeleteKind, prefixDeleteNum int
	var lastPrefixDelete uint64

	startTime := time.Now()
	if len(flushing) == 1 {
		iiter = flushing[0].newIter(&iterCompactOpts)
	} else {
		its := make([]internalIterator, 0, len(flushing))
		for i := range flushing {
			its = append(its, flushing[i].newIter(&iterCompactOpts))
		}
		iiter = newMergingIter(p.bp.opts.Logger, p.bp.opts.Cmp, its...)
	}

	iter := newCompactionIter(p.bp, iiter)
	defer func() {
		if iter != nil {
			_ = iter.Close()
		}

		if retErr != nil && atEntry != nil {
			atEntry.setObsolete()
			atEntry.readerUnref()
		}
	}()

	deleteBitableKey := func(ik *internalKey) {
		if !p.bp.opts.UseBitable {
			return
		}

		if err := p.bp.opts.BitableDeleteCB(ik.UserKey); err != nil {
			p.bp.opts.Logger.Errorf("%s BitableDeleteCB fail key:%s err:%s", logTag, ik.String(), err)
		}
	}

	checkKeyPrefixDelete := func(ik *internalKey) bool {
		if lastPrefixDelete == 0 {
			return false
		}

		keyPrefixDelete := p.bp.opts.KeyPrefixDeleteFunc(ik.UserKey)
		if lastPrefixDelete == keyPrefixDelete {
			deleteBitableKey(ik)
			return true
		} else {
			lastPrefixDelete = 0
			return false
		}
	}

	p.bp.opts.Logger.Infof("%s runFlush start flushing(%d)", logTag, len(flushing))

	for iterKey, iterValue := iter.First(); iterKey != nil; iterKey, iterValue = iter.Next() {
		switch iterKey.Kind() {
		case internalKeyKindSet:
			if checkKeyPrefixDelete(iterKey) {
				prefixDeleteNum++
				continue
			}

			if p.bp.opts.CheckExpireCB(iterKey.UserKey, iterValue) {
				deleteBitableKey(iterKey)
			} else {
				if at == nil {
					at, atEntry, retErr = p.makeNewArrayTable()
					if retErr != nil {
						return nil, retErr
					}
				}
				if _, err := at.writeItem(iterKey.UserKey, iterValue); err != nil {
					p.bp.opts.Logger.Errorf("%s writeItem fail err:%s", logTag, err)
				}
			}

		case internalKeyKindDelete:
			deleteBitableKey(iterKey)

		case internalKeyKindPrefixDelete:
			lastPrefixDelete = p.bp.opts.KeyPrefixDeleteFunc(iterKey.UserKey)
			keyPrefixDeleteKind++
		}
	}

	if at == nil {
		p.bp.opts.Logger.Infof("%s runFlush finish atNil oldSize(%s) newSize(0) in %.3fs",
			logTag, humanize.Uint64(oldSize), time.Since(startTime).Seconds())
		return nil, nil
	}

	if retErr = at.writeFinish(); retErr != nil {
		return nil, retErr
	}

	p.bp.meta.setNextArrayTableFileNum(p.pn)

	duration := time.Since(startTime)
	p.bp.opts.Logger.Infof("%s runFlush finish flushed(%s) at(%s) atVersion(%d) atSize(%s) keys(%d) keysPdKind(%d) pdNum(%d), in %.3fs",
		logTag,
		humanize.Uint64(oldSize),
		at.filename,
		at.getVersion(),
		humanize.Uint64(uint64(at.size)),
		at.itemCount(),
		keyPrefixDeleteKind,
		prefixDeleteNum,
		duration.Seconds(),
	)

	return atEntry, nil
}

func (p *page) split(logTag string, pages []*page) (retErr error) {
	splitNum := len(pages)
	p.bp.opts.Logger.Infof("%s start splitNum:%d", logTag, splitNum)
	startTime := time.Now()
	defer func() {
		if r := recover(); r != any(nil) {
			p.bp.opts.Logger.Errorf("%s split panic err:%v stack:%s", logTag, r, string(debug.Stack()))
		}
	}()

	var flushing flushableList
	var oldSize uint64

	p.mu.Lock()
	flushing = append(flushing, p.mu.stQueue...)
	flushing = append(flushing, p.mu.arrtable)
	p.mu.Unlock()

	its := make([]internalIterator, 0, len(flushing))
	for i := range flushing {
		its = append(its, flushing[i].newIter(&iterCompactOpts))
		oldSize += flushing[i].dataBytes()
	}
	iiter := newMergingIter(p.bp.opts.Logger, p.bp.opts.Cmp, its...)
	iter := newCompactionIter(p.bp, iiter)
	defer iter.Close()

	var ats [consts.BitpageSplitNum]*arrayTable
	var atEntrys [consts.BitpageSplitNum]*flushableEntry

	for i := 0; i < splitNum; i++ {
		at, atEntry, err := pages[i].makeNewArrayTable()
		if retErr != nil {
			retErr = err
			return
		}

		ats[i] = at
		atEntrys[i] = atEntry
	}

	var current int
	var wn uint32
	var writeBytes uint64
	var atCurrent *arrayTable
	var lastPage, finished bool

	atCurrent = ats[current]
	splitSize := oldSize / uint64(splitNum)
	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		if key.Kind() != internalKeyKindSet {
			continue
		}

		wn, retErr = atCurrent.writeItem(key.UserKey, val)
		if retErr != nil {
			return
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
				if retErr = atCurrent.writeFinish(); retErr != nil {
					return
				}
				atCurrent = ats[current]
				writeBytes = 0
				finished = true
			}
		}
	}
	if !finished {
		if retErr = atCurrent.writeFinish(); retErr != nil {
			return
		}
	}

	var newPageInfo strings.Builder
	for i := 0; i < splitNum; i++ {
		if !ats[i].empty() {
			pages[i].maxKey = ats[i].getMaxKey()
			pages[i].mu.arrtable = atEntrys[i]
			pages[i].updateReadState()
			newPageInfo.WriteString(fmt.Sprintf("%s ", humanize.Uint64(ats[i].inuseBytes())))
		} else {
			p.bp.opts.Logger.Infof("%s free empty page pn:%d", logTag, pages[i].pn)
			_ = p.bp.FreePage(pages[i].pn, false)
			pages[i] = nil
		}
	}

	p.bp.opts.Logger.Infof("%s finish splitSize(%s) oldSize(%s) newSize(%s), cost:%.3fs",
		logTag,
		humanize.Uint64(splitSize),
		humanize.Uint64(oldSize),
		newPageInfo.String(),
		time.Since(startTime).Seconds(),
	)

	return
}
