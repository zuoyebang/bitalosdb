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
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"golang.org/x/exp/rand"
)

const (
	pageFlushStateNone uint32 = iota
	pageFlushStateSendTask
	pageFlushStateStart
	pageFlushStateFinish
)

const (
	pageSplitStateNone uint8 = iota
	pageSplitStateSendTask
	pageSplitStateStart
	pageSplitStateFinish
)

type page struct {
	bp         *Bitpage
	pn         PageNum
	dirname    string
	maxKey     []byte
	flushState atomic.Uint32

	mu struct {
		sync.RWMutex
		stMutable *superTable
		stQueue   flushableList
		arrtable  *flushableEntry
	}

	readState struct {
		sync.RWMutex
		val *readState
	}
}

func newPage(bp *Bitpage, pn PageNum) *page {
	return &page{
		bp:      bp,
		dirname: bp.dirname,
		pn:      pn,
	}
}

func (p *page) openFiles(pm *pagemetaItem, files []fileInfo) error {
	var deleteFiles []string

	addDeleteFile := func(name string) {
		if utils.IsFileNotExist(name) {
			return
		}
		deleteFiles = append(deleteFiles, name)
	}

	for _, f := range files {
		switch f.ft {
		case fileTypeSuperTable:
			if f.fn >= pm.nextStFileNum || f.fn < pm.minUnflushedStFileNum {
				addDeleteFile(f.path)
			} else if err := p.newSuperTable(f.path, f.fn, true); err != nil {
				return err
			}
		case fileTypeArrayTable:
			if f.fn == pm.curAtFileNum {
				_, atEntry, err := p.newArrayTable(f.path, f.fn, true)
				if err != nil {
					return err
				}
				p.mu.arrtable = atEntry
			} else {
				addDeleteFile(f.path)
			}
		}
	}

	if len(deleteFiles) > 0 {
		p.bp.opts.DeleteFilePacer.AddFiles(deleteFiles)
	}

	p.updateReadState()

	return nil
}

func (p *page) makeMutableForWrite(flushIdx bool) error {
	st := p.mu.stMutable
	if st != nil {
		if st.empty() {
			return nil
		}

		if flushIdx {
			if err := st.writeIdxToFile(); err != nil {
				return err
			}
		}
	}

	fn := p.bp.meta.getNextStFileNum(p.pn)
	path := p.bp.makeFilePath(fileTypeSuperTable, p.pn, fn)
	if err := p.newSuperTable(path, fn, false); err != nil {
		return err
	}

	p.updateReadState()
	return nil
}

func (p *page) newSuperTable(path string, fn FileNum, exist bool) error {
	st, err := newSuperTable(p, path, fn, exist)
	if err != nil {
		return err
	}

	invariants.SetFinalizer(st, checkSuperTable)

	idxPath := st.getIdxFilePath()
	entry := p.newFlushableEntry(st, fn)
	entry.release = func() {
		if entry.obsolete {
			st.indexModified = false
		}

		if err := st.close(); err != nil {
			p.bp.opts.Logger.Errorf("bitpage close superTable fail file:%s err:%s", path, err.Error())
		}

		if entry.obsolete {
			p.deleteObsoleteFile(path)
			p.deleteObsoleteFile(idxPath)
		}
	}

	p.mu.stMutable = st
	p.mu.stQueue = append(p.mu.stQueue, entry)

	return nil
}

func (p *page) newArrayTable(path string, fn FileNum, exist bool) (*arrayTable, *flushableEntry, error) {
	var err error
	var at *arrayTable

	cacheOpts := atCacheOptions{
		cache: p.bp.cache,
		id:    (uint64(p.pn) << 32) | uint64(fn),
	}

	if exist {
		at, err = openArrayTable(path, &cacheOpts)
	} else {
		opts := atOptions{
			useMapIndex:       p.bp.opts.UseMapIndex,
			usePrefixCompress: p.bp.opts.UsePrefixCompress,
			useBlockCompress:  p.bp.opts.UseBlockCompress,
			blockSize:         consts.BitpageBlockSize,
		}
		at, err = newArrayTable(path, &opts, &cacheOpts)
	}
	if err != nil {
		return nil, nil, err
	}

	invariants.SetFinalizer(at, checkArrayTable)

	entry := p.newFlushableEntry(at, fn)
	entry.release = func() {
		if err := at.close(); err != nil {
			p.bp.opts.Logger.Errorf("bitpage close arrayTable fail file:%s err:%s", path, err.Error())
		}

		if entry.obsolete {
			p.deleteObsoleteFile(path)
		}
	}

	return at, entry, nil
}

func (p *page) newFlushableEntry(f flushable, fn FileNum) *flushableEntry {
	entry := &flushableEntry{
		flushable: f,
		fileNum:   fn,
		obsolete:  false,
	}
	entry.readerRefs.Store(1)
	return entry
}

func (p *page) getFilesPath() []string {
	var paths []string
	for _, st := range p.mu.stQueue {
		paths = append(paths, st.path())
		idxFile := st.idxFilePath()
		if utils.IsFileExist(idxFile) {
			paths = append(paths, idxFile)
		}
	}
	if p.mu.arrtable != nil {
		paths = append(paths, p.mu.arrtable.path())
	}
	return paths
}

func (p *page) close(delete bool) error {
	p.readState.Lock()
	if p.readState.val != nil {
		p.readState.val.unref()
	}
	p.readState.Unlock()

	for i := range p.mu.stQueue {
		if delete {
			p.mu.stQueue[i].setObsolete()
		}
		p.mu.stQueue[i].readerUnref()
	}

	if p.mu.arrtable != nil {
		if delete {
			p.mu.arrtable.setObsolete()
		}
		p.mu.arrtable.readerUnref()
	}

	return nil
}

func (p *page) loadReadState() (*readState, func()) {
	p.readState.RLock()
	state := p.readState.val
	state.stMutable.mmapRLock()
	state.ref()
	p.readState.RUnlock()
	return state, func() {
		state.stMutable.mmapRUnLock()
		state.unref()
	}
}

func (p *page) updateReadState() {
	s := &readState{
		stMutable: p.mu.stMutable,
		stQueue:   p.mu.stQueue,
		arrtable:  p.mu.arrtable,
	}
	s.refcnt.Store(1)

	for i := range s.stQueue {
		s.stQueue[i].readerRef()
	}

	if s.arrtable != nil {
		s.arrtable.readerRef()
	}

	p.readState.Lock()
	old := p.readState.val
	p.readState.val = s
	p.readState.Unlock()

	if old != nil {
		old.unref()
	}
}

func (p *page) get(key []byte, khash uint32) ([]byte, bool, func(), internalKeyKind) {
	rs, rsCloser := p.loadReadState()

	stIndex := len(rs.stQueue) - 1
	for stIndex >= 0 {
		st := rs.stQueue[stIndex]
		val, exist, kind, _ := st.get(key, khash)
		if exist {
			switch kind {
			case internalKeyKindSet, internalKeyKindPrefixDelete:
				return val, true, rsCloser, kind
			case internalKeyKindDelete:
				rsCloser()
				return nil, false, nil, kind
			}
		}

		stIndex--
	}

	if rs.arrtable != nil {
		val, exist, _, atCloser := rs.arrtable.get(key, khash)
		if exist {
			closer := func() {
				rsCloser()
				if atCloser != nil {
					atCloser()
				}
			}
			return val, true, closer, internalKeyKindSet
		}
	}

	rsCloser()
	return nil, false, nil, internalKeyKindInvalid
}

func (p *page) newIter(o *iterOptions) *PageIterator {
	rs, rsCloser := p.loadReadState()

	buf := pageIterAllocPool.Get().(*pageIterAlloc)
	dbi := &buf.dbi
	*dbi = PageIterator{
		alloc:               buf,
		cmp:                 bytes.Compare,
		equal:               bytes.Equal,
		readState:           rs,
		readStateCloser:     rsCloser,
		iter:                &buf.merging,
		key:                 &buf.key,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
	}
	if o != nil {
		dbi.opts = *o
	}
	dbi.opts.Logger = p.bp.opts.Logger

	sts := rs.stQueue
	mlevels := buf.mlevels[:0]
	numMergingLevels := len(sts)
	if rs.arrtable != nil {
		numMergingLevels++
	}
	if numMergingLevels > cap(mlevels) {
		mlevels = make([]mergingIterLevel, 0, numMergingLevels)
	}

	for i := len(sts) - 1; i >= 0; i-- {
		mlevels = append(mlevels, mergingIterLevel{
			iter: sts[i].newIter(&dbi.opts),
		})
	}

	if rs.arrtable != nil {
		mlevels = append(mlevels, mergingIterLevel{
			iter: rs.arrtable.newIter(&dbi.opts),
		})
	}

	buf.merging.Init(&dbi.opts, dbi.cmp, mlevels...)
	return dbi
}

func (p *page) set(key internalKey, value []byte) error {
	p.mu.RLock()
	st := p.mu.stMutable
	p.mu.RUnlock()

	st.kindStatis(key.Kind())
	return st.set(key, value)
}

func (p *page) setMulti(key internalKey, values ...[]byte) error {
	p.mu.RLock()
	st := p.mu.stMutable
	p.mu.RUnlock()

	st.kindStatis(key.Kind())
	return st.setMulti(key, values...)
}

func (p *page) deleteObsoleteFile(filename string) {
	if utils.IsFileNotExist(filename) {
		return
	}

	p.bp.opts.DeleteFilePacer.AddFile(filename)
}

func (p *page) canSendFlushTask() bool {
	return p.getFlushState() == pageFlushStateNone
}

func (p *page) canFlush() bool {
	return p.getFlushState() == pageFlushStateSendTask
}

func (p *page) getFlushState() uint32 {
	return p.flushState.Load()
}

func (p *page) setFlushState(v uint32) {
	p.flushState.Store(v)
}

func (p *page) memFlushFinish() error {
	return p.mu.stMutable.mergeIndexes()
}

func (p *page) maybeScheduleFlush(flushSize uint64, isForce bool) bool {
	if !p.canSendFlushTask() {
		return false
	}

	var (
		stNum         int
		mtKeyNum      int
		mtSize        uint64
		mtKeyTotal    int
		mtDelKeyCount int
		mtPdKeyCount  int
		mtModTime     int64
		mtDelKeyRate  float64
		isFlush       bool
	)

	if isForce {
		p.setFlushState(pageFlushStateSendTask)
		p.bp.opts.Logger.Infof("[BITPAGE %d] need flush by force pn:%s", p.bp.index, p.pn)
		return true
	}

	stNum = len(p.mu.stQueue)
	if stNum == 1 {
		mtKeyNum = p.mu.stMutable.itemCount()
		mtSize = p.mu.stMutable.inuseBytes()
		mtModTime = p.mu.stMutable.getModTime()
		mtKeyTotal, mtDelKeyCount, mtPdKeyCount = p.mu.stMutable.getKeyStats()
	}

	if stNum > 1 {
		isFlush = true
		p.bp.opts.Logger.Infof("[BITPAGE %d] pn:%s need flush by stNum:%d", p.bp.index, p.pn, stNum)
	} else if mtSize > flushSize {
		isFlush = true
		p.bp.opts.Logger.Infof("[BITPAGE %d] pn:%s need flush by size:%d|%d", p.bp.index, p.pn, mtSize, flushSize)
	} else if consts.CheckFlushItemCount(mtKeyNum, mtSize, flushSize) {
		isFlush = true
		p.bp.opts.Logger.Infof("[BITPAGE %d] pn:%s need flush by mtKeyNum:%d size:%d|%d", p.bp.index, p.pn, mtKeyNum, mtSize, flushSize)
	} else {
		mtDelKeyRate = p.getDeleteKeyRate(mtKeyTotal, mtDelKeyCount, mtPdKeyCount)
		if consts.CheckFlushDelPercent(mtDelKeyRate, mtSize, flushSize) {
			isFlush = true
			p.bp.opts.Logger.Infof("[BITPAGE %d] pn:%s need flush by mtDelKeyRate:%.2f multiplier:%d mtStatKey:%d|%d|%d size:%d|%d",
				p.bp.index, p.pn, mtDelKeyRate, p.bp.opts.FlushPrefixDeleteKeyMultiplier, mtKeyTotal, mtDelKeyCount, mtPdKeyCount, mtSize, flushSize)
		} else if mtModTime > 0 {
			mtLifeTime := int64(p.bp.opts.FlushFileLifetime*(rand.Intn(10)+10)/10) + mtModTime
			if consts.CheckFlushLifeTime(mtLifeTime, mtSize, flushSize) {
				isFlush = true
				p.bp.opts.Logger.Infof("[BITPAGE %d] pn:%s need flush by mtLifeTime:%s mtModTime:%s size:%d|%d",
					p.bp.index, p.pn, utils.FmtUnixTime(mtLifeTime), utils.FmtUnixTime(mtModTime), mtSize, flushSize)
			}
		}
	}

	if isFlush {
		p.setFlushState(pageFlushStateSendTask)
	}

	return isFlush
}

func (p *page) getDeleteKeyRate(totalCount, delCount, pdCount int) float64 {
	if totalCount == 0 {
		return 0
	}
	delKeyCount := delCount + pdCount*p.bp.opts.FlushPrefixDeleteKeyMultiplier
	return float64(delKeyCount) / float64(totalCount)
}
