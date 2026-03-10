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
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/invariants"
	"github.com/zuoyebang/bitalosdb/v2/internal/iterator"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

const (
	pageFlushStateNone uint32 = iota
	pageFlushStateSendTask
	pageFlushStateStart
	pageFlushStateFinish
)

const (
	pageStateInit uint8 = iota
	pageStateSplitSendTask
	pageStateSplitStart
	pageStateFreed
)

var pageStateDescription = map[uint8]string{
	pageStateInit:          "init",
	pageStateSplitSendTask: "splitSendTask",
	pageStateSplitStart:    "splitStart",
	pageStateFreed:         "freed",
}

type page struct {
	bp         *Bitpage
	pn         PageNum
	dirname    string
	maxKey     []byte
	flushState atomic.Uint32
	stMutable  *flushableEntry
	stQueue    flushableList
	arrtable   *flushableEntry

	readState struct {
		sync.RWMutex
		val *pageReadState
	}
}

func newPage(bp *Bitpage, pn PageNum) *page {
	return &page{
		bp:      bp,
		dirname: bp.dirname,
		pn:      pn,
	}
}

func (p *page) openFiles(pm *pagemetaItem, files []fileInfo) (err error) {
	for _, finfo := range files {
		switch finfo.ft {
		case fileTypeSuperTable:
			if finfo.fn >= pm.nextStFileNum || finfo.fn < pm.minUnflushedStFileNum {
				p.deleteObsoleteFiles(finfo.path)
			} else {
				err = p.newSuperTable(finfo.path, finfo.fn, true)
			}
		case fileTypeSuperTableIndex, fileTypeSklTableIndex:
			if finfo.fn >= pm.nextStFileNum || finfo.fn < pm.minUnflushedStFileNum {
				p.deleteObsoleteFiles(finfo.path)
			}
		case fileTypeVectorArrayTable:
			if finfo.fn == pm.curAtFileNum {
				_, p.arrtable, err = p.newVectorArrayTable(finfo.path, finfo.fn, true)
			} else {
				p.deleteObsoleteFiles(finfo.path)
			}
		case fileTypeArrayTableIndex:
			if finfo.fn != pm.curAtFileNum {
				p.deleteObsoleteFiles(finfo.path)
			}
		default:
		}
		if err != nil {
			return err
		}
	}

	p.updateReadState()

	return nil
}

func (p *page) makeMutableForWrite() error {
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

	entry := p.newFlushableEntry(st, fn)
	entry.release = func() {
		if entry.obsolete {
			st.indexModified = false
		}

		if err = st.close(); err != nil {
			p.bp.opts.Logger.Errorf("bitpage close superTable fail file:%s err:%s", path, err)
		}

		if entry.obsolete {
			files := entry.getFilePath()
			p.deleteObsoleteFiles(files...)
		}
	}

	p.stMutable = entry
	p.stQueue = append(p.stQueue, entry)

	return nil
}

func (p *page) newSklTable(path string, fn FileNum, exist bool) error {
	st, err := newSklTable(p, path, fn, exist, consts.BitpageStiCompressCountDefault)
	if err != nil {
		return err
	}

	invariants.SetFinalizer(st, checkSklTable)

	entry := p.newFlushableEntry(st, fn)
	entry.release = func() {
		if entry.obsolete {
			st.indexModified = false
		}

		if err = st.close(); err != nil {
			p.bp.opts.Logger.Errorf("bitpage close sklTable fail file:%s err:%s", path, err)
		}

		if entry.obsolete {
			files := entry.getFilePath()
			p.deleteObsoleteFiles(files...)
		}
	}

	p.stMutable = entry
	p.stQueue = append(p.stQueue, entry)

	return nil
}

func (p *page) newVectorArrayTable(path string, fn FileNum, exist bool) (*vectorArrayTable, *flushableEntry, error) {
	at, err := newVectorArrayTable(p, path, fn, exist)
	if err != nil {
		return nil, nil, err
	}

	entry := p.newFlushableEntry(at, fn)
	entry.release = func() {
		if err = at.close(); err != nil {
			p.bp.opts.Logger.Errorf("bitpage close vectorArrayTable fail file:%s err:%s", path, err)
		}

		if entry.obsolete {
			files := entry.getFilePath()
			p.deleteObsoleteFiles(files...)
		}
	}

	return at, entry, nil
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
		opts := atOptions{}
		at, err = newArrayTable(path, &opts, &cacheOpts)
	}
	if err != nil {
		return nil, nil, err
	}

	invariants.SetFinalizer(at, checkArrayTable)

	entry := p.newFlushableEntry(at, fn)
	entry.release = func() {
		if err = at.close(); err != nil {
			p.bp.opts.Logger.Errorf("bitpage close arrayTable fail file:%s err:%s", path, err)
		}

		if entry.obsolete {
			files := entry.getFilePath()
			p.deleteObsoleteFiles(files...)
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

func (p *page) getFilesPath() (files []string) {
	for _, st := range p.stQueue {
		stFiles := st.getFilePath()
		files = append(files, stFiles...)
	}

	if p.arrtable != nil {
		atFiles := p.arrtable.getFilePath()
		files = append(files, atFiles...)
	}

	return files
}

func (p *page) close(delete bool) error {
	p.readState.Lock()
	if p.readState.val != nil {
		p.readState.val.unref()
	}
	p.readState.Unlock()

	for i := range p.stQueue {
		if delete {
			p.stQueue[i].setObsolete()
		}
		p.stQueue[i].readerUnref()
	}

	if p.arrtable != nil {
		if delete {
			p.arrtable.setObsolete()
		}
		p.arrtable.readerUnref()
	}

	return nil
}

func (p *page) loadReadState() (*pageReadState, func()) {
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
	s := &pageReadState{
		stMutable: p.stMutable,
		stQueue:   p.stQueue,
		arrtable:  p.arrtable,
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
	for stIndex := len(rs.stQueue) - 1; stIndex >= 0; stIndex-- {
		st := rs.stQueue[stIndex]
		val, exist, kind := st.get(key, khash)
		if exist {
			switch kind {
			case internalKeyKindSet, internalKeyKindPrefixDelete:
				if val == nil {
					val = []byte{}
				}
				return val, true, rsCloser, kind
			case internalKeyKindDelete:
				rsCloser()
				return nil, false, nil, kind
			}
		}
	}

	if rs.arrtable != nil {
		val, exist, _ := rs.arrtable.get(key, khash)
		if exist {
			if val == nil {
				val = []byte{}
			}
			return val, true, func() { rsCloser() }, internalKeyKindSet
		}
	}

	rsCloser()
	return nil, false, nil, internalKeyKindInvalid
}

func (p *page) exist(key []byte, khash uint32) bool {
	rs, rsCloser := p.loadReadState()
	defer rsCloser()
	for stIndex := len(rs.stQueue) - 1; stIndex >= 0; stIndex-- {
		st := rs.stQueue[stIndex]
		exist, kind := st.exist(key, khash)
		if exist {
			switch kind {
			case internalKeyKindSet, internalKeyKindPrefixDelete:
				return true
			case internalKeyKindDelete:
				return false
			}
		}
	}

	if rs.arrtable == nil {
		return false
	}

	exist, _ := rs.arrtable.exist(key, khash)
	return exist
}

func (p *page) newIter(o *iterOptions) *PageIterator {
	if o == nil {
		o = &iterOptions{}
	}
	rs, rsCloser := p.loadReadState()
	buf := pageIterAllocPool.Get().(*pageIterAlloc)
	dbi := &buf.dbi
	*dbi = PageIterator{
		alloc:  buf,
		closer: rsCloser,
		iter:   &buf.merging,
		key:    buf.key,
		opts:   *o,
	}
	dbi.opts.Logger = p.bp.opts.Logger
	sts := rs.stQueue
	mLevels := buf.mLevels[:0]
	numMergingLevels := len(sts)
	if rs.arrtable != nil {
		numMergingLevels++
	}
	if numMergingLevels > cap(mLevels) {
		mLevels = make([]iterator.KKVMergingIterLevel, 0, numMergingLevels)
	}

	for i := len(sts) - 1; i >= 0; i-- {
		iter := sts[i].newIter(&dbi.opts)
		if iter != nil {
			mLevels = append(mLevels, iterator.NewKKVMergingIterLevel(iter))
		}
	}

	if rs.arrtable != nil {
		iter := rs.arrtable.newIter(&dbi.opts)
		mLevels = append(mLevels, iterator.NewKKVMergingIterLevel(iter))
	}

	buf.merging.Init(&dbi.opts, mLevels...)
	return dbi
}

func (p *page) deleteObsoleteFiles(filenames ...string) {
	var files []string
	for _, filename := range filenames {
		if os2.IsExist(filename) {
			files = append(files, filename)
		}
	}

	if len(files) > 0 {
		p.bp.opts.DeleteFilePacer.AddFiles(files)
	}
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

func (p *page) inuseBytes() uint64 {
	stSize := p.stMutable.inuseBytes()
	if stSize < consts.BitpageMutableMinSize {
		return 0
	}
	if p.arrtable == nil {
		return stSize
	}
	atSize := p.arrtable.inuseBytes()
	return stSize + atSize
}

func (p *page) memFlushFinish() error {
	return p.stMutable.flushFinish()
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
		p.bp.opts.Logger.Infof("[BITPAGE %d] need flush by force pn:%s", p.bp.index, p.pn)
		return true
	}

	stNum = len(p.stQueue)
	if stNum == 1 {
		mtKeyNum = p.stMutable.itemCount()
		mtSize = p.stMutable.inuseBytes()
		mtModTime = p.stMutable.getModTime()
		mtKeyTotal, mtDelKeyCount, mtPdKeyCount = p.stMutable.getKeyStats()
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

	return isFlush
}

func (p *page) getDeleteKeyRate(totalCount, delCount, pdCount int) float64 {
	if totalCount == 0 {
		return 0
	}
	delKeyCount := delCount + pdCount*p.bp.opts.FlushPrefixDeleteKeyMultiplier
	return float64(delKeyCount) / float64(totalCount)
}
