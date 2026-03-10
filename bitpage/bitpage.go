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
	"arena"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
	"github.com/zuoyebang/bitalosdb/v2/internal/cache/lrucache"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

type FS vfs.FS
type File vfs.File

type Bitpage struct {
	meta            *bitpagemeta
	opts            *options.BitpageOptions
	pages           sync.Map
	pageWriters     sync.Map
	dirname         string
	index           int
	stats           *Stats
	dbState         *statemachine.DbStateMachine
	cache           *lrucache.LruCache
	stKeyArena      *arena.Arena
	stKeyArenaBuf   []byte
	stValueArena    *arena.Arena
	stValueArenaBuf []byte
}

type SplitPageInfo struct {
	Pn       PageNum
	IsEmpty  bool
	Sentinel []byte
}

type PageDebugInfo struct {
	Ct    int64
	Ut    int64
	State string
}

type fileInfo struct {
	ft            FileType
	fn            FileNum
	path          string
	keyFilename   string
	valueFilename string
}

func Open(dirname string, opts *options.BitpageOptions) (b *Bitpage, err error) {
	b = &Bitpage{
		dirname:     dirname,
		opts:        opts,
		pages:       sync.Map{},
		pageWriters: sync.Map{},
		index:       opts.Index,
		stats:       newStats(),
		dbState:     opts.DbState,
		cache:       nil,
	}

	if err = b.opts.FS.MkdirAll(dirname, 0755); err != nil {
		return nil, err
	}

	if err = openManifest(b); err != nil {
		return nil, err
	}

	defer b.freeStArenaBuf()

	if err = b.openPages(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Bitpage) openPages() error {
	files, err := b.opts.FS.List(b.dirname)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	sort.Strings(files)

	pageFiles := make(map[PageNum][]fileInfo, 16)
	for i := range files {
		ft, pn, fn, ok := parseFilename(files[i])
		if !ok || ft == fileTypeManifest {
			continue
		}

		if _, exist := pageFiles[pn]; !exist {
			b.SetPage(pn, newPage(b, pn))
		}

		pageFiles[pn] = append(pageFiles[pn], fileInfo{
			ft:   ft,
			fn:   fn,
			path: b.opts.FS.PathJoin(b.dirname, files[i]),
		})
	}

	for pageNum, pm := range b.meta.mu.pagemetaMap {
		p := b.GetPage(pageNum)
		if p == nil {
			p = newPage(b, pageNum)
			b.SetPage(pageNum, p)
		} else if err = p.openFiles(pm, pageFiles[pageNum]); err != nil {
			return err
		}

		if b.IsPageFreed(pageNum) {
			if err = b.FreePage(pageNum, false); err != nil {
				b.opts.Logger.Fatalf("[BITPAGE %d] page(%d) freePage fail err:%v", b.index, pageNum, err)
			}
		} else {
			b.setPageNoneSplit(pageNum)
			if p.stMutable == nil {
				if err = p.makeMutableForWrite(); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *Bitpage) NewPage() (PageNum, error) {
	p, err := b.newPageInternal()
	if err != nil {
		return PageNum(0), err
	}
	return p.pn, nil
}

func (b *Bitpage) newPageInternal() (*page, error) {
	pn := b.meta.getNextPageNum()
	b.meta.newPagemetaItem(pn)
	p := newPage(b, pn)
	if err := p.makeMutableForWrite(); err != nil {
		b.meta.freePagemetaItem(pn)
		return nil, err
	}

	b.SetPage(pn, p)
	return p, nil
}

func (b *Bitpage) FreePage(pn PageNum, checked bool) error {
	p := b.GetPage(pn)
	if p == nil {
		return base.ErrPageNotFound
	}

	if checked && !b.IsPageFreed(pn) {
		return base.ErrPageNotFreed
	}

	if err := p.close(true); err != nil {
		return err
	}

	b.pages.Delete(pn)
	b.pageWriters.Delete(pn)
	b.meta.freePagemetaItem(p.pn)
	b.opts.Logger.Infof("[BITPAGE %d] free page(%d) done", b.index, pn)
	return nil
}

func (b *Bitpage) GetDirname() string {
	return b.dirname
}

func (b *Bitpage) GetPageDebugInfo(pn PageNum) PageDebugInfo {
	pinfo := PageDebugInfo{}
	pm := b.meta.getPagemetaItem(pn)
	if pm != nil {
		pinfo.Ct = int64(pm.createTimestamp)
		pinfo.Ut = int64(pm.updateTimestamp)
		pinfo.State = pageStateDescription[pm.state]

	}

	return pinfo
}

func (b *Bitpage) GetPage(pn PageNum) *page {
	p, ok := b.pages.Load(pn)
	if !ok {
		return nil
	}

	return p.(*page)
}

func (b *Bitpage) SetPage(pn PageNum, p *page) {
	b.pages.Store(pn, p)
}

func (b *Bitpage) GetCacheMetrics() string {
	if b.cache == nil {
		return ""
	}
	return b.cache.MetricsInfo()
}

func (b *Bitpage) GetPageStMutableDeleteKeyRate(pn PageNum) float64 {
	p := b.GetPage(pn)
	if p == nil {
		return 0
	}

	total, delCount, pdCount := p.stMutable.getKeyStats()
	return p.getDeleteKeyRate(total, delCount, pdCount)
}

func (b *Bitpage) pageNoneSplit(pn PageNum) bool {
	return b.meta.getPageState(pn) == pageStateInit
}

func (b *Bitpage) setPageNoneSplit(pn PageNum) {
	b.meta.setPageState(pn, pageStateInit)
}

func (b *Bitpage) IsPageFreed(pn PageNum) bool {
	return b.meta.getPageState(pn) == pageStateFreed
}

func (b *Bitpage) SetPageFreed(pn PageNum) {
	b.meta.setPageState(pn, pageStateFreed)
}

func (b *Bitpage) Get(pn PageNum, key []byte, khash uint32) ([]byte, bool, func(), base.InternalKeyKind) {
	p := b.GetPage(pn)
	if p == nil {
		return nil, false, nil, internalKeyKindInvalid
	}
	return p.get(key, khash)
}

func (b *Bitpage) Exist(pn PageNum, key []byte, khash uint32) bool {
	p := b.GetPage(pn)
	if p == nil {
		return false
	}
	return p.exist(key, khash)
}

func (b *Bitpage) makeFilePath(ft FileType, pn PageNum, fn FileNum) string {
	return makeFilepath(b.dirname, ft, pn, fn)
}

func (b *Bitpage) NewIter(pn PageNum, o *iterOptions) *PageIterator {
	p := b.GetPage(pn)
	if p == nil {
		return nil
	}

	return p.newIter(o)
}

func (b *Bitpage) GetPageWriter(pn PageNum, sentinel []byte) *PageWriter {
	pageWriter, ok := b.pageWriters.Load(pn)
	if ok {
		return pageWriter.(*PageWriter)
	}

	p := b.GetPage(pn)
	if p == nil {
		return nil
	}

	writer := &PageWriter{
		p:        p,
		sentinel: utils.CloneBytes(sentinel),
	}
	b.pageWriters.Store(pn, writer)
	return writer
}

func (b *Bitpage) Close() (err error) {
	b.pages.Range(func(pn, p interface{}) bool {
		err = p.(*page).close(false)
		return true
	})

	if b.meta != nil {
		if err = b.meta.close(); err != nil {
			b.opts.Logger.Errorf("bitpage close meta fail dir:%s err:%s", b.dirname, err.Error())
		}
	}

	if b.cache != nil {
		b.cache.Close()
	}

	return nil
}

func (b *Bitpage) GetPageInuseSize() []uint64 {
	var res []uint64
	b.pages.Range(func(pn, p interface{}) bool {
		if pg, ok := p.(*page); ok {
			inuse := pg.inuseBytes()
			if inuse > 0 {
				res = append(res, uint64(pg.pn), inuse)
			}
		}
		return true
	})
	return res
}

func (b *Bitpage) GetPageCount() int {
	count := 0
	b.pages.Range(func(pn, p interface{}) bool {
		count++
		return true
	})
	return count
}

func (b *Bitpage) PageFlush(pn PageNum, sentinel []byte) error {
	p := b.GetPage(pn)
	if p == nil {
		return base.ErrPageNotFound
	}

	defer p.setFlushState(pageFlushStateNone)

	if b.IsPageFreed(pn) {
		return nil
	}

	if !p.canFlush() {
		return base.ErrPageFlushState
	}

	return p.flush(sentinel)
}

func (b *Bitpage) MaybeScheduleFlush(getSentinel func(uint32) []byte) {
	b.pages.Range(func(k, v interface{}) bool {
		if pg, ok := v.(*page); ok {
			if pg.maybeScheduleFlush(b.opts.BitpageFlushSize, true) {
				pn := k.(PageNum)
				b.PushPageFlushTask(pn, getSentinel(uint32(pn)))
			}
		}
		return true
	})
}

func (b *Bitpage) PushPageFlushTask(pn PageNum, sentinel []byte) {
	p := b.GetPage(pn)
	if p != nil && p.canSendFlushTask() {
		p.setFlushState(pageFlushStateSendTask)
		b.meta.updatePageTimestamp(pn)
		b.opts.BitpageTaskPushFunc(&bitask.BitpageTaskData{
			Index:    b.opts.Index,
			Event:    bitask.BitpageEventFlush,
			Pn:       uint32(pn),
			Sentinel: sentinel,
		})
	}
}

func (b *Bitpage) ManualFlushIndexes() {
	b.pages.Range(func(pn, p interface{}) bool {
		if pg, ok := p.(*page); ok {
			st := pg.stMutable
			st.flushIndexes()
		}
		return true
	})
}

func (b *Bitpage) PageSplitStart(pn PageNum, log string) (sps []*SplitPageInfo, err error) {
	p := b.GetPage(pn)
	if p == nil {
		err = base.ErrPageNotFound
		return
	}

	if b.meta.getPageState(pn) >= pageStateSplitStart {
		err = base.ErrPageSplitted
		return
	}
	b.meta.setPageState(pn, pageStateSplitStart)

	splitNum := b.opts.SplitNum
	pages := make([]*page, splitNum)
	pns := make([]uint32, splitNum)

	for i := 0; i < splitNum; i++ {
		pages[i], err = b.newPageInternal()
		if err != nil {
			return
		}

		pns[i] = uint32(pages[i].pn)
	}

	logTag := fmt.Sprintf("%s split %s to %v", log, p.pn, pns)
	if err = p.split(logTag, pages); err != nil {
		return
	}

	for i := 0; i < splitNum; i++ {
		if pages[i] != nil {
			sps = append(sps, &SplitPageInfo{
				Pn:       pages[i].pn,
				Sentinel: pages[i].maxKey,
			})
		}
	}

	return
}

func (b *Bitpage) PageSplitEnd(pn PageNum, sps []*SplitPageInfo, retErr error) {
	if retErr == nil {
		for i := range sps {
			b.meta.setNextArrayTableFileNum(sps[i].Pn)
		}
		b.SetPageFreed(pn)
	} else {
		if retErr == base.ErrPageSplitted || retErr == base.ErrPageNotFound {
			return
		}

		b.setPageNoneSplit(pn)
		for i := range sps {
			if err := b.FreePage(sps[i].Pn, false); err != nil {
				b.opts.Logger.Errorf("PageSplitEnd FreePage fail index:%d pn:%s err:%s", b.index, sps[i].Pn, err)
			}
		}
	}
}

func (b *Bitpage) ResetStats() {
	b.stats.Reset()
}

func (b *Bitpage) Stats() *Stats {
	b.stats.Size = os2.GetDirSize(b.dirname)

	return b.stats
}

func (b *Bitpage) deleteBithashKey(value []byte) {
	if b.opts.BithashDeleteCB == nil || len(value) == 0 {
		return
	}

	isSeparate, _, dv := base.DecodeInternalValue(value)
	if isSeparate && base.CheckValueBithashValid(dv.UserValue) {
		fn := binary.LittleEndian.Uint32(dv.UserValue)
		if err := b.opts.BithashDeleteCB(fn); err != nil {
			b.opts.Logger.Errorf("delete bithash key fail err:%v", err)
		}
	}
}

func (b *Bitpage) getStKeyArenaBuf(sz int) []byte {
	alloc := utils.CalcBitsSize(sz)

	if b.stKeyArena == nil {
		b.stKeyArena = arena.NewArena()
		b.stKeyArenaBuf = arena.MakeSlice[byte](b.stKeyArena, alloc, alloc)
	} else if cap(b.stKeyArenaBuf) < sz {
		b.stKeyArena.Free()
		b.stKeyArena = arena.NewArena()
		b.stKeyArenaBuf = arena.MakeSlice[byte](b.stKeyArena, alloc, alloc)
	}

	return b.stKeyArenaBuf
}

func (b *Bitpage) getStValueArenaBuf(sz int) []byte {
	alloc := utils.CalcBitsSize(sz)

	if b.stValueArena == nil {
		b.stValueArena = arena.NewArena()
		b.stValueArenaBuf = arena.MakeSlice[byte](b.stValueArena, alloc, alloc)
	} else if cap(b.stValueArenaBuf) < sz {
		b.stValueArena.Free()
		b.stValueArena = arena.NewArena()
		b.stValueArenaBuf = arena.MakeSlice[byte](b.stValueArena, alloc, alloc)
	}

	return b.stValueArenaBuf
}

func (b *Bitpage) freeStArenaBuf() {
	if b.stKeyArena != nil {
		b.stKeyArena.Free()
		b.stKeyArena = nil
		b.stKeyArenaBuf = nil
	}
	if b.stValueArena != nil {
		b.stValueArena.Free()
		b.stValueArena = nil
		b.stValueArenaBuf = nil
	}
}
