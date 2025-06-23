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
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/cache/lrucache"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type FS vfs.FS
type File vfs.File

var (
	ErrPageNotFound    = errors.New("page not exist")
	ErrPageSplitted    = errors.New("page splitted")
	ErrPageNotSplitted = errors.New("page not splitted")
	ErrPageFlushState  = errors.New("page flush state err")
	ErrTableFull       = errors.New("allocation failed because table is full")
	ErrTableSize       = errors.New("tbl size is not large enough to hold the header")
	ErrTableOpenType   = errors.New("tbl open type not support")
)

type Bitpage struct {
	meta           *bitpagemeta
	opts           *options.BitpageOptions
	pages          sync.Map
	pageWriters    sync.Map
	splittedPages  sync.Map
	dirname        string
	index          int
	stats          *Stats
	dbState        *statemachine.DbStateMachine
	cache          *lrucache.LruCache
	stArena        *arena.Arena
	stArenaBuf     []byte
	BytesFlushed   atomic.Uint64
	BytesCompacted atomic.Uint64
}

type SplitPageInfo struct {
	Pn       PageNum
	IsEmpty  bool
	Sentinel []byte
}

type PageDebugInfo struct {
	Ct         int64
	Ut         int64
	SplitState uint8
}

type fileInfo struct {
	ft   FileType
	fn   FileNum
	path string
}

func Open(dirname string, opts *options.BitpageOptions) (b *Bitpage, err error) {
	b = &Bitpage{
		dirname:       dirname,
		opts:          opts,
		pages:         sync.Map{},
		pageWriters:   sync.Map{},
		splittedPages: sync.Map{},
		index:         opts.Index,
		stats:         newStats(),
		dbState:       opts.DbState,
		cache:         nil,
	}

	if err = b.opts.FS.MkdirAll(dirname, 0755); err != nil {
		return nil, err
	}

	if err = openManifest(b); err != nil {
		return nil, err
	}

	if b.opts.UseBlockCompress {
		cacheOpts := &options.CacheOptions{
			Size:     b.opts.BitpageBlockCacheSize,
			Shards:   consts.BitpageBlockCacheShards,
			HashSize: consts.BitpageBlockCacheHashSize,
			Logger:   b.opts.Logger,
		}
		b.cache = lrucache.NewLrucache(cacheOpts)
		b.opts.Logger.Infof("bitpage new block cache ok index:%d size:%d", opts.Index, cacheOpts.Size)
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

	pageFiles := make(map[PageNum][]fileInfo, 1<<10)
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

		if b.PageSplitted(pageNum) {
			if err = b.FreePage(pageNum, false); err != nil {
				b.opts.Logger.Errorf("bitpage freePage fail index:%d pn:%s err:%s", b.index, pageNum, err.Error())
			}
		} else {
			b.setPageNoneSplit(pageNum)
			if p.mu.stMutable == nil {
				if err = p.makeMutableForWrite(false); err != nil {
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
		return PageNum(0), nil
	}
	return p.pn, nil
}

func (b *Bitpage) newPageInternal() (*page, error) {
	pn := b.meta.getNextPageNum()
	b.meta.newPagemetaItem(pn)
	p := newPage(b, pn)
	if err := p.makeMutableForWrite(false); err != nil {
		b.meta.freePagemetaItem(pn)
		return nil, err
	}

	b.SetPage(pn, p)
	return p, nil
}

func (b *Bitpage) FreePage(pn PageNum, checked bool) error {
	p := b.GetPage(pn)
	if p == nil {
		return ErrPageNotFound
	}

	if checked && !b.PageSplitted(pn) {
		return ErrPageNotSplitted
	}

	if err := p.close(true); err != nil {
		return err
	}

	b.pages.Delete(pn)
	b.pageWriters.Delete(pn)
	b.splittedPages.Delete(pn)
	b.meta.freePagemetaItem(p.pn)

	return nil
}

func (b *Bitpage) GetPageDebugInfo(pn PageNum) PageDebugInfo {
	pinfo := PageDebugInfo{}
	pm := b.meta.getPagemetaItem(pn)
	if pm != nil {
		pinfo.Ct = int64(pm.createTimestamp)
		pinfo.Ut = int64(pm.updateTimestamp)
		pinfo.SplitState = pm.splitState
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

	total, delCount, pdCount := p.mu.stMutable.getKeyStats()
	return p.getDeleteKeyRate(total, delCount, pdCount)
}

func (b *Bitpage) pageNoneSplit(pn PageNum) bool {
	return b.meta.getSplitState(pn) == pageSplitStateNone
}

func (b *Bitpage) setPageNoneSplit(pn PageNum) {
	b.meta.setSplitState(pn, pageSplitStateNone)
}

func (b *Bitpage) PageSplitted(pn PageNum) bool {
	return b.meta.getSplitState(pn) == pageSplitStateFinish
}

func (b *Bitpage) PageSplitted2(pn PageNum) bool {
	_, ok := b.splittedPages.Load(pn)
	return ok
}

func (b *Bitpage) MarkFreePages(pns []PageNum) {
	for i := range pns {
		if b.GetPage(pns[i]) == nil {
			continue
		}
		b.markFreePage(pns[i])
	}
}

func (b *Bitpage) markFreePage(pn PageNum) {
	b.splittedPages.Store(pn, true)
	b.meta.setSplitState(pn, pageSplitStateFinish)
}

func (b *Bitpage) CheckFreePages(except PageNum) bool {
	isAllFree := true
	b.pages.Range(func(pn, p interface{}) bool {
		if pn == except {
			return true
		}

		if b.PageSplitted(pn.(PageNum)) {
			return true
		}

		isAllFree = false
		return false
	})

	return isAllFree
}

func (b *Bitpage) Get(pn PageNum, key []byte, khash uint32) ([]byte, bool, func(), base.InternalKeyKind) {
	p := b.GetPage(pn)
	if p == nil {
		return nil, false, nil, internalKeyKindInvalid
	}
	return p.get(key, khash)
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
		Sentinel: utils.CloneBytes(sentinel),
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

func (b *Bitpage) GetNeedFlushPageNums(isForce bool) []PageNum {
	var pns []PageNum
	b.pages.Range(func(pn, p interface{}) bool {
		if pg, ok := p.(*page); ok {
			if pg.maybeScheduleFlush(b.opts.BitpageFlushSize, isForce) {
				pns = append(pns, pn.(PageNum))
			}
		}
		return true
	})
	return pns
}

func (b *Bitpage) GetPageCount() int {
	count := 0
	b.pages.Range(func(pn, p interface{}) bool {
		count++
		return true
	})
	return count
}

func (b *Bitpage) PageFlush(pn PageNum, sentinel []byte, logTag string) error {
	p := b.GetPage(pn)
	if p == nil {
		return ErrPageNotFound
	}

	defer p.setFlushState(pageFlushStateNone)

	if !p.canFlush() {
		return ErrPageFlushState
	}

	if b.PageSplitted(pn) {
		return ErrPageSplitted
	}

	return p.flush(sentinel, logTag)
}

func (b *Bitpage) ManualPageFlush(pn PageNum) error {
	p := b.GetPage(pn)
	if p == nil {
		return ErrPageNotFound
	}

	p.setFlushState(pageFlushStateSendTask)
	return b.PageFlush(pn, nil, "")
}

func (b *Bitpage) PageSplitStart(pn PageNum, log string) (sps []*SplitPageInfo, err error) {
	p := b.GetPage(pn)
	if p == nil {
		err = ErrPageNotFound
		return
	}

	if b.meta.getSplitState(pn) >= pageSplitStateStart {
		err = ErrPageSplitted
		return
	}
	b.meta.setSplitState(pn, pageSplitStateStart)

	var pages [consts.BitpageSplitNum]*page
	var pns [consts.BitpageSplitNum]uint32
	splitNum := consts.BitpageSplitNum
	for i := 0; i < splitNum; i++ {
		pages[i], err = b.newPageInternal()
		if err != nil {
			return
		}

		pns[i] = uint32(pages[i].pn)
	}

	logTag := fmt.Sprintf("%s split %s to %v", log, p.pn, pns)
	if err = p.split(logTag, pages[:]); err != nil {
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
		b.markFreePage(pn)
	} else {
		if retErr == ErrPageSplitted || retErr == ErrPageNotFound {
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
	b.stats.Size = utils.GetDirSize(b.dirname)

	return b.stats
}

func (b *Bitpage) StatsToString() string {
	return b.stats.String()
}

func (b *Bitpage) deleteBithashKey(value []byte) {
	if len(value) == 0 {
		return
	}

	dv := base.DecodeInternalValue(value)
	if dv.Kind() == base.InternalKeyKindSetBithash && base.CheckValueValidByKeySetBithash(dv.UserValue) {
		fn := binary.LittleEndian.Uint32(dv.UserValue)
		if err := b.opts.BithashDeleteCB(fn); err != nil {
			b.opts.Logger.Errorf("delete bithash key fail err:%v", err)
		}
	}
}

func (b *Bitpage) getStArenaBuf(sz int) []byte {
	alloc := utils.CalcBitsSize(sz)

	if b.stArena == nil {
		b.stArena = arena.NewArena()
		b.stArenaBuf = arena.MakeSlice[byte](b.stArena, alloc, alloc)
	} else if cap(b.stArenaBuf) < sz {
		b.stArena.Free()
		b.stArena = arena.NewArena()
		b.stArenaBuf = arena.MakeSlice[byte](b.stArena, alloc, alloc)
	}

	return b.stArenaBuf
}

func (b *Bitpage) freeStArenaBuf() {
	if b.stArena != nil {
		b.stArena.Free()
		b.stArena = nil
		b.stArenaBuf = nil
	}
}
