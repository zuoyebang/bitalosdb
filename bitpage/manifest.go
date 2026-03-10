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
	"bufio"
	"encoding/binary"
	"errors"
	"io/fs"
	"sync"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/list2"
	"github.com/zuoyebang/bitalosdb/v2/internal/mmap"
)

const (
	versionV1 uint16 = iota + 1
)

const (
	upgradeV1 uint16 = iota
	upgradeV140
)

const (
	versionCurrent = versionV1

	pageMetadataNum = 10000
	pageMetadataLen = 40
	pagemetaMapLen  = pageMetadataLen * pageMetadataNum

	manifestHeaderLen = 8
	manifestMagicLen  = 8
	manifestFooterLen = manifestMagicLen
	manifestMagic     = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	manifestLen       = manifestHeaderLen + pagemetaMapLen + manifestFooterLen

	versionOffset     = 0
	nextPageNumOffset = 4
	pagemetaMapOffset = 8
	footerOffset      = manifestLen - manifestFooterLen

	itemPageNumOffset      = 0
	itemStFileNumOffset    = 4
	itemAtFileNumOffset    = 8
	itemMinUnflushedOffset = 12
	itemPageState          = 16
	itemCreateTimestamp    = 17
	itemUpdateTimestamp    = 25
	itemUpgrade            = 33
)

type bitpagemeta struct {
	b       *Bitpage
	version uint16
	mu      struct {
		sync.RWMutex
		manifest      *mmap.MMap
		curPageNum    PageNum
		pageFreelist  *list2.IntQueue
		pagemetaMap   map[PageNum]*pagemetaItem
		pagemetaArray [pageMetadataNum]pagemetaItem
	}
}

type pagemetaItem struct {
	pagemeta
	pos int
}

type pagemeta struct {
	pageNum               PageNum
	nextStFileNum         FileNum
	curAtFileNum          FileNum
	minUnflushedStFileNum FileNum
	state                 uint8
	createTimestamp       uint64
	updateTimestamp       uint64
	upgrade               uint16
}

func openManifest(b *Bitpage) error {
	b.meta = &bitpagemeta{b: b}
	b.meta.mu.pagemetaMap = make(map[PageNum]*pagemetaItem, 1<<4)
	b.meta.mu.pageFreelist = list2.NewIntQueue(pageMetadataNum)

	filename := makeFilepath(b.dirname, fileTypeManifest, 0, 0)
	if _, err := b.opts.FS.Stat(filename); errors.Is(err, fs.ErrNotExist) {
		if err = b.meta.createManifest(filename); err != nil {
			return err
		}
	}

	if err := b.meta.loadManifest(filename); err != nil {
		return err
	}

	//b.opts.Logger.Infof("[BITPAGE %d] open manifest success version:%d len:%d uses:%d frees:%d",
	//	b.index,
	//	b.meta.version,
	//	b.meta.mu.manifest.Len(),
	//	len(b.meta.mu.pagemetaMap),
	//	b.meta.mu.pageFreelist.Len())

	return nil
}

func (m *bitpagemeta) createManifest(filename string) (err error) {
	var (
		manifestFile File
		manifest     *bufio.Writer
	)

	manifestFile, err = m.b.opts.FS.Create(filename)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = m.b.opts.FS.Remove(filename)
		}
		if manifestFile != nil {
			err = manifestFile.Close()
		}
	}()

	manifest = bufio.NewWriterSize(manifestFile, manifestLen)
	buf := make([]byte, manifestLen)
	binary.LittleEndian.PutUint16(buf[0:2], versionCurrent)
	copy(buf[footerOffset:footerOffset+manifestFooterLen], manifestMagic)

	if _, err = manifest.Write(buf); err != nil {
		return err
	}
	if err = manifest.Flush(); err != nil {
		return err
	}
	if err = manifestFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (m *bitpagemeta) loadManifest(filename string) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mu.manifest, err = mmap.Open(filename, 0)
	if err != nil {
		return err
	}

	m.version = m.mu.manifest.ReadUInt16At(versionOffset)
	m.mu.curPageNum = PageNum(m.mu.manifest.ReadUInt32At(nextPageNumOffset))

	pos := pagemetaMapOffset
	for arrIdx := 0; arrIdx < pageMetadataNum; arrIdx++ {
		pm := m.pagemetaInBuffer(pos)
		m.mu.pagemetaArray[arrIdx] = pagemetaItem{pm, pos}
		if pm.pageNum > 0 {
			m.mu.pagemetaMap[pm.pageNum] = &(m.mu.pagemetaArray[arrIdx])
		} else {
			m.mu.pageFreelist.Push(int32(pos))
		}

		pos += pageMetadataLen
	}

	return nil
}

func (m *bitpagemeta) close() error {
	if m.mu.manifest != nil {
		return m.mu.manifest.Close()
	}
	return nil
}

func (m *bitpagemeta) getPagesFreelistLen() int {
	return m.mu.pageFreelist.Len()
}

func (m *bitpagemeta) getNextPagemetaPos() int {
	if m.mu.pageFreelist.Empty() {
		panic("bitpage has no free meta")
	}

	value, _ := m.mu.pageFreelist.Pop()
	return int(value)
}

func (m *bitpagemeta) newPagemetaItem(pageNum PageNum) *pagemetaItem {
	m.mu.Lock()
	defer m.mu.Unlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem != nil {
		return pmItem
	}

	pos := m.getNextPagemetaPos()

	arrIdx := (pos - pagemetaMapOffset) / pageMetadataLen
	m.resetTupleMate(pageNum, pos, arrIdx, true)
	pmItem = &(m.mu.pagemetaArray[arrIdx])
	m.mu.pagemetaMap[pageNum] = pmItem

	return pmItem
}

func (m *bitpagemeta) freePagemetaItem(pageNum PageNum) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return
	}

	arrIdx := (pmItem.pos - pagemetaMapOffset) / pageMetadataLen
	m.resetTupleMate(PageNum(0), pmItem.pos, arrIdx, false)
	m.mu.pageFreelist.Push(int32(pmItem.pos))
	delete(m.mu.pagemetaMap, pageNum)
}

func (m *bitpagemeta) getPagemetaItem(pageNum PageNum) *pagemetaItem {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pmItem, ok := m.mu.pagemetaMap[pageNum]
	if !ok {
		return nil
	}
	return pmItem
}

func (m *bitpagemeta) pagemetaInBuffer(pos int) pagemeta {
	return pagemeta{
		pageNum:               PageNum(m.mu.manifest.ReadUInt32At(pos + itemPageNumOffset)),
		nextStFileNum:         FileNum(m.mu.manifest.ReadUInt32At(pos + itemStFileNumOffset)),
		curAtFileNum:          FileNum(m.mu.manifest.ReadUInt32At(pos + itemAtFileNumOffset)),
		minUnflushedStFileNum: FileNum(m.mu.manifest.ReadUInt32At(pos + itemMinUnflushedOffset)),
		state:                 m.mu.manifest.ReadUInt8At(pos + itemPageState),
		createTimestamp:       m.mu.manifest.ReadUInt64At(pos + itemCreateTimestamp),
		updateTimestamp:       m.mu.manifest.ReadUInt64At(pos + itemUpdateTimestamp),
		upgrade:               m.mu.manifest.ReadUInt16At(pos + itemUpgrade),
	}
}

func (m *bitpagemeta) resetTupleMate(pn PageNum, pos, arrIdx int, reuse bool) {
	pm := pagemeta{
		pageNum:               pn,
		nextStFileNum:         FileNum(1),
		curAtFileNum:          FileNum(0),
		minUnflushedStFileNum: FileNum(1),
		state:                 0,
		createTimestamp:       0,
		updateTimestamp:       0,
		upgrade:               upgradeV140,
	}

	if reuse {
		pm.createTimestamp = uint64(time.Now().UnixMilli())
	}

	m.mu.pagemetaArray[arrIdx] = pagemetaItem{pm, pos}
	m.mu.manifest.WriteUInt32At(uint32(pm.pageNum), pos+itemPageNumOffset)
	m.mu.manifest.WriteUInt32At(uint32(pm.nextStFileNum), pos+itemStFileNumOffset)
	m.mu.manifest.WriteUInt32At(uint32(pm.curAtFileNum), pos+itemAtFileNumOffset)
	m.mu.manifest.WriteUInt32At(uint32(pm.minUnflushedStFileNum), pos+itemMinUnflushedOffset)
	m.mu.manifest.WriteUInt8At(pm.state, pos+itemPageState)
	m.mu.manifest.WriteUInt64At(pm.createTimestamp, pos+itemCreateTimestamp)
	m.mu.manifest.WriteUInt64At(pm.updateTimestamp, pos+itemUpdateTimestamp)
	m.mu.manifest.WriteUInt16At(pm.upgrade, pos+itemUpgrade)
}

func (m *bitpagemeta) getPagemetaItemLocked(pageNum PageNum) *pagemetaItem {
	pmItem, ok := m.mu.pagemetaMap[pageNum]
	if !ok {
		return nil
	}
	return pmItem
}

func (m *bitpagemeta) getCurrentPageNum() PageNum {
	return m.mu.curPageNum
}

func (m *bitpagemeta) getNextPageNum() PageNum {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mu.curPageNum++
	m.mu.manifest.WriteUInt32At(uint32(m.mu.curPageNum), nextPageNumOffset)
	return m.mu.curPageNum
}

func (m *bitpagemeta) getNextStFileNum(pageNum PageNum) FileNum {
	m.mu.Lock()
	defer m.mu.Unlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return FileNum(0)
	}

	fn := pmItem.nextStFileNum
	pmItem.nextStFileNum++
	m.mu.manifest.WriteUInt32At(uint32(pmItem.nextStFileNum), pmItem.pos+itemStFileNumOffset)
	return fn
}

func (m *bitpagemeta) getNextAtFileNum(pageNum PageNum) FileNum {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return FileNum(0)
	}

	fn := pmItem.curAtFileNum + FileNum(1)
	return fn
}

func (m *bitpagemeta) setNextArrayTableFileNum(pageNum PageNum) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return
	}

	pmItem.curAtFileNum++
	m.mu.manifest.WriteUInt32At(uint32(pmItem.curAtFileNum), pmItem.pos+itemAtFileNumOffset)
}

func (m *bitpagemeta) getMinUnflushedStFileNum(pageNum PageNum) FileNum {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return FileNum(0)
	}

	return pmItem.minUnflushedStFileNum
}

func (m *bitpagemeta) setMinUnflushedStFileNum(pageNum PageNum, fn FileNum) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return
	}

	pmItem.minUnflushedStFileNum = fn
	m.mu.manifest.WriteUInt32At(uint32(fn), pmItem.pos+itemMinUnflushedOffset)
}

func (m *bitpagemeta) getPageState(pageNum PageNum) uint8 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return 0
	}

	return pmItem.state
}

func (m *bitpagemeta) setPageState(pageNum PageNum, state uint8) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil || pmItem.state == state {
		return
	}

	pmItem.state = state
	m.mu.manifest.WriteUInt8At(state, pmItem.pos+itemPageState)
}

func (m *bitpagemeta) updatePageTimestamp(pageNum PageNum) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return
	}

	pmItem.updateTimestamp = uint64(time.Now().UnixMilli())
	m.mu.manifest.WriteUInt64At(pmItem.updateTimestamp, pmItem.pos+itemUpdateTimestamp)
}

func (m *bitpagemeta) getPageUpgrade(pageNum PageNum) uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil {
		return 0
	}

	return pmItem.upgrade
}

func (m *bitpagemeta) setPageUpgrade(pageNum PageNum, upgrade uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pmItem := m.getPagemetaItemLocked(pageNum)
	if pmItem == nil || pmItem.upgrade == upgrade {
		return
	}

	pmItem.upgrade = upgrade
	m.mu.manifest.WriteUInt16At(upgrade, pmItem.pos+itemUpgrade)
}
