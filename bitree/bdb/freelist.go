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

package bdb

import (
	"fmt"
	"sort"
	"unsafe"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

type txPending struct {
	ids              []pgid
	alloctx          []txid
	lastReleaseBegin txid
}

type pidSet map[pgid]struct{}

type freelist struct {
	freelistType   string
	ids            []pgid
	allocs         map[pgid]txid
	pending        map[txid]*txPending
	cache          map[pgid]bool
	freemaps       map[uint64]pidSet
	forwardMap     map[pgid]uint64
	backwardMap    map[pgid]uint64
	allocate       func(txid txid, n int) pgid
	free_count     func() int
	mergeSpans     func(ids pgids)
	getFreePageIDs func() []pgid
	readIDs        func(pgids []pgid)
	convertBitmap  func() (rb *roaring64.Bitmap, count int)
}

func newFreelist(freelistType string) *freelist {
	f := &freelist{
		freelistType: freelistType,
		allocs:       make(map[pgid]txid),
		pending:      make(map[txid]*txPending),
		cache:        make(map[pgid]bool),
		freemaps:     make(map[uint64]pidSet),
		forwardMap:   make(map[pgid]uint64),
		backwardMap:  make(map[pgid]uint64),
	}

	if freelistType == consts.BdbFreelistMapType {
		f.allocate = f.hashmapAllocate
		f.free_count = f.hashmapFreeCount
		f.mergeSpans = f.hashmapMergeSpans
		f.getFreePageIDs = f.hashmapGetFreePageIDs
		f.readIDs = f.hashmapReadIDs
		f.convertBitmap = f.hashmapConvertBitmap
	} else {
		f.allocate = f.arrayAllocate
		f.free_count = f.arrayFreeCount
		f.mergeSpans = f.arrayMergeSpans
		f.getFreePageIDs = f.arrayGetFreePageIDs
		f.readIDs = f.arrayReadIDs
		f.convertBitmap = f.arrayConvertBitmap
	}

	return f
}

func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		n++
	}
	return int(pageHeaderSize) + (int(unsafe.Sizeof(pgid(0))) * n)
}

func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

func (f *freelist) arrayFreeCount() int {
	return len(f.ids)
}

func (f *freelist) pending_count() int {
	var count int
	for _, txp := range f.pending {
		count += len(txp.ids)
	}
	return count
}

func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, txp := range f.pending {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	mergepgids(dst, f.getFreePageIDs(), m)
}

func (f *freelist) arrayAllocate(txid txid, n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		if previd == 0 || id-previd != 1 {
			initial = id
		}

		if (id-initial)+1 == pgid(n) {
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}
			f.allocs[initial] = txid
			return initial
		}

		previd = id
	}
	return 0
}

func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	txp := f.pending[txid]
	if txp == nil {
		txp = &txPending{}
		f.pending[txid] = txp
	}
	allocTxid, ok := f.allocs[p.id]
	if ok {
		delete(f.allocs, p.id)
	} else if (p.flags & freelistPageFlag) != 0 {
		allocTxid = txid - 1
	}

	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		txp.ids = append(txp.ids, id)
		txp.alloctx = append(txp.alloctx, allocTxid)
		f.cache[id] = true
	}
}

func (f *freelist) release(txid txid) pgids {
	m := make(pgids, 0)
	for tid, txp := range f.pending {
		if tid <= txid {
			m = append(m, txp.ids...)
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
	return m
}

func (f *freelist) releaseRange(begin, end txid) pgids {
	if begin > end {
		return nil
	}
	var m pgids
	for tid, txp := range f.pending {
		if tid < begin || tid > end {
			continue
		}
		if txp.lastReleaseBegin == begin {
			continue
		}
		for i := 0; i < len(txp.ids); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			m = append(m, txp.ids[i])
			txp.ids[i] = txp.ids[len(txp.ids)-1]
			txp.ids = txp.ids[:len(txp.ids)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		txp.lastReleaseBegin = begin
		if len(txp.ids) == 0 {
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
	return m
}

func (f *freelist) rollback(txid txid) {
	txp := f.pending[txid]
	if txp == nil {
		return
	}
	var m pgids
	for i, pgid := range txp.ids {
		delete(f.cache, pgid)
		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			f.allocs[pgid] = tx
		} else {
			m = append(m, pgid)
		}
	}

	delete(f.pending, txid)
	f.mergeSpans(m)
}

func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

func (f *freelist) read(p *page) {
	if (p.flags & freelistPageFlag) == 0 {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.id, p.typ()))
	}
	var idx, count = 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		c := *(*pgid)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
		count = int(c)
		if count < 0 {
			panic(fmt.Sprintf("leading element count %d overflows int", c))
		}
	}

	if count == 0 {
		f.ids = nil
	} else {
		var ids []pgid
		data := unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p), unsafe.Sizeof(ids[0]), idx)
		unsafeSlice(unsafe.Pointer(&ids), data, count)

		idsCopy := make([]pgid, count)
		copy(idsCopy, ids)
		sort.Sort(pgids(idsCopy))

		f.readIDs(idsCopy)
	}
}

func (f *freelist) arrayReadIDs(ids []pgid) {
	f.ids = ids
	f.reindex()
}

func (f *freelist) arrayGetFreePageIDs() []pgid {
	return f.ids
}

func (f *freelist) arrayConvertBitmap() (rb *roaring64.Bitmap, count int) {
	rb = roaring64.NewBitmap()
	for _, txp := range f.pending {
		for _, id := range txp.ids {
			rb.Add(uint64(id))
			count++
		}
	}
	for _, id := range f.ids {
		rb.Add(uint64(id))
		count++
	}

	return
}

func (f *freelist) write(p *page) error {
	p.flags |= freelistPageFlag
	l := f.count()
	if l == 0 {
		p.count = uint16(l)
	} else if l < 0xFFFF {
		p.count = uint16(l)
		var ids []pgid
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		unsafeSlice(unsafe.Pointer(&ids), data, l)
		f.copyall(ids)
	} else {
		p.count = 0xFFFF
		var ids []pgid
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		unsafeSlice(unsafe.Pointer(&ids), data, l+1)
		ids[0] = pgid(l)
		f.copyall(ids[1:])
	}

	return nil
}

func (f *freelist) reload(p *page, version uint32) {
	if version == versionFreelistBitmap {
		f.readFromBitmap(p)
	} else {
		f.read(p)
	}

	pcache := make(map[pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	var a []pgid
	for _, id := range f.getFreePageIDs() {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

func (f *freelist) noSyncReload(pgids []pgid) {
	pcache := make(map[pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	var a []pgid
	for _, id := range pgids {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

func (f *freelist) reindex() {
	ids := f.getFreePageIDs()
	f.cache = make(map[pgid]bool, len(ids))
	for _, id := range ids {
		f.cache[id] = true
	}
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			f.cache[pendingID] = true
		}
	}
}

func (f *freelist) arrayMergeSpans(ids pgids) {
	sort.Sort(ids)
	f.ids = pgids(f.ids).merge(ids)
}
