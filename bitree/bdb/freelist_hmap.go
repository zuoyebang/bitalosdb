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

package bdb

import (
	"sort"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func (f *freelist) hashmapFreeCount() int {
	count := 0
	for _, size := range f.forwardMap {
		count += int(size)
	}
	return count
}

func (f *freelist) hashmapAllocate(txid txid, n int) pgid {
	if n == 0 {
		return 0
	}

	if bm, ok := f.freemaps[uint64(n)]; ok {
		for pid := range bm {
			f.delSpan(pid, uint64(n))

			f.allocs[pid] = txid

			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, pid+i)
			}
			return pid
		}
	}

	for size, bm := range f.freemaps {
		if size < uint64(n) {
			continue
		}

		for pid := range bm {
			f.delSpan(pid, size)

			f.allocs[pid] = txid

			remain := size - uint64(n)

			f.addSpan(pid+pgid(n), remain)

			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, pid+i)
			}
			return pid
		}
	}

	return 0
}

func (f *freelist) hashmapReadIDs(pgids []pgid) {
	f.init(pgids)

	f.reindex()
}

func (f *freelist) hashmapGetFreePageIDs() []pgid {
	count := f.free_count()
	if count == 0 {
		return nil
	}

	m := make([]pgid, 0, count)
	for start, size := range f.forwardMap {
		for i := 0; i < int(size); i++ {
			m = append(m, start+pgid(i))
		}
	}
	sort.Sort(pgids(m))

	return m
}

func (f *freelist) hashmapConvertBitmap() (rb *roaring64.Bitmap, count int) {
	rb = roaring64.NewBitmap()
	for _, txp := range f.pending {
		for _, id := range txp.ids {
			rb.Add(uint64(id))
			count++
		}
	}

	for start, size := range f.forwardMap {
		for i := 0; i < int(size); i++ {
			rb.Add(uint64(start + pgid(i)))
			count++
		}
	}

	return
}

func (f *freelist) hashmapMergeSpans(ids pgids) {
	for _, id := range ids {
		f.mergeWithExistingSpan(id)
	}
}

func (f *freelist) mergeWithExistingSpan(pid pgid) {
	prev := pid - 1
	next := pid + 1

	preSize, mergeWithPrev := f.backwardMap[prev]
	nextSize, mergeWithNext := f.forwardMap[next]
	newStart := pid
	newSize := uint64(1)

	if mergeWithPrev {
		start := prev + 1 - pgid(preSize)
		f.delSpan(start, preSize)

		newStart -= pgid(preSize)
		newSize += preSize
	}

	if mergeWithNext {
		f.delSpan(next, nextSize)
		newSize += nextSize
	}

	f.addSpan(newStart, newSize)
}

func (f *freelist) addSpan(start pgid, size uint64) {
	f.backwardMap[start-1+pgid(size)] = size
	f.forwardMap[start] = size
	if _, ok := f.freemaps[size]; !ok {
		f.freemaps[size] = make(map[pgid]struct{})
	}

	f.freemaps[size][start] = struct{}{}
}

func (f *freelist) delSpan(start pgid, size uint64) {
	delete(f.forwardMap, start)
	delete(f.backwardMap, start+pgid(size-1))
	delete(f.freemaps[size], start)
	if len(f.freemaps[size]) == 0 {
		delete(f.freemaps, size)
	}
}

func (f *freelist) init(pids []pgid) {
	if len(pids) == 0 {
		return
	}

	size := uint64(1)
	start := pids[0]

	if !sort.SliceIsSorted(pids, func(i, j int) bool { return pids[i] < pids[j] }) {
		sort.Sort(pgids(pids))
	}

	f.freemaps = make(map[uint64]pidSet)
	f.forwardMap = make(map[pgid]uint64)
	f.backwardMap = make(map[pgid]uint64)

	for i := 1; i < len(pids); i++ {
		if pids[i] == pids[i-1]+1 {
			size++
		} else {
			f.addSpan(start, size)

			size = 1
			start = pids[i]
		}
	}

	if size != 0 && start != 0 {
		f.addSpan(start, size)
	}
}
