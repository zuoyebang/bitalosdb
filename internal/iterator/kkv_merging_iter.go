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

package iterator

import (
	"bytes"
	"fmt"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

type KKVMergingIterLevel struct {
	iter      kkv.InternalIterator
	iterKey   *kkv.InternalKey
	iterValue []byte
}

func NewKKVMergingIterLevel(iter kkv.InternalIterator) KKVMergingIterLevel {
	return KKVMergingIterLevel{
		iter: iter,
	}
}

type KKVMergingIter struct {
	logger base.Logger
	dir    int
	levels []KKVMergingIterLevel
	heap   KKVMergingIterHeap
	err    error
	lower  []byte
	upper  []byte
}

var _ kkv.InternalIterator = (*KKVMergingIter)(nil)

func NewKKVMergingIter(logger base.Logger, iters ...kkv.InternalIterator) *KKVMergingIter {
	m := &KKVMergingIter{}
	levels := make([]KKVMergingIterLevel, len(iters))
	for i := range levels {
		levels[i].iter = iters[i]
	}
	m.Init(&options.IterOptions{Logger: logger}, levels...)
	return m
}

func (m *KKVMergingIter) Init(
	opts *options.IterOptions, levels ...KKVMergingIterLevel,
) {
	m.err = nil
	m.logger = opts.GetLogger()
	if opts != nil {
		m.lower = opts.LowerBound
		m.upper = opts.UpperBound
	}
	m.levels = levels
	if cap(m.heap.items) < len(levels) {
		m.heap.items = make([]KKVMergingIterItem, 0, len(levels))
	} else {
		m.heap.items = m.heap.items[:0]
	}
}

func (m *KKVMergingIter) initHeap() {
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		if l := &m.levels[i]; l.iterKey != nil {
			m.heap.items = append(m.heap.items, KKVMergingIterItem{
				index: i,
				key:   *l.iterKey,
				value: l.iterValue,
			})
		} else {
			m.err = utils.FirstError(m.err, l.iter.Error())
			if m.err != nil {
				return
			}
		}
	}
	m.heap.init()
}

func (m *KKVMergingIter) initMinHeap() {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()
}

func (m *KKVMergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
}

func (m *KKVMergingIter) switchToMinHeap() {
	if m.heap.len() == 0 {
		if m.lower != nil {
			m.SeekGE(m.lower)
		} else {
			m.First()
		}
		return
	}

	key := m.heap.items[0].key
	cur := &m.levels[m.heap.items[0].index]

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}

		if l.iterKey == nil {
			if m.lower != nil {
				l.iterKey, l.iterValue = l.iter.SeekGE(m.lower)
			} else {
				l.iterKey, l.iterValue = l.iter.First()
			}
		}
		for ; l.iterKey != nil; l.iterKey, l.iterValue = l.iter.Next() {
			if kkv.InternalKeyCompare(key, *l.iterKey) < 0 {
				break
			}
		}
	}

	cur.iterKey, cur.iterValue = cur.iter.Next()
	m.initMinHeap()
}

func (m *KKVMergingIter) switchToMaxHeap() {
	if m.heap.len() == 0 {
		if m.upper != nil {
			m.SeekLT(m.upper)
		} else {
			m.Last()
		}
		return
	}

	key := m.heap.items[0].key
	cur := &m.levels[m.heap.items[0].index]

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}

		if l.iterKey == nil {
			if m.upper != nil {
				l.iterKey, l.iterValue = l.iter.SeekLT(m.upper)
			} else {
				l.iterKey, l.iterValue = l.iter.Last()
			}
		}
		for ; l.iterKey != nil; l.iterKey, l.iterValue = l.iter.Prev() {
			if kkv.InternalKeyCompare(key, *l.iterKey) > 0 {
				break
			}
		}
	}

	cur.iterKey, cur.iterValue = cur.iter.Prev()
	m.initMaxHeap()
}

func (m *KKVMergingIter) nextEntry(item *KKVMergingIterItem) {
	l := &m.levels[item.index]
	if l.iterKey, l.iterValue = l.iter.Next(); l.iterKey != nil {
		item.key, item.value = *l.iterKey, l.iterValue
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
	} else {
		m.err = l.iter.Error()
		if m.err == nil {
			m.heap.pop()
		}
	}
}

func (m *KKVMergingIter) findNextEntry() (*kkv.InternalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		return &item.key, item.value
	}
	return nil, nil
}

func (m *KKVMergingIter) prevEntry(item *KKVMergingIterItem) {
	l := &m.levels[item.index]
	if l.iterKey, l.iterValue = l.iter.Prev(); l.iterKey != nil {
		item.key, item.value = *l.iterKey, l.iterValue
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
	} else {
		m.err = l.iter.Error()
		if m.err == nil {
			m.heap.pop()
		}
	}
}

func (m *KKVMergingIter) findPrevEntry() (*kkv.InternalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		return &item.key, item.value
	}
	return nil, nil
}

func (m *KKVMergingIter) seekGE(key []byte, level int) {
	for ; level < len(m.levels); level++ {
		l := &m.levels[level]
		l.iterKey, l.iterValue = l.iter.SeekGE(key)
	}

	m.initMinHeap()
}

func (m *KKVMergingIter) String() string {
	return "KKVMergingIter"
}

func (m *KKVMergingIter) SeekGE(key []byte) (*kkv.InternalKey, []byte) {
	m.err = nil
	m.seekGE(key, 0)
	return m.findNextEntry()
}

func (m *KKVMergingIter) seekLT(key []byte, level int) {
	for ; level < len(m.levels); level++ {
		l := &m.levels[level]
		l.iterKey, l.iterValue = l.iter.SeekLT(key)
	}

	m.initMaxHeap()
}

func (m *KKVMergingIter) SeekLT(key []byte) (*kkv.InternalKey, []byte) {
	m.err = nil
	m.seekLT(key, 0)
	return m.findPrevEntry()
}

func (m *KKVMergingIter) First() (*kkv.InternalKey, []byte) {
	m.err = nil
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.First()
	}
	m.initMinHeap()
	return m.findNextEntry()
}

func (m *KKVMergingIter) Last() (*kkv.InternalKey, []byte) {
	m.err = nil
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.Last()
	}
	m.initMaxHeap()
	return m.findPrevEntry()
}

func (m *KKVMergingIter) Next() (*kkv.InternalKey, []byte) {
	if m.err != nil {
		return nil, nil
	}

	if m.dir != 1 {
		m.switchToMinHeap()
		return m.findNextEntry()
	}

	if m.heap.len() == 0 {
		return nil, nil
	}

	m.nextEntry(&m.heap.items[0])
	return m.findNextEntry()
}

func (m *KKVMergingIter) Prev() (*kkv.InternalKey, []byte) {
	if m.err != nil {
		return nil, nil
	}

	if m.dir != -1 {
		m.switchToMaxHeap()
		return m.findPrevEntry()
	}

	if m.heap.len() == 0 {
		return nil, nil
	}

	m.prevEntry(&m.heap.items[0])
	return m.findPrevEntry()
}

func (m *KKVMergingIter) Error() error {
	if m.heap.len() == 0 || m.err != nil {
		return m.err
	}
	return m.levels[m.heap.items[0].index].iter.Error()
}

func (m *KKVMergingIter) Close() error {
	for i := range m.levels {
		iter := m.levels[i].iter
		if err := iter.Close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	m.levels = nil
	m.heap.items = m.heap.items[:0]
	return m.err
}

func (m *KKVMergingIter) SetBounds(lower, upper []byte) {
	m.lower = lower
	m.upper = upper
	for i := range m.levels {
		m.levels[i].iter.SetBounds(lower, upper)
	}
	m.heap.clear()
}

func (m *KKVMergingIter) DebugString() string {
	var buf bytes.Buffer
	sep := ""
	for m.heap.len() > 0 {
		item := m.heap.pop()
		fmt.Fprintf(&buf, "%s%s", sep, item.key)
		sep = " "
	}
	if m.dir == 1 {
		m.initMinHeap()
	} else {
		m.initMaxHeap()
	}
	return buf.String()
}

type KKVMergingIterItem struct {
	index int
	key   kkv.InternalKey
	value []byte
}

type KKVMergingIterHeap struct {
	reverse bool
	items   []KKVMergingIterItem
}

func (h *KKVMergingIterHeap) len() int {
	return len(h.items)
}

func (h *KKVMergingIterHeap) clear() {
	h.items = h.items[:0]
}

func (h *KKVMergingIterHeap) less(i, j int) bool {
	ikey, jkey := h.items[i].key, h.items[j].key
	if c := kkv.UserKeyCompare(ikey, jkey); c != 0 {
		if h.reverse {
			return c > 0
		}
		return c < 0
	}
	if h.reverse {
		return ikey.Trailer < jkey.Trailer
	}
	return ikey.Trailer > jkey.Trailer
}

func (h *KKVMergingIterHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *KKVMergingIterHeap) init() {
	n := h.len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *KKVMergingIterHeap) fix(i int) {
	if !h.down(i, h.len()) {
		h.up(i)
	}
}

func (h *KKVMergingIterHeap) pop() *KKVMergingIterItem {
	n := h.len() - 1
	h.swap(0, n)
	h.down(0, n)
	item := &h.items[n]
	h.items = h.items[:n]
	return item
}

func (h *KKVMergingIterHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *KKVMergingIterHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		i = j
	}
	return i > i0
}
