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
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

type MergingIterLevel struct {
	iter      base.InternalIterator
	iterKey   *base.InternalKey
	iterValue []byte
}

func NewMergingIterLevel(iter base.InternalIterator) MergingIterLevel {
	return MergingIterLevel{
		iter: iter,
	}
}

type MergingIter struct {
	logger base.Logger
	dir    int
	levels []MergingIterLevel
	heap   MergingIterHeap
	err    error
	lower  []byte
	upper  []byte
}

var _ base.InternalIterator = (*MergingIter)(nil)

func NewMergingIter(logger base.Logger, cmp base.Compare, iters ...base.InternalIterator) *MergingIter {
	m := &MergingIter{}
	levels := make([]MergingIterLevel, len(iters))
	for i := range levels {
		levels[i].iter = iters[i]
	}
	m.Init(&options.IterOptions{Logger: logger}, cmp, levels...)
	return m
}

func (m *MergingIter) Init(
	opts *options.IterOptions, cmp base.Compare, levels ...MergingIterLevel,
) {
	m.err = nil
	m.logger = opts.GetLogger()
	if opts != nil {
		m.lower = opts.LowerBound
		m.upper = opts.UpperBound
	}
	m.levels = levels
	m.heap.cmp = cmp
	if cap(m.heap.items) < len(levels) {
		m.heap.items = make([]MergingIterItem, 0, len(levels))
	} else {
		m.heap.items = m.heap.items[:0]
	}
}

func (m *MergingIter) initHeap() {
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		if l := &m.levels[i]; l.iterKey != nil {
			m.heap.items = append(m.heap.items, MergingIterItem{
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

func (m *MergingIter) initMinHeap() {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()
}

func (m *MergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
}

func (m *MergingIter) switchToMinHeap() {
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
			if base.InternalCompare(m.heap.cmp, key, *l.iterKey) < 0 {
				break
			}
		}
	}

	cur.iterKey, cur.iterValue = cur.iter.Next()
	m.initMinHeap()
}

func (m *MergingIter) switchToMaxHeap() {
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
			if base.InternalCompare(m.heap.cmp, key, *l.iterKey) > 0 {
				break
			}
		}
	}

	cur.iterKey, cur.iterValue = cur.iter.Prev()
	m.initMaxHeap()
}

func (m *MergingIter) nextEntry(item *MergingIterItem) {
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

func (m *MergingIter) findNextEntry() (*base.InternalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		return &item.key, item.value
	}
	return nil, nil
}

func (m *MergingIter) prevEntry(item *MergingIterItem) {
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

func (m *MergingIter) findPrevEntry() (*base.InternalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		return &item.key, item.value
	}
	return nil, nil
}

func (m *MergingIter) seekGE(key []byte, level int) {
	for ; level < len(m.levels); level++ {
		l := &m.levels[level]
		l.iterKey, l.iterValue = l.iter.SeekGE(key)
	}

	m.initMinHeap()
}

func (m *MergingIter) String() string {
	return "MergingIter"
}

func (m *MergingIter) SeekGE(key []byte) (*base.InternalKey, []byte) {
	m.err = nil
	m.seekGE(key, 0)
	return m.findNextEntry()
}

func (m *MergingIter) seekLT(key []byte, level int) {
	for ; level < len(m.levels); level++ {
		l := &m.levels[level]
		l.iterKey, l.iterValue = l.iter.SeekLT(key)
	}

	m.initMaxHeap()
}

func (m *MergingIter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	m.err = nil
	m.seekLT(key, 0)
	return m.findPrevEntry()
}

func (m *MergingIter) First() (*base.InternalKey, []byte) {
	m.err = nil
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.First()
	}
	m.initMinHeap()
	return m.findNextEntry()
}

func (m *MergingIter) Last() (*base.InternalKey, []byte) {
	m.err = nil
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.Last()
	}
	m.initMaxHeap()
	return m.findPrevEntry()
}

func (m *MergingIter) Next() (*base.InternalKey, []byte) {
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

func (m *MergingIter) Prev() (*base.InternalKey, []byte) {
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

func (m *MergingIter) Error() error {
	if m.heap.len() == 0 || m.err != nil {
		return m.err
	}
	return m.levels[m.heap.items[0].index].iter.Error()
}

func (m *MergingIter) Close() error {
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

func (m *MergingIter) SetBounds(lower, upper []byte) {

	m.lower = lower
	m.upper = upper
	for i := range m.levels {
		m.levels[i].iter.SetBounds(lower, upper)
	}
	m.heap.clear()
}

func (m *MergingIter) DebugString() string {
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

type MergingIterItem struct {
	index int
	key   base.InternalKey
	value []byte
}

type MergingIterHeap struct {
	cmp     base.Compare
	reverse bool
	items   []MergingIterItem
}

func (h *MergingIterHeap) len() int {
	return len(h.items)
}

func (h *MergingIterHeap) clear() {
	h.items = h.items[:0]
}

func (h *MergingIterHeap) less(i, j int) bool {
	ikey, jkey := h.items[i].key, h.items[j].key
	if c := h.cmp(ikey.UserKey, jkey.UserKey); c != 0 {
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

func (h *MergingIterHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *MergingIterHeap) init() {
	n := h.len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *MergingIterHeap) fix(i int) {
	if !h.down(i, h.len()) {
		h.up(i)
	}
}

func (h *MergingIterHeap) pop() *MergingIterItem {
	n := h.len() - 1
	h.swap(0, n)
	h.down(0, n)
	item := &h.items[n]
	h.items = h.items[:n]
	return item
}

func (h *MergingIterHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *MergingIterHeap) down(i0, n int) bool {
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
