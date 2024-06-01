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
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

type mergingIterLevel struct {
	iter      internalIterator
	iterKey   *internalKey
	iterValue []byte
}

type mergingIter struct {
	logger base.Logger
	dir    int
	levels []mergingIterLevel
	heap   mergingIterHeap
	err    error
	prefix []byte
	lower  []byte
	upper  []byte
}

var _ base.InternalIterator = (*mergingIter)(nil)

func newMergingIter(
	logger base.Logger, cmp base.Compare, iters ...internalIterator,
) *mergingIter {
	m := &mergingIter{}
	levels := make([]mergingIterLevel, len(iters))
	for i := range levels {
		levels[i].iter = iters[i]
	}
	m.Init(&iterOptions{Logger: logger}, cmp, levels...)
	return m
}

func (m *mergingIter) Init(
	opts *iterOptions, cmp base.Compare, levels ...mergingIterLevel,
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
		m.heap.items = make([]mergingIterItem, 0, len(levels))
	} else {
		m.heap.items = m.heap.items[:0]
	}
}

func (m *mergingIter) initHeap() {
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		if l := &m.levels[i]; l.iterKey != nil {
			m.heap.items = append(m.heap.items, mergingIterItem{
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

func (m *mergingIter) initMinHeap() {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()
}

func (m *mergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
}

func (m *mergingIter) switchToMinHeap() {
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

func (m *mergingIter) switchToMaxHeap() {
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

func (m *mergingIter) nextEntry(item *mergingIterItem) {
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

func (m *mergingIter) findNextEntry() (*internalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		return &item.key, item.value
	}
	return nil, nil
}

func (m *mergingIter) prevEntry(item *mergingIterItem) {
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

func (m *mergingIter) findPrevEntry() (*internalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		return &item.key, item.value
	}
	return nil, nil
}

func (m *mergingIter) seekGE(key []byte, level int, trySeekUsingNext bool) {
	for ; level < len(m.levels); level++ {
		if invariants.Enabled && m.lower != nil && m.heap.cmp(key, m.lower) < 0 {
			m.logger.Fatalf("mergingIter: lower bound violation: %s < %s\n%s", key, m.lower, debug.Stack())
		}

		l := &m.levels[level]
		if m.prefix != nil {
			l.iterKey, l.iterValue = l.iter.SeekPrefixGE(m.prefix, key, trySeekUsingNext)
		} else {
			l.iterKey, l.iterValue = l.iter.SeekGE(key)
		}
	}

	m.initMinHeap()
}

func (m *mergingIter) String() string {
	return "merging"
}

func (m *mergingIter) SeekGE(key []byte) (*internalKey, []byte) {
	m.err = nil
	m.prefix = nil
	m.seekGE(key, 0, false)
	return m.findNextEntry()
}

func (m *mergingIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	m.err = nil
	m.prefix = prefix
	m.seekGE(key, 0, trySeekUsingNext)
	return m.findNextEntry()
}

func (m *mergingIter) seekLT(key []byte, level int) {
	m.prefix = nil
	for ; level < len(m.levels); level++ {
		if invariants.Enabled && m.upper != nil && m.heap.cmp(key, m.upper) > 0 {
			m.logger.Fatalf("mergingIter: upper bound violation: %s > %s\n%s", key, m.upper, debug.Stack())
		}

		l := &m.levels[level]
		l.iterKey, l.iterValue = l.iter.SeekLT(key)
	}

	m.initMaxHeap()
}

func (m *mergingIter) SeekLT(key []byte) (*internalKey, []byte) {
	m.err = nil
	m.prefix = nil
	m.seekLT(key, 0)
	return m.findPrevEntry()
}

func (m *mergingIter) First() (*internalKey, []byte) {
	m.err = nil
	m.prefix = nil
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.First()
	}
	m.initMinHeap()
	return m.findNextEntry()
}

func (m *mergingIter) Last() (*internalKey, []byte) {
	m.err = nil
	m.prefix = nil
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.Last()
	}
	m.initMaxHeap()
	return m.findPrevEntry()
}

func (m *mergingIter) Next() (*internalKey, []byte) {
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

func (m *mergingIter) Prev() (*internalKey, []byte) {
	if m.err != nil {
		return nil, nil
	}

	if m.dir != -1 {
		if m.prefix != nil {
			m.err = errors.New("bitalosdb: unsupported reverse prefix iteration")
			return nil, nil
		}
		m.switchToMaxHeap()
		return m.findPrevEntry()
	}

	if m.heap.len() == 0 {
		return nil, nil
	}

	m.prevEntry(&m.heap.items[0])
	return m.findPrevEntry()
}

func (m *mergingIter) Error() error {
	if m.heap.len() == 0 || m.err != nil {
		return m.err
	}
	return m.levels[m.heap.items[0].index].iter.Error()
}

func (m *mergingIter) Close() error {
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

func (m *mergingIter) SetBounds(lower, upper []byte) {
	m.prefix = nil
	m.lower = lower
	m.upper = upper
	for i := range m.levels {
		m.levels[i].iter.SetBounds(lower, upper)
	}
	m.heap.clear()
}

func (m *mergingIter) DebugString() string {
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
