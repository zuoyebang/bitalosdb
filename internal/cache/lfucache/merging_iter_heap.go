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

package lfucache

import "bytes"

type mergingIterItem struct {
	index int
	key   internalKey
	value []byte
}

type mergingIterHeap struct {
	reverse bool
	items   []mergingIterItem
}

func (h *mergingIterHeap) len() int {
	return len(h.items)
}

func (h *mergingIterHeap) clear() {
	h.items = h.items[:0]
}

func (h *mergingIterHeap) less(i, j int) bool {
	ikey, jkey := h.items[i].key, h.items[j].key
	if c := bytes.Compare(ikey.UserKey, jkey.UserKey); c != 0 {
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

func (h *mergingIterHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergingIterHeap) init() {
	n := h.len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *mergingIterHeap) fix(i int) {
	if !h.down(i, h.len()) {
		h.up(i)
	}
}

func (h *mergingIterHeap) pop() *mergingIterItem {
	n := h.len() - 1
	h.swap(0, n)
	h.down(0, n)
	item := &h.items[n]
	h.items = h.items[:n]
	return item
}

func (h *mergingIterHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *mergingIterHeap) down(i0, n int) bool {
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
