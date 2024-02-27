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

package lrucache

import (
	"fmt"
	"math/bits"
	"os"
	"runtime/debug"
	"strings"
	"time"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/manual"
)

var hashSeed = uint64(time.Now().UnixNano())

func robinHoodHash(k key, shift uint32) uint32 {
	const m = 11400714819323198485
	h := hashSeed
	h ^= k.id * m
	h ^= k.offset * m
	return uint32(h >> shift)
}

type robinHoodEntry struct {
	key   key
	value *entry
	dist  uint32
}

type robinHoodEntries struct {
	ptr unsafe.Pointer
	len uint32
}

func newRobinHoodEntries(n uint32) robinHoodEntries {
	size := uintptr(n) * unsafe.Sizeof(robinHoodEntry{})
	return robinHoodEntries{
		ptr: unsafe.Pointer(&(manual.New(int(size)))[0]),
		len: n,
	}
}

func (e robinHoodEntries) at(i uint32) *robinHoodEntry {
	return (*robinHoodEntry)(unsafe.Pointer(uintptr(e.ptr) +
		uintptr(i)*unsafe.Sizeof(robinHoodEntry{})))
}

func (e robinHoodEntries) free() {
	size := uintptr(e.len) * unsafe.Sizeof(robinHoodEntry{})
	buf := (*[manual.MaxArrayLen]byte)(e.ptr)[:size:size]
	manual.Free(buf)
}

type robinHoodMap struct {
	entries robinHoodEntries
	size    uint32
	shift   uint32
	count   uint32
	maxDist uint32
}

func maxDistForSize(size uint32) uint32 {
	desired := uint32(bits.Len32(size))
	if desired < 4 {
		desired = 4
	}
	return desired
}

func newRobinHoodMap(initialCapacity int) *robinHoodMap {
	m := &robinHoodMap{}
	m.init(initialCapacity)

	invariants.SetFinalizer(m, func(obj interface{}) {
		m := obj.(*robinHoodMap)
		if m.entries.ptr != nil {
			fmt.Fprintf(os.Stderr, "%p: robin-hood map not freed\n", m)
			os.Exit(1)
		}
	})
	return m
}

func (m *robinHoodMap) init(initialCapacity int) {
	if initialCapacity < 1 {
		initialCapacity = 1
	}
	targetSize := 1 << (uint(bits.Len(uint(2*initialCapacity-1))) - 1)
	m.rehash(uint32(targetSize))
}

func (m *robinHoodMap) free() {
	if m.entries.ptr != nil {
		m.entries.free()
		m.entries.ptr = nil
	}
}

func (m *robinHoodMap) rehash(size uint32) {
	oldEntries := m.entries

	m.size = size
	m.shift = uint32(64 - bits.Len32(m.size-1))
	m.maxDist = maxDistForSize(size)
	m.entries = newRobinHoodEntries(size + m.maxDist)
	m.count = 0

	for i := uint32(0); i < oldEntries.len; i++ {
		e := oldEntries.at(i)
		if e.value != nil {
			m.Put(e.key, e.value)
		}
	}

	if oldEntries.ptr != nil {
		oldEntries.free()
	}
}

func (m *robinHoodMap) findByValue(v *entry) *robinHoodEntry {
	for i := uint32(0); i < m.entries.len; i++ {
		e := m.entries.at(i)
		if e.value == v {
			return e
		}
	}
	return nil
}

func (m *robinHoodMap) Count() int {
	return int(m.count)
}

func (m *robinHoodMap) Put(k key, v *entry) {
	maybeExists := true
	n := robinHoodEntry{key: k, value: v, dist: 0}
	for i := robinHoodHash(k, m.shift); ; i++ {
		e := m.entries.at(i)
		if maybeExists && k == e.key {
			e.value = n.value
			m.checkEntry(i)
			return
		}

		if e.value == nil {
			*e = n
			m.count++
			m.checkEntry(i)
			return
		}

		if e.dist < n.dist {
			n, *e = *e, n
			m.checkEntry(i)
			maybeExists = false
		}

		n.dist++

		if n.dist == m.maxDist {
			m.rehash(2 * m.size)
			i = robinHoodHash(n.key, m.shift) - 1
			n.dist = 0
			maybeExists = false
		}
	}
}

func (m *robinHoodMap) Get(k key) *entry {
	var dist uint32
	for i := robinHoodHash(k, m.shift); ; i++ {
		e := m.entries.at(i)
		if k == e.key {
			return e.value
		}
		if e.dist < dist {
			return nil
		}
		dist++
	}
}

func (m *robinHoodMap) Delete(k key) {
	var dist uint32
	for i := robinHoodHash(k, m.shift); ; i++ {
		e := m.entries.at(i)
		if k == e.key {
			m.checkEntry(i)
			m.count--
			for j := i + 1; ; j++ {
				t := m.entries.at(j)
				if t.dist == 0 {
					*e = robinHoodEntry{}
					return
				}
				e.key = t.key
				e.value = t.value
				e.dist = t.dist - 1
				e = t
				m.checkEntry(j)
			}
		}
		if dist > e.dist {
			return
		}
		dist++
	}
}

func (m *robinHoodMap) checkEntry(i uint32) {
	if invariants.Enabled {
		e := m.entries.at(i)
		if e.value != nil {
			pos := robinHoodHash(e.key, m.shift)
			if (uint32(i) - pos) != e.dist {
				fmt.Fprintf(os.Stderr, "%d: invalid dist=%d, expected %d: %s\n%s",
					i, e.dist, uint32(i)-pos, e.key, debug.Stack())
				os.Exit(1)
			}
			if e.dist > m.maxDist {
				fmt.Fprintf(os.Stderr, "%d: invalid dist=%d > maxDist=%d: %s\n%s",
					i, e.dist, m.maxDist, e.key, debug.Stack())
				os.Exit(1)
			}
		}
	}
}

func (m *robinHoodMap) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "count: %d\n", m.count)
	for i := uint32(0); i < m.entries.len; i++ {
		e := m.entries.at(i)
		if e.value != nil {
			fmt.Fprintf(&buf, "%d: [%s,%p,%d]\n", i, e.key, e.value, e.dist)
		}
	}
	return buf.String()
}
