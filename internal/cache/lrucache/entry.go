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

type entryType int8

const (
	etTest entryType = iota
	etCold
	etHot
)

func (p entryType) String() string {
	switch p {
	case etTest:
		return "test"
	case etCold:
		return "cold"
	case etHot:
		return "hot"
	}
	return "unknown"
}

type entry struct {
	key       key
	val       *Value
	blockLink struct {
		next *entry
		prev *entry
	}
	size       int64
	ptype      entryType
	referenced int32
	shard      *shard
	ref        refcnt
}

func newEntry(s *shard, key key, size int64) *entry {
	e := entryAllocNew()
	*e = entry{
		key:   key,
		size:  size,
		ptype: etCold,
		shard: s,
	}
	e.blockLink.next = e
	e.blockLink.prev = e
	e.ref.init(1)
	return e
}

func (e *entry) free() {
	e.setValue(nil)
	*e = entry{}
	entryAllocFree(e)
}

func (e *entry) next() *entry {
	if e == nil {
		return nil
	}
	return e.blockLink.next
}

func (e *entry) prev() *entry {
	if e == nil {
		return nil
	}
	return e.blockLink.prev
}

func (e *entry) link(s *entry) {
	s.blockLink.prev = e.blockLink.prev
	s.blockLink.prev.blockLink.next = s
	s.blockLink.next = e
	s.blockLink.next.blockLink.prev = s
}

func (e *entry) unlink() *entry {
	next := e.blockLink.next
	e.blockLink.prev.blockLink.next = e.blockLink.next
	e.blockLink.next.blockLink.prev = e.blockLink.prev
	e.blockLink.prev = e
	e.blockLink.next = e
	return next
}

func (e *entry) setValue(v *Value) {
	if v != nil {
		v.acquire()
	}
	old := e.val
	e.val = v
	if old != nil {
		old.release()
	}
}

func (e *entry) peekValue() *Value {
	return e.val
}

func (e *entry) acquireValue() *Value {
	v := e.val
	if v != nil {
		v.acquire()
	}
	return v
}
