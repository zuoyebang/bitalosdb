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

package lfucache

import "sync/atomic"

type readState struct {
	refcnt    int32
	memtables flushableList
	arrtable  *flushableEntry
}

func (s *readState) ref() {
	atomic.AddInt32(&s.refcnt, 1)
}

func (s *readState) unref() {
	if atomic.AddInt32(&s.refcnt, -1) != 0 {
		return
	}

	if s.arrtable != nil {
		s.arrtable.readerUnref()
	}

	for _, t := range s.memtables {
		t.readerUnref()
	}
}

func (s *shard) loadReadState() *readState {
	s.readState.RLock()
	state := s.readState.val
	state.ref()
	s.readState.RUnlock()
	return state
}

func (s *shard) updateReadStateLocked() {
	rs := &readState{
		refcnt:    1,
		memtables: s.mu.memQueue,
		arrtable:  s.mu.arrtable,
	}

	if rs.arrtable != nil {
		rs.arrtable.readerRef()
	}

	for _, mem := range rs.memtables {
		mem.readerRef()
	}

	s.readState.Lock()
	old := s.readState.val
	s.readState.val = rs
	s.readState.Unlock()

	if old != nil {
		old.unref()
	}

	s.setMemtableNum(int32(len(rs.memtables)))
}
