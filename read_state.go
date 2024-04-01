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

package bitalosdb

import "sync/atomic"

type readState struct {
	db        *DB
	refcnt    int32
	memtables flushableList
}

func (s *readState) ref() {
	atomic.AddInt32(&s.refcnt, 1)
}

func (s *readState) unref() {
	if atomic.AddInt32(&s.refcnt, -1) != 0 {
		return
	}
	for _, mem := range s.memtables {
		mem.readerUnref()
	}
}

func (d *DB) loadReadState() *readState {
	d.readState.RLock()
	state := d.readState.val
	state.ref()
	d.readState.RUnlock()
	return state
}

func (d *DB) updateReadStateLocked() {
	s := &readState{
		db:        d,
		refcnt:    1,
		memtables: d.mu.mem.queue,
	}

	for _, mem := range s.memtables {
		mem.readerRef()
	}

	d.readState.Lock()
	old := d.readState.val
	d.readState.val = s
	d.readState.Unlock()

	if old != nil {
		old.unref()
	}
}
