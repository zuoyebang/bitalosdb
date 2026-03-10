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

package bitalosdb

import "sync/atomic"

type vmReadState struct {
	refcnt    atomic.Int32
	memtables vmFlushableList
}

func (s *vmReadState) ref() {
	s.refcnt.Add(1)
}

func (s *vmReadState) unref() {
	if s.refcnt.Add(-1) != 0 {
		return
	}
	for _, mem := range s.memtables {
		mem.readerUnref()
	}
}

type memReadState struct {
	refcnt    atomic.Int32
	memtables memFlushableList
}

func (s *memReadState) ref() {
	s.refcnt.Add(1)
}

func (s *memReadState) unref() {
	if s.refcnt.Add(-1) != 0 {
		return
	}
	for _, mem := range s.memtables {
		mem.readerUnref()
	}
}
