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

package bitpage

import "sync/atomic"

type readState struct {
	refcnt    atomic.Int32
	stMutable *superTable
	stQueue   flushableList
	arrtable  *flushableEntry
}

func (s *readState) ref() {
	s.refcnt.Add(1)
}

func (s *readState) unref() {
	if s.refcnt.Add(-1) != 0 {
		return
	}

	for i := range s.stQueue {
		s.stQueue[i].readerUnref()
	}

	if s.arrtable != nil {
		s.arrtable.readerUnref()
	}
}
