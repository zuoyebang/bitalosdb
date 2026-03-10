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

import "sync/atomic"

type pageReadState struct {
	refcnt    atomic.Int32
	stMutable *flushableEntry
	stQueue   flushableList
	arrtable  *flushableEntry
}

func (s *pageReadState) ref() {
	s.refcnt.Add(1)
}

func (s *pageReadState) unref() {
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
