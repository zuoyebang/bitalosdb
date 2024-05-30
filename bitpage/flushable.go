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
	"fmt"
	"sync/atomic"
)

type flushable interface {
	get([]byte, uint32) ([]byte, bool, internalKeyKind, func())
	newIter(*iterOptions) internalIterator
	delPercent() float64
	itemCount() int
	inuseBytes() uint64
	dataBytes() uint64
	readyForFlush() bool
	close() error
	path() string
	idxFilePath() string
	empty() bool
}

type flushableEntry struct {
	flushable
	obsolete   bool
	fileNum    FileNum
	readerRefs atomic.Int32
	release    func()
}

func (e *flushableEntry) readerRef() {
	e.readerRefs.Add(1)
}

func (e *flushableEntry) readerUnref() {
	switch v := e.readerRefs.Add(-1); {
	case v < 0:
		fmt.Printf("bitpage: inconsistent reference path:%s count:%d\n", e.path(), v)
	case v == 0:
		if e.release != nil {
			e.release()
			e.release = nil
		}
	}
}

func (e *flushableEntry) setObsolete() {
	e.obsolete = true
}

type flushableList []*flushableEntry
