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

import (
	"fmt"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/base"
)

type flushable interface {
	get(k []byte) ([]byte, bool, base.InternalKeyKind)
	newIter(o *IterOptions) internalIterator
	newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator
	inuseBytes() uint64
	totalBytes() uint64
	empty() bool
	readyForFlush() bool
}

type flushableEntry struct {
	flushable
	flushed              chan struct{}
	flushForced          bool
	logNum               FileNum
	logSize              uint64
	logSeqNum            uint64
	readerRefs           atomic.Int32
	releaseMemAccounting func()
}

func (e *flushableEntry) readerRef() {
	e.readerRefs.Add(1)
}

func (e *flushableEntry) readerUnref() {
	switch v := e.readerRefs.Add(-1); {
	case v == 0:
		if e.releaseMemAccounting == nil {
			fmt.Println("panic: flushableEntry readerUnref reservation already released")
			return
		}
		e.releaseMemAccounting()
		e.releaseMemAccounting = nil
	case v < 0:
		fmt.Printf("panic: flushableEntry readerUnref logNum:%d count:%d\n", e.logNum, v)
	}
}

type flushableList []*flushableEntry

func newFlushableEntry(f flushable, logNum FileNum, logSeqNum uint64) *flushableEntry {
	entry := &flushableEntry{
		flushable: f,
		flushed:   make(chan struct{}),
		logNum:    logNum,
		logSeqNum: logSeqNum,
	}
	entry.readerRefs.Store(1)
	return entry
}
