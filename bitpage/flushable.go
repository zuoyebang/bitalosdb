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
	"sync/atomic"
)

type flushable interface {
	get([]byte, uint32) ([]byte, bool, internalKeyKind)
	exist([]byte, uint32) (bool, internalKeyKind)
	newIter(*iterOptions) InternalKKVIterator
	set(internalKey, ...[]byte) error
	getKeyStats() (int, int, int)
	itemCount() int
	inuseBytes() uint64
	dataBytes() uint64
	close() error
	writeIdxToFile() error
	getFilePath() []string
	idxFilePath() string
	getID() string
	empty() bool
	getModTime() int64
	mmapRLock()
	mmapRUnLock()
	kindStatis(kind internalKeyKind)
	flushFinish() error
	flushIndexes() error
	getFileType() FileType
	getMaxKey() []byte
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
	if e.readerRefs.Add(-1) == 0 {
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

var (
	_ flushable = (*sklTable)(nil)
	_ flushable = (*superTable)(nil)
	_ flushable = (*vectorArrayTable)(nil)
	_ flushable = (*arrayTable)(nil)
)
