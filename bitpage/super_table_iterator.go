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
	"bytes"

	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

func (s *superTable) newIter(opts *iterOptions) InternalKKVIterator {
	indexes := s.readIndexes()
	if indexes == nil {
		return nil
	}

	if opts == nil {
		opts = &iterOptions{}
	}

	it := &superTableIterator{
		st:          s,
		indexes:     s.readIndexes(),
		isUseVi:     s.isUseVi,
		isSeekFirst: kkv.IsUseIterFirst(opts.DataType),
		oiStartPos:  0,
		oiEndPos:    indexes.oiNum - 1,
	}

	if opts.IsTest {
		it.SetBounds(opts.LowerBound, opts.UpperBound)
	}

	return it
}

type superTableIterator struct {
	st          *superTable
	indexes     *stIndexes
	indexPos    int
	isUseVi     bool
	isSeekFirst bool
	oiStartPos  int
	oiEndPos    int
	lower       []byte
	upper       []byte
}

var _ InternalKKVIterator = (*superTableIterator)(nil)

func (i *superTableIterator) findItem() (*InternalKKVKey, []byte) {
	if i.indexPos < i.oiStartPos || i.indexPos > i.oiEndPos {
		return nil, nil
	}

	offset := i.indexes.oi[i.indexPos]
	key, value := i.st.dataFile.GetKV(offset)
	if key == nil {
		return nil, nil
	}

	ikey := kkv.DecodeInternalKey(key)
	return &ikey, value
}

func (i *superTableIterator) seekOffsetIndex(key []byte, num int) bool {
	pos := binarySearch(num, func(j int) int {
		offset := i.indexes.oi[i.oiStartPos+j]
		ikey := i.st.getKey(offset)
		return bytes.Compare(ikey.UserKey, key)
	})
	if pos == num {
		return false
	}
	i.indexPos = i.oiStartPos + pos
	return true
}

func (i *superTableIterator) seekVersionIndex(seek []byte) (int, bool) {
	if !i.isUseVi {
		return i.indexes.oiNum, true
	}

	startPos, endPos, found := i.st.findKeyInVersionIndex(i.indexes, seek)
	if !found {
		return 0, false
	}

	i.oiStartPos = startPos
	i.oiEndPos = endPos
	oiNum := i.oiEndPos - i.oiStartPos + 1
	return oiNum, true
}

func (i *superTableIterator) First() (*InternalKKVKey, []byte) {
	i.indexPos = i.oiStartPos
	key, value := i.findItem()
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}
	return key, value
}

func (i *superTableIterator) Next() (*InternalKKVKey, []byte) {
	i.indexPos++
	key, value := i.findItem()
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}
	return key, value
}

func (i *superTableIterator) Prev() (*InternalKKVKey, []byte) {
	i.indexPos--
	key, value := i.findItem()
	if kkv.IsUserKeyLTLowerBound(key, i.lower) {
		return nil, nil
	}
	return key, value
}

func (i *superTableIterator) Last() (*InternalKKVKey, []byte) {
	i.indexPos = i.oiEndPos
	key, value := i.findItem()
	if kkv.IsUserKeyLTLowerBound(key, i.lower) {
		return nil, nil
	}
	return key, value
}

func (i *superTableIterator) SeekGE(seek []byte) (*InternalKKVKey, []byte) {
	num, found := i.seekVersionIndex(seek)
	if !found {
		return nil, nil
	}

	if i.isSeekFirst && !i.st.isUseMiniVi {
		i.indexPos = i.oiStartPos
	} else if !i.seekOffsetIndex(seek, num) {
		return nil, nil
	}

	key, value := i.findItem()
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}

	return key, value
}

func (i *superTableIterator) SeekLT(seek []byte) (*InternalKKVKey, []byte) {
	num, found := i.seekVersionIndex(seek)
	if !found {
		return nil, nil
	}

	if i.seekOffsetIndex(seek, num) {
		return i.Prev()
	} else {
		return i.Last()
	}
}

func (i *superTableIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *superTableIterator) Error() error {
	return nil
}

func (i *superTableIterator) Close() error {
	return nil
}

func (i *superTableIterator) String() string {
	return "superTableIterator"
}
