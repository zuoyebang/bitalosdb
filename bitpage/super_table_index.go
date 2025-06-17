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
	"io"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/errors"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

func (s *superTable) rebuildIndexes() (err error) {
	var its []internalIterator
	var sstIter *sstIterator
	var keySize uint16
	var valueSize uint32
	var offset, keyOffset, n int
	var prevKey []byte
	var newIndexes stIndexes
	var readCost, compactCost float64

	file := s.tbl.file
	fileSize := int(s.tbl.Size())
	readBuf := s.p.bp.getStArenaBuf(fileSize)
	readBuf = readBuf[:fileSize]

	startTime := time.Now()
	s.p.bp.opts.Logger.Infof("superTable rebuild indexes start file:%s fileSize:%d", s.filename, fileSize)
	defer func() {
		prevKey = nil
		readBuf = nil
		its = nil
		sstIter = nil
		s.p.bp.opts.Logger.Infof("superTable rebuild indexes finish its:%d idx:%d offset:%d keyOffset:%d readCost:%.3fs compactCost:%.3fs",
			len(its), len(newIndexes), offset, keyOffset, readCost, compactCost)
	}()

	n, err = file.ReadAt(readBuf, 0)
	if err != nil {
		return err
	}
	if n != fileSize {
		return errors.Errorf("bitpage: superTable rebuild indexes readAt return not eq %d != %d", n, fileSize)
	}

	sstIter = &sstIterator{}
	offset = stDataOffset
	for {
		keyOffset = offset

		if len(readBuf[offset:]) < stItemHeaderSize {
			break
		}
		keySize, valueSize = s.writer.decodeHeader(readBuf[offset : offset+stItemHeaderSize])
		if keySize == 0 {
			break
		}
		offset += stItemHeaderSize
		kvSize := int(keySize) + int(valueSize)
		if len(readBuf[offset:]) < kvSize {
			break
		}

		key := readBuf[offset : offset+int(keySize)]
		ikey := base.DecodeInternalKey(key)
		offset += kvSize

		if prevKey != nil && bytes.Compare(prevKey, ikey.UserKey) >= 0 {
			its = append(its, sstIter)
			sstIter = &sstIterator{}
		}
		sstIter.data = append(sstIter.data, sstItem{
			key:    ikey,
			offset: uint32(keyOffset),
		})
		sstIter.num += 1
		prevKey = ikey.UserKey
	}
	readCost = time.Since(startTime).Seconds()
	if err != nil && err != io.EOF {
		return err
	}
	if len(sstIter.data) > 0 {
		its = append(its, sstIter)
	}
	if len(its) == 0 {
		return nil
	}

	if err = s.writer.reset(keyOffset); err != nil {
		return err
	}

	iiter := newMergingIter(s.p.bp.opts.Logger, s.p.bp.opts.Cmp, its...)
	iter := newCompactionIter(s.p.bp, iiter)
	defer func() {
		_ = iter.Close()
	}()

	for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
		newIndexes = append(newIndexes, utils.BytesToUint32(iv))
	}

	compactCost = time.Since(startTime).Seconds() - readCost
	s.reading.Store(&newIndexes)
	s.indexModified = true

	return nil
}

type sstItem struct {
	key    internalKey
	offset uint32
}

type sstIterator struct {
	data     []sstItem
	num      int
	iterPos  int
	iterItem *sstItem
}

func (i *sstIterator) findItem() (*internalKey, []byte) {
	if i.iterPos < 0 || i.iterPos >= i.num {
		return nil, nil
	}

	item := i.data[i.iterPos]
	return &(item.key), utils.Uint32ToBytes(item.offset)
}

func (i *sstIterator) First() (*internalKey, []byte) {
	i.iterPos = 0
	return i.findItem()
}

func (i *sstIterator) Next() (*internalKey, []byte) {
	i.iterPos++
	return i.findItem()
}

func (i *sstIterator) Prev() (*internalKey, []byte) {
	i.iterPos--
	return i.findItem()
}

func (i *sstIterator) Last() (*internalKey, []byte) {
	i.iterPos = i.num - 1
	return i.findItem()
}

func (i *sstIterator) SeekGE(key []byte) (*internalKey, []byte) {
	return nil, nil
}

func (i *sstIterator) SeekLT(key []byte) (*internalKey, []byte) {
	return nil, nil
}

func (i *sstIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *internalKey, value []byte) {
	return i.SeekGE(key)
}

func (i *sstIterator) SetBounds(lower, upper []byte) {
}

func (i *sstIterator) Error() error {
	return nil
}

func (i *sstIterator) Close() error {
	i.data = nil
	i.iterItem = nil
	return nil
}

func (i *sstIterator) String() string {
	return "sstIterator"
}
