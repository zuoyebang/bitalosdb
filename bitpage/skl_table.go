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
	"os"
	"runtime/debug"
)

func sklTableEntrySize(keyBytes, valueBytes int) uint64 {
	return uint64(sklNodeMaxSize(uint32(keyBytes), uint32(valueBytes)))
}

type sklTable struct {
	bp         *Bitpage
	tbl        *table
	sl         *skl
	ins        *Inserter
	totalCount float64
	delCount   float64
}

func checkSklTable(obj interface{}) {
	s := obj.(*sklTable)
	if s.tbl != nil {
		fmt.Fprintf(os.Stderr, "sklTable(%s) buffer was not freed\n", s.path())
		os.Exit(1)
	}
}

func newSklTable(path string, exist bool, bp *Bitpage) (*sklTable, error) {
	tbl, err := openTable(path, defaultTableOptions)
	if err != nil {
		return nil, err
	}

	s := &sklTable{
		bp:  bp,
		tbl: tbl,
		ins: &Inserter{},
	}

	if exist {
		s.sl = openSkl(tbl, s, false)
	} else {
		sl, err := newSkl(tbl, s, false)
		if err != nil {
			return nil, err
		}
		s.sl = sl
	}

	return s, nil
}

func (s *sklTable) kindStatis(kind internalKeyKind) {
	s.totalCount++
	if kind == internalKeyKindDelete {
		s.delCount++
	}
}

func (s *sklTable) delPercent() float64 {
	if s.delCount == 0 {
		return 0
	}
	return s.delCount / s.totalCount
}

func (s *sklTable) itemCount() int {
	return int(s.totalCount)
}

func (s *sklTable) readyForFlush() bool {
	return true
}

func (s *sklTable) get(key []byte, khash uint32) ([]byte, bool, internalKeyKind, func()) {
	value, exist, kind := s.sl.Get(key, khash)
	return value, exist, kind, nil
}

func (s *sklTable) prepare(size uint64) error {
	return s.tbl.checkTableFull(int(size))
}

func (s *sklTable) newIter(o *iterOptions) internalIterator {
	return s.sl.NewIter(o.GetLowerBound(), o.GetUpperBound())
}

func (s *sklTable) inuseBytes() uint64 {
	return uint64(s.sl.Size())
}

func (s *sklTable) dataBytes() uint64 {
	return uint64(s.sl.Size())
}

func (s *sklTable) close() error {
	if s.tbl == nil {
		return nil
	}

	if err := s.tbl.close(); err != nil {
		return err
	}

	s.tbl = nil
	s.sl.cache.index = nil
	return nil
}

func (s *sklTable) path() string {
	if s.tbl == nil {
		return ""
	}
	return s.tbl.path
}

func (s *sklTable) idxFilePath() string {
	return ""
}

func (s *sklTable) empty() bool {
	return s.sl.isEmpty()
}

func (s *sklTable) checkIntegrity() {
	defer func() {
		if r := recover(); r != nil {
			s.bp.opts.Logger.Errorf("sklTable checkIntegrity panic path:%s err:%v stack:%s", s.path(), r, string(debug.Stack()))
		}
	}()

	s.bp.opts.Logger.Infof("check data integrity start file:%s", s.path())

	iter := s.newIter(&iterOptions{})
	defer iter.Close()

	var count int
	for k, v := iter.First(); k != nil; k, v = iter.Next() {
		_ = k.String()
		_ = v
		count++
	}

	if !s.sl.isEmpty() && count == 0 {
		panic("empty data")
	}

	s.bp.opts.Logger.Infof("check data integrity success file:%s", s.path())
}
