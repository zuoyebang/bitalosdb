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

package lfucache

import (
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/cache/lfucache/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/humanize"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

const (
	keyFlushInvalid int = iota
	keyFlushDelete
	keyFlushReserve
)

const (
	compactTypeMemFlush int = iota + 1
	compactTypeClosed
)

func (s *shard) compact(ctype int) {
	if ctype == compactTypeMemFlush && s.getMemtableNum() <= 1 {
		return
	}

	var n, memFlushNum int
	var flushing flushableList

	s.mu.Lock()

	if ctype == compactTypeClosed {
		s.mu.memMutable.writerUnref()
		memFlushNum = len(s.mu.memQueue)
	} else {
		memFlushNum = len(s.mu.memQueue) - 1
	}

	for ; n < memFlushNum; n++ {
		if !s.mu.memQueue[n].readyForFlush() {
			break
		}
	}

	if n > 0 {
		flushing = append(flushing, s.mu.memQueue[:n]...)
		if s.mu.arrtable != nil {
			flushing = append(flushing, s.mu.arrtable)
		}
	}

	s.mu.Unlock()

	if len(flushing) == 0 {
		return
	}

	arrtable, err := s.runCompaction(flushing, ctype)
	if err != nil {
		s.lc.logger.Errorf("[FLUSHLFUCACHE] shard:%d flushCnt:%d ctype:%d runCompaction err:%s", s.index, s.flushCnt, ctype, err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.memQueue = s.mu.memQueue[n:]
	s.mu.arrtable = arrtable
	s.updateReadStateLocked()
	for i := range flushing {
		flushing[i].readerUnref()
	}
}

func (s *shard) runCompaction(flushing flushableList, ctype int) (_ *flushableEntry, retErr error) {
	var bytesIterated uint64
	var prevVal []byte
	var prevValLen int
	var prevFreq uint16
	var prevEntrySize int

	s.flushCnt++
	logTag := fmt.Sprintf("[FLUSHLFUCACHE] shard:%d flushCnt:%d ctype:%d", s.index, s.flushCnt, ctype)
	s.lc.logger.Infof("%s flush start", logTag)
	startTime := time.Now()
	defer func() {
		if r := recover(); r != nil {
			s.lc.logger.Errorf("%s runCompaction panic err:%v stack=%s", logTag, r, string(debug.Stack()))
		}
	}()

	iiter := newInputIter(flushing, &bytesIterated)
	iter := newCompactionIter(iiter)
	defer func() {
		if iter != nil {
			retErr = utils.FirstError(retErr, iter.Close())
		}
	}()

	nowTime := uint16((time.Now().Unix() - s.lc.launchTime) / 3600)

	checkKeyStatus := func(k *internalKey, v []byte, t uint16, f uint16) int {
		if k.Kind() != internalKeyKindSet {
			return keyFlushDelete
		}

		vLen := len(v)
		if t > 0 && nowTime-binary.BigEndian.Uint16(v[vLen-LFU_META_LENGTH:]) > t {
			return keyFlushInvalid
		}

		if f > 0 && binary.BigEndian.Uint16(v[vLen-LFU_FREQ_LENGTH:]) < f {
			return keyFlushInvalid
		}

		return keyFlushReserve
	}

	s.clearFreqLevelStat()

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		keyStatus := checkKeyStatus(key, val, 0, 0)
		if keyStatus == keyFlushReserve {
			if prevValLen > 2 {
				prevFreq = binary.BigEndian.Uint16(prevVal[prevValLen-LFU_FREQ_LENGTH:])
				tm := nowTime - binary.BigEndian.Uint16(prevVal[prevValLen-LFU_META_LENGTH:])
				s.updateFreqLevelStat(tm, prevFreq, prevEntrySize)
			}

			prevVal = val
			prevValLen = len(prevVal)
			prevEntrySize = arrayTableEntrySize(len(key.UserKey), len(val))
		} else {
			prevVal = nil
			prevValLen = 0
			prevEntrySize = 0
		}
	}

	if prevValLen > 2 {
		prevFreq = binary.BigEndian.Uint16(prevVal[prevValLen-LFU_FREQ_LENGTH:])
		tm := nowTime - binary.BigEndian.Uint16(prevVal[prevValLen-LFU_META_LENGTH:])
		s.updateFreqLevelStat(tm, prevFreq, prevEntrySize)
	}

	atSize := s.memSize + s.memSize/2
	at, atEntry := s.newArrayTable(atSize)
	defer func() {
		if retErr != nil {
			atEntry.readerUnref()
		}
	}()

	freqThreshold, freqTm, freqAvg := s.calculateFreqLevel(atSize)

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		keyStatus := checkKeyStatus(key, val, freqTm, freqThreshold)
		if keyStatus == keyFlushReserve {
			freq := binary.BigEndian.Uint16(val[len(val)-LFU_FREQ_LENGTH:])
			if freq <= freqAvg {
				freq = 0
			} else {
				freq -= freqAvg
			}
			binary.BigEndian.PutUint16(val[len(val)-LFU_FREQ_LENGTH:], freq)

			retErr = at.add(key.UserKey, val)
			if retErr != nil {
				if retErr == arenaskl.ErrArenaFull {
					break
				} else {
					return nil, retErr
				}
			}
		}
	}

	duration := time.Since(startTime)
	s.lc.logger.Infof("%s flush finish table(%d) iterated(%s) written(%s), in %.3fs, output rate %s/s",
		logTag,
		len(flushing),
		humanize.Uint64(bytesIterated),
		humanize.Uint64(uint64(atSize)),
		duration.Seconds(),
		humanize.Uint64(uint64(float64(atSize)/duration.Seconds())),
	)

	return atEntry, nil
}

func newInputIter(flushing flushableList, bytesIterated *uint64) internalIterator {
	if len(flushing) == 1 {
		iter := flushing[0].newFlushIter(nil, bytesIterated)
		return iter
	}

	iters := make([]internalIterator, 0, len(flushing))
	for i := range flushing {
		iters = append(iters, flushing[i].newFlushIter(nil, bytesIterated))
	}
	return newMergingIter(iters...)
}
