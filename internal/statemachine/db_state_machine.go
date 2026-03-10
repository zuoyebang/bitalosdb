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

package statemachine

import (
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
)

type DbStateMachine struct {
	TaskLock             sync.RWMutex                            // [vm-flush | mem-flush | bitpage-flush/split | checkpoint]
	KKVWrite             [consts.MemIndexShardNum + 1]sync.Mutex // [mem-flush | bitpage-flush/split]
	MemTableHighPriority [consts.MemIndexShardNum + 1]atomic.Bool
	VmTableHighPriority  [consts.MemIndexShardNum]atomic.Bool

	bitpage struct {
		flushCount atomic.Uint64
		splitCount atomic.Uint64
	}
}

func NewDbStateMachine() *DbStateMachine {
	return &DbStateMachine{}
}

func (s *DbStateMachine) LockTask() {
	s.TaskLock.Lock()
}

func (s *DbStateMachine) UnlockTask() {
	s.TaskLock.Unlock()
}

func (s *DbStateMachine) RLockTask() {
	s.TaskLock.RLock()
}

func (s *DbStateMachine) RUnlockTask() {
	s.TaskLock.RUnlock()
}

func (s *DbStateMachine) LockDbKKVWrite() {
	for i := range s.KKVWrite {
		s.KKVWrite[i].Lock()
	}
}

func (s *DbStateMachine) UnlockDbKKVWrite() {
	for i := range s.KKVWrite {
		s.KKVWrite[i].Unlock()
	}
}

func (s *DbStateMachine) LockKKVWrite(i int) {
	index := consts.KKVSlotToShard(i)
	s.KKVWrite[index].Lock()
}

func (s *DbStateMachine) UnlockKKVWrite(i int) {
	index := consts.KKVSlotToShard(i)
	s.KKVWrite[index].Unlock()
}

func (s *DbStateMachine) LockKKVMemTableWrite(i int) {
	s.KKVWrite[i].Lock()
}

func (s *DbStateMachine) UnlockKKVMemTableWrite(i int) {
	s.KKVWrite[i].Unlock()
}

func (s *DbStateMachine) SetMemTableHighPriority(i int, v bool) {
	s.MemTableHighPriority[i].Store(v)
}

func (s *DbStateMachine) WaitMemTableHighPriority(i int) bool {
	index := consts.KKVSlotToShard(i)
	return s.MemTableHighPriority[index].Load()
}

func (s *DbStateMachine) SetVmTableHighPriority(i int, v bool) {
	index := consts.KVSlotToShard(i)
	s.VmTableHighPriority[index].Store(v)
}

func (s *DbStateMachine) WaitVmTableHighPriority(i int) bool {
	index := consts.KVSlotToShard(i)
	return s.VmTableHighPriority[index].Load()
}

func (s *DbStateMachine) AddBitpageFlushCount() {
	s.bitpage.flushCount.Add(1)
}

func (s *DbStateMachine) GetBitpageFlushCount() uint64 {
	return s.bitpage.flushCount.Load()
}

func (s *DbStateMachine) AddBitpageSplitCount() {
	s.bitpage.splitCount.Add(1)
}

func (s *DbStateMachine) GetBitpageSplitCount() uint64 {
	return s.bitpage.splitCount.Load()
}
