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
	"time"

	"github.com/zuoyebang/bitalosdb/internal/consts"
)

type DbStateMachine struct {
	TaskLock            sync.Mutex // compact-bithash｜checkpoint
	BitowerHighPriority [consts.DefaultBitowerNum]atomic.Bool
	BitowerWrite        [consts.DefaultBitowerNum]sync.Mutex // mem-flush｜bitpage-flush/split ｜compact-bitable｜checkpoint

	bitpage struct {
		flushCount atomic.Uint64
		splitCount atomic.Uint64
	}
}

func NewDbStateMachine() *DbStateMachine {
	return &DbStateMachine{}
}

func (s *DbStateMachine) LockDbWrite() {
	for i := range s.BitowerWrite {
		s.BitowerWrite[i].Lock()
	}
}

func (s *DbStateMachine) UnlockDbWrite() {
	for i := range s.BitowerWrite {
		s.BitowerWrite[i].Unlock()
	}
}

func (s *DbStateMachine) LockBitowerWrite(i int) {
	s.BitowerWrite[i].Lock()
}

func (s *DbStateMachine) UnlockBitowerWrite(i int) {
	s.BitowerWrite[i].Unlock()
}

func (s *DbStateMachine) LockTask() {
	s.TaskLock.Lock()
}

func (s *DbStateMachine) UnlockTask() {
	s.TaskLock.Unlock()
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

func (s *DbStateMachine) SetDbHighPriority(v bool) {
	for i := range s.BitowerHighPriority {
		s.BitowerHighPriority[i].Store(v)
	}
}

func (s *DbStateMachine) SetBitowerHighPriority(i int, v bool) {
	s.BitowerHighPriority[i].Store(v)
}

func (s *DbStateMachine) WaitBitowerHighPriority(i int) {
	for s.BitowerHighPriority[i].Load() {
		time.Sleep(1 * time.Second)
	}
}
