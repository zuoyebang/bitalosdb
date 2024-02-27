// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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
)

type DbStateMachine struct {
	MemFlushing  atomic.Bool
	HighPriority atomic.Bool
	TaskLock     sync.Mutex // compact-bithash｜checkpoint
	DbWrite      sync.Mutex // mem-flush｜bitpage-flush｜compact-bitable｜checkpoint

	bitpage struct {
		flushCount atomic.Uint64
		splitCount atomic.Uint64
	}
}

func NewDbStateMachine() *DbStateMachine {
	return &DbStateMachine{}
}

func (s *DbStateMachine) LockDbWrite() {
	s.DbWrite.Lock()
}

func (s *DbStateMachine) UnlockDbWrite() {
	s.DbWrite.Unlock()
}

func (s *DbStateMachine) LockTask() {
	s.TaskLock.Lock()
}

func (s *DbStateMachine) UnlockTask() {
	s.TaskLock.Unlock()
}

func (s *DbStateMachine) LockMemFlushing() {
	s.MemFlushing.Store(true)
}

func (s *DbStateMachine) UnLockMemFlushing() {
	s.MemFlushing.Store(false)
}

func (s *DbStateMachine) IsMemFlushing() bool {
	return s.MemFlushing.Load()
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

func (s *DbStateMachine) SetHighPriority(v bool) {
	s.HighPriority.Store(v)
}

func (s *DbStateMachine) WaitHighPriority() {
	for s.HighPriority.Load() {
		time.Sleep(1 * time.Second)
	}
}
