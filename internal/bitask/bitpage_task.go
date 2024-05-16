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

package bitask

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/statemachine"
)

const (
	BitpageEventFlush int = 1 + iota
	BitpageEventSplit
	BitpageEventFreePage
)

func GetBitpageEventName(e int) string {
	switch e {
	case BitpageEventFlush:
		return "flush"
	case BitpageEventSplit:
		return "split"
	case BitpageEventFreePage:
		return "freePage"
	default:
		return ""
	}
}

type BitpageTaskData struct {
	Index        int
	Event        int
	Pn           uint32
	Sentinel     []byte
	Pns          []uint32
	SendTime     time.Time
	WaitDuration time.Duration
}

type BitpageTaskOptions struct {
	Size    int
	DbState *statemachine.DbStateMachine
	DoFunc  func(*BitpageTaskData)
	Logger  base.Logger
	TaskWg  *sync.WaitGroup
}

type BitpageTask struct {
	taskWg     *sync.WaitGroup
	recvCh     chan *BitpageTaskData
	closed     atomic.Bool
	logger     base.Logger
	dbState    *statemachine.DbStateMachine
	doTaskFunc func(*BitpageTaskData)
}

func NewBitpageTask(opts *BitpageTaskOptions) *BitpageTask {
	bpTask := &BitpageTask{
		recvCh:     make(chan *BitpageTaskData, opts.Size),
		dbState:    opts.DbState,
		doTaskFunc: opts.DoFunc,
		logger:     opts.Logger,
		taskWg:     opts.TaskWg,
	}
	bpTask.Run()
	return bpTask
}

func (t *BitpageTask) Run() {
	t.taskWg.Add(1)
	go func() {
		defer func() {
			t.taskWg.Done()
			t.logger.Infof("bitpage task background exit...")
		}()

		do := func() bool {
			defer func() {
				if r := recover(); r != nil {
					t.logger.Errorf("bitpage task do panic err:%v stack:%s", r, string(debug.Stack()))
				}
			}()

			task, ok := <-t.recvCh
			if !ok || t.isClosed() || task == nil {
				return true
			}

			task.WaitDuration = time.Since(task.SendTime)
			t.dbState.WaitBitowerHighPriority(task.Index)
			t.doTaskFunc(task)
			return false
		}

		var exit bool
		for {
			if exit = do(); exit {
				break
			}
		}
	}()

	t.logger.Infof("bitpage task background running...")
}

func (t *BitpageTask) isClosed() bool {
	return t.closed.Load() == true
}

func (t *BitpageTask) Close() {
	t.closed.Store(true)
	t.recvCh <- nil
}

func (t *BitpageTask) PushTask(task *BitpageTaskData) {
	if !t.isClosed() {
		task.SendTime = time.Now()
		t.recvCh <- task
	}
}
