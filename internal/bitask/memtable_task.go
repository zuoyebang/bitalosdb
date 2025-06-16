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

package bitask

import (
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/base"
)

type MemFlushTaskData struct {
	Index      int
	NeedReport bool
}

type MemFlushTaskOptions struct {
	Size   int
	DoFunc func(*MemFlushTaskData)
	Logger base.Logger
	TaskWg *sync.WaitGroup
}

type MemFlushTask struct {
	taskWg     *sync.WaitGroup
	recvCh     chan *MemFlushTaskData
	taskCount  uint64
	closed     atomic.Bool
	logger     base.Logger
	doTaskFunc func(*MemFlushTaskData)
}

func NewMemFlushTask(opts *MemFlushTaskOptions) *MemFlushTask {
	task := &MemFlushTask{
		recvCh:     make(chan *MemFlushTaskData, opts.Size),
		doTaskFunc: opts.DoFunc,
		logger:     opts.Logger,
		taskWg:     opts.TaskWg,
	}
	task.Run()
	return task
}

func (t *MemFlushTask) Run() {
	t.taskWg.Add(1)
	go func() {
		defer func() {
			t.taskWg.Done()
			t.logger.Infof("memtable flush task background exit...")
		}()

		do := func() bool {
			defer func() {
				if r := recover(); r != nil {
					t.logger.Errorf("memtable flush task do panic err:%v stack:%s", r, string(debug.Stack()))
				}
			}()

			task, ok := <-t.recvCh
			if !ok || task == nil {
				return true
			}

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

	t.logger.Infof("memtable flush task background running...")
}

func (t *MemFlushTask) isClosed() bool {
	return t.closed.Load() == true
}

func (t *MemFlushTask) Count() uint64 {
	return t.taskCount
}

func (t *MemFlushTask) Close() {
	t.closed.Store(true)
	t.recvCh <- nil
}

func (t *MemFlushTask) PushTask(task *MemFlushTaskData) {
	if !t.isClosed() {
		t.taskCount++
		t.recvCh <- task
		return
	}
}
