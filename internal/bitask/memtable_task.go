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

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
)

type MemTableFlushTaskData struct {
	Index      int
	NeedReport bool
	IsForce    bool
}

type MemTableFlushTaskOptions struct {
	Size   int
	DoFunc func(*MemTableFlushTaskData)
	Logger base.Logger
	TaskWg *sync.WaitGroup
}

type MemTableFlushTask struct {
	taskWg     *sync.WaitGroup
	recvCh     chan *MemTableFlushTaskData
	taskCount  uint64
	closed     atomic.Bool
	logger     base.Logger
	doTaskFunc func(*MemTableFlushTaskData)
}

func NewMemTableFlushTask(opts *MemTableFlushTaskOptions) *MemTableFlushTask {
	task := &MemTableFlushTask{
		recvCh:     make(chan *MemTableFlushTaskData, opts.Size),
		doTaskFunc: opts.DoFunc,
		logger:     opts.Logger,
		taskWg:     opts.TaskWg,
	}
	task.Run()
	return task
}

func (t *MemTableFlushTask) Run() {
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

func (t *MemTableFlushTask) isClosed() bool {
	return t.closed.Load()
}

func (t *MemTableFlushTask) Count() uint64 {
	return t.taskCount
}

func (t *MemTableFlushTask) Close() {
	t.closed.Store(true)
	t.recvCh <- nil
}

func (t *MemTableFlushTask) PushTask(task *MemTableFlushTaskData) {
	if !t.isClosed() {
		t.taskCount++
		t.recvCh <- task
		return
	}
}
