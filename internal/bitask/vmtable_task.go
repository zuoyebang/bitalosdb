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

type VmTableFlushTaskData struct {
	Index      int
	NeedReport bool
}

type VmTableFlushTaskOptions struct {
	Size      int
	WorkerNum int
	DoFunc    func(*VmTableFlushTaskData)
	Logger    base.Logger
	TaskWg    *sync.WaitGroup
}

type VmTableFlushTask struct {
	workNum    int
	taskWg     *sync.WaitGroup
	recvChs    []chan *VmTableFlushTaskData
	closed     atomic.Bool
	logger     base.Logger
	doTaskFunc func(*VmTableFlushTaskData)
}

func NewVmTableFlushTask(opts *VmTableFlushTaskOptions) *VmTableFlushTask {
	workerNum := opts.WorkerNum
	size := opts.Size / workerNum
	task := &VmTableFlushTask{
		recvChs:    make([]chan *VmTableFlushTaskData, workerNum),
		doTaskFunc: opts.DoFunc,
		logger:     opts.Logger,
		taskWg:     opts.TaskWg,
		workNum:    workerNum,
	}

	for i := 0; i < workerNum; i++ {
		task.recvChs[i] = make(chan *VmTableFlushTaskData, size)
		task.consume(i)
	}

	task.logger.Infof("vmTable task workerNum:%d size:%d background running...", workerNum, size)
	return task
}

func (t *VmTableFlushTask) consume(i int) {
	t.taskWg.Add(1)
	go func(index int) {
		defer func() {
			t.taskWg.Done()
			t.logger.Infof("bituple task %d background exit...", index)
		}()

		do := func() bool {
			defer func() {
				if r := recover(); r != nil {
					t.logger.Errorf("bituple task %d do panic err:%v stack:%s", index, r, string(debug.Stack()))
				}
			}()

			task, ok := <-t.recvChs[index]
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
	}(i)

	t.logger.Infof("bituple task background running...")
}

func (t *VmTableFlushTask) isClosed() bool {
	return t.closed.Load()
}

func (t *VmTableFlushTask) Close() {
	if t.isClosed() {
		return
	}

	t.closed.Store(true)
	for i := range t.recvChs {
		t.recvChs[i] <- nil
	}
}

func (t *VmTableFlushTask) PushTask(task *VmTableFlushTaskData) {
	if t.isClosed() {
		return
	}

	index := task.Index % t.workNum
	t.recvChs[index] <- task
}
