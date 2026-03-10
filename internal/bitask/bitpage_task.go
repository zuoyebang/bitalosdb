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
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
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
	Size      int
	WorkerNum int
	DoFunc    func(*BitpageTaskData)
	Logger    base.Logger
	TaskWg    *sync.WaitGroup
}

type BitpageTask struct {
	workNum    int
	taskWg     *sync.WaitGroup
	recvChs    []chan *BitpageTaskData
	closed     atomic.Bool
	logger     base.Logger
	doTaskFunc func(*BitpageTaskData)
}

func NewBitpageTask(opts *BitpageTaskOptions) *BitpageTask {
	workerNum := opts.WorkerNum
	size := opts.Size / workerNum
	bpTask := &BitpageTask{
		recvChs:    make([]chan *BitpageTaskData, workerNum),
		doTaskFunc: opts.DoFunc,
		logger:     opts.Logger,
		taskWg:     opts.TaskWg,
		workNum:    workerNum,
	}

	for i := 0; i < workerNum; i++ {
		bpTask.recvChs[i] = make(chan *BitpageTaskData, size)
		bpTask.consume(i)
	}

	bpTask.logger.Infof("bitpage task workerNum:%d size:%d background running...", workerNum, size)

	return bpTask
}

func (t *BitpageTask) consume(i int) {
	t.taskWg.Add(1)
	go func(index int) {
		defer func() {
			t.taskWg.Done()
			t.logger.Infof("bitpage task %d background exit...", index)
		}()

		do := func() bool {
			defer func() {
				if r := recover(); r != nil {
					t.logger.Errorf("bitpage task %d do panic err:%v stack:%s", index, r, string(debug.Stack()))
				}
			}()

			task, ok := <-t.recvChs[index]
			if !ok || t.isClosed() || task == nil {
				return true
			}

			task.WaitDuration = time.Since(task.SendTime)
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
}

func (t *BitpageTask) isClosed() bool {
	return t.closed.Load()
}

func (t *BitpageTask) Close() {
	t.closed.Store(true)
	for i := range t.recvChs {
		t.recvChs[i] <- nil
	}
}

func (t *BitpageTask) PushTask(task *BitpageTaskData) {
	if t.isClosed() {
		return
	}

	task.SendTime = time.Now()
	index := task.Index % t.workNum
	t.recvChs[index] <- task
}
