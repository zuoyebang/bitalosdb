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

package base

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/list2"
)

type DFLOption struct {
	IOWriteLoadThresholdCB func() bool
	Logger                 Logger
	DeleteInterval         int
}

type DeletionFileLimiter struct {
	opts   *DFLOption
	recvCh chan []string
	closed atomic.Bool
	exitCh chan struct{}
	exitWg sync.WaitGroup

	fileList struct {
		sync.Mutex
		l *list2.Queue
	}
}

func NewDeletionFileLimiter(opts *DFLOption) *DeletionFileLimiter {
	l := &DeletionFileLimiter{
		recvCh: make(chan []string, 1024),
		exitCh: make(chan struct{}),
		opts:   opts,
	}
	l.fileList.l = list2.NewQueue()
	l.closed.Store(false)
	return l
}

func (d *DeletionFileLimiter) Run(opts *DFLOption) {
	if opts != nil {
		d.opts = opts
	}

	d.exitWg.Add(2)

	go func() {
		defer d.exitWg.Done()

		d.opts.Logger.Info("[DELETELIMITER] produce running...")

		for {
			select {
			case <-d.exitCh:
				return
			case files := <-d.recvCh:
				if d.isClosed() {
					return
				}

				d.pushFiles(files)
			}
		}
	}()

	go func() {
		defer d.exitWg.Done()

		duration := time.Duration(d.opts.DeleteInterval) * time.Second
		d.opts.Logger.Infof("[DELETELIMITER] consume running interval:%s", duration)
		t := time.NewTimer(duration)
		defer t.Stop()

		for {
			select {
			case <-d.exitCh:
				return
			case <-t.C:
				if d.isClosed() {
					return
				}

				if d.opts.IOWriteLoadThresholdCB() {
					d.deleteFile()
				}

				t.Reset(duration)
			}
		}
	}()
}

func (d *DeletionFileLimiter) AddFile(file string) {
	if d.isClosed() {
		return
	}

	d.recvCh <- []string{file}
}

func (d *DeletionFileLimiter) AddFiles(files []string) {
	if d.isClosed() {
		return
	}

	d.recvCh <- files
}

func (d *DeletionFileLimiter) Flush() {
	if d.isClosed() {
		return
	}

	for !d.fileList.l.Empty() {
		d.deleteFile()
	}
}

func (d *DeletionFileLimiter) Close() {
	if d.isClosed() {
		return
	}

	d.opts.Logger.Infof("[DELETELIMITER] closed start fileListLen:%d", d.fileList.l.Len())

	d.closed.Store(true)
	close(d.exitCh)
	d.exitWg.Wait()

	for !d.fileList.l.Empty() {
		d.deleteFile()
	}

	d.opts.Logger.Info("[DELETELIMITER] closed...")
}

func (d *DeletionFileLimiter) isClosed() bool {
	return d.closed.Load()
}

func (d *DeletionFileLimiter) pushFiles(files []string) {
	d.fileList.Lock()
	defer d.fileList.Unlock()

	for _, file := range files {
		if len(file) == 0 {
			continue
		}

		d.fileList.l.Push(file)
	}
}

func (d *DeletionFileLimiter) popFile() (string, bool) {
	d.fileList.Lock()
	defer d.fileList.Unlock()

	if d.fileList.l.Empty() {
		return "", false
	}

	v := d.fileList.l.Pop()
	if v == nil {
		return "", true
	}
	if file, ok := v.(string); ok {
		return file, true
	}
	return "", true
}

func (d *DeletionFileLimiter) deleteFile() {
	path, ok := d.popFile()
	if !ok || path == "" {
		return
	}

	filename := GetFilePathBase(path)
	if err := os.Remove(path); err != nil {
		d.opts.Logger.Errorf("[DELETELIMITER] delete fail file:%s err:%s", filename, err.Error())
	} else {
		d.opts.Logger.Infof("[DELETELIMITER] delete success file:%s", filename)
	}
}
