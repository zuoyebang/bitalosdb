// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitalosdb

import (
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/list2"
	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
	"github.com/zuoyebang/bitalosdb/v2/internal/vectormap"
)

type vmapPool struct {
	size   int
	vmSize int
	opts   *vectormap.VectorMapOptions
	mu     sync.Mutex
	queue  *list2.Queue
	logger base.Logger
	taskWg sync.WaitGroup
	recvCh chan *vectormap.VectorMap
	closed atomic.Bool
}

func (p *vmapPool) newVectorMap() *vectormap.VectorMap {
	return vectormap.NewVectorMap(p.opts)
}

func (p *vmapPool) worker() {
	p.taskWg.Add(1)
	go func() {
		defer func() {
			p.taskWg.Done()
			p.logger.Infof("vectorma pool worker background exit")
		}()

		do := func() bool {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Errorf("vectorma pool worker do panic err:%v stack:%s", r, string(debug.Stack()))
				}
			}()

			vm, ok := <-p.recvCh
			if !ok || vm == nil {
				return true
			}

			vm.Reset()

			p.mu.Lock()
			curSize := p.queue.Len()
			p.mu.Unlock()

			if curSize < p.size {
				vm.Reset()
				p.mu.Lock()
				p.queue.Push(vm)
				p.mu.Unlock()
			} else {
				_ = vm.Close(false)
			}

			return false
		}

		var exit bool
		for {
			if exit = do(); exit {
				break
			}
		}
	}()

	p.logger.Infof("vectormap pool worker background running size:%d", p.size)
}

func (p *vmapPool) get(vid string) *vectormap.VectorMap {
	var vm *vectormap.VectorMap

	p.mu.Lock()
	if !p.queue.Empty() {
		vm = p.queue.Pop().(*vectormap.VectorMap)
	} else {
		vm = p.newVectorMap()
	}
	p.mu.Unlock()

	arenaBuf := manual.New(p.vmSize)
	vm.NewData(arenaBuf)
	vm.SetId(vid)
	return vm
}

func (p *vmapPool) put(vm *vectormap.VectorMap) {
	p.recvCh <- vm
}

func (p *vmapPool) close() {
	if p.closed.Load() {
		return
	}

	p.closed.Store(true)
	p.recvCh <- nil
	p.taskWg.Wait()
	for !p.queue.Empty() {
		vm := p.queue.Pop().(*vectormap.VectorMap)
		_ = vm.Close(false)
	}
}
