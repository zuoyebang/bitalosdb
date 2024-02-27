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

package bitalosdb

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/record"
)

type commitQueue struct {
	headTail uint64
	slots    [record.SyncConcurrency]unsafe.Pointer
}

const dequeueBits = 32

func (q *commitQueue) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (q *commitQueue) pack(head, tail uint32) uint64 {
	const mask = 1<<dequeueBits - 1
	return (uint64(head) << dequeueBits) |
		uint64(tail&mask)
}

func (q *commitQueue) enqueue(b *Batch) {
	ptrs := atomic.LoadUint64(&q.headTail)
	head, tail := q.unpack(ptrs)
	if (tail+uint32(len(q.slots)))&(1<<dequeueBits-1) == head {
		panic("bitalosdb: not reached")
	}
	slot := &q.slots[head&uint32(len(q.slots)-1)]

	for atomic.LoadPointer(slot) != nil {
		runtime.Gosched()
	}

	atomic.StorePointer(slot, unsafe.Pointer(b))

	atomic.AddUint64(&q.headTail, 1<<dequeueBits)
}

func (q *commitQueue) dequeue() *Batch {
	for {
		ptrs := atomic.LoadUint64(&q.headTail)
		head, tail := q.unpack(ptrs)
		if tail == head {
			return nil
		}

		slot := &q.slots[tail&uint32(len(q.slots)-1)]
		b := (*Batch)(atomic.LoadPointer(slot))
		if b == nil || atomic.LoadUint32(&b.applied) == 0 {
			return nil
		}

		ptrs2 := q.pack(head, tail+1)
		if atomic.CompareAndSwapUint64(&q.headTail, ptrs, ptrs2) {
			atomic.StorePointer(slot, nil)
			return b
		}
	}
}

type commitEnv struct {
	logSeqNum     *uint64
	visibleSeqNum *uint64
	apply         func(b *Batch, mem *memTable) error
	write         func(b *Batch, wg *sync.WaitGroup, err *error) (*memTable, error)
	useQueue      bool
}

type commitPipeline struct {
	pending commitQueue
	env     commitEnv
	sem     chan struct{}
	mu      sync.Mutex
}

func newCommitPipeline(env commitEnv) *commitPipeline {
	p := &commitPipeline{
		env: env,
		sem: make(chan struct{}, record.SyncConcurrency-1),
	}
	return p
}

func (p *commitPipeline) Commit(b *Batch, syncWAL bool) error {
	if b.Empty() {
		return nil
	}

	if p.env.useQueue {
		p.sem <- struct{}{}
		defer func() {
			<-p.sem
		}()
	}

	mem, err := p.prepare(b, syncWAL)
	if err != nil {
		b.db = nil
		return err
	}

	if err = p.env.apply(b, mem); err != nil {
		b.db = nil
		return err
	}

	p.publish(b)

	if b.commitErr != nil {
		b.db = nil
	}
	return b.commitErr
}

func (p *commitPipeline) AllocateSeqNum(count int, prepare func(), apply func(seqNum uint64)) {
	b := newBatch(nil)
	defer b.release()

	b.data = make([]byte, batchHeaderLen)
	b.setCount(uint32(count))
	b.commit.Add(1)

	p.sem <- struct{}{}

	p.mu.Lock()

	p.pending.enqueue(b)

	logSeqNum := atomic.AddUint64(p.env.logSeqNum, uint64(count)) - uint64(count)
	seqNum := logSeqNum
	if seqNum == 0 {
		atomic.AddUint64(p.env.logSeqNum, 1)
		seqNum++
	}
	b.setSeqNum(seqNum)

	for {
		visibleSeqNum := atomic.LoadUint64(p.env.visibleSeqNum)
		if visibleSeqNum == logSeqNum {
			break
		}
		runtime.Gosched()
	}

	prepare()

	p.mu.Unlock()

	apply(b.SeqNum())

	p.publish(b)

	<-p.sem
}

func (p *commitPipeline) prepare(b *Batch, syncWAL bool) (*memTable, error) {
	n := uint64(b.Count())
	if n == invalidBatchCount {
		return nil, ErrInvalidBatch
	}

	if p.env.useQueue {
		count := 1
		if syncWAL {
			count++
		}
		b.commit.Add(count)
	}

	var syncWG *sync.WaitGroup
	var syncErr *error
	if syncWAL {
		syncWG, syncErr = &b.commit, &b.commitErr
	}

	p.mu.Lock()

	if p.env.useQueue {
		p.pending.enqueue(b)
	}

	b.setSeqNum(atomic.AddUint64(p.env.logSeqNum, n) - n)

	mem, err := p.env.write(b, syncWG, syncErr)

	p.mu.Unlock()

	return mem, err
}

func (p *commitPipeline) publish(b *Batch) {
	atomic.StoreUint32(&b.applied, 1)

	if p.env.useQueue {
		for {
			t := p.pending.dequeue()
			if t == nil {
				b.commit.Wait()
				break
			}

			if atomic.LoadUint32(&t.applied) != 1 {
				b.db.opts.Logger.Errorf("panic: commitPipeline publish batch applied err")
			}

			for {
				curSeqNum := atomic.LoadUint64(p.env.visibleSeqNum)
				newSeqNum := t.SeqNum() + uint64(t.Count())
				if newSeqNum <= curSeqNum {
					break
				}
				if atomic.CompareAndSwapUint64(p.env.visibleSeqNum, curSeqNum, newSeqNum) {
					break
				}
			}

			t.commit.Done()
		}
	} else {
		for {
			curSeqNum := atomic.LoadUint64(p.env.visibleSeqNum)
			newSeqNum := b.SeqNum() + uint64(b.Count())
			if newSeqNum <= curSeqNum {
				break
			}
			if atomic.CompareAndSwapUint64(p.env.visibleSeqNum, curSeqNum, newSeqNum) {
				break
			}
		}
	}
}

func (p *commitPipeline) ratchetSeqNum(nextSeqNum uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	logSeqNum := atomic.LoadUint64(p.env.logSeqNum)
	if logSeqNum >= nextSeqNum {
		return
	}
	count := nextSeqNum - logSeqNum
	_ = atomic.AddUint64(p.env.logSeqNum, uint64(count)) - uint64(count)
	atomic.StoreUint64(p.env.visibleSeqNum, nextSeqNum)
}
