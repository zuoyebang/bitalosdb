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

package bitalosdb

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/record"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
	"golang.org/x/exp/rand"
)

type testCommitEnv struct {
	logSeqNum     uint64
	visibleSeqNum uint64
	writePos      int64
	writeCount    uint64
	applyBuf      struct {
		sync.Mutex
		buf []uint64
	}
}

func (e *testCommitEnv) env() commitEnv {
	return commitEnv{
		logSeqNum:     &e.logSeqNum,
		visibleSeqNum: &e.visibleSeqNum,
		apply:         e.apply,
		write:         e.write,
	}
}

func (e *testCommitEnv) apply(b *BatchBitower, mem *memTable) error {
	e.applyBuf.Lock()
	e.applyBuf.buf = append(e.applyBuf.buf, b.SeqNum())
	e.applyBuf.Unlock()
	return nil
}

func (e *testCommitEnv) write(b *BatchBitower, _ *sync.WaitGroup, _ *error) (*memTable, error) {
	n := int64(len(b.data))
	atomic.AddInt64(&e.writePos, n)
	atomic.AddUint64(&e.writeCount, 1)
	return nil, nil
}

func TestCommitQueue(t *testing.T) {
	var q commitQueue
	var batches [16]BatchBitower
	for i := range batches {
		q.enqueue(&batches[i])
	}
	if b := q.dequeue(); b != nil {
		t.Fatalf("unexpectedly dequeued batch: %p", b)
	}
	atomic.StoreUint32(&batches[1].applied, 1)
	if b := q.dequeue(); b != nil {
		t.Fatalf("unexpectedly dequeued batch: %p", b)
	}
	for i := range batches {
		atomic.StoreUint32(&batches[i].applied, 1)
		if b := q.dequeue(); b != &batches[i] {
			t.Fatalf("%d: expected batch %p, but found %p", i, &batches[i], b)
		}
	}
	if b := q.dequeue(); b != nil {
		t.Fatalf("unexpectedly dequeued batch: %p", b)
	}
}

func TestCommitPipeline(t *testing.T) {
	var e testCommitEnv
	p := newCommitPipeline(e.env())

	n := 10000
	if invariants.RaceEnabled {
		n = 1000
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			var b BatchBitower
			_ = b.Set([]byte(fmt.Sprint(i)), nil, nil)
			_ = p.Commit(&b, false)
		}(i)
	}
	wg.Wait()

	if s := atomic.LoadUint64(&e.writeCount); uint64(n) != s {
		t.Fatalf("expected %d written batches, but found %d", n, s)
	}
	if n != len(e.applyBuf.buf) {
		t.Fatalf("expected %d written batches, but found %d",
			n, len(e.applyBuf.buf))
	}
	if s := atomic.LoadUint64(&e.logSeqNum); uint64(n) != s {
		t.Fatalf("expected %d, but found %d", n, s)
	}
	if s := atomic.LoadUint64(&e.visibleSeqNum); uint64(n) != s {
		t.Fatalf("expected %d, but found %d", n, s)
	}
}

type syncDelayFile struct {
	vfs.File
	done chan struct{}
}

func (f *syncDelayFile) Sync() error {
	<-f.done
	return nil
}

func TestCommitPipelineWALClose(t *testing.T) {
	mem := vfs.NewMem()
	f, err := mem.Create("test-wal")
	require.NoError(t, err)

	sf := &syncDelayFile{
		File: f,
		done: make(chan struct{}),
	}

	wal := record.NewLogWriter(sf, 0)
	var walDone sync.WaitGroup
	testEnv := commitEnv{
		logSeqNum:     new(uint64),
		visibleSeqNum: new(uint64),
		apply: func(b *BatchBitower, mem *memTable) error {
			walDone.Done()
			return nil
		},
		write: func(b *BatchBitower, syncWG *sync.WaitGroup, syncErr *error) (*memTable, error) {
			_, err := wal.SyncRecord(b.data, syncWG, syncErr)
			return nil, err
		},
		useQueue: true,
	}
	p := newCommitPipeline(testEnv)

	errCh := make(chan error, cap(p.sem))
	walDone.Add(cap(errCh))
	for i := 0; i < cap(errCh); i++ {
		go func(i int) {
			b := &BatchBitower{}
			if err := b.LogData([]byte("foo"), nil); err != nil {
				errCh <- err
				return
			}
			errCh <- p.Commit(b, true)
		}(i)
	}

	walDone.Wait()
	close(sf.done)

	require.NoError(t, wal.Close())
	for i := 0; i < cap(errCh); i++ {
		require.NoError(t, <-errCh)
	}
}

func BenchmarkCommitPipeline(b *testing.B) {
	for _, parallelism := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("parallel=%d", parallelism), func(b *testing.B) {
			b.SetParallelism(parallelism)
			mem := newMemTable(memTableOptions{})
			wal := record.NewLogWriter(ioutil.Discard, 0 /* logNum */)

			nullCommitEnv := commitEnv{
				logSeqNum:     new(uint64),
				visibleSeqNum: new(uint64),
				apply: func(b *BatchBitower, mem *memTable) error {
					err := mem.apply(b, b.SeqNum())
					if err != nil {
						return err
					}
					mem.writerUnref()
					return nil
				},
				write: func(b *BatchBitower, syncWG *sync.WaitGroup, syncErr *error) (*memTable, error) {
					for {
						err := mem.prepare(b, false)
						if err == arenaskl.ErrArenaFull {
							mem = newMemTable(memTableOptions{})
							continue
						}
						if err != nil {
							return nil, err
						}
						break
					}

					_, err := wal.SyncRecord(b.data, syncWG, syncErr)
					return mem, err
				},
			}
			p := newCommitPipeline(nullCommitEnv)

			const keySize = 8
			b.SetBytes(2 * keySize)
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
				buf := make([]byte, keySize)

				for pb.Next() {
					batch := newBatchBitower(nil)
					binary.BigEndian.PutUint64(buf, rng.Uint64())
					batch.Set(buf, buf, nil)
					if err := p.Commit(batch, true /* sync */); err != nil {
						b.Fatal(err)
					}
					batch.release()
				}
			})
		})
	}
}
