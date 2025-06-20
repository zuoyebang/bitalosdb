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

package bdb_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/errors"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

func TestSimulate_1op_1p(t *testing.T)     { testSimulate(t, nil, 1, 1, 1) }
func TestSimulate_10op_1p(t *testing.T)    { testSimulate(t, nil, 1, 10, 1) }
func TestSimulate_100op_1p(t *testing.T)   { testSimulate(t, nil, 1, 100, 1) }
func TestSimulate_1000op_1p(t *testing.T)  { testSimulate(t, nil, 1, 1000, 1) }
func TestSimulate_10000op_1p(t *testing.T) { testSimulate(t, nil, 1, 10000, 1) }

func TestSimulate_10op_10p(t *testing.T)    { testSimulate(t, nil, 1, 10, 10) }
func TestSimulate_100op_10p(t *testing.T)   { testSimulate(t, nil, 1, 100, 10) }
func TestSimulate_1000op_10p(t *testing.T)  { testSimulate(t, nil, 1, 1000, 10) }
func TestSimulate_10000op_10p(t *testing.T) { testSimulate(t, nil, 1, 10000, 10) }

func TestSimulate_100op_100p(t *testing.T)   { testSimulate(t, nil, 1, 100, 100) }
func TestSimulate_1000op_100p(t *testing.T)  { testSimulate(t, nil, 1, 1000, 100) }
func TestSimulate_10000op_100p(t *testing.T) { testSimulate(t, nil, 1, 10000, 100) }

func TestSimulate_10000op_1000p(t *testing.T) { testSimulate(t, nil, 1, 10000, 1000) }

func testSimulate(t *testing.T, openOption *options.BdbOptions, round, threadCount, parallelism int) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	rand.Seed(int64(qseed))

	var readerHandlers = []simulateHandler{simulateGetHandler}
	var writerHandlers = []simulateHandler{simulateGetHandler, simulatePutHandler}

	var versions = make(map[int]*QuickDB)
	versions[1] = NewQuickDB()

	db := MustOpenWithOption(openOption)
	defer db.MustClose()

	var mutex sync.Mutex

	for n := 0; n < round; n++ {
		var threads = make(chan bool, parallelism)
		var wg sync.WaitGroup

		var opCount int64

		var igCount int64

		var errCh = make(chan error, threadCount)

		var i int
		for {
			threads <- true
			wg.Add(1)
			writable := ((rand.Int() % 100) < 20)
			var handler simulateHandler
			if writable {
				handler = writerHandlers[rand.Intn(len(writerHandlers))]
			} else {
				handler = readerHandlers[rand.Intn(len(readerHandlers))]
			}

			go func(writable bool, handler simulateHandler) {
				defer wg.Done()
				atomic.AddInt64(&opCount, 1)
				tx, err := db.Begin(writable)
				if err != nil {
					errCh <- errors.Errorf("error tx begin: %v", err)
					return
				}

				mutex.Lock()
				var qdb = versions[tx.ID()]
				if writable {
					qdb = versions[tx.ID()-1].Copy()
				}
				mutex.Unlock()

				if writable {
					defer func() {
						mutex.Lock()
						versions[tx.ID()] = qdb
						mutex.Unlock()

						if err := tx.Commit(); err != nil {
							errCh <- err
							return
						}
					}()
				} else {
					defer func() { _ = tx.Rollback() }()
				}

				if qdb == nil {
					atomic.AddInt64(&igCount, 1)
					return
				}

				handler(tx, qdb)

				<-threads
			}(writable, handler)

			i++
			if i >= threadCount {
				break
			}
		}

		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				t.Fatalf("error from inside goroutine: %v", err)
			}
		}

		db.MustClose()
		db.MustReopen()
	}

}

type simulateHandler func(tx *bdb.Tx, qdb *QuickDB)

func simulateGetHandler(tx *bdb.Tx, qdb *QuickDB) {
	keys := qdb.Rand()
	if len(keys) == 0 {
		return
	}

	b := tx.Bucket(keys[0])
	if b == nil {
		panic(fmt.Sprintf("bucket[0] expected: %08x\n", trunc(keys[0], 4)))
	}

	for _, key := range keys[1 : len(keys)-1] {
		b = b.Bucket(key)
		if b == nil {
			panic(fmt.Sprintf("bucket[n] expected: %v -> %v\n", keys, key))
		}
	}

	expected := qdb.Get(keys)
	actual := b.Get(keys[len(keys)-1])
	if !bytes.Equal(actual, expected) {
		fmt.Println("=== EXPECTED ===")
		fmt.Println(expected)
		fmt.Println("=== ACTUAL ===")
		fmt.Println(actual)
		fmt.Println("=== END ===")
		panic("value mismatch")
	}
}

func simulatePutHandler(tx *bdb.Tx, qdb *QuickDB) {
	var err error
	keys, value := randKeys(), randValue()

	b := tx.Bucket(keys[0])
	if b == nil {
		b, err = tx.CreateBucket(keys[0])
		if err != nil {
			panic("create bucket: " + err.Error())
		}
	}

	for _, key := range keys[1 : len(keys)-1] {
		child := b.Bucket(key)
		if child != nil {
			b = child
		} else {
			b, err = b.CreateBucket(key)
			if err != nil {
				panic("create bucket: " + err.Error())
			}
		}
	}

	if err := b.Put(keys[len(keys)-1], value); err != nil {
		panic("put: " + err.Error())
	}

	qdb.Put(keys, value)
}

type QuickDB struct {
	sync.RWMutex
	m map[string]interface{}
}

func NewQuickDB() *QuickDB {
	return &QuickDB{m: make(map[string]interface{})}
}

func (db *QuickDB) Get(keys [][]byte) []byte {
	db.RLock()
	defer db.RUnlock()

	m := db.m
	for _, key := range keys[:len(keys)-1] {
		value := m[string(key)]
		if value == nil {
			return nil
		}
		switch value := value.(type) {
		case map[string]interface{}:
			m = value
		case []byte:
			return nil
		}
	}

	if value, ok := m[string(keys[len(keys)-1])].([]byte); ok {
		return value
	}
	return nil
}

func (db *QuickDB) Put(keys [][]byte, value []byte) {
	db.Lock()
	defer db.Unlock()

	m := db.m
	for _, key := range keys[:len(keys)-1] {
		if _, ok := m[string(key)].([]byte); ok {
			return
		}

		if m[string(key)] == nil {
			m[string(key)] = make(map[string]interface{})
		}
		m = m[string(key)].(map[string]interface{})
	}

	m[string(keys[len(keys)-1])] = value
}

func (db *QuickDB) Rand() [][]byte {
	db.RLock()
	defer db.RUnlock()
	if len(db.m) == 0 {
		return nil
	}
	var keys [][]byte
	db.rand(db.m, &keys)
	return keys
}

func (db *QuickDB) rand(m map[string]interface{}, keys *[][]byte) {
	i, index := 0, rand.Intn(len(m))
	for k, v := range m {
		if i == index {
			*keys = append(*keys, []byte(k))
			if v, ok := v.(map[string]interface{}); ok {
				db.rand(v, keys)
			}
			return
		}
		i++
	}
	panic("quickdb rand: out-of-range")
}

func (db *QuickDB) Copy() *QuickDB {
	db.RLock()
	defer db.RUnlock()
	return &QuickDB{m: db.copy(db.m)}
}

func (db *QuickDB) copy(m map[string]interface{}) map[string]interface{} {
	clone := make(map[string]interface{}, len(m))
	for k, v := range m {
		switch v := v.(type) {
		case map[string]interface{}:
			clone[k] = db.copy(v)
		default:
			clone[k] = v
		}
	}
	return clone
}

func randKey() []byte {
	var min, max = 1, 1024
	n := rand.Intn(max-min) + min
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(255))
	}
	return b
}

func randKeys() [][]byte {
	var keys [][]byte
	var count = rand.Intn(2) + 2
	for i := 0; i < count; i++ {
		keys = append(keys, randKey())
	}
	return keys
}

func randValue() []byte {
	n := rand.Intn(8192)
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(255))
	}
	return b
}
