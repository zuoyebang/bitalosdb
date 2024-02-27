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

package bdb_test

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"testing/quick"
	"time"
)

var qcount, qseed, qmaxitems, qmaxksize, qmaxvsize int

func TestMain(m *testing.M) {
	flag.IntVar(&qcount, "quick.count", 5, "")
	flag.IntVar(&qseed, "quick.seed", int(time.Now().UnixNano())%100000, "")
	flag.IntVar(&qmaxitems, "quick.maxitems", 1000, "")
	flag.IntVar(&qmaxksize, "quick.maxksize", 1024, "")
	flag.IntVar(&qmaxvsize, "quick.maxvsize", 1024, "")
	flag.Parse()
	fmt.Fprintln(os.Stderr, "seed:", qseed)
	fmt.Fprintf(os.Stderr, "quick settings: count=%v, items=%v, ksize=%v, vsize=%v\n", qcount, qmaxitems, qmaxksize, qmaxvsize)

	os.Exit(m.Run())
}

func qconfig() *quick.Config {
	return &quick.Config{
		MaxCount: qcount,
		Rand:     rand.New(rand.NewSource(int64(qseed))),
	}
}

type testdata []testdataitem

func (t testdata) Len() int           { return len(t) }
func (t testdata) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t testdata) Less(i, j int) bool { return bytes.Compare(t[i].Key, t[j].Key) == -1 }

func (t testdata) Generate(rand *rand.Rand, size int) reflect.Value {
	n := rand.Intn(qmaxitems-1) + 1
	items := make(testdata, n)
	used := make(map[string]bool)
	for i := 0; i < n; i++ {
		item := &items[i]
		for {
			item.Key = randByteSlice(rand, 1, qmaxksize)
			if !used[string(item.Key)] {
				used[string(item.Key)] = true
				break
			}
		}
		item.Value = randByteSlice(rand, 0, qmaxvsize)
	}
	return reflect.ValueOf(items)
}

type revtestdata []testdataitem

func (t revtestdata) Len() int           { return len(t) }
func (t revtestdata) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t revtestdata) Less(i, j int) bool { return bytes.Compare(t[i].Key, t[j].Key) == 1 }

type testdataitem struct {
	Key   []byte
	Value []byte
}

func randByteSlice(rand *rand.Rand, minSize, maxSize int) []byte {
	n := rand.Intn(maxSize-minSize) + minSize
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(255))
	}
	return b
}
