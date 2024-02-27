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

package fastrand

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/rand"
)

type defaultRand struct {
	mu  sync.Mutex
	src rand.PCGSource
}

func newDefaultRand() *defaultRand {
	r := &defaultRand{}
	r.src.Seed(uint64(time.Now().UnixNano()))
	return r
}

func (r *defaultRand) Uint32() uint32 {
	r.mu.Lock()
	i := uint32(r.src.Uint64())
	r.mu.Unlock()
	return i
}

func BenchmarkFastRand(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Uint32()
		}
	})
}

func BenchmarkDefaultRand(b *testing.B) {
	r := newDefaultRand()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Uint32()
		}
	})
}

var xg uint32

func BenchmarkSTFastRand(b *testing.B) {
	var x uint32
	for i := 0; i < b.N; i++ {
		x = Uint32n(2097152)
	}
	xg = x
}

func BenchmarkSTDefaultRand(b *testing.B) {
	for _, newPeriod := range []int{0, 10, 100, 1000} {
		name := "no-new"
		if newPeriod > 0 {
			name = fmt.Sprintf("new-period=%d", newPeriod)
		}
		b.Run(name, func(b *testing.B) {
			r := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
			b.ResetTimer()
			var x uint32
			for i := 0; i < b.N; i++ {
				if newPeriod > 0 && i%newPeriod == 0 {
					r = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
				}
				x = uint32(r.Uint64n(2097152))
			}
			xg = x
		})
	}
}
