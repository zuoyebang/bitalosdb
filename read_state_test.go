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
	"fmt"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/vfs"
	"golang.org/x/exp/rand"
)

func BenchmarkReadState(b *testing.B) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		b.Fatal(err)
	}

	for _, updateFrac := range []float32{0, 0.1, 0.5} {
		b.Run(fmt.Sprintf("updates=%.0f", updateFrac*100), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

				for pb.Next() {
					if rng.Float32() < updateFrac {
						d.mu.Lock()
						d.updateReadStateLocked()
						d.mu.Unlock()
					} else {
						s := d.loadReadState()
						s.unref()
					}
				}
			})
		})
	}

	if err := d.Close(); err != nil {
		b.Fatal(err)
	}
}
