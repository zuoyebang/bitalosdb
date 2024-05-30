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

package rawalloc

import (
	"fmt"
	"testing"
)

var sizes = []int{16, 100, 1024, 1024 * 10, 1024 * 100, 1024 * 1024}

func BenchmarkRawalloc(b *testing.B) {
	for _, size := range sizes {
		b.Run(fmt.Sprintf("rawalloc-%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = New(size, size)
			}
		})
	}
}

func BenchmarkMake(b *testing.B) {
	for _, size := range sizes {
		b.Run(fmt.Sprintf("make-%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = make([]byte, size)
			}
		})
	}
}
