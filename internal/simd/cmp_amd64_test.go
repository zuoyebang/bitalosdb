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

//go:build amd64

package simd

import (
	"testing"
	"unsafe"
)

func TestEqual(t *testing.T) {
	t.Run("Equal", func(t *testing.T) {
		a := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		b := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		if !Equal128(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])) {
			t.Errorf("Expected true, got false")
		}
	})

	t.Run("NotEqual", func(t *testing.T) {
		a := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		b := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17}
		if Equal128(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])) {
			t.Errorf("Expected true, got false")
		}
	})
}

// 测试数据准备
var (
	equalX    = [16]byte{1: 1, 15: 1} // 首尾不同
	equalY    = [16]byte{1: 1, 15: 1}
	notEqualY = [16]byte{1: 1, 15: 2}
)

// 防止编译器优化
var globalBool bool

func BenchmarkEqualSIMD(b *testing.B) {
	cases := []struct {
		name string
		y    *[16]byte
	}{
		{"Equal", &equalY},
		{"NotEqual", &notEqualY},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// 交叉测试防止分支预测优化
				if i%2 == 0 {
					globalBool = Equal128(unsafe.Pointer(&equalX), unsafe.Pointer(c.y))
				} else {
					globalBool = Equal128(unsafe.Pointer(c.y), unsafe.Pointer(&equalX))
				}
			}
		})
	}
}

func BenchmarkEqualNormal(b *testing.B) {
	normalEqual := func(a, b []byte) bool {
		return string(a) == string(b)
	}

	cases := []struct {
		name string
		y    *[16]byte
	}{
		{"Equal", &equalY},
		{"NotEqual", &notEqualY},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if i%2 == 0 {
					globalBool = normalEqual(equalX[:], c.y[:])
				} else {
					globalBool = normalEqual(c.y[:], equalX[:])
				}
			}
		})
	}
}
