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

//go:build noasm || !amd64

package simd

import "unsafe"

func InitMem128(a, b unsafe.Pointer, n int) {
	sa := (*[1 << 30]byte)(a)[:]
	sb := (*[1 << 30]byte)(b)[:]
	for i := 0; i < n; i++ {
		copy(sa[i<<4:i<<4+16], sb)
	}
}
