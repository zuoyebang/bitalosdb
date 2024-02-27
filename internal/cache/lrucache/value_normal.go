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

//go:build (!invariants && !tracing) || race
// +build !invariants,!tracing race

package lrucache

import (
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/manual"
)

const valueSize = int(unsafe.Sizeof(Value{}))

func newValue(n int) *Value {
	if n == 0 {
		return nil
	}

	if !cgoEnabled {
		v := &Value{buf: make([]byte, n)}
		v.ref.init(1)
		return v
	}

	b := manual.New(valueSize + n)
	v := (*Value)(unsafe.Pointer(&b[0]))
	v.buf = b[valueSize:]
	v.ref.init(1)
	return v
}

func (v *Value) free() {
	if !cgoEnabled {
		return
	}

	n := valueSize + cap(v.buf)
	buf := (*[manual.MaxArrayLen]byte)(unsafe.Pointer(v))[:n:n]
	v.buf = nil
	manual.Free(buf)
}
