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

//go:build (invariants && !race) || (tracing && !race)
// +build invariants,!race tracing,!race

package lrucache

import (
	"fmt"
	"os"

	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/manual"
)

func newValue(n int) *Value {
	if n == 0 {
		return nil
	}
	b := manual.New(n)
	v := &Value{buf: b}
	v.ref.init(1)
	invariants.SetFinalizer(v, func(obj interface{}) {
		v := obj.(*Value)
		if v.buf != nil {
			fmt.Fprintf(os.Stderr, "%p: cache value was not freed: refs=%d\n%s",
				v, v.refs(), v.ref.traces())
			os.Exit(1)
		}
	})
	return v
}

func (v *Value) free() {
	for i := range v.buf {
		v.buf[i] = 0xff
	}
	manual.Free(v.buf)
	v.buf = nil
}
