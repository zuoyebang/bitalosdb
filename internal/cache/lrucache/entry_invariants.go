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

//go:build (invariants && !race) || (tracing && !race)
// +build invariants,!race tracing,!race

package lrucache

import (
	"fmt"
	"os"

	"github.com/zuoyebang/bitalosdb/internal/invariants"
)

const entriesGoAllocated = true

func entryAllocNew() *entry {
	e := &entry{}
	invariants.SetFinalizer(e, func(obj interface{}) {
		e := obj.(*entry)
		if v := e.ref.refs(); v != 0 {
			fmt.Fprintf(os.Stderr, "%p: cache entry has non-zero reference count: %d\n%s",
				e, v, e.ref.traces())
			os.Exit(1)
		}
	})
	return e
}

func entryAllocFree(e *entry) {
}
