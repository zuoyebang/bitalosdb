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

//go:build tracing
// +build tracing

package lrucache

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
)

type refcnt struct {
	val int32
	sync.Mutex
	msgs []string
}

func (v *refcnt) init(val int32) {
	v.val = val
	v.trace("init")
}

func (v *refcnt) refs() int32 {
	return atomic.LoadInt32(&v.val)
}

func (v *refcnt) acquire() {
	switch n := atomic.AddInt32(&v.val, 1); {
	case n <= 1:
		panic(fmt.Sprintf("cache: inconsistent reference count: %d", n))
	}
	v.trace("acquire")
}

func (v *refcnt) release() bool {
	n := atomic.AddInt32(&v.val, -1)
	switch {
	case n < 0:
		panic(fmt.Sprintf("cache: inconsistent reference count: %d", n))
	}
	v.trace("release")
	return n == 0
}

func (v *refcnt) trace(msg string) {
	s := fmt.Sprintf("%s: refs=%d\n%s", msg, v.refs(), debug.Stack())
	v.Lock()
	v.msgs = append(v.msgs, s)
	v.Unlock()
}

func (v *refcnt) traces() string {
	v.Lock()
	s := strings.Join(v.msgs, "\n")
	v.Unlock()
	return s
}
