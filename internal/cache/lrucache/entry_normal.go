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
	"runtime"
	"sync"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/invariants"
	"github.com/zuoyebang/bitalosdb/internal/manual"
)

const (
	entrySize            = int(unsafe.Sizeof(entry{}))
	entryAllocCacheLimit = 128
	entriesGoAllocated   = invariants.RaceEnabled || !cgoEnabled
)

var entryAllocPool = sync.Pool{
	New: func() interface{} {
		return newEntryAllocCache()
	},
}

func entryAllocNew() *entry {
	a := entryAllocPool.Get().(*entryAllocCache)
	e := a.alloc()
	entryAllocPool.Put(a)
	return e
}

func entryAllocFree(e *entry) {
	a := entryAllocPool.Get().(*entryAllocCache)
	a.free(e)
	entryAllocPool.Put(a)
}

type entryAllocCache struct {
	entries []*entry
}

func newEntryAllocCache() *entryAllocCache {
	c := &entryAllocCache{}
	if !entriesGoAllocated {
		runtime.SetFinalizer(c, freeEntryAllocCache)
	}
	return c
}

func freeEntryAllocCache(obj interface{}) {
	c := obj.(*entryAllocCache)
	for i, e := range c.entries {
		c.dealloc(e)
		c.entries[i] = nil
	}
}

func (c *entryAllocCache) alloc() *entry {
	n := len(c.entries)
	if n == 0 {
		if entriesGoAllocated {
			return &entry{}
		}
		b := manual.New(entrySize)
		return (*entry)(unsafe.Pointer(&b[0]))
	}
	e := c.entries[n-1]
	c.entries = c.entries[:n-1]
	return e
}

func (c *entryAllocCache) dealloc(e *entry) {
	if !entriesGoAllocated {
		buf := (*[manual.MaxArrayLen]byte)(unsafe.Pointer(e))[:entrySize:entrySize]
		manual.Free(buf)
	}
}

func (c *entryAllocCache) free(e *entry) {
	if len(c.entries) == entryAllocCacheLimit {
		c.dealloc(e)
		return
	}
	c.entries = append(c.entries, e)
}
