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

package lfucache

import (
	"fmt"
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/humanize"
)

func TestCacheMergingIter(t *testing.T) {
	memSize := 40 << 10
	maxSize := uint64(10 << 20)
	mc := testNewCache()
	c := newCache(mc, 0, memSize, maxSize)
	num := 1000
	for i := 0; i < num; i++ {
		key := []byte(fmt.Sprintf("cacheTestKey_%d", i))
		value := []byte(fmt.Sprintf("cacheTestValue_%d", i))
		c.set(key, value)
	}

	for i := 0; i < num; i++ {
		if i%2 == 0 {
			key := []byte(fmt.Sprintf("cacheTestKey_%d", i))
			c.delete(key)
		}
	}

	var bytesIterated uint64
	memNum := len(c.mu.memQueue)
	fmt.Println("memNum", memNum)
	flushing := c.mu.memQueue[:memNum-1]

	iter := newInputIter(flushing, &bytesIterated)
	i := 0
	for key, _ := iter.First(); key != nil; key, _ = iter.Next() {
		fmt.Println(key.String())
		i++
	}
	fmt.Println("end", i, humanize.Uint64(bytesIterated))
}
