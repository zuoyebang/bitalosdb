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

package bitpage

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

const (
	letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func pMem() {
	var memStats runtime.MemStats

	runtime.ReadMemStats(&memStats)

	fmt.Printf("Allocated memory: %dMB\n", memStats.Alloc/1024/1024)
	fmt.Printf("Total memory allocated: %dMB\n", memStats.TotalAlloc/1024/1024)
	fmt.Printf("Memory obtained from OS: %dMB\n", memStats.Sys/1024/1024)
}

func TestBitrie_String(t *testing.T) {
	os.Remove("bitrie.db")

	trie := NewBitrie()
	trie.InitWriter()

	totalNum := 1<<20 + 128
	keylist := make([][]byte, totalNum)
	valuelist := make([][]byte, totalNum)
	gomap := make(map[string][]byte, totalNum)

	for i := 0; i < totalNum; i++ {
		key := fmt.Sprintf("sorted_key_prefix_%s_bitalosdb_%s_%d", randBytes(1), randBytes(16), i)
		value := []byte(fmt.Sprintf("value_%d", i))
		keylist[i] = []byte(key)
		valuelist[i] = value
	}

	pMem()

	for i := 0; i < totalNum; i++ {
		gomap[string(keylist[i])] = valuelist[i]
	}

	pMem()

	bt := time.Now()
	for i := 0; i < totalNum; i++ {
		if v, ok := gomap[string(keylist[i])]; !ok || !bytes.Equal(v, valuelist[i]) {
			fmt.Printf("get map i=%d not exist or v=%s error\n", i, v)
		}
	}
	et := time.Since(bt)
	fmt.Printf("gomap get time cost = %v\n", et)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		trie.Add(keylist[i], valuelist[i])
	}
	et = time.Since(bt)
	fmt.Printf("build trie-index time cost = %v; item-count = %d\n", et, trie.length)

	tbl, _ := openTable("bitrie.db", defaultTableOptions)
	defer func() {
		os.Remove("bitrie.db")
	}()

	tblalloc := func(size uint32) uint32 {
		offset, _ := tbl.alloc(size)
		return offset
	}

	tblbytes := func(offset uint32, size uint32) []byte {
		return tbl.getBytes(offset, size)
	}

	tblsize := func() uint32 {
		return tbl.Size()
	}

	bt = time.Now()
	trie.Serialize(tblalloc, tblbytes, tblsize)
	et = time.Since(bt)
	fmt.Printf("flush trie-index time cost = %v, tbl-size=%d\n", et, tbl.Size())

	trie.SetReader(tblbytes(0, tbl.Size()), tableDataOffset)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		if v, ok := trie.Get(keylist[i]); !ok || !bytes.Equal(v, valuelist[i]) {
			fmt.Printf("get trie-index i=%d not exist or v=%s != %s\n", i, v, valuelist[i])
		}
	}
	et = time.Since(bt)
	fmt.Printf("trie-index get time cost = %v\n", et)
	trie.Finish()
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}
