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
	"os"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

func TestBitrie_String(t *testing.T) {
	os.Remove("bitrie.db")

	trie := NewBitrie()
	trie.InitWriter()

	totalNum := 1<<20 + 128
	keylist := make([][]byte, totalNum)
	valuelist := make([][]byte, totalNum)
	gomap := make(map[string][]byte, totalNum)

	for i := 0; i < totalNum; i++ {
		key := fmt.Sprintf("key_prefix_%s_bitalosdb_%s_%d", utils.FuncRandBytes(1), utils.FuncRandBytes(8), i)
		value := []byte(fmt.Sprintf("value_%d", i))
		keylist[i] = []byte(key)
		valuelist[i] = value
	}

	bt := time.Now()
	for i := 0; i < totalNum; i++ {
		gomap[string(keylist[i])] = valuelist[i]
	}
	et := time.Since(bt)
	fmt.Printf("gomap add time cost = %v\n", et)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		if v, ok := gomap[string(keylist[i])]; !ok || !bytes.Equal(v, valuelist[i]) {
			fmt.Printf("get map i=%d not exist or v=%s error\n", i, v)
		}
	}
	et = time.Since(bt)
	fmt.Printf("gomap get time cost = %v\n", et)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		trie.Add(keylist[i], valuelist[i])
	}
	et = time.Since(bt)
	fmt.Printf("build trie-index time cost = %v; item-count = %d\n", et, trie.length)

	tbl, _ := OpenTable("bitrie.db", defaultTableOptions)
	defer func() {
		os.Remove("bitrie.db")
	}()

	tblalloc := func(size uint32) uint32 {
		offset, _ := tbl.alloc(size)
		return offset
	}

	tblbytes := func(offset uint32, size uint32) []byte {
		return tbl.GetBytes(offset, size)
	}

	tblsize := func() uint32 {
		return tbl.Size()
	}

	bt = time.Now()
	trie.Serialize(tblalloc, tblbytes, tblsize)
	et = time.Since(bt)
	fmt.Printf("flush trie-index time cost = %v, tbl-size=%dMB\n", et, tbl.Size()/1024/1024)

	trie.SetReader(tblbytes(0, tbl.Size()), TblDataOffset)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		if v, ok := trie.Get(keylist[i]); !ok || !bytes.Equal(v, valuelist[i]) {
			fmt.Printf("trie-index get i=%d not exist or v=%s != %s\n", i, v, valuelist[i])
		}
	}
	et = time.Since(bt)
	fmt.Printf("trie-index get time cost = %v\n", et)
	trie.Finish()
}

func TestBitrieV_String(t *testing.T) {
	os.Remove("bitrie.db")

	trie := NewBitriev()
	trie.InitWriter()

	totalNum := 1<<20 + 128
	keylist := make([][]byte, totalNum)
	valuelist := make([]uint32, totalNum)
	gomap := make(map[string]uint32, totalNum)

	for i := 0; i < totalNum; i++ {
		key := fmt.Sprintf("key_prefix_%s_bitalosdb_%s_%d", utils.FuncRandBytes(1), utils.FuncRandBytes(8), i)
		keylist[i] = []byte(key)
		valuelist[i] = uint32(i + 1)
	}

	bt := time.Now()
	for i := 0; i < totalNum; i++ {
		gomap[string(keylist[i])] = valuelist[i]
	}
	et := time.Since(bt)
	fmt.Printf("gomap add time cost = %v\n", et)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		if v, ok := gomap[string(keylist[i])]; !ok || v != valuelist[i] {
			fmt.Printf("get map i=%d not exist or v=%d error\n", i, v)
		}
	}
	et = time.Since(bt)
	fmt.Printf("gomap get time cost = %v\n", et)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		trie.Add(keylist[i], valuelist[i])
	}
	et = time.Since(bt)
	fmt.Printf("build trie-index time cost = %v; item-count = %d\n", et, trie.length)

	tbl, _ := OpenTable("bitrie.db", defaultTableOptions)
	defer func() {
		os.Remove("bitrie.db")
	}()

	tblalloc := func(size uint32) uint32 {
		offset, _ := tbl.alloc(size)
		return offset
	}

	tblbytes := func(offset uint32, size uint32) []byte {
		return tbl.GetBytes(offset, size)
	}

	tblsize := func() uint32 {
		return tbl.Size()
	}

	bt = time.Now()
	trie.Serialize(tblalloc, tblbytes, tblsize)
	et = time.Since(bt)
	fmt.Printf("flush trie-index time cost = %v, tbl-size=%dMB\n", et, tbl.Size()/1024/1024)

	trie.SetReader(tblbytes(0, tbl.Size()), TblDataOffset)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		if v, ok := trie.Get(keylist[i]); !ok || v != valuelist[i] {
			fmt.Printf("trie-index get i=%d not exist or v=%d != %d\n", i, v, valuelist[i])
		}
	}
	et = time.Since(bt)
	fmt.Printf("trie-index get time cost = %v\n", et)
	trie.Finish()
}

func TestFindNode(t *testing.T) {
	buf := []byte{'b', 'c', 'e', 'j', 'r', 's', 'u', 'x', 'y'}

	test := []byte{'a', 'c', 'd', 't', 'm', 'z'}

	for i := 0; i < len(test); i++ {
		ok, pos := findNode(test[i], buf, uint32(len(buf)))
		if pos != 4<<30-1 {
			fmt.Printf("ok=%v, pos=%d, value=%c\n", ok, pos, buf[pos])
		} else {
			fmt.Printf("ok=%v, pos=%d, not find\n", ok, pos)
		}

	}
}

func findNode(key uint8, buf []byte, n uint32) (bool, uint32) {
	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if buf[h] < key {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < n {
		if buf[i] == key {
			return true, i
		} else {
			return false, i
		}
	}

	return false, 4<<30 - 1
}
