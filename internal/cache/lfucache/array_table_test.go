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

package lfucache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestArrayTable_Set(t *testing.T) {
	num := 100
	size := 52 * num
	at := newArrayTable(1, size)
	for i := 0; i < num; i++ {
		key := []byte(fmt.Sprintf("cacheTestAtKey_%d", i))
		value := []byte(fmt.Sprintf("cacheTestAtValue_%d", i))
		if err := at.add(key, value); err != nil {
			t.Fatalf("add err key:%s err:%v", string(key), err)
		}
	}

	require.Equal(t, num, at.count())

	for i := 0; i < num; i++ {
		expKey := []byte(fmt.Sprintf("cacheTestAtKey_%d", i))
		expValue := []byte(fmt.Sprintf("cacheTestAtValue_%d", i))
		key, vaule := at.getKV(i)
		require.Equal(t, expKey, key)
		require.Equal(t, expValue, vaule)
	}
}

func TestArrayTable_VS_MAP(t *testing.T) {
	var buf [4]byte
	num := uint32(1 << 20)
	size := arrayTableEntrySize(4, 4) * int(num)
	at := newArrayTable(1, size)

	bt := time.Now()
	i := uint32(0)
	for ; i < num; i++ {
		binary.BigEndian.PutUint32(buf[:], i)
		if err := at.add(buf[:], buf[:]); err != nil {
			t.Fatalf("add err key:%s err:%v", buf, err)
		}
	}
	et := time.Since(bt)
	fmt.Printf("arrtable add(include crc32, if annotate crc32, more faster then hashmap) time cost = %v\n", et)

	bt = time.Now()
	i = uint32(0)
	for ; i < num; i++ {
		binary.BigEndian.PutUint32(buf[:], i)
		val, _, _ := at.get(buf[:])
		if !bytes.Equal(buf[:], val[:]) {
			t.Fatalf("arrtable get err key:%s\n", buf)
		}
	}
	et = time.Since(bt)
	fmt.Printf("arrtable get time cost = %v\n", et)

	bt = time.Now()
	i = uint32(0)
	for ; i < num; i++ {
		binary.BigEndian.PutUint32(buf[:], i)
		iter := at.newIter(nil)
		_, iterValue := iter.SeekGE(buf[:])
		if !bytes.Equal(buf[:], iterValue[:]) {
			t.Fatalf("arrtable use-iter-ßget err key:%s\n", buf)
		}
		iter.Close()
	}
	et = time.Since(bt)
	fmt.Printf("arrtable use-iter-get time cost = %v\n", et)

	hashmap := make(map[uint32]uint32, 1<<10)
	bt = time.Now()
	i = uint32(0)
	for ; i < num; i++ {
		binary.BigEndian.PutUint32(buf[:], i)
		hashmap[i] = i
	}
	et = time.Since(bt)
	fmt.Printf("hashmap add time cost = %v\n", et)

	bt = time.Now()
	i = uint32(0)
	for ; i < num; i++ {
		binary.BigEndian.PutUint32(buf[:], i)
		if _, ok := hashmap[i]; !ok {
			t.Fatalf("hashmap get err key:%s\n", buf)
		}
	}
	et = time.Since(bt)
	fmt.Printf("hashmap get time cost = %v\n", et)
}

type keySlice [][]byte

func (x keySlice) Len() int           { return len(x) }
func (x keySlice) Less(i, j int) bool { return bytes.Compare(x[i], x[j]) == -1 }
func (x keySlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func TestArrayTable_Iter(t *testing.T) {
	num := 100000
	size := 52 * num
	at := newArrayTable(1, size)
	keys := make(keySlice, num, num)
	for i := 0; i < num; i++ {
		keys[i] = []byte(fmt.Sprintf("cacheTestAtKey_%d", i))
	}

	sort.Sort(keys)
	for i := 0; i < num; i++ {
		key := keys[i]
		if err := at.add(key, key); err != nil {
			t.Fatalf("add err key:%s err:%v", string(key), err)
		}
	}

	var bytesFlushed *uint64
	iter := at.newFlushIter(nil, bytesFlushed)
	key, vaule := iter.First()
	for i := 0; i < num; i++ {
		expKey := keys[i]
		require.Equal(t, expKey, key.UserKey)
		require.Equal(t, expKey, vaule)
		key, vaule = iter.Next()
	}
	require.NoError(t, iter.Close())

	seekExist := func(skey []byte) {
		ik, v := at.seek(skey)
		require.Equal(t, skey, ik.UserKey)
		require.Equal(t, skey, v)
	}
	seekNotExist := func(skey []byte) {
		ik, _ := at.seek(skey)
		require.Equal(t, (*internalKey)(nil), ik)
	}

	for _, v := range []int{7, 71, 711} {
		seekExist([]byte(fmt.Sprintf("cacheTestAtKey_%d", v)))
	}
	for _, v := range []int{100007, 100071, 100711} {
		seekNotExist([]byte(fmt.Sprintf("cacheTestAtKey_%d", v)))
	}
}
