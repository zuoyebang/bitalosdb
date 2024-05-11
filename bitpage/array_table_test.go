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

package bitpage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/bindex"
	"github.com/zuoyebang/bitalosdb/internal/cache/lrucache"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
)

func testNewBlockCache() *lrucache.LruCache {
	cacheOpts := &options.CacheOptions{
		Size:     consts.BitpageDefaultBlockCacheSize,
		Shards:   consts.BitpageBlockCacheShards,
		HashSize: consts.BitpageBlockCacheHashSize,
	}
	cache := lrucache.NewLrucache(cacheOpts)

	return cache
}

func testNewArrayTable(dir string, params []bool) (*arrayTable, *atOptions) {
	atCacheOpts := &atCacheOptions{
		cache: testNewBlockCache(),
		id:    1 << 10,
	}
	opts := &atOptions{
		useMapIndex:       params[0],
		usePrefixCompress: params[1],
		useBlockCompress:  params[2],
	}
	at, err := newArrayTable(dir, opts, atCacheOpts)
	if err != nil {
		panic(err)
	}
	return at, opts
}

func testOpenArrayTable(dir string) *arrayTable {
	atCacheOpts := &atCacheOptions{
		cache: testNewBlockCache(),
		id:    1 << 10,
	}
	at, err := openArrayTable(dir, atCacheOpts)
	if err != nil {
		panic(err)
	}
	return at
}

func TestArrayTable_Stmap(t *testing.T) {
	os.Remove(testPath)
	count := 100
	var keys [][]byte
	for i := 0; i < count; i++ {
		keys = append(keys, []byte(fmt.Sprintf("test_at_key_%d", i)))
	}

	hindex := bindex.NewHashIndex(true)
	hindex.InitWriter()
	for i := range keys {
		key := keys[i]
		khash := hash.Crc32(key)
		hindex.Add(khash, uint32(i+1))
	}

	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()

	fmt.Println("stmap.Size()", hindex.Size())
	mapOffset, err := tbl.alloc(hindex.Size())
	if err != nil {
		t.Fatal(err)
	}

	hindex.SetWriter(tbl.data[mapOffset:])
	if !hindex.Serialize() {
		t.Fatal(errors.New("hash_index Serialize Fail"))
	}

	hindex.Finish()

	for i := range keys {
		key := keys[i]
		khash := hash.Crc32(key)
		v, ok := hindex.Get32(khash)
		if ok != true || v != uint32(i+1) {
			t.Fatalf("Get---key:%s, ok:%#v, expact-val:%d, real-val:%d\n", string(key), ok, uint32(i+1), v)
		}
	}
}

func TestArrayTable_WriteRead(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testPath
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		count := 10000
		kvList := testMakeSortedKV(count, uint64(1), 100)

		at, atOpts := testNewArrayTable(dir, params)
		atVersion := getAtVersionByOpts(atOpts)
		require.Equal(t, atVersion, at.header.version)

		for i := 0; i < count; i++ {
			_, err := at.writeItem(kvList[i].Key.UserKey, kvList[i].Value)
			require.NoError(t, err)
		}
		require.NoError(t, at.writeFinish())
		require.Equal(t, at.tbl.fileStatSize(), int64(at.size))
		fmt.Println(index, atVersion, at.size)

		seekFunc := func(a *arrayTable) {
			iter := a.newIter(nil)
			i := 0
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, key.UserKey)
				require.Equal(t, kvList[i].Value, val)
				i++
			}
			require.NoError(t, iter.Close())
			require.Equal(t, count, i)

			for j := 0; j < count; j++ {
				skey := kvList[j].Key.UserKey
				val, exist, _, closer := a.get(skey, hash.Crc32(skey))
				require.Equal(t, true, exist)
				require.Equal(t, kvList[j].Value, val)
				if closer != nil {
					closer()
				}

				it1 := a.newIter(nil)
				ik, v := it1.SeekGE(skey)
				require.Equal(t, skey, ik.UserKey)
				require.Equal(t, kvList[j].Value, v)
				require.NoError(t, it1.Close())
			}

			for _, n := range []int{99999, 90001} {
				skey := sortedkv.MakeSortedKey(n)
				_, exist, _, closer := a.get(skey, hash.Crc32(skey))
				require.Equal(t, false, exist)
				if closer != nil {
					closer()
				}
			}
		}

		seekFunc(at)
		require.NoError(t, at.close())

		at1 := testOpenArrayTable(dir)
		require.Equal(t, atVersion, at1.header.version)
		seekFunc(at1)
		require.NoError(t, at1.close())
	})
}

func TestArrayTable_IterCompact(t *testing.T) {
	for _, params := range [][]bool{
		{true, true, true},
		{true, false, true},
	} {
		func(param []bool) {
			dir := testPath
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			count := 10000
			kvList := testMakeSortedKV(count, uint64(1), 10)

			at, atOpts := testNewArrayTable(dir, param)
			atVersion := getAtVersionByOpts(atOpts)
			require.Equal(t, atVersion, at.header.version)

			for i := 0; i < count; i++ {
				_, err := at.writeItem(kvList[i].Key.UserKey, kvList[i].Value)
				require.NoError(t, err)
			}
			require.NoError(t, at.writeFinish())

			require.Equal(t, at.tbl.fileStatSize(), int64(at.size))
			require.Equal(t, int64(0), at.blockCache.Size())

			seekFunc := func(a *arrayTable, opts *iterOptions) {
				iter := a.newIter(opts)
				i := 0
				for key, val := iter.First(); key != nil; key, val = iter.Next() {
					require.Equal(t, kvList[i].Key.UserKey, key.UserKey)
					require.Equal(t, kvList[i].Value, val)
					i++
				}
				require.NoError(t, iter.Close())
				require.Equal(t, count, i)

				for j := 0; j < count; j++ {
					skey := kvList[j].Key.UserKey
					it1 := a.newIter(opts)
					ik, v := it1.SeekGE(skey)
					require.Equal(t, skey, ik.UserKey)
					require.Equal(t, kvList[j].Value, v)
					require.NoError(t, it1.Close())
				}

				for _, n := range []int{99999, 90001} {
					skey := sortedkv.MakeSortedKey(n)
					_, exist, _, closer := a.get(skey, hash.Crc32(skey))
					require.Equal(t, false, exist)
					require.Equal(t, true, closer == nil)
				}
			}

			seekFunc(at, &iterCompactOpts)
			require.Equal(t, int64(1), at.blockCache.Metrics().Count)
			seekFunc(at, nil)
			require.Equal(t, int64(250), at.blockCache.Metrics().Count)
			require.NoError(t, at.close())
		}(params)
	}
}

func TestArrayTable_IterSeek(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testPath
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		at, atOpts := testNewArrayTable(dir, params)
		atVersion := getAtVersionByOpts(atOpts)
		count := 120
		kvList := testMakeSortedKV(count, uint64(1), 200)

		for i := 0; i < 100; i++ {
			_, err := at.writeItem(kvList[i].Key.UserKey, kvList[i].Value)
			require.NoError(t, err)
		}
		require.NoError(t, at.writeFinish())

		checkKV := func(i int, ik *internalKey, iv []byte) {
			if i == -1 {
				require.Equal(t, nilInternalKey, ik)
			} else {
				require.Equal(t, kvList[i].Key.UserKey, ik.UserKey)
				require.Equal(t, kvList[i].Value, iv)
			}
		}

		seekFunc := func(a *arrayTable) {
			iter := a.newIter(nil)
			ik, iv := iter.First()
			checkKV(0, ik, iv)

			ik, iv = iter.Next()
			checkKV(1, ik, iv)

			ik, iv = iter.Prev()
			checkKV(0, ik, iv)

			ik, iv = iter.Last()
			checkKV(99, ik, iv)

			ik, iv = iter.SeekGE(kvList[15].Key.UserKey)
			checkKV(15, ik, iv)
			ik, iv = iter.SeekGE(kvList[51].Key.UserKey)
			checkKV(51, ik, iv)
			ik, iv = iter.SeekGE(sortedkv.MakeSortedKey(-1))
			checkKV(0, ik, iv)
			ik, iv = iter.SeekGE(kvList[110].Key.UserKey)
			checkKV(-1, ik, iv)

			ik, iv = iter.SeekLT(kvList[15].Key.UserKey)
			checkKV(14, ik, iv)
			ik, iv = iter.SeekLT(kvList[51].Key.UserKey)
			checkKV(50, ik, iv)
			ik, iv = iter.Prev()
			checkKV(49, ik, iv)
			ik, iv = iter.SeekLT(kvList[1].Key.UserKey)
			checkKV(0, ik, iv)
			ik, iv = iter.SeekLT(kvList[110].Key.UserKey)
			checkKV(99, ik, iv)
			ik, iv = iter.SeekLT(kvList[0].Key.UserKey)
			checkKV(-1, ik, iv)
			ik, iv = iter.SeekLT(sortedkv.MakeSortedKey(-1))
			checkKV(-1, ik, iv)

			require.NoError(t, iter.Close())
		}

		seekFunc(at)
		require.NoError(t, at.close())

		at1 := testOpenArrayTable(dir)
		require.Equal(t, atVersion, at1.header.version)
		seekFunc(at1)
		require.NoError(t, at1.close())
	})
}

func TestArrayTable_IterRange(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testPath
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		at, atOpts := testNewArrayTable(dir, params)
		atVersion := getAtVersionByOpts(atOpts)
		count := 10000
		kvList := testMakeSortedKV(count+1, uint64(1), 100)

		for i := 0; i < count; i++ {
			_, err := at.writeItem(kvList[i].Key.UserKey, kvList[i].Value)
			require.NoError(t, err)
		}
		require.NoError(t, at.writeFinish())

		rangeIter := func(a *arrayTable) {
			iter := a.newIter(nil)
			i := 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, ik.UserKey)
				require.Equal(t, kvList[i].Value, iv)
				i++
			}
			require.Equal(t, i, count)
			require.NoError(t, iter.Close())

			iter = a.newIter(nil)
			for i = 0; i < count; i++ {
				ik, iv := iter.SeekGE(kvList[i].Key.UserKey)
				require.Equal(t, kvList[i].Key.UserKey, ik.UserKey)
				require.Equal(t, kvList[i].Value, iv)
			}
			require.NoError(t, iter.Close())
		}

		rangeReverseIter := func(a *arrayTable) {
			iter := a.newIter(nil)
			i := count - 1
			for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
				require.Equal(t, kvList[i].Key.UserKey, ik.UserKey)
				require.Equal(t, kvList[i].Value, iv)
				i--
			}
			require.Equal(t, -1, i)
			require.NoError(t, iter.Close())

			iter = a.newIter(nil)
			for i = count; i >= 0; i-- {
				ik, iv := iter.SeekLT(kvList[i].Key.UserKey)
				if i == 0 {
					require.Equal(t, nilInternalKey, ik)
					require.Equal(t, []byte(nil), iv)
				} else {
					require.Equal(t, kvList[i-1].Key.UserKey, ik.UserKey)
					require.Equal(t, kvList[i-1].Value, iv)
				}
			}
			require.NoError(t, iter.Close())
		}

		rangeIter(at)
		rangeReverseIter(at)
		require.NoError(t, at.close())

		at1 := testOpenArrayTable(dir)
		require.Equal(t, atVersion, at1.header.version)
		rangeIter(at1)
		rangeReverseIter(at1)
		require.NoError(t, at1.close())
	})
}

func TestArrayTable_Empty(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testPath
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		at, _ := testNewArrayTable(dir, params)
		require.Equal(t, true, at.empty())

		count := 1
		kvList := testMakeSortedKV(count, uint64(1), 10)
		for i := 0; i < count; i++ {
			_, err := at.writeItem(kvList[i].Key.UserKey, kvList[i].Value)
			require.NoError(t, err)
		}
		require.NoError(t, at.writeFinish())
		require.Equal(t, false, at.empty())
		require.NoError(t, at.close())
	})
}

func Test_SharedPage_Perf(t *testing.T) {
	os.Remove(testPath)

	count := 1 << 20
	kvList := testMakeSortedKV(count, uint64(1), 16)

	bt := time.Now()
	for _, useMapIndex := range []bool{false, true} {
		fmt.Printf("Test_SharedPage_Perf, useMapIndex=%#v\n", useMapIndex)
		atCacheOpts := atCacheOptions{
			cache: testNewBlockCache(),
			id:    1 << 10,
		}
		opts := &atOptions{
			useMapIndex:       useMapIndex,
			usePrefixCompress: false,
		}
		at, err := newArrayTable(testPath, opts, &atCacheOpts)
		require.NoError(t, err)

		bt1 := time.Now()
		for i := 0; i < count; i++ {
			key := kvList[i].Key.UserKey
			if _, err = at.writeItem(key, kvList[i].Value); err != nil {
				t.Fatalf("add err key:%s err:%v", string(key), err)
			}
		}
		require.NoError(t, at.writeFinish())
		require.Equal(t, uint32(count), at.header.num)
		et1 := time.Since(bt1)
		fmt.Printf("Test_SharedPage_Perf writeArrayTable time cost = %v\n", et1)

		fmt.Printf("useMapIndex=%#v; at.size=%dKB; at.tbl.fileStatSize=%dKB\n", useMapIndex, at.size/1024, at.tbl.fileStatSize()/1024)

		seekFunc := func(a *arrayTable) {
			iter := a.newIter(nil)
			i := 0
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				expKey := kvList[i].Key.UserKey
				require.Equal(t, expKey, key.UserKey)
				require.Equal(t, kvList[i].Value, val)
				i++
			}

			require.NoError(t, iter.Close())

			getFunc := func(skey []byte) {
				_, exist, _, closer := a.get(skey, hash.Crc32(skey))
				require.Equal(t, true, exist)
				if closer != nil {
					closer()
				}
			}

			seekFunc := func(skey []byte) {
				it1 := a.newIter(nil)
				ik, _ := it1.SeekGE(skey)
				if ik == nil {
					t.Fatal("SeekGE fail", string(skey))
				}
				require.Equal(t, skey, ik.UserKey)
				require.NoError(t, it1.Close())
			}

			seekNotExist := func(skey []byte) {
				_, exist, _, closer := a.get(skey, hash.Crc32(skey))
				require.Equal(t, false, exist)
				if closer != nil {
					closer()
				}
			}

			bt1_1 := time.Now()
			for j := 0; j < count; j++ {
				getFunc(kvList[j].Key.UserKey)
			}
			et1_1 := time.Since(bt1_1)
			fmt.Printf("Test_SharedPage_Perf GET time cost = %v\n", et1_1)

			bt1_2 := time.Now()
			for j := 0; j < count; j++ {
				seekFunc(kvList[j].Key.UserKey)
			}
			et1_2 := time.Since(bt1_2)
			fmt.Printf("Test_SharedPage_Perf SeekGE time cost = %v\n", et1_2)

			for _, n := range []int{count + 7, count + 71, count + 711} {
				seekNotExist(sortedkv.MakeSortedKey(n))
			}
		}

		bt2 := time.Now()
		seekFunc(at)
		et2 := time.Since(bt2)
		fmt.Printf("Test_SharedPage_Perf seekArrayTable time cost = %v\n", et2)

		require.NoError(t, at.close())

		at1, err1 := openArrayTable(testPath, &atCacheOpts)
		require.NoError(t, err1)

		bt3 := time.Now()
		seekFunc(at1)
		et3 := time.Since(bt3)
		fmt.Printf("Test_SharedPage_Perf reopen&seekArrayTable time cost = %v\n", et3)

		require.NoError(t, at1.close())

		os.Remove(testPath)
	}
	et := time.Since(bt)
	fmt.Printf("Test_SharedPage_Perf time cost = %v\n", et)
}

const item_count = 1<<20 - 1

func Test_PageBlock_Perf(t *testing.T) {
	defer os.Remove(testPath)
	os.Remove(testPath)

	count := item_count
	seqNum := uint64(1)
	kvList := testMakeSortedKVForBitrie(count, seqNum, 10)
	seqNum += uint64(count)

	bt := time.Now()
	for _, opts := range [][]bool{
		{false, false, false},
		//{false, true, false},
		{true, false, false},
		//{true, true, false},
	} {
		bt0 := time.Now()
		fmt.Printf("Start Test_PageBlock_Perf, opts(usePrefixCompress, useBlockCompress)=%#v\n", opts)

		atCacheOpts := &atCacheOptions{
			cache: testNewBlockCache(),
			id:    1 << 10,
		}
		option := &atOptions{
			useMapIndex:       false,
			usePrefixCompress: opts[0],
			useBlockCompress:  opts[1],
			blockSize:         32 << 10,
		}
		at, err := newArrayTable(testPath, option, atCacheOpts)
		require.NoError(t, err)

		bt1 := time.Now()
		for i := 0; i < count; i++ {
			_, err = at.writeItem(kvList[i].Key.UserKey, kvList[i].Value)
			require.NoError(t, err)
		}
		require.NoError(t, at.writeFinish())
		et1 := time.Since(bt1)
		fmt.Printf("Test_PageBlock_Perf writeArrayTable time cost = %v; header=%v\n", et1, at.header.version)

		fmt.Printf("opts(usePrefixCompress, useBlockCompress)=%#v; at.size=%dKB; tbl.size=%dKB, at.tbl.fileStatSize=%dKB; totalNum=%d\n", opts, at.size/1024, at.tbl.Size()/1024, at.tbl.fileStatSize()/1024, at.num)

		seekFunc := func(a *arrayTable) {
			bt10_0 := time.Now()
			iter := a.newIter(nil)
			i := 0
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, key.UserKey)
				require.Equal(t, kvList[i].Value, val)
				i++
			}
			require.Equal(t, count, i)
			require.NoError(t, iter.Close())
			et10_0 := time.Since(bt10_0)
			fmt.Printf("Test_PageBlock_Perf FlushIter_Next_Ranges time cost = %v\n", et10_0)

			getFunc := func(i int) {
				skey := kvList[i].Key.UserKey
				val, exist, _, closer := a.get(skey, hash.Crc32(skey))
				require.Equal(t, true, exist)
				require.Equal(t, kvList[i].Value, val)
				if closer != nil {
					closer()
				}
			}

			seekFunc := func(i int) {
				it1 := a.newIter(nil)
				skey := kvList[i].Key.UserKey
				ik, v := it1.SeekGE(skey)
				require.Equal(t, skey, ik.UserKey)
				require.Equal(t, kvList[i].Value, v)
				require.NoError(t, it1.Close())
			}

			seekltFunc := func(seek, exp int) {
				it1 := a.newIter(nil)
				skey := kvList[i].Key.UserKey
				ik, v := it1.SeekLT(skey)
				if exp != -1 {
					require.Equal(t, kvList[exp].Key.UserKey, ik.UserKey)
					require.Equal(t, kvList[exp].Value, v)
				}
				require.NoError(t, it1.Close())
			}

			seekNotExist := func(skey []byte) {
				_, exist, _, closer := a.get(skey, hash.Crc32(skey))
				require.Equal(t, false, exist)
				if closer != nil {
					closer()
				}
			}

			bt1_0 := time.Now()
			iter = a.newIter(nil)
			i = 0
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, key.UserKey)
				require.Equal(t, kvList[i].Value, val)
				i++
			}
			require.Equal(t, count, i)
			require.NoError(t, iter.Close())
			et1_0 := time.Since(bt1_0)
			fmt.Printf("Test_PageBlock_Perf Next_Ranges time cost = %v\n", et1_0)

			bt1_00 := time.Now()
			iter = a.newIter(nil)
			i = count - 1
			for key, val := iter.Last(); key != nil; key, val = iter.Prev() {
				require.Equal(t, kvList[i].Key.UserKey, key.UserKey)
				require.Equal(t, kvList[i].Value, val)
				i--
			}
			require.Equal(t, -1, i)
			require.NoError(t, iter.Close())
			et1_00 := time.Since(bt1_00)
			fmt.Printf("Test_PageBlock_Perf Prev_Ranges time cost = %v\n", et1_00)

			bt1_1 := time.Now()
			for i = 0; i < count; i++ {
				getFunc(i)
			}
			et1_1 := time.Since(bt1_1)
			fmt.Printf("Test_PageBlock_Perf GET time cost = %v\n", et1_1)

			bt1_2 := time.Now()
			for i = 0; i < count; i++ {
				if i == 0 {
					seekltFunc(0, -1)
				} else {
					seekltFunc(i, i-1)
				}
			}
			et1_2 := time.Since(bt1_2)
			fmt.Printf("Test_PageBlock_Perf SeekLT time cost = %v\n", et1_2)

			for _, j := range []int{100007 * 100, 100071 * 100, 100711 * 100} {
				seekNotExist(sortedkv.MakeSortedKey(j))
			}

			bt1_3 := time.Now()
			for i = 0; i < count; i++ {
				seekFunc(i)
			}
			et1_3 := time.Since(bt1_3)
			fmt.Printf("Test_PageBlock_Perf SeekGE time cost = %v\n", et1_3)
		}

		bt2 := time.Now()
		seekFunc(at)
		et2 := time.Since(bt2)
		fmt.Printf("Test_PageBlock_Perf seekArrayTable time cost = %v\n", et2)

		require.NoError(t, at.close())

		et0 := time.Since(bt0)
		fmt.Printf("Finish Test_PageBlock_Perf, opts(usePrefixCompress, useBlockCompress)=%#v, time cost = %v\n======================\n", opts, et0)

		os.Remove(testPath)
	}
	et := time.Since(bt)
	fmt.Printf("Test_PageBlock_Perf Total End, time cost = %v\n", et)
}

func Test_Bitrie_Perf(t *testing.T) {
	defer os.Remove(testPath)
	os.Remove(testPath)

	count := item_count
	seqNum := uint64(1)
	kvList := testMakeSortedKVForBitrie(count, seqNum, 10)
	seqNum += uint64(count)

	if count <= 50 {
		for i := 0; i < count; i++ {
			fmt.Printf("%s\n", kvList[i].Key.UserKey)
		}
	}

	bt := time.Now()
	for _, opts := range [][]bool{
		{false, false, true},
	} {
		bt0 := time.Now()
		fmt.Printf("Start Test_Bitrie_Perf, opts(usePrefixCompress, useBlockCompress)=%#v\n", opts)

		atCacheOpts := &atCacheOptions{
			cache: testNewBlockCache(),
			id:    1 << 10,
		}
		option := &atOptions{
			useMapIndex:       false,
			usePrefixCompress: opts[0],
			useBlockCompress:  opts[1],
			useBitrieCompress: opts[2],
			blockSize:         32 << 10,
		}
		at, err := newArrayTable(testPath, option, atCacheOpts)
		require.NoError(t, err)

		bt1 := time.Now()
		for i := 0; i < count; i++ {
			_, err = at.writeItem(kvList[i].Key.UserKey, kvList[i].Value)
			require.NoError(t, err)
		}

		require.NoError(t, at.writeFinish())
		et1 := time.Since(bt1)
		fmt.Printf("Test_Bitrie_Perf writeArrayTable time cost = %v; header=%v\n", et1, at.header.version)

		if count <= 50 {
			fmt.Printf("Test_Bitrie_Perf BitrieToString:\n%s\n", at.bitrieIndex.AnalyzeBytes())
		}

		fmt.Printf("opts(usePrefixCompress, useBlockCompress, useBitrieCompress)=%#v; at.size=%dKB; tbl.size=%dKB, at.tbl.fileStatSize=%dKB; totalNum=%d\n", opts, at.size/1024, at.tbl.Size()/1024, at.tbl.fileStatSize()/1024, at.num)

		seekFunc := func(a *arrayTable) {
			i := 0
			/*bt10_0 := time.Now()
			iter := a.newIter(nil)
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, key.UserKey)
				require.Equal(t, kvList[i].Value, val)
				i++
			}
			require.Equal(t, count, i)
			require.NoError(t, iter.Close())
			et10_0 := time.Since(bt10_0)
			fmt.Printf("Test_PageBlock_Perf FlushIter_Next_Ranges time cost = %v\n", et10_0)
			*/

			getFunc := func(i int) {
				skey := kvList[i].Key.UserKey
				val, exist, _, closer := a.get(skey, 0 /*hash.Crc32(skey)*/)
				require.Equal(t, true, exist)
				require.Equal(t, kvList[i].Value, val)
				if closer != nil {
					closer()
				}
			}

			/*seekFunc := func(i int) {
				it1 := a.newIter(nil)
				skey := kvList[i].Key.UserKey
				ik, v := it1.SeekGE(skey)
				require.Equal(t, skey, ik.UserKey)
				require.Equal(t, kvList[i].Value, v)
				require.NoError(t, it1.Close())
			}

			seekltFunc := func(seek, exp int) {
				it1 := a.newIter(nil)
				skey := kvList[i].Key.UserKey
				ik, v := it1.SeekLT(skey)
				if exp != -1 {
					require.Equal(t, kvList[exp].Key.UserKey, ik.UserKey)
					require.Equal(t, kvList[exp].Value, v)
				}
				require.NoError(t, it1.Close())
			}

			seekNotExist := func(skey []byte) {
				_, exist, _, closer := a.get(skey, hash.Crc32(skey))
				require.Equal(t, false, exist)
				if closer != nil {
					closer()
				}
			}

			bt1_0 := time.Now()
			iter = a.newIter(nil)
			i = 0
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, key.UserKey)
				require.Equal(t, kvList[i].Value, val)
				i++
			}
			require.Equal(t, count, i)
			require.NoError(t, iter.Close())
			et1_0 := time.Since(bt1_0)
			fmt.Printf("Test_Bitrie_Perf Next_Ranges time cost = %v\n", et1_0)

			bt1_00 := time.Now()
			iter = a.newIter(nil)
			i = count - 1
			for key, val := iter.Last(); key != nil; key, val = iter.Prev() {
				require.Equal(t, kvList[i].Key.UserKey, key.UserKey)
				require.Equal(t, kvList[i].Value, val)
				i--
			}
			require.Equal(t, -1, i)
			require.NoError(t, iter.Close())
			et1_00 := time.Since(bt1_00)
			fmt.Printf("Test_Bitrie_Perf Prev_Ranges time cost = %v\n", et1_00)
			*/

			bt1_1 := time.Now()
			for i = 0; i < count; i++ {
				getFunc(i)
			}
			et1_1 := time.Since(bt1_1)
			fmt.Printf("Test_Bitrie_Perf GET time cost = %v\n", et1_1)

			/*bt1_2 := time.Now()
			for i = 0; i < count; i++ {
				if i == 0 {
					seekltFunc(0, -1)
				} else {
					seekltFunc(i, i-1)
				}
			}
			et1_2 := time.Since(bt1_2)
			fmt.Printf("Test_Bitrie_Perf SeekLT time cost = %v\n", et1_2)

			for _, j := range []int{100007, 100071, 100711} {
				seekNotExist(utils.MakeSortedKey(j))
			}

			bt1_3 := time.Now()
			for i = 0; i < count; i++ {
				seekFunc(i)
			}
			et1_3 := time.Since(bt1_3)
			fmt.Printf("Test_Bitrie_Perf SeekGE time cost = %v\n", et1_3)*/
		}

		bt2 := time.Now()
		seekFunc(at)
		et2 := time.Since(bt2)
		fmt.Printf("Test_Bitrie_Perf seekArrayTable time cost = %v\n", et2)

		require.NoError(t, at.close())

		et0 := time.Since(bt0)
		fmt.Printf("Finish Test_Bitrie_Perf, opts(usePrefixCompress, useBlockCompress)=%#v, time cost = %v\n======================\n", opts, et0)

		os.Remove(testPath)
	}
	et := time.Since(bt)
	fmt.Printf("Test_Bitrie_Perf Total End, time cost = %v\n", et)
}

func Test_AtEqual(t *testing.T) {
	key1 := []byte("asdfghjklpoiuytrewq")
	key2 := []byte("asdfghjklpoiuytrewq")
	sk1 := []byte("asdfghjkl")
	sk2 := []byte("poiuytrewq")

	totalNum := 2 << 20
	startNum := 0

	fmt.Println(bytes.Equal(key1, key2), atEqual(sk1, sk2, key1))

	bt := time.Now()
	for i := startNum; i <= totalNum; i++ {
		if !bytes.Equal(key1, key2) {
			fmt.Printf("bytes.Equal error")
		}
	}
	et := time.Since(bt)
	fmt.Printf("bytes.Equal time cost = %v\n", et)

	bt = time.Now()
	for i := startNum; i <= totalNum; i++ {
		if !atEqual(sk1, sk2, key2) {
			fmt.Printf("atEqual error")
		}
	}
	et = time.Since(bt)
	fmt.Printf("atEqual time cost = %v\n", et)

	bt = time.Now()
	for i := startNum; i <= totalNum; i++ {
		tmpk := make([]byte, 0, len(key1))
		tmpk = append(tmpk, sk1...)
		tmpk = append(tmpk, sk2...)
		if !bytes.Equal(tmpk, key2) {
			fmt.Printf("atEqual error")
		}
	}
	et = time.Since(bt)
	fmt.Printf("make&bytes.Equal time cost = %v\n", et)
}

func Test_AtCompare(t *testing.T) {
	key1 := []byte("asdfghjklpoiuytrewq")
	key2 := []byte("asdfghjklpoiuytrewq")
	sk1 := []byte("asdfghjkl")
	sk2 := []byte("poiuytrewq")

	totalNum := 1 << 20
	startNum := 0

	bt := time.Now()
	for i := startNum; i <= totalNum; i++ {
		if bytes.Compare(key1, key2) != 0 {
			fmt.Printf("bytes.Compare error")
		}
	}
	et := time.Since(bt)
	fmt.Printf("bytes.Compare time cost = %v\n", et)

	bt = time.Now()
	for i := startNum; i <= totalNum; i++ {
		if atCompare(sk1, sk2, key2) != 0 {
			fmt.Printf("atCompare error")
		}
	}
	et = time.Since(bt)
	fmt.Printf("atCompare time cost = %v\n", et)

	bt = time.Now()
	for i := startNum; i <= totalNum; i++ {
		tmpk := make([]byte, 0, len(key1))
		tmpk = append(tmpk, sk1...)
		tmpk = append(tmpk, sk2...)
		if bytes.Compare(tmpk, key2) != 0 {
			fmt.Printf("bytes.Compare error")
		}
	}
	et = time.Since(bt)
	fmt.Printf("make&bytes.Compare time cost = %v\n", et)
}

func Test_AtCompare_Case(t *testing.T) {
	type compareCase struct {
		key []byte
		sk1 []byte
		sk2 []byte
		ret int
	}

	caseList := make([]compareCase, 0, 2<<4)

	caseList = append(caseList, compareCase{key: []byte("asdfghjklpoiuytrewq"), sk1: []byte("asdfghjkl"), sk2: []byte("poiuytrewq"), ret: 0})
	caseList = append(caseList, compareCase{key: []byte("asdfghjkl"), sk1: []byte("asdfghjkl"), sk2: []byte(""), ret: 0})
	caseList = append(caseList, compareCase{key: []byte("asdfghjkl"), sk1: []byte(""), sk2: []byte("asdfghjkl"), ret: 0})
	caseList = append(caseList, compareCase{key: []byte("qwer"), sk1: []byte("asdfghjklpoiuytrewq"), sk2: []byte("a"), ret: -1})
	caseList = append(caseList, compareCase{key: []byte("fdafa"), sk1: []byte("qsdfghjklpoiuytrewq"), sk2: []byte("a"), ret: 1})
	caseList = append(caseList, compareCase{key: []byte("qsdfghjklpoiuytrewqqqqqq"), sk1: []byte("qsdfghjklpoiuytrewq"), sk2: []byte("a"), ret: -1})
	caseList = append(caseList, compareCase{key: []byte("qsdfghjklpoiuytrewqqqqqq"), sk1: []byte("qsdfghjklpoiuytrewq"), sk2: []byte("zzzz"), ret: 1})
	caseList = append(caseList, compareCase{key: []byte("abcd"), sk1: []byte("a"), sk2: []byte("zzz"), ret: 1})
	caseList = append(caseList, compareCase{key: []byte("abcd"), sk1: []byte("a"), sk2: []byte("azzz"), ret: -1})

	for i := 0; i < len(caseList); i++ {
		r := atCompare(caseList[i].sk1, caseList[i].sk2, caseList[i].key)
		require.Equal(t, r, caseList[i].ret)
	}
}
