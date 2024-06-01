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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

const testDir = "test"
const testSlotId uint16 = 1

var nilInternalKey = (*internalKey)(nil)

var testParams = [][]bool{
	{true, false, false},
	{true, true, false},
	{true, false, true},
	{true, true, true},
}

func testMakeSortedKV(num int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKVList(0, num, seqNum, vsize)
}

func testMakeSortedKVForBitrie(num int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKVListForBitrie(0, num, seqNum, vsize)
}

func testInitDir() {
	_, err := os.Stat(testDir)
	if nil != err && !os.IsExist(err) {
		err = os.MkdirAll(testDir, 0775)
	}
}

func testInitOpts() *options.BitpageOptions {
	optspool := options.InitTestDefaultsOptionsPool()
	opts := optspool.CloneBitpageOptions()
	return opts
}

func testOpenBitpage(UseMapIndex bool) (*Bitpage, error) {
	testInitDir()
	opts := testInitOpts()
	opts.Index = 1
	opts.UseMapIndex = UseMapIndex
	return Open(testDir, opts)
}

func testOpenBitpage2(dir string, mapIndex, prefix, block bool) (*Bitpage, error) {
	testInitDir()
	opts := testInitOpts()
	opts.Index = 1
	opts.UseMapIndex = mapIndex
	opts.UsePrefixCompress = prefix
	opts.UseBlockCompress = block
	return Open(dir, opts)
}

func testCloseBitpage(t *testing.T, b *Bitpage) {
	require.NoError(t, b.Close())
	b.opts.DeleteFilePacer.Close()
}

func makeTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitpage_key_%d", i))
}

func TestBitpage_Open(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	bp, err := testOpenBitpage(true)
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)
	p := bp.GetPage(pn)
	paths := p.getFilesPath()
	testCloseBitpage(t, bp)

	bp, err = testOpenBitpage(true)
	require.NoError(t, err)
	for i := range paths {
		require.Equal(t, true, utils.IsFileExist(paths[i]))
	}
	testCloseBitpage(t, bp)
}

func TestBitpage_Writer(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage(true)
	require.NoError(t, err)

	pn, err1 := bp.NewPage()
	require.NoError(t, err1)

	count := 1000
	seqNum := uint64(1)
	kvList := testMakeSortedKV(count, seqNum, 10)
	seqNum += uint64(count)

	wr := bp.GetPageWriter(pn, nil)
	writeKV := func(pos int) {
		require.NoError(t, wr.Set(*kvList[pos].Key, kvList[pos].Value))
	}

	for i := 0; i < count; i++ {
		if i >= 10 && i < 20 {
			kvList[i].Key.SetKind(internalKeyKindDelete)
		}
		writeKV(i)
	}
	require.NoError(t, wr.FlushFinish())

	p := bp.GetPage(pn)
	for i := 0; i < count; i++ {
		uk := kvList[i].Key.UserKey
		v, vexist, vcloser, kind := p.get(uk, hash.Crc32(uk))
		if i >= 10 && i < 20 {
			require.Equal(t, false, vexist)
			require.Equal(t, internalKeyKindDelete, kind)
		} else {
			require.Equal(t, kvList[i].Value, v)
			require.Equal(t, internalKeyKindSet, kind)
		}
		if vcloser != nil {
			vcloser()
		}
	}

	for i := 10; i < 20; i++ {
		kvList[i].Key.SetKind(internalKeyKindSet)
		kvList[i].Key.SetSeqNum(seqNum)
		seqNum++
		writeKV(i)
	}
	require.NoError(t, wr.FlushFinish())

	for i := 0; i < count; i++ {
		uk := kvList[i].Key.UserKey
		v, vexist, vcloser, kind := p.get(uk, hash.Crc32(uk))
		require.Equal(t, true, vexist)
		require.Equal(t, kvList[i].Value, v)
		require.Equal(t, internalKeyKindSet, kind)
		vcloser()
	}

	testCloseBitpage(t, bp)
}

func TestBitpage_StMmapExpand(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage(true)
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)

	count := 10000
	seqNum := uint64(1)
	kvList := testMakeSortedKV(count, seqNum, 10)
	seqNum += uint64(count)
	wr := bp.GetPageWriter(pn, nil)
	for i := 0; i < count; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	p := bp.GetPage(pn)
	wg := sync.WaitGroup{}
	closeCh := make(chan struct{})
	wg.Add(100)
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-closeCh:
						return
					default:
						ri := rand.Intn(count)
						uk := kvList[ri].Key.UserKey
						v, vexist, vcloser, kind := p.get(uk, hash.Crc32(uk))
						require.Equal(t, internalKeyKindSet, kind)
						require.Equal(t, true, vexist)
						require.Equal(t, kvList[ri].Value, v)
						vcloser()
					}
				}
			}()
		} else {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-closeCh:
						return
					default:
						iter := p.newIter(nil)
						pos := rand.Intn(count - 20)
						uk := kvList[pos].Key.UserKey
						j := 0
						for ik, iv := iter.SeekGE(uk); ik != nil; ik, iv = iter.Next() {
							require.Equal(t, kvList[pos].Key, ik)
							require.Equal(t, kvList[pos].Value, iv)
							if j == 10 {
								break
							}
							j++
							pos++
						}
						require.NoError(t, iter.Close())
					}
				}
			}()
		}

	}

	time.Sleep(2 * time.Second)
	wr = bp.GetPageWriter(pn, nil)
	kvList1 := sortedkv.MakeSortedKVList(count, count+10, seqNum, 10)
	for i := 0; i < 10; i++ {
		require.NoError(t, wr.Set(*kvList1[i].Key, kvList1[i].Value))
	}

	wr.p.mu.stMutable.grow(consts.BitpageInitMmapSize + 1)
	require.NoError(t, wr.FlushFinish())
	require.Equal(t, consts.BitpageInitMmapSize*2, wr.p.mu.stMutable.tbl.Capacity())

	close(closeCh)
	wg.Wait()

	testCloseBitpage(t, bp)
}

func TestBitpage_OpenPages(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	var pageNums []PageNum
	count := 1000
	pagesCnt := 10
	stCnt := 5
	seqNum := uint64(1)
	num := count * stCnt
	kvList := testMakeSortedKV(num, seqNum, 10)
	seqNum += uint64(num)

	writePage := func(b *Bitpage) {
		for i := 0; i < pagesCnt; i++ {
			pn, err1 := b.NewPage()
			require.NoError(t, err1)
			pageNums = append(pageNums, pn)
			wr := b.GetPageWriter(pn, nil)
			for j := 0; j < stCnt; j++ {
				start := j * count
				end := start + count
				for index := start; index < end; index++ {
					require.NoError(t, wr.Set(*kvList[index].Key, kvList[index].Value))
				}
				wr.FlushFinish()
				require.NoError(t, wr.p.makeMutableForWrite(false))
			}
		}
	}

	readPage := func(b *Bitpage, pn PageNum) {
		p := b.GetPage(pn)
		for i := 0; i < num; i++ {
			key := kvList[i].Key.UserKey
			v, vexist, vcloser, kind := p.get(key, hash.Crc32(key))
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, v)
			require.Equal(t, internalKeyKindSet, kind)
			vcloser()
		}
	}

	bp, err := testOpenBitpage(true)
	require.NoError(t, err)
	writePage(bp)
	testCloseBitpage(t, bp)

	bp1, err1 := testOpenBitpage(true)
	require.NoError(t, err1)
	for _, pn := range pageNums {
		readPage(bp1, pn)
	}
	writePage(bp1)
	testCloseBitpage(t, bp1)

	bp2, err2 := testOpenBitpage(true)
	require.NoError(t, err2)
	for _, pn := range pageNums {
		readPage(bp2, pn)
	}
	testCloseBitpage(t, bp2)
}

func TestBitpage_Iter_SameKey(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage(true)
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)

	wr := bp.GetPageWriter(pn, nil)
	seqNum := uint64(1)
	count := 100
	kvList := testMakeSortedKV(count, seqNum, 10)
	seqNum += uint64(count)

	for j := 0; j < 100; j++ {
		for i := 0; i < count; i++ {
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			kvList[i].Value = utils.FuncRandBytes(10)
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())
	}

	checkKV := func(i int, ikey *internalKey, ival []byte) {
		require.Equal(t, kvList[i].Key.UserKey, ikey.UserKey)
		require.Equal(t, kvList[i].Value, ival)
		require.Equal(t, internalKeyKindSet, ikey.Kind())
	}

	rangeFunc := func(p *page) {
		iter := p.newIter(nil)
		i := 0
		for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
			checkKV(i, ik, iv)
			i++
		}
		require.NoError(t, iter.Close())

		iter = p.newIter(nil)
		i = len(kvList) - 1
		for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
			checkKV(i, ik, iv)
			i--
		}
		require.NoError(t, iter.Close())
	}

	seekFunc := func(p *page) {
		iter := p.newIter(nil)
		for i := 0; i < count; i++ {
			ik, iv := iter.SeekGE(kvList[i].Key.UserKey)
			checkKV(i, ik, iv)
		}
		require.NoError(t, iter.Close())
	}

	pg := bp.GetPage(pn)
	rangeFunc(pg)
	seekFunc(pg)
	testCloseBitpage(t, bp)

	bp2, err2 := testOpenBitpage(true)
	require.NoError(t, err2)
	pg2 := bp2.GetPage(pn)
	rangeFunc(pg2)
	seekFunc(pg2)
	testCloseBitpage(t, bp2)
}

func TestBitpage_Iter_StGet(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage(true)
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)
	wr := bp.GetPageWriter(pn, nil)

	seqNum := uint64(1)
	count := 100
	kvList := testMakeSortedKV(count, seqNum, 10)
	seqNum += uint64(count)
	key := kvList[0].Key.UserKey
	for i := 0; i < count; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	seekFunc := func(p *page) {
		iter := p.newIter(nil)
		ik, iv := iter.SeekGE(key)
		require.Equal(t, kvList[0].Key, ik)
		require.Equal(t, kvList[0].Value, iv)
		ik, iv = iter.SeekLT(key)
		require.Equal(t, nilInternalKey, ik)
		require.NoError(t, iter.Close())
	}

	pg := bp.GetPage(pn)
	seekFunc(pg)
	testCloseBitpage(t, bp)

	bp2, err2 := testOpenBitpage(true)
	require.NoError(t, err2)
	pg2 := bp2.GetPage(pn)
	seekFunc(pg2)
	testCloseBitpage(t, bp2)
}

func testcase(caseFunc func(int, []bool)) {
	for i, params := range testParams {
		fmt.Printf("testcase params:%v\n", params)
		caseFunc(i, params)
	}
}

func TestBitpageWriterDelete(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)
		bp, err := testOpenBitpage2(dir, params[0], params[1], params[2])
		require.NoError(t, err)
		count := 1000
		seqNum := uint64(1)
		kvList := testMakeSortedKV(count, seqNum, 10)
		seqNum += uint64(count)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		wr := bp.GetPageWriter(pn, nil)
		writeKV := func(pos int) {
			require.NoError(t, wr.Set(*kvList[pos].Key, kvList[pos].Value))
		}

		for i := 0; i < count; i++ {
			writeKV(i)
		}
		require.NoError(t, wr.FlushFinish())

		p := bp.GetPage(pn)
		for i := 0; i < count; i++ {
			key := kvList[i].Key.UserKey
			v, vexist, vcloser, kind := p.get(key, hash.Crc32(key))
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, v)
			require.Equal(t, internalKeyKindSet, kind)
			vcloser()
		}

		require.NoError(t, p.flush(nil, ""))

		for i := 10; i < 20; i++ {
			kvList[i].Key.SetKind(internalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			writeKV(i)
		}
		require.NoError(t, wr.FlushFinish())

		for i := 0; i < count; i++ {
			key := kvList[i].Key.UserKey
			v, vexist, vcloser, kind := p.get(key, hash.Crc32(key))
			if i >= 10 && i < 20 {
				require.Equal(t, true, vcloser == nil)
				require.Equal(t, false, vexist)
				require.Equal(t, internalKeyKindDelete, kind)
			} else {
				require.Equal(t, true, vexist)
				require.Equal(t, kvList[i].Value, v)
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
			}
		}

		testCloseBitpage(t, bp)
	})
}

func TestBitpageIter(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)
		bp, err := testOpenBitpage2(dir, params[0], params[1], params[2])
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		wr := bp.GetPageWriter(pn, nil)
		seqNum := uint64(1)
		count := 100
		kvList := testMakeSortedKV(count, seqNum, 10)
		seqNum += uint64(count)

		for i := 0; i < count; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		pg := bp.GetPage(pn)
		require.NoError(t, pg.flush(nil, ""))
		for i := 20; i < 30; i++ {
			kvList[i].Key.SetKind(internalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = nil
			seqNum++
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		for i := 80; i < count; i++ {
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = utils.FuncRandBytes(10)
			seqNum++
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		checkKV := func(i int, ikey *internalKey, ival []byte) {
			if i < 0 {
				require.Equal(t, nilInternalKey, ikey)
				require.Equal(t, 0, len(ival))
			} else {
				require.Equal(t, kvList[i].Key.UserKey, ikey.UserKey)
				if i >= 20 && i < 30 {
					require.Equal(t, 0, len(ival))
					require.Equal(t, internalKeyKindDelete, ikey.Kind())
				} else {
					require.Equal(t, kvList[i].Value, ival)
					require.Equal(t, internalKeyKindSet, ikey.Kind())
				}
			}
		}

		rangeFunc := func(p *page) {
			iter := p.newIter(nil)
			setCnt := 0
			delCnt := 0
			index := 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				checkKV(index, ik, iv)
				if ik.Kind() == internalKeyKindSet {
					setCnt++
				} else if ik.Kind() == internalKeyKindDelete {
					delCnt++
				}
				index++
			}
			require.Equal(t, count-10, setCnt)
			require.Equal(t, 10, delCnt)

			setCnt = 0
			delCnt = 0
			index = 99
			for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
				checkKV(index, ik, iv)
				if ik.Kind() == internalKeyKindSet {
					setCnt++
				} else if ik.Kind() == internalKeyKindDelete {
					delCnt++
				}
				index--
			}
			require.Equal(t, count-10, setCnt)
			require.Equal(t, 10, delCnt)
			require.NoError(t, iter.Close())
		}

		seekFunc := func(p *page) {
			iter := p.newIter(nil)
			for i := 0; i < count; i++ {
				key := kvList[i].Key.UserKey
				ik, iv := iter.SeekGE(key)
				checkKV(i, ik, iv)
			}
			require.NoError(t, iter.Close())
		}

		seekFunc1 := func(p *page) {
			iter := p.newIter(nil)

			ik, iv := iter.Prev()
			checkKV(99, ik, iv)

			ik, iv = iter.First()
			checkKV(0, ik, iv)
			ik, iv = iter.Next()
			checkKV(1, ik, iv)
			ik, iv = iter.Prev()
			checkKV(0, ik, iv)
			ik, iv = iter.Last()
			checkKV(99, ik, iv)
			ik, iv = iter.Prev()
			checkKV(98, ik, iv)

			ik, iv = iter.SeekGE(kvList[15].Key.UserKey)
			checkKV(15, ik, iv)
			ik, iv = iter.SeekGE(kvList[99].Key.UserKey)
			checkKV(99, ik, iv)

			ik, iv = iter.SeekGE(sortedkv.MakeSortedKey(990))
			checkKV(-1, ik, iv)
			ik, iv = iter.SeekGE(sortedkv.MakeSortedKey(-1))
			checkKV(0, ik, iv)

			ik, iv = iter.SeekLT(kvList[35].Key.UserKey)
			checkKV(34, ik, iv)
			ik, iv = iter.Prev()
			checkKV(33, ik, iv)

			ik, iv = iter.SeekGE(kvList[20].Key.UserKey)
			checkKV(20, ik, iv)
			ik, iv = iter.SeekGE(kvList[29].Key.UserKey)
			checkKV(29, ik, iv)
			ik, iv = iter.Next()
			checkKV(30, ik, iv)

			ik, iv = iter.SeekLT(kvList[30].Key.UserKey)
			checkKV(29, ik, iv)
			ik, iv = iter.Prev()
			checkKV(28, ik, iv)

			ik, iv = iter.SeekLT(kvList[21].Key.UserKey)
			checkKV(20, ik, iv)
			ik, iv = iter.SeekLT(kvList[20].Key.UserKey)
			checkKV(19, ik, iv)
			ik, iv = iter.Prev()
			checkKV(18, ik, iv)

			ik, iv = iter.SeekLT(sortedkv.MakeSortedKey(990))
			checkKV(99, ik, iv)
			ik, iv = iter.SeekLT(sortedkv.MakeSortedKey(-1))
			checkKV(-1, ik, iv)

			require.NoError(t, iter.Close())
		}

		rangeFunc(pg)
		seekFunc(pg)
		seekFunc1(pg)
		testCloseBitpage(t, bp)

		bp2, err2 := testOpenBitpage2(dir, params[0], params[1], params[2])
		require.NoError(t, err2)
		pg2 := bp2.GetPage(pn)
		rangeFunc(pg2)
		seekFunc(pg2)
		seekFunc1(pg2)
		testCloseBitpage(t, bp2)
	})
}

func TestBitpageIterRange(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)
		bp, err := testOpenBitpage2(dir, params[0], params[1], params[2])
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		wr := bp.GetPageWriter(pn, nil)
		seqNum := uint64(1)
		count := 10000
		kvList := testMakeSortedKV(count+1, seqNum, 100)
		seqNum += uint64(count)

		rangeIter := func(p *page) {
			iter := p.newIter(nil)
			i := 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, ik.UserKey)
				require.Equal(t, kvList[i].Value, iv)
				i++
			}
			require.Equal(t, i, count)
			require.NoError(t, iter.Close())

			iter = p.newIter(nil)
			for i = 0; i < count; i++ {
				ik, iv := iter.SeekGE(kvList[i].Key.UserKey)
				require.Equal(t, kvList[i].Key.UserKey, ik.UserKey)
				require.Equal(t, kvList[i].Value, iv)
			}
			require.NoError(t, iter.Close())
		}

		rangeReverseIter := func(p *page) {
			iter := p.newIter(nil)
			i := count - 1
			for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
				require.Equal(t, kvList[i].Key.UserKey, ik.UserKey)
				require.Equal(t, kvList[i].Value, iv)
				i--
			}
			require.Equal(t, -1, i)
			require.NoError(t, iter.Close())

			iter = p.newIter(nil)
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

		for i := 0; i < count; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		pg := bp.GetPage(pn)
		rangeIter(pg)
		rangeReverseIter(pg)
		require.NoError(t, pg.flush(nil, ""))
		rangeIter(pg)
		rangeReverseIter(pg)
		testCloseBitpage(t, bp)

		bp2, err2 := testOpenBitpage2(dir, params[0], params[1], params[2])
		require.NoError(t, err2)
		pg2 := bp2.GetPage(pn)
		rangeIter(pg2)
		rangeReverseIter(pg2)
		testCloseBitpage(t, bp2)
	})
}

func TestBitpageCheckpoint(t *testing.T) {
	testcase(func(index int, params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)
		openBitpage := func(dir string) *Bitpage {
			b, err := testOpenBitpage2(dir, params[0], params[1], params[2])
			require.NoError(t, err)
			return b
		}
		bp := openBitpage(testDir)
		pn, err := bp.NewPage()
		require.NoError(t, err)
		pg := bp.GetPage(pn)
		dstDir := fmt.Sprintf("%s_ck", testDir)
		os.RemoveAll(dstDir)
		defer os.RemoveAll(dstDir)
		require.NoError(t, bp.Checkpoint(bp.opts.FS, dstDir))
		bp1 := openBitpage(dstDir)
		pg1 := bp1.GetPage(pn)
		require.Equal(t, 1, len(pg.mu.stQueue))
		require.Equal(t, 1, len(pg1.mu.stQueue))
		require.Equal(t, true, pg.mu.arrtable == nil)
		require.Equal(t, true, pg1.mu.arrtable == nil)
		require.NoError(t, bp1.Close())
		require.NoError(t, os.RemoveAll(dstDir))

		wr := bp.GetPageWriter(pn, nil)
		seqNum := uint64(1)
		count := 2000
		kvList := testMakeSortedKV(count, seqNum, 10)
		seqNum += uint64(count)

		for i := 0; i < 1000; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())
		require.NoError(t, pg.flush(nil, ""))
		for i := 1000; i < 2000; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		for i := 0; i < 3; i++ {
			fmt.Println("checkpoint", i)
			dstDir = fmt.Sprintf("%s_ck_%d", testDir, i)
			require.NoError(t, os.RemoveAll(dstDir))
			require.NoError(t, bp.Checkpoint(bp.opts.FS, dstDir))
			bp1 = openBitpage(dstDir)
			pg1 = bp1.GetPage(pn)
			require.Equal(t, 2, len(pg.mu.stQueue))
			require.Equal(t, 1, len(pg1.mu.stQueue))
			require.Equal(t, true, pg.mu.arrtable != nil)
			require.Equal(t, true, pg1.mu.arrtable != nil)
			for j := 0; j < 2000; j++ {
				key := kvList[j].Key.UserKey
				v, vexist, vcloser, kind := pg1.get(key, hash.Crc32(key))
				require.Equal(t, true, vexist)
				require.Equal(t, kvList[j].Value, v)
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
				v, vexist, vcloser, kind = pg.get(key, hash.Crc32(key))
				require.Equal(t, true, vexist)
				require.Equal(t, kvList[j].Value, v)
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
			}
			testCloseBitpage(t, bp1)
			require.NoError(t, os.RemoveAll(dstDir))
		}

		testCloseBitpage(t, bp)
	})
}

func TestBitpageCheckpoint1(t *testing.T) {
	testcase(func(index int, params []bool) {
		defer os.RemoveAll(testDir)
		os.RemoveAll(testDir)
		openBitpage := func(dir string) *Bitpage {
			b, err := testOpenBitpage2(dir, params[0], params[1], params[2])
			require.NoError(t, err)
			return b
		}
		bp := openBitpage(testDir)
		pn, err := bp.NewPage()
		require.NoError(t, err)
		wr := bp.GetPageWriter(pn, nil)
		seqNum := uint64(1)
		count := 20000
		kvList := testMakeSortedKV(count, seqNum, 100)
		seqNum += uint64(count)

		for i := 0; i < count/2; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		pg := bp.GetPage(pn)
		require.NoError(t, pg.flush(nil, ""))

		for i := count / 2; i < count; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		sps, err2 := bp.PageSplitStart(pn, "test")
		require.NoError(t, err2)
		bp.PageSplitEnd(pn, sps, nil)
		fmt.Println("page split ok")

		dstDir := fmt.Sprintf("%s_ck", testDir)
		os.RemoveAll(dstDir)
		defer os.RemoveAll(dstDir)
		require.NoError(t, bp.Checkpoint(bp.opts.FS, dstDir))
		require.Equal(t, consts.BitpageSplitNum+1, bp.GetPageCount())
		require.Equal(t, true, bp.PageSplitted2(pn))
		require.Equal(t, true, pg.mu.arrtable != nil)
		bp1 := openBitpage(dstDir)
		pg1 := bp1.GetPage(pn)
		require.Equal(t, true, pg1 == nil)
		for i := range sps {
			spn := bp1.GetPage(sps[i].Pn)
			require.Equal(t, true, spn != nil)
			require.Equal(t, 1, len(spn.mu.stQueue))
			require.Equal(t, true, spn.mu.arrtable != nil)
		}

		for j := 0; j < count; j++ {
			key := kvList[j].Key.UserKey
			v, vexist, vcloser, kind := pg.get(key, hash.Crc32(key))
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[j].Value, v)
			require.Equal(t, internalKeyKindSet, kind)
			vcloser()

			find := false
			for i := range sps {
				if bytes.Compare(key, sps[i].Sentinel) <= 0 {
					v, vexist, vcloser, kind = bp1.Get(sps[i].Pn, key, hash.Crc32(key))
					require.Equal(t, true, vexist)
					require.Equal(t, kvList[j].Value, v)
					require.Equal(t, internalKeyKindSet, kind)
					vcloser()
					find = true
					break
				}
			}
			require.Equal(t, true, find)
		}

		testCloseBitpage(t, bp)
	})
}
