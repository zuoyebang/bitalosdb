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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

const testDir = "test"

var nilInternalKKVKey = (*InternalKKVKey)(nil)

func testMakeSortedKV(num int, seqNum uint64, vsize int) sortedkv.SortedKVList {
	return sortedkv.MakeSortedKVList(0, num, seqNum, vsize)
}

func testCopySortedKV(list sortedkv.SortedKVList) sortedkv.SortedKVList {
	var kvList sortedkv.SortedKVList
	for i := range list {
		kvList = append(kvList, list[i])
	}
	return kvList
}

//func testMakeSortedKVForBitrie(num int, seqNum uint64, vsize int) sortedkv.SortedKVList {
//	return sortedkv.MakeSortedKVListForBitrie(0, num, seqNum, vsize)
//}

func testInitDir() {
	_, err := os.Stat(testDir)
	if nil != err && !os.IsExist(err) {
		os.MkdirAll(testDir, 0775)
	}
}

func testInitOpts() *options.BitpageOptions {
	optspool := options.InitTestOptionsPool()
	opts := optspool.CloneBitpageOptions()
	return opts
}

func testOpenBitpage() (*Bitpage, error) {
	testInitDir()
	opts := testInitOpts()
	opts.Index = 1
	return Open(testDir, opts)
}

func testNewOpts(isUseVi, isUseMiniVi bool) *options.BitpageOptions {
	optspool := options.InitTestOptionsPool()
	opts := optspool.CloneBitpageOptions()
	opts.BitpageDisableMiniVi = isUseMiniVi
	opts.UseVi = isUseVi
	return opts
}

func testOpen(isUseVi, isUseMiniVi bool) (*Bitpage, error) {
	testInitDir()
	opts := testNewOpts(isUseVi, isUseMiniVi)
	return Open(testDir, opts)
}

func testCloseBitpage(t *testing.T, b *Bitpage) {
	require.NoError(t, b.Close())
	b.opts.DeleteFilePacer.Close()
}

var testParams = [][]bool{
	{true, true},
	{true, false},
	{false, true},
}

func testcase(caseFunc func([]bool)) {
	for _, params := range testParams {
		fmt.Printf("testcase params:%v\n", params)
		caseFunc(params)
	}
}

func TestBitpageOpen(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)
	p := bp.GetPage(pn)
	count := 100
	seqNum := uint64(1)
	kvList := testMakeSortedKV(count, seqNum, 10)
	seqNum += uint64(count)
	wr := bp.GetPageWriter(pn, nil)
	for i := 0; i < count; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	paths := p.getFilesPath()
	testCloseBitpage(t, bp)

	bp, err = testOpenBitpage()
	require.NoError(t, err)
	require.Equal(t, 2, len(paths))
	for i := range paths {
		require.Equal(t, true, os2.IsExist(paths[i]))
	}
	testCloseBitpage(t, bp)
}

func TestBitpageReopenWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)
	wr := bp.GetPageWriter(pn, nil)
	seqNum := uint64(1)
	count := 100
	vsize := 10
	kvList := sortedkv.MakeSortedKKVKVList(kkv.DataTypeHash, 0, count, seqNum, 10)
	seqNum += uint64(count)
	pg := bp.GetPage(pn)

	readData := func(p *page) {
		for i := 0; i < count; i++ {
			kv := kvList[i]
			v, exist, closer, _ := p.get(kvList[i].Key.UserKey, kvList[i].KeyHash)
			require.Equal(t, true, exist)
			require.Equal(t, kv.Value, v)
			closer()
		}
	}

	for i := 0; i < count; i++ {
		kv := kvList[i]
		seqNum++
		kv.Key.SetSeqNum(seqNum)
		require.NoError(t, wr.Set(*kv.Key, kv.Value))
	}
	require.NoError(t, wr.FlushFinish())
	readData(pg)

	for i := 0; i < 10; i++ {
		kv := kvList[i]
		seqNum++
		kv.Key.SetSeqNum(seqNum)
		kv.Key.SetKind(internalKeyKindDelete)
		kv.Value = []byte(nil)
		require.NoError(t, wr.Set(*kv.Key, kv.Value))
	}
	require.NoError(t, wr.FlushFinish())
	for i := 0; i < 10; i++ {
		v, exist, _, kind := pg.get(kvList[i].Key.UserKey, kvList[i].KeyHash)
		require.Equal(t, false, exist)
		require.Equal(t, internalKeyKindDelete, kind)
		require.Equal(t, kvList[i].Value, v)
	}

	require.NoError(t, pg.makeMutableForWrite())

	for i := 0; i < count; i++ {
		kv := kvList[i]
		seqNum++
		kv.Key.SetSeqNum(seqNum)
		kv.Key.SetKind(internalKeyKindSet)
		kv.Value = utils.FuncRandBytes(vsize)
		require.NoError(t, wr.Set(*kv.Key, kv.Value))
	}
	require.NoError(t, wr.FlushFinish())
	readData(pg)

	testCloseBitpage(t, bp)

	bp, err = testOpenBitpage()
	require.NoError(t, err)
	pg = bp.GetPage(pn)
	readData(pg)
	testCloseBitpage(t, bp)
}

func TestBitpageWriteRead(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage()
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
		v, vexist, vcloser, kind := p.get(kvList[i].Key.UserKey, kvList[i].KeyHash)
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
		v, vexist, vcloser, kind := p.get(kvList[i].Key.UserKey, kvList[i].KeyHash)
		require.Equal(t, true, vexist)
		require.Equal(t, kvList[i].Value, v)
		require.Equal(t, internalKeyKindSet, kind)
		vcloser()
	}

	testCloseBitpage(t, bp)
}

func TestBitpageWriterStat(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)
	count := 1000
	seqNum := uint64(1)
	kvList := testMakeSortedKV(count, seqNum, 10)
	seqNum += uint64(count)
	wr := bp.GetPageWriter(pn, nil)
	delKeyNum := 0
	pdKeyNum := 0
	for i := 0; i < count; i++ {
		if i%4 == 0 {
			kvList[i].Key.SetKind(internalKeyKindDelete)
			delKeyNum++
		} else if i%5 == 0 {
			kvList[i].Key.SetKind(internalKeyKindPrefixDelete)
			pdKeyNum++
		}
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	p := bp.GetPage(pn)
	totalCount, delCount, prefixDelCount := p.stMutable.getKeyStats()
	deleteRate := p.getDeleteKeyRate(totalCount, delCount, prefixDelCount)
	require.Equal(t, count, totalCount)
	require.Equal(t, delKeyNum, delCount)
	require.Equal(t, pdKeyNum, prefixDelCount)
	require.Equal(t, 1.75, deleteRate)

	testCloseBitpage(t, bp)
}

func TestBitpageStModTime(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage()
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
		kvList[i].Key.SetKind(internalKeyKindSet)
		writeKV(i)
	}
	require.NoError(t, wr.FlushFinish())

	p := bp.GetPage(pn)
	require.Equal(t, p.stQueue[len(p.stQueue)-1].getModTime(), p.stMutable.getModTime())
	testCloseBitpage(t, bp)

	bp, err = testOpenBitpage()
	require.NoError(t, err)
	p = bp.GetPage(pn)
	require.Equal(t, p.stQueue[len(p.stQueue)-1].getModTime(), p.stMutable.getModTime())
	testCloseBitpage(t, bp)

	require.Equal(t, false, consts.CheckFlushLifeTime(0, 0, 0))
	require.Equal(t, false, consts.CheckFlushLifeTime(time.Now().Unix(), 0, 0))
	require.Equal(t, true, consts.CheckFlushLifeTime(time.Now().Unix()-6*86400, 3, 10))
	require.Equal(t, false, consts.CheckFlushLifeTime(time.Now().Unix()-6*86400, 1, 10))
}

func TestBitpageStMmapExpand(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage()
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

	checkKey := func(pos int, key InternalKKVKey) {
		expKey := kkv.MakeInternalKey(*kvList[pos].Key)
		require.Equal(t, true, kkv.InternalKeyEqual(&expKey, &key))
	}

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
						v, vexist, vcloser, kind := p.get(uk, hash.Fnv32(uk))
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
							checkKey(pos, *ik)
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

	require.NoError(t, wr.FlushFinish())

	close(closeCh)
	wg.Wait()

	testCloseBitpage(t, bp)
}

func TestBitpageOpenPages(t *testing.T) {
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
				require.NoError(t, wr.FlushFinish())
				require.NoError(t, wr.p.makeMutableForWrite())
			}
		}
	}

	readPage := func(b *Bitpage, pn PageNum) {
		p := b.GetPage(pn)
		for i := 0; i < num; i++ {
			key := kvList[i].Key.UserKey
			v, vexist, vcloser, kind := p.get(key, hash.Fnv32(key))
			require.Equal(t, true, vexist)
			require.Equal(t, kvList[i].Value, v)
			require.Equal(t, internalKeyKindSet, kind)
			vcloser()
		}
	}

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	writePage(bp)
	testCloseBitpage(t, bp)

	bp1, err1 := testOpenBitpage()
	require.NoError(t, err1)
	for _, pn := range pageNums {
		readPage(bp1, pn)
	}
	writePage(bp1)
	testCloseBitpage(t, bp1)

	bp2, err2 := testOpenBitpage()
	require.NoError(t, err2)
	for _, pn := range pageNums {
		readPage(bp2, pn)
	}
	testCloseBitpage(t, bp2)
}

func TestBitpageOpenPagesMissFile(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	var pageNums []PageNum
	pagesCnt := 5
	stCnt := 2
	kvList := sortedkv.MakeSortedAllKVList(1000, 10, 1, 10, 1)
	count := len(kvList)
	step := count / stCnt
	deletePageNum := PageNum(5)

	writePage := func(b *Bitpage) {
		for i := 0; i < pagesCnt; i++ {
			pn, err1 := b.NewPage()
			require.NoError(t, err1)
			pageNums = append(pageNums, pn)
			wr := b.GetPageWriter(pn, nil)
			for j := 0; j < stCnt; j++ {
				start := j * step
				end := start + step
				for index := start; index < end; index++ {
					require.NoError(t, wr.Set(*kvList[index].Key, kvList[index].Value))
				}
				require.NoError(t, wr.FlushFinish())
				require.NoError(t, wr.p.flush(wr.sentinel))
			}
		}
	}

	readPage := func(b *Bitpage, pn PageNum) {
		p := b.GetPage(pn)
		for i := 0; i < count; i++ {
			v, vexist, vcloser, kind := p.get(kvList[i].Key.UserKey, kvList[i].KeyHash)
			if pn == deletePageNum {
				require.Equal(t, false, vexist)
			} else {
				require.Equal(t, true, vexist)
				if !bytes.Equal(kvList[i].Value, v) {
					t.Fatalf("value not eq i:%d key:%s", i, string(kvList[i].Key.UserKey))
				}
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
			}
		}
	}

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	writePage(bp)
	testCloseBitpage(t, bp)

	var stFile, atFile string
	stFile = filepath.Join(testDir, "5_3.xt")
	atFile = filepath.Join(testDir, "5_2.vat")
	require.NoError(t, os.Remove(stFile))
	require.NoError(t, os.Remove(atFile))

	bp1, err1 := testOpenBitpage()
	require.NoError(t, err1)
	require.Equal(t, true, os2.IsNotExist(stFile))
	require.Equal(t, true, os2.IsNotExist(atFile))
	for _, pn := range pageNums {
		readPage(bp1, pn)
	}
	writePage(bp1)
	testCloseBitpage(t, bp1)

	bp2, err2 := testOpenBitpage()
	require.NoError(t, err2)
	for _, pn := range pageNums {
		readPage(bp2, pn)
	}
	testCloseBitpage(t, bp2)
}

func TestBitpageOpenPagesMinUnflushed(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	seqNum := uint64(1)
	kvList := sortedkv.MakeSortedAllKVList(100, 10, 1, 10, seqNum)
	count := len(kvList)
	seqNum += uint64(count)
	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)
	wr := bp.GetPageWriter(pn, nil)
	for i := 0; i < 5; i++ {
		for j := 0; j < count; j++ {
			require.NoError(t, wr.Set(*kvList[j].Key, kvList[j].Value))
		}
		require.NoError(t, wr.FlushFinish())
		if i < 4 {
			require.NoError(t, bp.GetPage(pn).flush(nil))
		}
	}

	minUnflushedFn := bp.meta.getMinUnflushedStFileNum(pn)
	bp.meta.setMinUnflushedStFileNum(pn, minUnflushedFn+1)
	testCloseBitpage(t, bp)

	var stFiles []string
	bp, err = testOpenBitpage()
	require.NoError(t, err)
	minUnflushedFn = bp.meta.getMinUnflushedStFileNum(pn)
	require.Equal(t, FileNum(6), minUnflushedFn)
	stFiles = append(stFiles, filepath.Join(dir, "1_5.xt"),
		filepath.Join(dir, "1_5.xti"))
	time.Sleep(2 * time.Second)
	for _, f := range stFiles {
		require.Equal(t, false, os2.IsExist(f))
	}
	testCloseBitpage(t, bp)
}

func TestBitpageIterSameKey(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	bp, err := testOpenBitpage()
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

	checkKV := func(i int, ikey *InternalKKVKey, ival []byte) {
		require.Equal(t, kvList[i].Key.UserKey, ikey.MakeUserKey())
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
		require.Equal(t, count, i)
		require.NoError(t, iter.Close())

		iter = p.newIter(nil)
		i = len(kvList) - 1
		for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
			checkKV(i, ik, iv)
			i--
		}
		require.Equal(t, -1, i)
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

	bp2, err2 := testOpenBitpage()
	require.NoError(t, err2)
	pg2 := bp2.GetPage(pn)
	rangeFunc(pg2)
	seekFunc(pg2)
	testCloseBitpage(t, bp2)
}

func TestBitpageWriterDelete(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	bp, err := testOpenBitpage()
	require.NoError(t, err)
	kvList := sortedkv.MakeSortedAllKVList(100, 5, 1, 100, 1)
	count := len(kvList)
	seqNum := uint64(count)
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
		v, vexist, vcloser, kind := p.get(key, hash.Fnv32(key))
		require.Equal(t, true, vexist)
		if !bytes.Equal(kvList[i].Value, v) {
			t.Fatalf("value not eq i:%d key:%s", i, string(key))
		}
		require.Equal(t, internalKeyKindSet, kind)
		vcloser()
	}

	require.NoError(t, p.flush(nil))

	for i := 10; i < 20; i++ {
		kvList[i].Key.SetKind(internalKeyKindDelete)
		kvList[i].Key.SetSeqNum(seqNum)
		seqNum++
		writeKV(i)
	}
	require.NoError(t, wr.FlushFinish())

	for i := 0; i < count; i++ {
		v, vexist, vcloser, kind := p.get(kvList[i].Key.UserKey, kvList[i].KeyHash)
		if i >= 10 && i < 20 {
			require.Equal(t, true, vcloser == nil)
			require.Equal(t, false, vexist)
			require.Equal(t, internalKeyKindDelete, kind)
		} else {
			require.Equal(t, true, vexist)
			if !bytes.Equal(kvList[i].Value, v) {
				t.Fatalf("value not eq i:%d key:%s", i, string(kvList[i].Key.UserKey))
			}
			require.Equal(t, internalKeyKindSet, kind)
			vcloser()
		}
	}

	testCloseBitpage(t, bp)
}

func testHashcase(caseFunc func([]bool, int)) {
	for _, params := range [][]bool{
		{true, true},
		{true, false},
	} {
		for _, kkn := range []int{1, 2, 3, 10} {
			fmt.Printf("testcase isUseVi:%v isUseMiniVi:%v kkn:%d\n", params[0], params[1], kkn)
			caseFunc(params, kkn)
		}
	}
}

func testZsetcase(caseFunc func([]bool, int)) {
	for _, params := range [][]bool{
		{true, true},
		{true, false},
		{false, true},
	} {
		for _, kkn := range []int{1, 2, 3, 10} {
			fmt.Printf("testcase isUseVi:%v isUseMiniVi:%v kkn:%d\n", params[0], params[1], kkn)
			caseFunc(params, kkn)
		}
	}
}

func TestBitpageHashIter(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	testHashcase(func(params []bool, kkn int) {
		os.RemoveAll(dir)
		bp, err := testOpen(params[0], params[1])
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		wr := bp.GetPageWriter(pn, nil)
		seqNum := uint64(1)
		kn := 1000
		kvList := sortedkv.MakeSortedHashKVList(kn, kkn, 10, seqNum)
		count := len(kvList) - kkn
		seqNum += uint64(count)

		rangeIter := func(p *page) {
			iter := p.newIter(nil)
			i := 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, ik.MakeUserKey())
				require.Equal(t, kvList[i].Value, iv)
				i++
			}
			require.Equal(t, i, count)
			require.NoError(t, iter.Close())

			for i = 0; i < count; i += kkn {
				kv := kvList[i]
				dt := kkv.GetKeyDataType(kv.Key.UserKey)
				ver := kkv.DecodeKeyVersion(kv.Key.UserKey)
				var lowerBound [kkv.SubKeyHeaderLength]byte
				var upperBound [kkv.SubKeyUpperBoundLength]byte
				kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, ver)
				kkv.EncodeSubKeyUpperBound(upperBound[:], dt, ver)
				opts := &iterOptions{
					DataType:   dt,
					LowerBound: lowerBound[:],
					UpperBound: upperBound[:],
					IsTest:     true,
				}
				iter = p.newIter(opts)
				ik, iv := iter.First()
				require.Equal(t, kv.Key.UserKey, ik.MakeUserKey())
				require.Equal(t, kv.Value, iv)
				require.NoError(t, iter.Close())
			}
		}

		for i := 0; i < count; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		pg := bp.GetPage(pn)
		rangeIter(pg)
		require.NoError(t, pg.flush(nil))
		rangeIter(pg)

		for i := 0; i < count; i++ {
			if i%3 == 0 {
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				kvList[i].Value = utils.FuncRandBytes(10)
				require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
			}
		}
		require.NoError(t, wr.FlushFinish())

		rangeIter(pg)
		testCloseBitpage(t, bp)

		bp2, err2 := testOpen(params[0], params[1])
		require.NoError(t, err2)
		pg2 := bp2.GetPage(pn)
		rangeIter(pg2)
		testCloseBitpage(t, bp2)
	})
}

func TestBitpageSetIter(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	testHashcase(func(params []bool, kkn int) {
		os.RemoveAll(dir)
		bp, err := testOpen(params[0], params[1])
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		wr := bp.GetPageWriter(pn, nil)
		seqNum := uint64(1)
		kn := 10000
		kvList := sortedkv.MakeSortedSetKVList(kn, kkn, seqNum)
		count := len(kvList) - kkn
		seqNum += uint64(count)

		rangeIter := func(p *page) {
			iter := p.newIter(nil)
			i := 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, ik.MakeUserKey())
				require.Equal(t, 0, len(iv))
				i++
			}
			require.Equal(t, i, count)
			require.NoError(t, iter.Close())

			for i = 0; i < count; i += kkn {
				kv := kvList[i]
				dt := kkv.GetKeyDataType(kv.Key.UserKey)
				ver := kkv.DecodeKeyVersion(kv.Key.UserKey)
				var lowerBound [kkv.SubKeyHeaderLength]byte
				var upperBound [kkv.SubKeyUpperBoundLength]byte
				kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, ver)
				kkv.EncodeSubKeyUpperBound(upperBound[:], dt, ver)
				opts := &iterOptions{
					DataType:   dt,
					LowerBound: lowerBound[:],
					UpperBound: upperBound[:],
					IsTest:     true,
				}
				iter = p.newIter(opts)
				ik, iv := iter.First()
				require.Equal(t, kv.Key.UserKey, ik.MakeUserKey())
				require.Equal(t, kv.Key.Kind(), ik.Kind())
				require.Equal(t, 0, len(iv))
				require.NoError(t, iter.Close())
			}
		}

		for i := 0; i < count; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		pg := bp.GetPage(pn)
		rangeIter(pg)
		require.NoError(t, pg.flush(nil))
		rangeIter(pg)

		for i := 0; i < count; i++ {
			if i%3 == 0 {
				kvList[i].Key.SetKind(internalKeyKindDelete)
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
			}
		}
		require.NoError(t, wr.FlushFinish())

		rangeIter(pg)
		testCloseBitpage(t, bp)

		bp2, err2 := testOpen(params[0], params[1])
		require.NoError(t, err2)
		pg2 := bp2.GetPage(pn)
		rangeIter(pg2)
		testCloseBitpage(t, bp2)
	})
}

func TestBitpageListIter(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	for _, dt := range []uint8{
		kkv.DataTypeList,
		kkv.DataTypeBitmap,
	} {
		testZsetcase(func(params []bool, kkn int) {
			os.RemoveAll(dir)
			bp, err := testOpen(params[0], params[1])
			require.NoError(t, err)
			pn, err1 := bp.NewPage()
			require.NoError(t, err1)
			wr := bp.GetPageWriter(pn, nil)
			seqNum := uint64(1)
			kn := 10
			kvList := sortedkv.MakeSortedListKVList(kn, kkn, dt, seqNum, 10)
			count := len(kvList) - kkn
			seqNum += uint64(count)

			newPageIter := func(p *page) *PageIterator {
				opts := &iterOptions{
					DataType: dt,
				}
				iter := p.newIter(opts)
				return iter
			}

			rangeIter := func(p *page) {
				iter := newPageIter(p)
				i := 0
				for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
					require.Equal(t, kvList[i].Key.UserKey, ik.MakeUserKey())
					require.Equal(t, kvList[i].Value, iv)
					i++
				}
				require.Equal(t, i, count)
				require.NoError(t, iter.Close())

				iter = newPageIter(p)
				for i = 0; i < count; i++ {
					ik, iv := iter.SeekGE(kvList[i].Key.UserKey)
					require.Equal(t, kvList[i].Key.UserKey, ik.MakeUserKey())
					require.Equal(t, kvList[i].Value, iv)
				}
				require.NoError(t, iter.Close())
			}

			rangeReverseIter := func(p *page) {
				for i := count; i >= 0; i -= kkn {
					ver := kkv.DecodeKeyVersion(kvList[i].Key.UserKey)
					var lowerBound [kkv.SubKeyListLength]byte
					var upperBound [kkv.SubKeyListLength]byte
					kkv.EncodeListKey(lowerBound[:], dt, ver, 0)
					kkv.EncodeListKey(upperBound[:], dt, ver, uint64(kkn))
					opts := &iterOptions{
						LowerBound: lowerBound[:],
						UpperBound: upperBound[:],
						DataType:   dt,
						IsTest:     true,
					}
					iter := p.newIter(opts)
					ik, iv := iter.SeekLT(kvList[i].Key.UserKey)
					require.Equal(t, nilInternalKKVKey, ik)
					require.Equal(t, []byte(nil), iv)

					if kkn > 1 {
						ik, iv = iter.SeekLT(kvList[i+1].Key.UserKey)
						if i == count {
							require.Equal(t, nilInternalKKVKey, ik)
							require.Equal(t, []byte(nil), iv)
						} else {
							require.Equal(t, kvList[i].Key.UserKey, ik.MakeUserKey())
							require.Equal(t, kvList[i].Value, iv)
						}
					}

					maxKey := sortedkv.MakeSortedListKey(ver, dt, kkn)
					ik, iv = iter.SeekLT(maxKey)
					if i == count {
						require.Equal(t, nilInternalKKVKey, ik)
						require.Equal(t, []byte(nil), iv)
					} else {
						require.Equal(t, kvList[i+kkn-1].Key.UserKey, ik.MakeUserKey())
						require.Equal(t, kvList[i+kkn-1].Value, iv)
					}

					require.NoError(t, iter.Close())
				}
			}

			for i := 0; i < count; i++ {
				require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, wr.FlushFinish())

			pg := bp.GetPage(pn)
			rangeIter(pg)
			rangeReverseIter(pg)
			require.NoError(t, pg.flush(nil))
			rangeIter(pg)
			rangeReverseIter(pg)

			for i := 0; i < count; i++ {
				if i%3 == 0 {
					kvList[i].Key.SetSeqNum(seqNum)
					seqNum++
					kvList[i].Value = utils.FuncRandBytes(10)
					require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
				}
			}
			require.NoError(t, wr.FlushFinish())

			rangeIter(pg)
			rangeReverseIter(pg)

			testCloseBitpage(t, bp)

			bp2, err2 := testOpen(params[0], params[1])
			require.NoError(t, err2)
			pg2 := bp2.GetPage(pn)
			rangeIter(pg2)
			rangeReverseIter(pg2)
			testCloseBitpage(t, bp2)
		})
	}
}

func TestBitpageZsetIndexIter(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	testZsetcase(func(params []bool, kkn int) {
		os.RemoveAll(dir)
		bp, err := testOpen(params[0], params[1])
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		wr := bp.GetPageWriter(pn, nil)
		seqNum := uint64(1)
		dt := kkv.DataTypeZsetIndex
		kn := 20
		mn := kkn
		kstep := kkn * mn
		kvList := sortedkv.MakeSortedZsetKVList(kn, kkn, mn, seqNum)
		seqNum += uint64(len(kvList))
		count := len(kvList) - kstep
		delNum := 0

		for i := 0; i < count; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		pg := bp.GetPage(pn)
		require.NoError(t, pg.flush(nil))
		for i := 0; i < count; i++ {
			if i%kstep == 0 {
				kvList[i].Key.SetKind(internalKeyKindDelete)
				kvList[i].Key.SetSeqNum(seqNum)
				seqNum++
				delNum++
				require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
			}
		}
		require.NoError(t, wr.FlushFinish())

		checkKV := func(i int, ikey *InternalKKVKey, ival []byte) {
			if i < 0 {
				require.Equal(t, nilInternalKKVKey, ikey)
			} else {
				require.Equal(t, kvList[i].Key.UserKey, ikey.MakeUserKey())
				if i%kstep == 0 {
					require.Equal(t, internalKeyKindDelete, ikey.Kind())
				} else {
					require.Equal(t, internalKeyKindSet, ikey.Kind())
				}
			}
			require.Equal(t, 0, len(ival))
		}

		rangeFunc := func(p *page) {
			opts := &iterOptions{
				DataType: dt,
				IsTest:   true,
			}
			iter := p.newIter(opts)
			setCnt := 0
			delCnt := 0
			pos := 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				checkKV(pos, ik, iv)
				if ik.Kind() == internalKeyKindSet {
					setCnt++
				} else if ik.Kind() == internalKeyKindDelete {
					delCnt++
				}
				pos++
			}
			require.Equal(t, count-delNum, setCnt)
			require.Equal(t, delNum, delCnt)

			setCnt = 0
			delCnt = 0
			pos = count - 1
			lastKey := kvList[pos].Key
			lastKeyVer := kkv.DecodeKeyVersion(lastKey.UserKey)
			var lowerBound [kkv.SubKeyHeaderLength]byte
			var upperBound [kkv.SubKeyUpperBoundLength]byte
			kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, lastKeyVer)
			kkv.EncodeSubKeyUpperBound(upperBound[:], dt, lastKeyVer)
			opts = &iterOptions{
				LowerBound: lowerBound[:],
				UpperBound: upperBound[:],
				DataType:   dt,
				IsTest:     true,
			}
			iter = p.newIter(opts)
			for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
				checkKV(pos, ik, iv)
				if ik.Kind() == internalKeyKindSet {
					setCnt++
				} else if ik.Kind() == internalKeyKindDelete {
					delCnt++
				}
				pos--
			}
			require.Equal(t, kstep-1, setCnt)
			require.Equal(t, 1, delCnt)
			require.NoError(t, iter.Close())
		}

		seekFunc := func(p *page) {
			for i := 0; i <= count; i += kstep {
				for j := i; j < i+kstep; j++ {
					ver := kkv.DecodeKeyVersion(kvList[j].Key.UserKey)
					var lowerBound [kkv.SubKeyHeaderLength]byte
					var upperBound [kkv.SubKeyUpperBoundLength]byte
					kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, ver)
					kkv.EncodeSubKeyUpperBound(upperBound[:], dt, ver)
					iterOpts := &iterOptions{
						LowerBound: lowerBound[:],
						UpperBound: upperBound[:],
						DataType:   dt,
						IsTest:     true,
					}
					iter := p.newIter(iterOpts)
					ik, iv := iter.SeekGE(kvList[j].Key.UserKey)
					if i == count {
						checkKV(-1, ik, iv)
					} else {
						checkKV(j, ik, iv)
					}

					ik, iv = iter.SeekLT(kvList[j].Key.UserKey)
					if j%kstep == 0 || kstep == 1 || i == count {
						checkKV(-1, ik, iv)
					} else {
						checkKV(j-1, ik, iv)
					}

					require.NoError(t, iter.Close())
				}
			}
		}

		rangeFunc(pg)
		seekFunc(pg)
		testCloseBitpage(t, bp)

		bp2, err2 := testOpen(params[0], params[1])
		require.NoError(t, err2)
		pg2 := bp2.GetPage(pn)
		rangeFunc(pg2)
		seekFunc(pg2)
		testCloseBitpage(t, bp2)
	})
}

func TestBitpageExpireIter(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)

	for _, kkn := range []int{1, 2, 3, 10} {
		os.RemoveAll(dir)
		bp, err := testOpen(false, false)
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		wr := bp.GetPageWriter(pn, nil)
		seqNum := uint64(1)
		dt := kkv.DataTypeExpireKey
		kn := 1000
		kvList := sortedkv.MakeSortedExpireKVList(kn, kkn, seqNum)
		count := len(kvList) - kkn
		seqNum += uint64(count)

		rangeIter := func(p *page) {
			iter := p.newIter(nil)
			i := 0
			for ik, iv := iter.First(); ik != nil; ik, iv = iter.Next() {
				require.Equal(t, kvList[i].Key.UserKey, ik.MakeUserKey())
				require.Equal(t, kvList[i].Value, iv)
				i++
			}
			require.Equal(t, i, count)
			require.NoError(t, iter.Close())

			iter = p.newIter(&iterOptions{DataType: dt})
			for i = 0; i < count; i += kkn {
				ik, iv := iter.SeekGE(kvList[i].Key.UserKey)
				require.Equal(t, kvList[i].Key.UserKey, ik.MakeUserKey())
				require.Equal(t, kvList[i].Value, iv)
			}
			require.NoError(t, iter.Close())
		}

		for i := 0; i < count; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		pg := bp.GetPage(pn)
		rangeIter(pg)
		require.NoError(t, pg.flush(nil))
		rangeIter(pg)

		require.NoError(t, wr.FlushFinish())

		rangeIter(pg)
		testCloseBitpage(t, bp)

		bp2, err2 := testOpenBitpage()
		require.NoError(t, err2)
		pg2 := bp2.GetPage(pn)
		rangeIter(pg2)
		testCloseBitpage(t, bp2)
	}
}

func TestBitpageCheckpoint(t *testing.T) {
	srcDir := testDir
	defer os.RemoveAll(srcDir)
	os.RemoveAll(srcDir)
	openBitpage := func(dir string) *Bitpage {
		testInitDir()
		opts := testInitOpts()
		b, err := Open(dir, opts)
		require.NoError(t, err)
		return b
	}
	bp := openBitpage(srcDir)
	pn, err := bp.NewPage()
	require.NoError(t, err)
	pg := bp.GetPage(pn)
	dstDir := fmt.Sprintf("%s_ck", srcDir)
	os.RemoveAll(dstDir)
	defer os.RemoveAll(dstDir)
	require.NoError(t, bp.Checkpoint(bp.opts.FS, dstDir))
	bp1 := openBitpage(dstDir)
	pg1 := bp1.GetPage(pn)
	require.Equal(t, 1, len(pg.stQueue))
	require.Equal(t, 1, len(pg1.stQueue))
	require.Equal(t, true, pg.arrtable == nil)
	require.Equal(t, true, pg1.arrtable == nil)
	require.NoError(t, bp1.Close())
	require.NoError(t, os.RemoveAll(dstDir))

	wr := bp.GetPageWriter(pn, nil)
	seqNum := uint64(1)
	kn := 100
	kkn := 100
	mn := 1
	vs := 100
	kvList := sortedkv.MakeSortedAllKVList(kn, kkn, mn, vs, seqNum)
	keyNum := len(kvList)
	for i := 0; i < keyNum/2; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())
	require.NoError(t, pg.flush(nil))
	for i := keyNum / 2; i < keyNum; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	for i := 0; i < 1; i++ {
		dstDir = fmt.Sprintf("%s_ck_%d", srcDir, i)
		require.NoError(t, os.RemoveAll(dstDir))
		require.NoError(t, bp.Checkpoint(bp.opts.FS, dstDir))
		bp1 = openBitpage(dstDir)
		pg1 = bp1.GetPage(pn)
		require.Equal(t, 2, len(pg.stQueue))
		require.Equal(t, 1, len(pg1.stQueue))
		require.Equal(t, true, pg.arrtable != nil)
		require.Equal(t, true, pg1.arrtable != nil)
		iter1 := pg1.newIter(&iterOptions{DataType: kkv.DataTypeZsetIndex})
		iter := pg.newIter(&iterOptions{DataType: kkv.DataTypeZsetIndex})
		for j := 0; j < keyNum; j++ {
			kv := kvList[j]
			dt := kkv.GetKeyDataType(kv.Key.UserKey)
			switch dt {
			case kkv.DataTypeZsetIndex:
				ik, iv := iter1.SeekGE(kkv.GetSiPrefix(kv.Key.UserKey))
				require.Equal(t, kv.Key.UserKey, ik.MakeUserKey())
				require.Equal(t, 0, len(iv))
				ik, iv = iter.SeekGE(kkv.GetSiPrefix(kv.Key.UserKey))
				require.Equal(t, kv.Key.UserKey, ik.MakeUserKey())
				require.Equal(t, 0, len(iv))
			case kkv.DataTypeExpireKey:
			default:
				v, vexist, vcloser, kind := pg1.get(kv.Key.UserKey, kv.KeyHash)
				require.Equal(t, true, vexist)
				if !bytes.Equal(kv.Value, v) {
					t.Fatalf("value not eq i:%d key:%s", i, string(kvList[i].Key.UserKey))
				}
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
				v, vexist, vcloser, kind = pg.get(kv.Key.UserKey, kv.KeyHash)
				require.Equal(t, true, vexist)
				if !bytes.Equal(kv.Value, v) {
					t.Fatalf("value not eq i:%d key:%s", i, string(kvList[i].Key.UserKey))
				}
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
			}
		}
		iter.Close()
		iter1.Close()
		testCloseBitpage(t, bp1)
		require.NoError(t, os.RemoveAll(dstDir))
	}

	testCloseBitpage(t, bp)
}

func TestBitpageCheckpoint1(t *testing.T) {
	srcDir := testDir
	defer os.RemoveAll(srcDir)
	os.RemoveAll(srcDir)
	openBitpage := func(dir string) *Bitpage {
		testInitDir()
		opts := testInitOpts()
		b, err := Open(dir, opts)
		require.NoError(t, err)
		return b
	}
	bp := openBitpage(srcDir)
	pn, err := bp.NewPage()
	require.NoError(t, err)
	wr := bp.GetPageWriter(pn, nil)
	seqNum := uint64(1)
	kn := 100
	kkn := 100
	mn := 1
	vs := 100
	kvList := sortedkv.MakeSortedAllKVList(kn, kkn, mn, vs, seqNum)
	keyNum := len(kvList)

	for i := 0; i < keyNum/2; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	pg := bp.GetPage(pn)
	require.NoError(t, pg.flush(nil))

	for i := keyNum / 2; i < keyNum; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	sps, err2 := bp.PageSplitStart(pn, "test")
	require.NoError(t, err2)
	bp.PageSplitEnd(pn, sps, nil)

	dstDir := fmt.Sprintf("%s_ck", srcDir)
	os.RemoveAll(dstDir)
	defer os.RemoveAll(dstDir)
	require.NoError(t, bp.Checkpoint(bp.opts.FS, dstDir))
	require.Equal(t, consts.BitpageSplitNum+1, bp.GetPageCount())
	require.Equal(t, true, bp.IsPageFreed(pn))
	require.Equal(t, true, pg.arrtable != nil)
	bp1 := openBitpage(dstDir)
	pg1 := bp1.GetPage(pn)
	require.Equal(t, true, pg1 == nil)
	for i := range sps {
		spn := bp1.GetPage(sps[i].Pn)
		require.Equal(t, true, spn != nil)
		require.Equal(t, 1, len(spn.stQueue))
		require.Equal(t, true, spn.arrtable != nil)
	}

	for j := 0; j < keyNum; j++ {
		kv := kvList[j]
		key := kv.Key.UserKey
		dt := kkv.GetKeyDataType(key)
		switch dt {
		case kkv.DataTypeZsetIndex:
			iter := pg.newIter(&iterOptions{DataType: kkv.DataTypeZsetIndex})
			ik, iv := iter.SeekGE(kkv.GetSiPrefix(key))
			require.Equal(t, key, ik.MakeUserKey())
			require.Equal(t, 0, len(iv))
			require.NoError(t, iter.Close())
		case kkv.DataTypeExpireKey:
		default:
			v, vexist, vcloser, kind := pg.get(key, kv.KeyHash)
			require.Equal(t, true, vexist)
			if !bytes.Equal(kv.Value, v) {
				t.Fatalf("value not eq i:%d key:%s", j, string(key))
			}
			require.Equal(t, internalKeyKindSet, kind)
			vcloser()
		}

		find := false
		for i := range sps {
			if bytes.Compare(key, sps[i].Sentinel) <= 0 {
				spn := sps[i].Pn
				switch dt {
				case kkv.DataTypeZsetIndex:
					iter1 := bp1.NewIter(spn, &iterOptions{DataType: kkv.DataTypeZsetIndex})
					ik, iv := iter1.SeekGE(kkv.GetSiPrefix(key))
					require.Equal(t, key, ik.MakeUserKey())
					require.Equal(t, 0, len(iv))
					require.NoError(t, iter1.Close())
				case kkv.DataTypeExpireKey:
				default:
					v, vexist, vcloser, kind := bp1.Get(spn, key, kv.KeyHash)
					require.Equal(t, true, vexist)
					if !bytes.Equal(kv.Value, v) {
						t.Fatalf("value not eq key:%s", string(key))
					}
					require.Equal(t, internalKeyKindSet, kind)
					vcloser()
				}

				find = true
				break
			}
		}
		require.Equal(t, true, find)
	}

	testCloseBitpage(t, bp)
}
