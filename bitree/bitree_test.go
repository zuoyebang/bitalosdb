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

package bitree

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/bitpage"
	"github.com/zuoyebang/bitalosdb/v2/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
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
const testBitreeIndex = 1

var testBtree *Bitree
var bpageTask *testBitpageTask

type testBitpageTask struct {
	task   *bitask.BitpageTask
	taskWg sync.WaitGroup
}

var testBithashSize = 128 << 20

func makeTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitree_key_%d", i))
}

func testNewBitreePages(t *Bitree) error {
	err := t.bdb.Update(func(tx *bdb.Tx) error {
		bkt := tx.Bucket(consts.BdbBucketName)
		if bkt == nil {
			return bdb.ErrBucketNotFound
		}
		for j := 1; j < 10; j++ {
			key := makeTestKey(j)
			pn, err := t.bpage.NewPage()
			if err != nil {
				return err
			}
			if err = bkt.Put(key, pn.ToByte()); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func resetTestOptsVal() {
	testBithashSize = 128 << 20
}

func testDoBitpageTask(task *bitask.BitpageTaskData) {
	testBtree.DoBitpageTask(task)
}

func testBitreeClose(bt *Bitree) error {
	bpageTask.task.Close()
	bpageTask.taskWg.Wait()
	err := bt.Close()
	bt.opts.DeleteFilePacer.Close()
	return err
}

func testOpenKKVBitree() (*Bitree, *options.BitreeOptions) {
	defer func() {
		resetTestOptsVal()
	}()
	optsPool := options.InitTestOptionsPool()
	optsPool.BaseOptions.BitpageFlushSize = 5 << 20
	optsPool.BaseOptions.BitpageSplitSize = 10 << 20
	return testOpenKKVBitree1(optsPool)
}

func testOpenKKVBitree1(optsPool *options.OptionsPool) (*Bitree, *options.BitreeOptions) {
	var err error
	bpageTask = &testBitpageTask{}
	bpageTask.task = bitask.NewBitpageTask(&bitask.BitpageTaskOptions{
		Size:      consts.BitpageTaskSize,
		WorkerNum: consts.BitpageTaskWorkerNum,
		Logger:    optsPool.BaseOptions.Logger,
		DoFunc:    testDoBitpageTask,
		TaskWg:    &bpageTask.taskWg,
	})
	optsPool.BaseOptions.BitpageTaskPushFunc = bpageTask.task.PushTask
	optsPool.BithashOptions.TableMaxSize = testBithashSize
	bitreeOpts := optsPool.CloneBitreeOptions()
	bitreeOpts.Index = testBitreeIndex
	testBtree, err = NewBitree(testDir, bitreeOpts)
	if err != nil {
		panic(err)
	}
	return testBtree, bitreeOpts
}

func TestBitreeWriteRead(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	btree, _ := testOpenKKVBitree()
	kvList := sortedkv.MakeSortedAllKVList(1000, 10, 1, 1000, uint64(0))
	keyCount := len(kvList)

	bw, err := btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 0; i < keyCount; i++ {
		require.NoError(t, bw.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, bw.Finish(true, false))
	require.NoError(t, testBitreeClose(btree))
	btree, _ = testOpenKKVBitree()
	for i := 0; i < keyCount; i++ {
		v, exist, vcloser := btree.Get(kvList[i].Key.UserKey, hash.Fnv32(kvList[i].Key.UserKey))
		require.Equal(t, true, exist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}
	require.NoError(t, testBitreeClose(btree))
}

func TestBitpagePreFlush(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	btree, _ := testOpenKKVBitree()
	step := 10000
	kvList := sortedkv.MakeSortedAllKVList(1000, 10, 1, 1000, uint64(0))

	var start, end int
	for i := 0; i < 5; i++ {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		start = i * step
		end = start + step
		for j := start; j < end; j++ {
			require.NoError(t, bw.Set(*kvList[j].Key, kvList[j].Value))
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree()
	for i := 0; i < end; i++ {
		ik := kvList[i].Key
		dt := kkv.GetKeyDataType(ik.UserKey)
		if dt == kkv.DataTypeZsetIndex {
			continue
		}
		v, exist, vcloser := btree.Get(ik.UserKey, hash.Fnv32(ik.UserKey))
		require.Equal(t, true, exist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}
	require.NoError(t, testBitreeClose(btree))
}

func TestBitpageFlushDeleteKeyRate(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	optsPool := options.InitTestOptionsPool()
	optsPool.BaseOptions.BitpageFlushSize = 40 << 10
	btree, _ := testOpenKKVBitree1(optsPool)
	require.NoError(t, testNewBitreePages(btree))

	var pageNum bitpage.PageNum
	kvList := sortedkv.MakeSortedAllKVList(1000, 10, 5, 100, uint64(1))
	keyCount := len(kvList)
	bw, err := btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 0; i < keyCount; i++ {
		k := makeTestKey(i)
		pn, sentinel, closer := btree.FindKeyPageNum(k)
		require.NotEqual(t, nilPageNum, pn)
		if i%2 == 0 || i > 700 {
			kvList[i].Key.SetKind(internalKeyKindDelete)
		}
		require.NoError(t, bw.setBitpage(*kvList[i].Key, kvList[i].Value, pn, sentinel))
		pageNum = pn
		closer()
	}

	deleteKeyRate := btree.bpage.GetPageStMutableDeleteKeyRate(pageNum)
	if deleteKeyRate < 0.5 {
		t.Fatalf("deleteKeyRate %.2f >= 0.5", deleteKeyRate)
	}
	require.NoError(t, bw.Finish(true, false))
	time.Sleep(2 * time.Second)
	require.Equal(t, uint64(1), btree.opts.DbState.GetBitpageFlushCount())

	for i := 0; i < keyCount; i++ {
		k := kvList[i].Key.UserKey
		v, exist, vcloser := btree.Get(k, hash.Fnv32(k))
		if i%2 == 0 || i > 700 {
			require.Equal(t, false, exist)
		} else {
			require.Equal(t, true, exist)
			require.Equal(t, kvList[i].Value, v)
			vcloser()
		}
	}

	require.NoError(t, testBitreeClose(btree))
}

func TestBitpageFlushEmpty(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	optsPool := options.InitTestOptionsPool()
	optsPool.BaseOptions.BitpageFlushSize = 1 << 20
	optsPool.BaseOptions.BitpageSplitSize = 4 << 20
	btree, _ := testOpenKKVBitree1(optsPool)

	kn := 5
	kkn := 10000
	kvList := sortedkv.MakeSortedHashKVList(kn, kkn, 200, uint64(0))
	keyCount := len(kvList)
	seqNum := uint64(keyCount)
	for i := 0; i < kn; i++ {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for j := i * kkn; j < (i+1)*kkn; j++ {
			item := kvList[j]
			require.NoError(t, bw.Set(*item.Key, item.Value))
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(2 * time.Second)
	}

	for i := 0; i < 5; i++ {
		k := kvList[i].Key.UserKey
		v, exist, vcloser := btree.Get(k, hash.Fnv32(k))
		require.Equal(t, true, exist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}

	pnMap1 := make(map[uint32]string)
	pnMap2 := make(map[uint32]string)

	rtx := btree.txPool.Load()
	cursor := rtx.Bucket().Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		pn := utils.BytesToUint32(v)
		pnMap1[pn] = string(k)
	}
	require.NoError(t, rtx.Unref(false))

	bw, err := btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 2; i < kn-1; i++ {
		for j := i * kkn; j < (i+1)*kkn; j++ {
			kvList[j].Key.SetKind(internalKeyKindDelete)
			seqNum++
			kvList[j].Key.SetSeqNum(seqNum)
			kvList[j].Value = nil
			require.NoError(t, bw.Set(*kvList[j].Key, nil))
		}
	}
	require.NoError(t, bw.Finish(true, true))
	time.Sleep(2 * time.Second)
	expDelPn := []uint32{6, 8}
	for _, pn := range expDelPn {
		delete(pnMap1, pn)
	}

	rtx = btree.txPool.Load()
	cursor = rtx.Bucket().Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		pn := utils.BytesToUint32(v)
		pnMap2[pn] = string(k)
	}
	require.NoError(t, rtx.Unref(false))

	require.Equal(t, pnMap1, pnMap2)

	var isExist bool
	for i := 0; i < kn; i++ {
		if i < 2 || i == kn-1 {
			isExist = true
		} else {
			isExist = false
		}
		for j := i * kkn; j < (i+1)*kkn; j++ {
			k := kvList[j].Key.UserKey
			v, exist, vcloser := btree.Get(k, hash.Fnv32(k))
			require.Equal(t, isExist, exist)
			require.Equal(t, kvList[j].Value, v)
			if exist {
				vcloser()
			}
		}
	}

	require.NoError(t, testBitreeClose(btree))
}

func TestBitpageFlushEmptyMaxSentinel(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	optsPool := options.InitTestOptionsPool()
	optsPool.BaseOptions.BitpageFlushSize = 1 << 20
	optsPool.BaseOptions.BitpageSplitSize = 4 << 20
	btree, _ := testOpenKKVBitree1(optsPool)

	kn := 2
	kkn := 10
	kvList := sortedkv.MakeSortedHashKVList(kn, kkn, 200, uint64(0))
	keyCount := len(kvList)
	seqNum := uint64(keyCount)
	for i := 0; i < kn; i++ {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for j := i * kkn; j < (i+1)*kkn; j++ {
			item := kvList[j]
			require.NoError(t, bw.Set(*item.Key, item.Value))
		}
		require.NoError(t, bw.Finish(true, true))
		time.Sleep(1 * time.Second)
	}

	pnMap1 := make(map[uint32]string)
	pnMap2 := make(map[uint32]string)

	rtx := btree.txPool.Load()
	cursor := rtx.Bucket().Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		pn := utils.BytesToUint32(v)
		pnMap1[pn] = string(k)
	}
	require.NoError(t, rtx.Unref(false))

	bw, err := btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := 0; i < kn; i++ {
		for j := i * kkn; j < (i+1)*kkn; j++ {
			kvList[j].Key.SetKind(internalKeyKindDelete)
			seqNum++
			kvList[j].Key.SetSeqNum(seqNum)
			kvList[j].Value = nil
			require.NoError(t, bw.Set(*kvList[j].Key, nil))
		}
	}
	require.NoError(t, bw.Finish(true, true))
	time.Sleep(1 * time.Second)
	expDelPn := []uint32{6, 8}
	for _, pn := range expDelPn {
		delete(pnMap1, pn)
	}

	rtx = btree.txPool.Load()
	cursor = rtx.Bucket().Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		pn := utils.BytesToUint32(v)
		pnMap2[pn] = string(k)
	}
	require.NoError(t, rtx.Unref(false))

	require.Equal(t, pnMap1, pnMap2)

	require.True(t, os2.IsNotExist(testDir+"/bitpage.1/1_3.xt"))
	require.True(t, os2.IsNotExist(testDir+"/bitpage.1/1_2.vat"))
	require.True(t, os2.IsNotExist(testDir+"/bitpage.1/1_2.vati"))

	require.NoError(t, testBitreeClose(btree))
}

func TestBitpageMultiPagePrefixDelete(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	optsPool := options.InitTestOptionsPool()
	optsPool.BaseOptions.BitpageFlushSize = 1 << 20
	optsPool.BaseOptions.BitpageSplitSize = 4 << 20
	btree, _ := testOpenKKVBitree1(optsPool)

	var pdVers []uint64
	seqNum := uint64(1)
	num := 3000
	oneKeyVersion := uint64(100)
	oneKeyNum := 10000
	var kvList sortedkv.SortedKVList

	makeKV := func(version uint64, i int) {
		key := sortedkv.MakeKKVSubKey(kkv.DataTypeHash, version, i)
		ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindSet)
		kvList = append(kvList, &sortedkv.SortedKVItem{
			Key:   &ikey,
			Value: utils.FuncRandBytes(500),
		})
		seqNum++
	}

	for i := 1; i <= num; i++ {
		version := uint64(i)
		if version == oneKeyVersion {
			for j := 0; j < oneKeyNum; j++ {
				makeKV(version, j)
			}
		} else {
			for j := 0; j < 10; j++ {
				makeKV(version, j)
			}
		}
	}

	sort.Sort(kvList)
	count := int(seqNum - 1)
	loop := count / 10

	for i := 0; i < count; i += loop {
		bw, err := btree.NewBitreeWriter()
		require.NoError(t, err)
		for j := i; j < i+loop; j++ {
			require.NoError(t, bw.Set(*kvList[j].Key, kvList[j].Value))
		}
		require.NoError(t, bw.Finish(true, false))
		time.Sleep(1 * time.Second)
	}

	for i := 0; i < count; i++ {
		k := kvList[i].Key.UserKey
		v, exist, vcloser := btree.Get(k, hash.Fnv32(k))
		require.Equal(t, true, exist)
		require.Equal(t, kvList[i].Value, v)
		vcloser()
	}

	pdVers = []uint64{40, 76, 150, oneKeyVersion}
	var pdKeyList sortedkv.SortedKVList
	for _, version := range pdVers {
		ikey := sortedkv.MakePrefixDeleteKey(version, seqNum)
		pdKeyList = append(pdKeyList, &sortedkv.SortedKVItem{
			Key:   &ikey,
			Value: nil,
		})
		seqNum++
	}
	sort.Sort(pdKeyList)
	bw, err := btree.NewBitreeWriter()
	require.NoError(t, err)
	for i := range pdKeyList {
		require.NoError(t, bw.Set(*pdKeyList[i].Key, pdKeyList[i].Value))
	}
	require.NoError(t, bw.Finish(true, true))
	time.Sleep(1 * time.Second)

	isPrefixDelete := func(v uint64) bool {
		for i := range pdVers {
			if pdVers[i] == v {
				return true
			}
		}
		return false
	}

	readData := func() {
		for i := 0; i < count; i++ {
			k := kvList[i].Key.UserKey
			v, exist, vcloser := btree.Get(k, hash.Fnv32(k))
			pd := kkv.DecodeKeyVersion(kvList[i].Key.UserKey)
			if isPrefixDelete(pd) {
				require.Equal(t, false, exist)
			} else {
				require.Equal(t, true, exist)
				require.Equal(t, kvList[i].Value, v)
				vcloser()
			}
		}
	}

	readData()

	rtx := btree.txPool.Load()
	cursor := rtx.Bucket().Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		pn := utils.BytesToUint32(v)
		if pn == 8 || pn == 9 {
			t.Fatalf("find delete pn:%d", pn)
		}
	}
	require.NoError(t, rtx.Unref(false))

	require.NoError(t, testBitreeClose(btree))

	btree, _ = testOpenKKVBitree1(optsPool)
	readData()
	require.NoError(t, testBitreeClose(btree))
}
