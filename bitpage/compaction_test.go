// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitpage

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/stretchr/testify/require"
)

func TestBitpageFlush(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	var endIndex int
	seqNum := uint64(1)
	loop := 10
	kn := 5
	kkn := 5
	mn := 5
	vs := 100
	kvList := sortedkv.MakeSortedAllKVList(kn, kkn, mn, vs, seqNum)
	count := len(kvList)
	seqNum += uint64(count)
	stepCount := count / (loop + 2)

	writeData := func(bp *Bitpage, pn PageNum) {
		wr := bp.GetPageWriter(pn, nil)

		startIndex := endIndex
		endIndex = startIndex + stepCount

		for i := startIndex; i < endIndex; i++ {
			if i%3 == 0 {
				kvList[i].Key.SetKind(internalKeyKindDelete)
				kvList[i].Value = nil
			}
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())
	}

	readData := func(pg *page) {
		for i := 0; i < endIndex; i++ {
			key := kvList[i].Key.UserKey
			v, vexist, vcloser, kind := pg.get(key, kvList[i].KeyHash)
			if i%3 == 0 {
				require.Equal(t, false, vexist)
			} else {
				require.Equal(t, true, vexist)
				require.Equal(t, kvList[i].Value, v)
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
			}
		}
	}

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)

	writeData(bp, pn)
	p := bp.GetPage(pn)

	iter := p.newIter(nil)
	require.NoError(t, p.flush(nil))
	require.NoError(t, iter.Close())

	writeData(bp, pn)
	readData(p)
	testCloseBitpage(t, bp)

	for i := 0; i < loop; i++ {
		bp2, err2 := testOpenBitpage()
		require.NoError(t, err2)
		writeData(bp2, pn)
		p2 := bp2.GetPage(pn)
		require.NoError(t, p2.flush(nil))
		readData(bp2.GetPage(pn))
		testCloseBitpage(t, bp2)
	}
}

func TestBitpagePreFlush(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	seqNum := uint64(1)
	kn := 1000
	kkn := 10
	mn := 1
	vs := 10240
	kvList := sortedkv.MakeSortedAllKVList(kn, kkn, mn, vs, seqNum)
	count := len(kvList)
	seqNum += uint64(count)

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)
	p := bp.GetPage(pn)

	wr := bp.GetPageWriter(pn, nil)
	for i := 0; i < count; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())
	require.NoError(t, p.flush(nil))
	require.Equal(t, pageFlushStateFinish, p.getFlushState())
	p.setFlushState(pageFlushStateNone)
	wr = bp.GetPageWriter(pn, nil)
	for i := 0; i < count; i++ {
		seqNum++
		if i%3 == 0 {
			kvList[i].Key.SetKind(internalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			kvList[i].Value = nil
		} else {
			kvList[i].Key.SetSeqNum(seqNum)
		}
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())
	require.NoError(t, p.flush(nil))
	require.Equal(t, pageFlushStateFinish, p.getFlushState())
	p.setFlushState(pageFlushStateNone)

	for i := 0; i < count; i++ {
		key := kvList[i].Key.UserKey
		v, vexist, vcloser, kind := p.get(key, hash.Fnv32(key))
		if i%3 == 0 {
			require.Equal(t, false, vexist)
		} else {
			if !bytes.Equal(kvList[i].Value, v) {
				t.Fatalf("value not eq i:%d key:%s", i, string(key))
			}
			require.Equal(t, true, vexist)
			require.Equal(t, internalKeyKindSet, kind)
			vcloser()
		}
	}

	testCloseBitpage(t, bp)
}

func TestBitpageSplit(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)

	seqNum := uint64(1)
	kn := 1000
	kkn := 50
	mn := 3
	vs := 100
	kvList := sortedkv.MakeSortedAllKVList(kn, kkn, mn, vs, seqNum)
	count := len(kvList)
	seqNum += uint64(count)
	wr := bp.GetPageWriter(pn, nil)

	for i := 0; i < count; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	p := bp.GetPage(pn)
	require.NoError(t, p.flush(nil))

	bp.opts.BitpageSplitSize = 1 << 18
	sps, err2 := bp.PageSplitStart(pn, "test")
	require.NoError(t, err2)
	bp.PageSplitEnd(pn, sps, nil)

	spsPn := 2
	for i := range sps {
		require.Equal(t, PageNum(spsPn), sps[i].Pn)
		spsPn++
		np := bp.GetPage(sps[i].Pn)
		require.NotNil(t, np)
		require.Equal(t, sps[i].Sentinel, np.arrtable.getMaxKey())
	}
	require.Equal(t, 3, len(sps))

	pfiles := p.getFilesPath()
	require.NoError(t, bp.FreePage(pn, true))
	time.Sleep(1 * time.Second)
	bp.opts.DeleteFilePacer.Flush()
	for i := range pfiles {
		if os2.IsExist(pfiles[i]) {
			t.Fatalf("%s is not delete", pfiles[i])
		}
	}

	testCloseBitpage(t, bp)
}

func TestBitpageFlushArrayTableEmpty(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)

	seqNum := uint64(1)
	kn := 100
	kkn := 10
	mn := 5
	vs := 100
	kvList := sortedkv.MakeSortedAllKVList(kn, kkn, mn, vs, seqNum)
	count := len(kvList)
	seqNum += uint64(count)
	wr := bp.GetPageWriter(pn, nil)

	for i := 0; i < count; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	p := bp.GetPage(pn)

	require.NoError(t, p.flush(nil))

	wr = bp.GetPageWriter(pn, nil)
	for i := 0; i < count; i++ {
		kvList[i].Key.SetKind(internalKeyKindDelete)
		kvList[i].Key.SetSeqNum(seqNum)
		seqNum++
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	require.Equal(t, base.ErrPageFlushEmpty, p.flush(nil))

	testCloseBitpage(t, bp)
}

func TestBitpageFlushPrefixDeleteKey(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	var endIndex int
	num := 5
	stepCount := 30
	seqNum := uint64(1)
	count := (num + 2) * stepCount
	kvList := sortedkv.MakeSortedSamePrefixDeleteKVList(0, count, seqNum, 10)
	seqNum += uint64(count)

	writeData := func(bp *Bitpage, pn PageNum) {
		wr := bp.GetPageWriter(pn, nil)
		startIndex := endIndex
		endIndex = startIndex + stepCount
		for i := startIndex; i < endIndex; i++ {
			if i%3 == 0 && kvList[i].Key.Kind() != internalKeyKindPrefixDelete {
				kvList[i].Key.SetKind(internalKeyKindDelete)
				kvList[i].Value = nil
			}
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())
	}

	readData := func(pg *page) {
		for i := 0; i < endIndex; i++ {
			key := kvList[i].Key.UserKey
			v, vexist, vcloser, kind := pg.get(key, hash.Fnv32(key))
			pd := kkv.DecodeKeyVersion(kvList[i].Key.UserKey)
			if sortedkv.IsPrefixDeleteKey(pd) || i%3 == 0 {
				if vexist {
					t.Log(pd, i, kvList[i].Key.String())
				}
				require.Equal(t, false, vexist)
			} else {
				require.Equal(t, kvList[i].Value, v)
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
			}
		}
	}

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)
	writeData(bp, pn)
	p := bp.GetPage(pn)
	require.NoError(t, p.flush(nil))
	readData(p)
	testCloseBitpage(t, bp)

	for i := 0; i < num; i++ {
		bp2, err2 := testOpenBitpage()
		require.NoError(t, err2)
		writeData(bp2, pn)
		p2 := bp2.GetPage(pn)
		require.NoError(t, p2.flush(nil))
		readData(p2)
		testCloseBitpage(t, bp2)
	}
}

func TestBitpageFlushPrefixDeleteKey2(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	var pdVers []uint64
	seqNum := uint64(1)
	count := 200
	kvList := sortedkv.MakeSlotSortedKVList2(0, count, seqNum, 10)
	seqNum += uint64(count)

	isPrefixDelete := func(v uint64) bool {
		if len(pdVers) == 0 {
			return false
		}
		for i := range pdVers {
			if pdVers[i] == v {
				return true
			}
		}
		return false
	}

	readData := func(pg *page) {
		for i := 0; i < count; i++ {
			key := kvList[i].Key.UserKey
			v, vexist, vcloser, kind := pg.get(key, hash.Fnv32(key))
			pd := kkv.DecodeKeyVersion(kvList[i].Key.UserKey)
			if isPrefixDelete(pd) {
				if vexist {
					t.Log(pd, i, kvList[i].Key.String())
				}
				require.Equal(t, false, vexist)
			} else {
				require.Equal(t, kvList[i].Value, v)
				require.Equal(t, internalKeyKindSet, kind)
				vcloser()
			}
		}
	}

	bp, err := testOpenBitpage()
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)

	wr := bp.GetPageWriter(pn, nil)
	for i := 0; i < count; i++ {
		require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
	}
	require.NoError(t, wr.FlushFinish())

	p := bp.GetPage(pn)
	require.NoError(t, p.flush(nil))
	readData(p)

	pdVers = []uint64{101, 103, 105}
	wr = bp.GetPageWriter(pn, nil)
	for _, version := range pdVers {
		seqNum++
		ikey := sortedkv.MakePrefixDeleteKey(version, seqNum)
		require.NoError(t, wr.Set(ikey, nil))
	}
	require.NoError(t, wr.FlushFinish())
	require.NoError(t, p.flush(nil))
	readData(p)

	testCloseBitpage(t, bp)
}
