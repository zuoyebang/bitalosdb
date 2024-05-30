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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

func TestBitpageCompact_Flush(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		var endIndex int
		num := 10
		stepCount := 10
		seqNum := uint64(1)
		count := (num + 2) * stepCount
		kvList := testMakeSortedKV(count, seqNum, 10)
		seqNum += uint64(count)

		writeData := func(bp *Bitpage, pn PageNum) {
			wr := bp.GetPageWriter(pn, nil)

			startIndex := endIndex
			endIndex = startIndex + stepCount

			for i := startIndex; i < endIndex; i++ {
				if i%3 == 0 {
					kvList[i].Key.SetKind(internalKeyKindDelete)
					kvList[i].Value = []byte{}
				}
				require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, wr.FlushFinish())
		}

		readData := func(pg *page) {
			for i := 0; i < endIndex; i++ {
				key := kvList[i].Key.UserKey
				v, vexist, vcloser, kind := pg.get(key, hash.Crc32(key))
				if i%3 == 0 {
					require.Equal(t, false, vexist)
				} else {
					require.Equal(t, kvList[i].Value, v)
					require.Equal(t, internalKeyKindSet, kind)
					vcloser()
				}
			}
		}

		bp, err := testOpenBitpage(true)
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)

		writeData(bp, pn)
		p := bp.GetPage(pn)

		iter := p.newIter(nil)
		require.NoError(t, p.flush(nil, ""))
		require.NoError(t, iter.Close())

		writeData(bp, pn)
		readData(p)
		testCloseBitpage(t, bp)

		for i := 0; i < num; i++ {
			fmt.Println("for start", i)
			bp2, err2 := testOpenBitpage(true)
			require.NoError(t, err2)
			writeData(bp2, pn)
			p2 := bp2.GetPage(pn)
			require.NoError(t, p2.flush(nil, ""))
			readData(bp2.GetPage(pn))
			testCloseBitpage(t, bp2)
		}
	})
}

func TestBitpageCompact_Flush_TableFull(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		var endIndex int
		stepCount := 15000
		seqNum := uint64(1)
		count := 2 * stepCount
		kvList := testMakeSortedKV(count, seqNum, 100)
		seqNum += uint64(count)

		writeData := func(bp *Bitpage, pn PageNum) {
			wr := bp.GetPageWriter(pn, nil)

			startIndex := endIndex
			endIndex = startIndex + stepCount
			for i := startIndex; i < endIndex; i++ {
				if i%3 == 0 {
					kvList[i].Key.SetKind(internalKeyKindDelete)
					kvList[i].Value = []byte{}
				}
				require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, wr.FlushFinish())
			require.Equal(t, true, wr.MaybePageFlush(1<<20))
		}

		readData := func(pg *page) {
			for i := 0; i < endIndex; i++ {
				key := kvList[i].Key.UserKey
				v, vexist, vcloser, kind := pg.get(key, hash.Crc32(key))
				if i%3 == 0 {
					require.Equal(t, false, vexist)
				} else {
					require.Equal(t, kvList[i].Value, v)
					require.Equal(t, internalKeyKindSet, kind)
					vcloser()
				}
			}
		}

		bp, err := testOpenBitpage(true)
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		p := bp.GetPage(pn)

		require.Equal(t, 1, len(p.mu.stQueue))
		writeData(bp, pn)
		require.NoError(t, p.flush(nil, ""))
		require.Equal(t, 1, len(p.mu.stQueue))
		require.Equal(t, pageFlushStateFinish, p.getFlushState())
		p.setFlushState(pageFlushStateNone)
		writeData(bp, pn)
		require.NoError(t, p.flush(nil, ""))
		require.Equal(t, 1, len(p.mu.stQueue))
		require.Equal(t, pageFlushStateFinish, p.getFlushState())
		readData(p)
		testCloseBitpage(t, bp)

		bp1, err2 := testOpenBitpage(true)
		require.NoError(t, err2)
		p1 := bp1.GetPage(pn)
		readData(p1)
		testCloseBitpage(t, bp1)
	})
}

func TestBitpageCompact_Split(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		bp, err := testOpenBitpage2(dir, params[0], params[1], params[2])
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)

		seqNum := uint64(1)
		count := 50000
		kvList := testMakeSortedKV(count, seqNum, 100)
		seqNum += uint64(count)
		wr := bp.GetPageWriter(pn, nil)

		for i := 0; i < count; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		p := bp.GetPage(pn)
		require.NoError(t, p.flush(nil, ""))

		bp.opts.BitpageSplitSize = 1 << 18
		sps, err2 := bp.PageSplitStart(pn, "test")
		require.NoError(t, err2)
		bp.PageSplitEnd(pn, sps, nil)

		spsPn := 2
		for i := range sps {
			require.Equal(t, PageNum(spsPn), sps[i].Pn)
			spsPn++
			fmt.Println("sp:", i, spsPn, string(sps[i].Sentinel))
		}

		pfiles := p.getFilesPath()
		require.NoError(t, bp.FreePage(pn, true))
		bp.opts.DeleteFilePacer.Flush()
		for i := range pfiles {
			if utils.IsFileExist(pfiles[i]) {
				t.Fatalf("%s is not delete", pfiles[i])
			}
		}

		testCloseBitpage(t, bp)
	})
}

func TestBitpageCompact_FlushArrayTableEmpty(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		bp, err := testOpenBitpage(true)
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)

		seqNum := uint64(1)
		count := 100
		kvList := testMakeSortedKV(count, seqNum, 10)
		seqNum += uint64(count)
		wr := bp.GetPageWriter(pn, nil)

		for i := 0; i < 100; i++ {
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		p := bp.GetPage(pn)

		require.NoError(t, p.flush(nil, "111"))

		wr = bp.GetPageWriter(pn, nil)
		for i := 0; i < 100; i++ {
			kvList[i].Key.SetKind(internalKeyKindDelete)
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		require.NoError(t, p.flush(nil, "222"))
		if p.mu.arrtable != nil {
			t.Fatal("arrtable is not nil")
		}

		for i := 0; i < 100; i++ {
			key := makeTestKey(i)
			_, vexist, vcloser, _ := p.get(key, hash.Crc32(key))
			require.Equal(t, false, vexist)
			if vcloser != nil {
				vcloser()
			}
		}

		wr = bp.GetPageWriter(pn, nil)
		for i := 0; i < 100; i++ {
			kvList[i].Key.SetKind(internalKeyKindSet)
			kvList[i].Key.SetSeqNum(seqNum)
			seqNum++
			require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
		}
		require.NoError(t, wr.FlushFinish())

		require.NoError(t, p.flush(nil, "333"))

		for i := 0; i < 100; i++ {
			key := kvList[i].Key.UserKey
			v, vexist, vcloser, kind := p.get(key, hash.Crc32(key))
			require.Equal(t, kvList[i].Value, v)
			require.Equal(t, true, vexist)
			require.Equal(t, internalKeyKindSet, kind)
			vcloser()
		}

		testCloseBitpage(t, bp)
	})
}

func TestBitpageCompact_PrefixDeleteKey(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		var endIndex int
		num := 5
		stepCount := 30
		seqNum := uint64(1)
		count := (num + 2) * stepCount
		kvList := sortedkv.MakeSortedSamePrefixDeleteKVList(0, count, seqNum, 10, testSlotId)
		seqNum += uint64(count)

		writeData := func(bp *Bitpage, pn PageNum) {
			wr := bp.GetPageWriter(pn, nil)
			startIndex := endIndex
			endIndex = startIndex + stepCount
			for i := startIndex; i < endIndex; i++ {
				if i%3 == 0 && kvList[i].Key.Kind() != internalKeyKindPrefixDelete {
					kvList[i].Key.SetKind(internalKeyKindDelete)
					kvList[i].Value = []byte{}
				}
				require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, wr.FlushFinish())
		}

		readData := func(pg *page) {
			for i := 0; i < endIndex; i++ {
				key := kvList[i].Key.UserKey
				v, vexist, vcloser, kind := pg.get(key, hash.Crc32(key))
				pd := pg.bp.opts.KeyPrefixDeleteFunc(kvList[i].Key.UserKey)
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

		bp, err := testOpenBitpage(true)
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		writeData(bp, pn)
		p := bp.GetPage(pn)
		require.NoError(t, p.flush(nil, ""))
		readData(p)
		testCloseBitpage(t, bp)

		for i := 0; i < num; i++ {
			bp2, err2 := testOpenBitpage(true)
			require.NoError(t, err2)
			writeData(bp2, pn)
			p2 := bp2.GetPage(pn)
			require.NoError(t, p2.flush(nil, ""))
			readData(p2)
			testCloseBitpage(t, bp2)
		}
	})
}

func TestBitpageCompact_PrefixDeleteKey2(t *testing.T) {
	testcase(func(index int, params []bool) {
		dir := testDir
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		var endIndex int
		var pdVers []uint64
		stepCount := 100
		seqNum := uint64(1)
		count := 2 * stepCount
		kvList := sortedkv.MakeSlotSortedKVList2(0, count, seqNum, 10, testSlotId)
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

		writeData := func(bp *Bitpage, pn PageNum) {
			wr := bp.GetPageWriter(pn, nil)
			startIndex := endIndex
			endIndex = startIndex + stepCount
			for i := startIndex; i < endIndex; i++ {
				require.NoError(t, wr.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, wr.FlushFinish())
		}

		writePrefixDeleteKey := func(bp *Bitpage, pn PageNum, versions []uint64) {
			if len(versions) == 0 {
				return
			}
			wr := bp.GetPageWriter(pn, nil)
			for _, version := range versions {
				key := sortedkv.MakeKey2(nil, testSlotId, version)
				seqNum++
				ikey := base.MakeInternalKey(key, seqNum, base.InternalKeyKindPrefixDelete)
				require.NoError(t, wr.Set(ikey, []byte{}))
			}
			require.NoError(t, wr.FlushFinish())
		}

		readData := func(pg *page) {
			for i := 0; i < endIndex; i++ {
				key := kvList[i].Key.UserKey
				v, vexist, vcloser, kind := pg.get(key, hash.Crc32(key))
				pd := pg.bp.opts.KeyPrefixDeleteFunc(kvList[i].Key.UserKey)
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

		bp, err := testOpenBitpage(true)
		require.NoError(t, err)
		pn, err1 := bp.NewPage()
		require.NoError(t, err1)
		writeData(bp, pn)
		p := bp.GetPage(pn)
		require.NoError(t, p.flush(nil, ""))
		readData(p)

		pdVers = []uint64{101, 103, 105}
		writePrefixDeleteKey(bp, pn, pdVers)
		require.NoError(t, p.flush(nil, ""))
		readData(p)

		testCloseBitpage(t, bp)
	})
}
