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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifest_Open(t *testing.T) {
	testInitDir()
	defer os.RemoveAll(testDir)

	opts := testInitOpts()
	bpage := &Bitpage{
		dirname: testDir,
		opts:    opts,
	}
	require.NoError(t, openManifest(bpage))

	freelistNum := pageMetadataNum
	require.Equal(t, freelistNum, bpage.meta.getPagesFreelistLen())

	for i := 1; i <= 100; i++ {
		pageNum := bpage.meta.getNextPageNum()
		bpage.meta.newPagemetaItem(pageNum)
	}
	require.Equal(t, PageNum(100), bpage.meta.getCurrentPageNum())

	freelistNum -= 100
	require.Equal(t, freelistNum, bpage.meta.getPagesFreelistLen())
	require.Equal(t, 100, len(bpage.meta.mu.pagemetaMap))

	num := 10

	for pageNum, pmItem := range bpage.meta.mu.pagemetaMap {
		require.Equal(t, pageNum, pmItem.pageNum)
		require.Equal(t, FileNum(1), pmItem.nextStFileNum)
		require.Equal(t, FileNum(0), pmItem.curAtFileNum)
		require.Equal(t, FileNum(1), pmItem.minUnflushedStFileNum)
		require.Equal(t, uint8(0), pmItem.splitState)
		for j := 0; j < num; j++ {
			fn := bpage.meta.getNextStFileNum(pageNum)
			require.Equal(t, pmItem.nextStFileNum, fn+FileNum(1))
		}
		for j := 0; j < num; j++ {
			fn := bpage.meta.getNextAtFileNum(pageNum)
			require.Equal(t, pmItem.curAtFileNum+FileNum(1), fn)
			bpage.meta.setNextArrayTableFileNum(pageNum)
		}
		bpage.meta.setSplitState(pageNum, 1)
	}

	for i := 1; i <= 10; i++ {
		pn := PageNum(i)
		bpage.meta.freePagemetaItem(pn)
		require.Equal(t, (*pagemetaItem)(nil), bpage.meta.getPagemetaItem(pn))
	}

	freelistNum += 10
	require.Equal(t, freelistNum, bpage.meta.getPagesFreelistLen())

	require.NoError(t, bpage.meta.close())

	require.NoError(t, openManifest(bpage))
	require.Equal(t, freelistNum, bpage.meta.getPagesFreelistLen())
	require.Equal(t, PageNum(100), bpage.meta.getCurrentPageNum())
	for i := 11; i <= 100; i++ {
		pageNum := PageNum(i)
		pmItem := bpage.meta.getPagemetaItem(pageNum)
		require.Equal(t, pageNum, pmItem.pageNum)
		require.Equal(t, FileNum(num+1), pmItem.nextStFileNum)
		require.Equal(t, FileNum(num), pmItem.curAtFileNum)
		require.Equal(t, uint8(1), pmItem.splitState)
		for j := 0; j < num; j++ {
			fn := bpage.meta.getNextStFileNum(pageNum)
			require.Equal(t, pmItem.nextStFileNum, fn+FileNum(1))
		}
		for j := 0; j < num; j++ {
			fn := bpage.meta.getNextAtFileNum(pageNum)
			require.Equal(t, pmItem.curAtFileNum+FileNum(1), fn)
			bpage.meta.setNextArrayTableFileNum(pageNum)
		}
	}

	for i := 1; i <= 100; i++ {
		pageNum := bpage.meta.getNextPageNum()
		bpage.meta.newPagemetaItem(pageNum)
	}
	require.Equal(t, PageNum(200), bpage.meta.getCurrentPageNum())
	for i := 101; i <= 200; i++ {
		pageNum := PageNum(i)
		pmItem := bpage.meta.getPagemetaItem(pageNum)
		require.Equal(t, pageNum, pmItem.pageNum)
		require.Equal(t, FileNum(1), pmItem.nextStFileNum)
		require.Equal(t, FileNum(0), pmItem.curAtFileNum)
		require.Equal(t, FileNum(1), pmItem.minUnflushedStFileNum)
		require.Equal(t, uint8(0), pmItem.splitState)
		bpage.meta.setSplitState(pageNum, 2)
		for j := 0; j < num; j++ {
			fn := bpage.meta.getNextStFileNum(pageNum)
			require.Equal(t, pmItem.nextStFileNum, fn+FileNum(1))
		}
		for j := 0; j < num; j++ {
			fn := bpage.meta.getNextAtFileNum(pageNum)
			require.Equal(t, pmItem.curAtFileNum+FileNum(1), fn)
			bpage.meta.setNextArrayTableFileNum(pageNum)
		}
		require.Equal(t, uint8(2), bpage.meta.getSplitState(pageNum))
	}
}

func TestManifest_Write(t *testing.T) {
	testInitDir()
	defer os.RemoveAll(testDir)

	opts := testInitOpts()
	bpage := &Bitpage{
		dirname: testDir,
		opts:    opts,
	}

	writeMeta := func(index int) {
		require.NoError(t, openManifest(bpage))
		defer func() {
			require.NoError(t, bpage.meta.close())
		}()

		start := (index - 1) * 100
		end := start + 100
		for i := start + 1; i <= end; i++ {
			pageNum := bpage.meta.getNextPageNum()
			pmItem := bpage.meta.newPagemetaItem(pageNum)
			for j := 0; j < 10; j++ {
				fn := bpage.meta.getNextStFileNum(pageNum)
				require.Equal(t, pmItem.nextStFileNum, fn+FileNum(1))
			}
			for j := 0; j < 10; j++ {
				fn := bpage.meta.getNextAtFileNum(pageNum)
				require.Equal(t, pmItem.curAtFileNum+FileNum(1), fn)
				bpage.meta.setNextArrayTableFileNum(pageNum)
			}
			require.Equal(t, uint8(0), pmItem.splitState)
			bpage.meta.setSplitState(pageNum, 2)
		}
		require.Equal(t, PageNum(index*100), bpage.meta.getCurrentPageNum())
		require.Equal(t, index*100, len(bpage.meta.mu.pagemetaMap))
	}

	for i := 1; i <= 100; i++ {
		writeMeta(i)
	}

	require.NoError(t, openManifest(bpage))
	require.Equal(t, PageNum(100*100), bpage.meta.getCurrentPageNum())
	require.Equal(t, pageMetadataNum-100*100, bpage.meta.getPagesFreelistLen())
	for pn := PageNum(1); pn <= PageNum(100*100); pn++ {
		pmItem := bpage.meta.getPagemetaItem(pn)
		require.Equal(t, pn, pmItem.pageNum)
		require.Equal(t, FileNum(10), pmItem.curAtFileNum)
		require.Equal(t, FileNum(11), pmItem.nextStFileNum)
		require.Equal(t, uint8(2), pmItem.splitState)
	}
	require.NoError(t, bpage.meta.close())
}

func TestManifest_PagemetaFull(t *testing.T) {
	testInitDir()
	defer os.RemoveAll(testDir)

	opts := testInitOpts()
	bpage := &Bitpage{
		dirname: testDir,
		opts:    opts,
	}
	require.NoError(t, openManifest(bpage))
	for i := 1; i <= pageMetadataNum; i++ {
		pageNum := bpage.meta.getNextPageNum()
		pmItem := bpage.meta.newPagemetaItem(pageNum)
		stFileNum := bpage.meta.getNextStFileNum(pageNum)
		require.Equal(t, pmItem.nextStFileNum, stFileNum+FileNum(1))
		atFileNum := bpage.meta.getNextAtFileNum(pageNum)
		require.Equal(t, pmItem.curAtFileNum+FileNum(1), atFileNum)
		bpage.meta.setNextArrayTableFileNum(pageNum)
	}
	for i := 1; i <= 1000; i++ {
		bpage.meta.freePagemetaItem(PageNum(i))
	}
	for i := 1; i <= 1000; i++ {
		pageNum := bpage.meta.getNextPageNum()
		pmItem := bpage.meta.newPagemetaItem(pageNum)
		stFileNum := bpage.meta.getNextStFileNum(pageNum)
		require.Equal(t, pmItem.nextStFileNum, stFileNum+FileNum(1))
		atFileNum := bpage.meta.getNextAtFileNum(pageNum)
		require.Equal(t, pmItem.curAtFileNum+FileNum(1), atFileNum)
		bpage.meta.setNextArrayTableFileNum(pageNum)
	}
	require.Equal(t, pageMetadataNum, len(bpage.meta.mu.pagemetaMap))
	require.NoError(t, bpage.meta.close())

	require.NoError(t, openManifest(bpage))
	require.Equal(t, pageMetadataNum, len(bpage.meta.mu.pagemetaMap))
	for pageNum, pmItem := range bpage.meta.mu.pagemetaMap {
		require.Equal(t, pageNum, pmItem.pageNum)
		require.Equal(t, FileNum(2), pmItem.nextStFileNum)
		require.Equal(t, FileNum(1), pmItem.curAtFileNum)
	}
	require.NoError(t, bpage.meta.close())
}
