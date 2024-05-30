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

package bitalosdb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/stretchr/testify/require"
)

func TestCheckpointFlush(t *testing.T) {
	defer func() {
		os.RemoveAll(testDirname)
		os.RemoveAll("./checkpoints")
	}()
	os.RemoveAll(testDirname)
	os.RemoveAll("./checkpoints")

	testOptsCacheType = consts.CacheTypeLru
	testOptsCacheSize = testCacheSize
	d1 := openTestDB(testDirname, nil)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer cancel()
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			if err := d1.Set(makeTestIntKey(i), nil, nil); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer cancel()
		defer wg.Done()
		for ctx.Err() == nil {
			if err := d1.Flush(); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	loopNum := 5
	check := make([]string, loopNum)
	go func() {
		defer ctx.Done()
		defer cancel()
		defer wg.Done()
		for i := 0; ctx.Err() == nil && i < loopNum; i++ {
			dir := fmt.Sprintf("checkpoints/checkpoint%d", i)
			if err := d1.Checkpoint(dir); err != nil {
				t.Error(err)
				return
			}
			check[i] = dir
		}
	}()
	<-ctx.Done()
	wg.Wait()
	require.NoError(t, d1.Close())

	for _, dir := range check {
		testOptsCacheType = consts.CacheTypeLru
		testOptsCacheSize = testCacheSize
		d2 := openTestDB(dir, nil)
		if err := d2.Close(); err != nil {
			t.Error(err)
		}
	}
}

func TestCheckpointFlushWAL(t *testing.T) {
	const checkpointPath = "./checkpoints/checkpoint"
	defer func() {
		os.RemoveAll(testDirname)
		os.RemoveAll("./checkpoints")
	}()
	os.RemoveAll(testDirname)
	os.RemoveAll("./checkpoints")

	fs := vfs.Default
	key := makeTestSlotKey([]byte("key"))
	value := []byte("value")

	{
		d := openTestDB(testDirname, nil)
		wb := d.NewBatch()
		err := wb.Set(key, value, nil)
		require.NoError(t, err)
		err = d.Apply(wb, NoSync)
		require.NoError(t, err)
		err = d.Checkpoint(checkpointPath, WithFlushedWAL())
		require.NoError(t, err)
		require.NoError(t, d.Close())
	}

	{
		files, err := fs.List(checkpointPath)
		require.NoError(t, err)
		hasLogFile := false
		for _, f := range files {
			info, err := fs.Stat(fs.PathJoin(checkpointPath, f))
			require.NoError(t, err)
			if strings.HasSuffix(f, ".log") {
				hasLogFile = true
				require.NotZero(t, info.Size())
			}
		}
		require.True(t, hasLogFile)
	}

	{
		d := openTestDB(testDirname, nil)
		iter := d.NewIter(&options.IterOptions{SlotId: uint32(testSlotId)})
		require.True(t, iter.First())
		require.Equal(t, key, iter.Key())
		require.Equal(t, value, iter.Value())
		require.False(t, iter.Next())
		require.NoError(t, iter.Close())
		require.NoError(t, d.Close())
	}
}

func TestBtreeCheckpoint(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	for _, vlen := range []int{200, 2048} {
		repeatLoop := 4
		step := 3000
		count := repeatLoop * step
		seqNum := uint64(0)
		kvList := testMakeSortedKVList(0, count, seqNum, vlen)
		seqNum += uint64(count)

		writeData := func(loop int) {
			db := testOpenDB0(true)
			w, err := db.newFlushWriter()
			require.NoError(t, err)
			start := loop * step
			end := start + step
			for i := start; i < end; i++ {
				require.NoError(t, w.Set(*kvList[i].Key, kvList[i].Value))
			}
			require.NoError(t, w.Finish())

			time.Sleep(2 * time.Second)

			if loop == 1 {
				db.CompactBitree(loop)
			}

			destDir := fmt.Sprintf("test_checkpoint_%d", loop)
			require.NoError(t, os.RemoveAll(destDir))

			require.NoError(t, os.Mkdir(destDir, 0755))

			for i := range db.bitowers {
				require.NoError(t, db.bitowers[i].checkpointBitree(db.opts.FS, destDir))
			}

			readKV := func() {
				for i := start; i < end; i++ {
					require.NoError(t, verifyGet(db, kvList[i].Key.UserKey, kvList[i].Value))
				}
			}

			readKV()
			require.NoError(t, db.Close())

			db = testOpenDB0(true)
			readKV()
			require.NoError(t, db.Close())
			require.NoError(t, os.RemoveAll(destDir))
		}

		for i := 0; i < repeatLoop; i++ {
			writeData(i)
		}
	}
}
