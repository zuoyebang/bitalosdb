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
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

func TestOpenCloseOpenClose(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	fs := vfs.Default
	opts := &Options{FS: fs}

	for _, startFromEmpty := range []bool{false, true} {
		for _, walDirname := range []string{"", "wal"} {
			for _, length := range []int{-1, 0, 1, 1000, 10000, 100000} {
				dirname := "/sharedDatabase" + walDirname
				if startFromEmpty {
					dirname = "/startFromEmpty" + walDirname + strconv.Itoa(length)
				}
				dirname = fs.PathJoin(dir, dirname)
				if walDirname == "" {
					opts.WALDir = ""
				} else {
					opts.WALDir = fs.PathJoin(dirname, walDirname)
				}

				got, xxx := []byte(nil), ""
				if length >= 0 {
					xxx = strings.Repeat("x", length)
				}

				d0, err := Open(dirname, opts)
				if err != nil {
					t.Fatalf("sfe=%t, length=%d: Open #0: %v", startFromEmpty, length, err)
					continue
				}
				if length >= 0 {
					err = d0.Set(makeTestKey([]byte("key")), []byte(xxx), nil)
					if err != nil {
						t.Errorf("sfe=%t, length=%d: Set: %v", startFromEmpty, length, err)
						continue
					}
				}
				err = d0.Close()
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Close #0: %v",
						startFromEmpty, length, err)
					continue
				}

				d1, err := Open(dirname, opts)
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Open #1: %v", startFromEmpty, length, err)
					continue
				}
				if length >= 0 {
					var closer func()
					got, closer, err = d1.Get(makeTestKey([]byte("key")))
					if err != nil {
						t.Errorf("sfe=%t, length=%d: Get: %v", startFromEmpty, length, err)
						continue
					}
					got = append([]byte(nil), got...)
					closer()
				}
				err = d1.Close()
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Close #1: %v", startFromEmpty, length, err)
					continue
				}

				if length >= 0 && string(got) != xxx {
					t.Errorf("sfe=%t, length=%d: got value differs from set value", startFromEmpty, length)
					continue
				}
			}
		}
	}
}

func TestOpenWALReplay(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	largeValue := []byte(strings.Repeat("a", 100<<10))
	hugeValue := []byte(strings.Repeat("b", 1<<20))
	keys := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		keys[i] = makeTestSlotKey([]byte(fmt.Sprintf("%d", i)))
	}

	checkIter := func(iter *Iterator) {
		t.Helper()

		i := 0
		for valid := iter.First(); valid; valid = iter.Next() {
			require.Equal(t, keys[i], iter.Key())
			i++
		}
		require.NoError(t, iter.Close())
	}

	fs := vfs.Default
	d, err := Open(dir, &Options{
		FS:           fs,
		MemTableSize: 32 << 20,
	})
	require.NoError(t, err)
	require.NoError(t, d.Set(keys[0], largeValue, nil))
	require.NoError(t, d.Set(keys[1], largeValue, nil))
	require.NoError(t, d.Set(keys[2], largeValue, nil))
	require.NoError(t, d.Set(keys[3], hugeValue, nil))
	require.NoError(t, d.Set(keys[4], largeValue, nil))
	iterOpts := &IterOptions{SlotId: uint32(testSlotId)}
	checkIter(d.NewIter(iterOpts))
	require.NoError(t, d.Close())

	index := base.GetBitowerIndex(int(testSlotId))
	bitower := d.bitowers[index]
	files, err := d.opts.FS.List(bitower.walDirname)
	require.NoError(t, err)
	sort.Strings(files)
	logCount := 0
	for _, fname := range files {
		if strings.HasSuffix(fname, ".log") {
			logCount++
		}
	}

	require.Equal(t, 1, logCount)

	d, err = Open(dir, &Options{
		FS:           fs,
		MemTableSize: 32 << 20,
	})
	require.NoError(t, err)

	checkIter(d.NewIter(iterOpts))
	require.NoError(t, d.Close())
}

func TestOpenWALReplay2(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	for _, reason := range []string{"forced", "size"} {
		t.Run(reason, func(t *testing.T) {
			fs := vfs.Default
			d, err := Open(dir, &Options{
				FS:           fs,
				MemTableSize: 1 << 20,
			})
			require.NoError(t, err)

			switch reason {
			case "forced":
				require.NoError(t, d.Set(makeTestKey([]byte("1")), nil, nil))
				require.NoError(t, d.Flush())
				require.NoError(t, d.Set(makeTestKey([]byte("2")), nil, nil))
			case "size":
				largeValue := []byte(strings.Repeat("a", 100<<10))
				require.NoError(t, d.Set(makeTestKey([]byte("1")), largeValue, nil))
				require.NoError(t, d.Set(makeTestKey([]byte("2")), largeValue, nil))
				require.NoError(t, d.Set(makeTestKey([]byte("3")), largeValue, nil))
			}
			require.NoError(t, d.Close())
			d, err = Open(dir, &Options{
				FS:           fs,
				MemTableSize: 1 << 20,
			})
			require.NoError(t, err)
			require.NoError(t, d.Close())
		})
	}
}

func TestOpenWALReplay3(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	fs := vfs.Default
	d, err := Open(dir, &Options{
		FS:           fs,
		MemTableSize: 1 << 20,
	})
	require.NoError(t, err)

	bitower := d.bitowers[testSlotId]
	bitowerWalDir := bitower.walDirname
	bitower.mu.compact.flushing = true

	num := 500
	val := testRandBytes(1024)
	for i := 0; i < num; i++ {
		key := makeTestSlotIntKey(i)
		require.NoError(t, d.Set(key, val, nil))
	}

	bitower.mu.compact.flushing = false
	memNum := len(bitower.mu.mem.queue)
	require.Equal(t, 5, memNum)
	require.NoError(t, d.Close())

	var expFns, actFns []FileNum
	for i := 1; i <= memNum; i++ {
		expFns = append(expFns, FileNum(i))
	}
	ls, _ := fs.List(bitowerWalDir)
	sort.Strings(ls)
	for _, filename := range ls {
		if _, fn, ok := base.ParseFilename(fs, filename); ok {
			actFns = append(actFns, fn)
		}
	}
	require.Equal(t, expFns, actFns)

	d, err = Open(dir, &Options{
		FS:           fs,
		MemTableSize: 1 << 20,
	})
	require.NoError(t, err)

	bitower = d.bitowers[testSlotId]
	require.Equal(t, FileNum(6), bitower.getMinUnflushedLogNum())
	d.optspool.BaseOptions.DeleteFilePacer.Flush()

	for _, fn := range actFns {
		walFile := bitower.makeWalFilename(fn)
		if utils.IsFileExist(walFile) {
			t.Fatalf("obsolete wal exist file:%s", walFile)
		}
	}

	require.NoError(t, d.Close())
}
