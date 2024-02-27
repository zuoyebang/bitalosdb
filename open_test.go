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

package bitalosdb

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

func TestOpenCloseOpenClose(t *testing.T) {
	root := testDirname
	os.RemoveAll(root)
	defer os.RemoveAll(root)

	fs := vfs.Default
	opts := &Options{FS: fs}

	for _, startFromEmpty := range []bool{false, true} {
		for _, walDirname := range []string{"", "wal"} {
			for _, length := range []int{-1, 0, 1, 1000, 10000, 100000} {
				dirname := "sharedDatabase" + walDirname
				if startFromEmpty {
					dirname = "startFromEmpty" + walDirname + strconv.Itoa(length)
				}
				dirname = fs.PathJoin(root, dirname)
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
					t.Fatalf("sfe=%t, length=%d: Open #0: %v",
						startFromEmpty, length, err)
					continue
				}
				if length >= 0 {
					err = d0.Set([]byte("key"), []byte(xxx), nil)
					if err != nil {
						t.Errorf("sfe=%t, length=%d: Set: %v",
							startFromEmpty, length, err)
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
					t.Errorf("sfe=%t, length=%d: Open #1: %v",
						startFromEmpty, length, err)
					continue
				}
				if length >= 0 {
					var closer func()
					got, closer, err = d1.Get([]byte("key"))
					if err != nil {
						t.Errorf("sfe=%t, length=%d: Get: %v",
							startFromEmpty, length, err)
						continue
					}
					got = append([]byte(nil), got...)
					closer()
				}
				err = d1.Close()
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Close #1: %v",
						startFromEmpty, length, err)
					continue
				}

				if length >= 0 && string(got) != xxx {
					t.Errorf("sfe=%t, length=%d: got value differs from set value",
						startFromEmpty, length)
					continue
				}
			}
		}
	}
}

func TestOpenOptionsCheck(t *testing.T) {
	opts := &Options{}

	dir := "./data"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	d, err := Open(dir, opts)
	require.NoError(t, err)
	require.NoError(t, d.Close())
}

func TestOpenReadOnly(t *testing.T) {
	fs := vfs.Default
	dir := "data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	var contents []string
	{
		d, err := Open(dir, &Options{
			FS: fs,
		})
		require.NoError(t, err)
		require.NoError(t, d.Set([]byte("test"), nil, nil))
		require.NoError(t, d.Close())
		contents, err = fs.List(dir)
		require.NoError(t, err)
		sort.Strings(contents)
	}

	{
		d, err := Open(dir, &Options{
			FS:       fs,
			ReadOnly: true,
		})
		require.NoError(t, err)

		require.EqualValues(t, ErrReadOnly, d.Flush())
		require.EqualValues(t, ErrReadOnly, func() error { _, err := d.AsyncFlush(); return err }())
		require.EqualValues(t, ErrReadOnly, d.Delete(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.LogData(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Set([]byte("a"), nil, nil))

		require.NoError(t, func() error {
			_, closer, err := d.Get([]byte("test"))
			if closer != nil {
				closer()
			}
			return err
		}())

		checkIter := func(iter *Iterator) {
			t.Helper()

			var keys []string
			for valid := iter.First(); valid; valid = iter.Next() {
				keys = append(keys, string(iter.Key()))
			}
			require.NoError(t, iter.Close())
			expectedKeys := []string{"test"}
			if diff := pretty.Diff(keys, expectedKeys); diff != nil {
				t.Fatalf("%s\n%s", strings.Join(diff, "\n"), keys)
			}
		}

		checkIter(d.NewIter(nil))

		b := d.NewIndexedBatch()
		checkIter(b.NewIter(nil))
		require.EqualValues(t, ErrReadOnly, b.Commit(nil))
		require.EqualValues(t, ErrReadOnly, d.Apply(b, nil))

		require.NoError(t, d.Close())

		newContents, err := fs.List(dir)
		require.NoError(t, err)

		sort.Strings(newContents)
		if diff := pretty.Diff(contents, newContents); diff != nil {
			t.Fatalf("%s", strings.Join(diff, "\n"))
		}
	}
}

func TestOpenWALReplay(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	largeValue := []byte(strings.Repeat("a", 100<<10))
	hugeValue := []byte(strings.Repeat("b", 10<<20))
	checkIter := func(iter *Iterator) {
		t.Helper()

		var keys []string
		for valid := iter.First(); valid; valid = iter.Next() {
			keys = append(keys, string(iter.Key()))
		}
		require.NoError(t, iter.Close())
		expectedKeys := []string{"1", "2", "3", "4", "5"}
		if diff := pretty.Diff(keys, expectedKeys); diff != nil {
			t.Fatalf("%s\n%s", strings.Join(diff, "\n"), keys)
		}
	}

	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("read-only=%t", readOnly), func(t *testing.T) {
			fs := vfs.Default
			d, err := Open(dir, &Options{
				FS:           fs,
				MemTableSize: 32 << 20,
			})
			require.NoError(t, err)
			require.NoError(t, d.Set([]byte("1"), largeValue, nil))
			require.NoError(t, d.Set([]byte("2"), largeValue, nil))
			require.NoError(t, d.Set([]byte("3"), largeValue, nil))
			require.NoError(t, d.Set([]byte("4"), hugeValue, nil))
			require.NoError(t, d.Set([]byte("5"), largeValue, nil))
			checkIter(d.NewIter(nil))
			require.NoError(t, d.Close())
			files, err := fs.List(dir)
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
				MemTableSize: 300 << 10,
				ReadOnly:     readOnly,
			})
			require.NoError(t, err)

			if readOnly {
				d.mu.Lock()
				require.NotNil(t, d.mu.mem.mutable)
				d.mu.Unlock()
			}
			checkIter(d.NewIter(nil))
			require.NoError(t, d.Close())
		})
	}
}

func TestOpenWALReplay2(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("read-only=%t", readOnly), func(t *testing.T) {
			for _, reason := range []string{"forced", "size", "large-batch"} {
				t.Run(reason, func(t *testing.T) {
					fs := vfs.Default
					d, err := Open(dir, &Options{
						FS:           fs,
						MemTableSize: 256 << 10,
					})
					require.NoError(t, err)

					switch reason {
					case "forced":
						require.NoError(t, d.Set([]byte("1"), nil, nil))
						require.NoError(t, d.Flush())
						require.NoError(t, d.Set([]byte("2"), nil, nil))
					case "size":
						largeValue := []byte(strings.Repeat("a", 100<<10))
						require.NoError(t, d.Set([]byte("1"), largeValue, nil))
						require.NoError(t, d.Set([]byte("2"), largeValue, nil))
						require.NoError(t, d.Set([]byte("3"), largeValue, nil))
					case "large-batch":
						largeValue := []byte(strings.Repeat("a", d.largeBatchThreshold))
						require.NoError(t, d.Set([]byte("1"), nil, nil))
						require.NoError(t, d.Set([]byte("2"), largeValue, nil))
						require.NoError(t, d.Set([]byte("3"), nil, nil))
					}
					require.NoError(t, d.Close())

					d, err = Open(dir, &Options{
						FS:           fs,
						MemTableSize: 300 << 10,
						ReadOnly:     readOnly,
					})
					require.NoError(t, err)
					require.NoError(t, d.Close())
				})
			}
		})
	}
}

func TestOpenWALReplayReadOnlySeqNums(t *testing.T) {
	fs := vfs.Default
	const root = "./data"
	defer os.RemoveAll(root)
	os.RemoveAll(root)

	dir := fs.PathJoin(root, "original")
	d, err := Open(dir, &Options{
		FS:                          fs,
		MemTableStopWritesThreshold: 3,
	})
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("a"), nil, nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Set([]byte("a"), nil, nil))
	require.NoError(t, d.Flush())

	d.mu.Lock()
	d.mu.compact.flushing = true
	d.mu.Unlock()

	require.NoError(t, d.Set([]byte("b"), nil, nil))
	d.AsyncFlush()
	require.NoError(t, d.Set([]byte("c"), nil, nil))
	d.AsyncFlush()
	require.NoError(t, d.Set([]byte("e"), nil, nil))

	d.mu.Lock()
	d.mu.compact.flushing = false
	d.mu.Unlock()
	require.NoError(t, d.Close())

	d, err = Open(dir, &Options{
		FS:       fs,
		ReadOnly: true,
	})
	require.NoError(t, err)
	require.NoError(t, d.Close())
}

func TestOpenWALReplayMemtableGrowth(t *testing.T) {
	fs := vfs.Default
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	const memTableSize = 64 * 1024 * 1024
	opts := &Options{
		MemTableSize: memTableSize,
		FS:           fs,
	}
	func() {
		db, err := Open(dir, opts)
		require.NoError(t, err)
		defer db.Close()
		b := db.NewBatch()
		defer b.Close()
		key := make([]byte, 8)
		val := make([]byte, 16*1024*1024)
		b.Set(key, val, nil)
		require.NoError(t, db.Apply(b, Sync))
	}()
	db, err := Open(dir, opts)
	require.NoError(t, err)
	db.Close()
}
