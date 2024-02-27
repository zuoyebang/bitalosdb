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
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func try(initialSleep, maxTotalSleep time.Duration, f func() error) error {
	totalSleep := time.Duration(0)
	for d := initialSleep; ; d *= 2 {
		time.Sleep(d)
		totalSleep += d
		if err := f(); err == nil || totalSleep >= maxTotalSleep {
			return err
		}
	}
}

func TestTry(t *testing.T) {
	c := make(chan struct{})
	go func() {
		time.Sleep(1 * time.Millisecond)
		close(c)
	}()

	attemptsMu := sync.Mutex{}
	attempts := 0

	err := try(100*time.Microsecond, 20*time.Second, func() error {
		attemptsMu.Lock()
		attempts++
		attemptsMu.Unlock()

		select {
		default:
			return errors.New("timed out")
		case <-c:
			return nil
		}
	})
	require.NoError(t, err)

	attemptsMu.Lock()
	a := attempts
	attemptsMu.Unlock()

	if a == 0 {
		t.Fatalf("attempts: got 0, want > 0")
	}
}

func TestBasicWrites(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)

	names := []string{
		"Alatar",
		"Gandalf",
		"Pallando",
		"Radagast",
		"Saruman",
		"Joe",
	}
	wantMap := map[string]string{}

	inBatch, batch, pending := false, &Batch{}, [][]string(nil)
	set0 := func(k, v string) error {
		return d.Set([]byte(k), []byte(v), nil)
	}
	del0 := func(k string) error {
		return d.Delete([]byte(k), nil)
	}
	set1 := func(k, v string) error {
		batch.Set([]byte(k), []byte(v), nil)
		return nil
	}
	del1 := func(k string) error {
		batch.Delete([]byte(k), nil)
		return nil
	}
	set, del := set0, del0

	testCases := []string{
		"set Gandalf Grey",
		"set Saruman White",
		"set Radagast Brown",
		"delete Saruman",
		"set Gandalf White",
		"batch",
		"  set Alatar AliceBlue",
		"apply",
		"delete Pallando",
		"set Alatar AntiqueWhite",
		"set Pallando PapayaWhip",
		"batch",
		"apply",
		"set Pallando PaleVioletRed",
		"batch",
		"  delete Alatar",
		"  set Gandalf GhostWhite",
		"  set Saruman Seashell",
		"  delete Saruman",
		"  set Saruman SeaGreen",
		"  set Radagast RosyBrown",
		"  delete Pallando",
		"apply",
		"delete Radagast",
		"delete Radagast",
		"delete Radagast",
		"set Gandalf Goldenrod",
		"set Pallando PeachPuff",
		"batch",
		"  delete Joe",
		"  delete Saruman",
		"  delete Radagast",
		"  delete Pallando",
		"  delete Gandalf",
		"  delete Alatar",
		"apply",
		"set Joe Plumber",
	}
	for i, tc := range testCases {
		s := strings.Split(strings.TrimSpace(tc), " ")
		switch s[0] {
		case "set":
			if err := set(s[1], s[2]); err != nil {
				t.Fatalf("#%d %s: %v", i, tc, err)
			}
			if inBatch {
				pending = append(pending, s)
			} else {
				wantMap[s[1]] = s[2]
			}
		case "delete":
			if err := del(s[1]); err != nil {
				t.Fatalf("#%d %s: %v", i, tc, err)
			}
			if inBatch {
				pending = append(pending, s)
			} else {
				delete(wantMap, s[1])
			}
		case "batch":
			inBatch, batch, set, del = true, &Batch{}, set1, del1
		case "apply":
			if err := d.Apply(batch, nil); err != nil {
				t.Fatalf("#%d %s: %v", i, tc, err)
			}
			for _, p := range pending {
				switch p[0] {
				case "set":
					wantMap[p[1]] = p[2]
				case "delete":
					delete(wantMap, p[1])
				}
			}
			inBatch, pending, set, del = false, nil, set0, del0
		default:
			t.Fatalf("#%d %s: bad test case: %q", i, tc, s)
		}

		fail := false
		for _, name := range names {
			g, closer, err := d.Get([]byte(name))
			if err != nil && err != ErrNotFound {
				t.Errorf("#%d %s: Get(%q): %v", i, tc, name, err)
				fail = true
			}
			_, errno := d.Exist([]byte(name))
			if errno != nil && errno != ErrNotFound {
				t.Errorf("#%d %s: Exist(%q): %v", i, tc, name, err)
				fail = true
			}
			got, gOK := string(g), err == nil
			want, wOK := wantMap[name]
			if got != want || gOK != wOK {
				t.Errorf("#%d %s: Get(%q): got %q, %t, want %q, %t",
					i, tc, name, got, gOK, want, wOK)
				fail = true
			}
			if closer != nil {
				closer()
			}
		}
		if fail {
			return
		}
	}

	require.NoError(t, d.Close())
}

func TestEmptyMemtable(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)
	d.mu.Lock()
	empty := true
	for i := range d.mu.mem.queue {
		if !d.mu.mem.queue[i].empty() {
			empty = false
			break
		}
	}
	d.mu.Unlock()
	require.Equal(t, true, empty)
	require.NoError(t, d.Close())
}

func TestNotEmptyMemtable(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)
	d.Set([]byte("a"), []byte("a"), nil)
	d.mu.Lock()
	empty := true
	for i := range d.mu.mem.queue {
		if !d.mu.mem.queue[i].empty() {
			empty = false
			break
		}
	}
	d.mu.Unlock()
	require.Equal(t, false, empty)
	require.NoError(t, d.Close())
}

func TestRandomWrites(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS:           vfs.Default,
		MemTableSize: 8 * 1024,
	})
	require.NoError(t, err)

	keys := [64][]byte{}
	wants := [64]int{}
	for k := range keys {
		keys[k] = []byte(strconv.Itoa(k))
		wants[k] = -1
	}
	xxx := bytes.Repeat([]byte("x"), 512)

	rng := rand.New(rand.NewSource(123))
	const N = 1000
	for i := 0; i < N; i++ {
		k := rng.Intn(len(keys))
		if rng.Intn(20) != 0 {
			wants[k] = rng.Intn(len(xxx) + 1)
			if err := d.Set(keys[k], xxx[:wants[k]], nil); err != nil {
				t.Fatalf("i=%d: Set: %v", i, err)
			}
		} else {
			wants[k] = -1
			if err := d.Delete(keys[k], nil); err != nil {
				t.Fatalf("i=%d: Delete: %v", i, err)
			}
		}

		if i != N-1 || rng.Intn(50) != 0 {
			continue
		}
		for k := range keys {
			got := -1
			if v, closer, err := d.Get(keys[k]); err != nil {
				if err != ErrNotFound {
					t.Fatalf("Get: %v", err)
				}
			} else {
				got = len(v)
				closer()
			}
			exist, err := d.Exist(keys[k])
			if exist {
				t.Fatalf("Exist: %v", err)
			}
			if got != wants[k] {
				t.Errorf("i=%d, k=%d: got %d, want %d", i, k, got, wants[k])
			}
		}
	}

	require.NoError(t, d.Close())
}

func TestLargeBatch(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS:                          vfs.Default,
		MemTableSize:                1400,
		MemTableStopWritesThreshold: 100,
	})
	require.NoError(t, err)

	logNum := func() FileNum {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.log.queue[len(d.mu.log.queue)-1].fileNum
	}
	fileSize := func(fileNum FileNum) int64 {
		info, err := d.opts.FS.Stat(base.MakeFilepath(d.opts.FS, dir, fileTypeLog, fileNum))
		require.NoError(t, err)
		return info.Size()
	}
	memTableCreationSeqNum := func() uint64 {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.mem.mutable.logSeqNum
	}

	startLogNum := logNum()
	startLogStartSize := fileSize(startLogNum)
	startSeqNum := atomic.LoadUint64(&d.mu.meta.atomic.logSeqNum)

	require.NoError(t, d.Set([]byte("a"), bytes.Repeat([]byte("a"), 512), nil))

	endLogNum := logNum()
	if startLogNum == endLogNum {
		t.Fatal("expected WAL rotation")
	}
	startLogEndSize := fileSize(startLogNum)
	if startLogEndSize == startLogStartSize {
		t.Fatalf("expected large batch to be written to %s.log, but file size unchanged at %d",
			startLogNum, startLogEndSize)
	}
	endLogSize := fileSize(endLogNum)
	if endLogSize != 0 {
		t.Fatalf("expected %s.log to be empty, but found %d", endLogNum, endLogSize)
	}
	if creationSeqNum := memTableCreationSeqNum(); creationSeqNum <= startSeqNum {
		t.Fatalf("expected memTable.logSeqNum=%d > largeBatch.seqNum=%d", creationSeqNum, startSeqNum)
	}

	require.NoError(t, d.Set([]byte("b"), bytes.Repeat([]byte("b"), 512), nil))

	for i := 0; i < 10; i++ {
		b := d.NewBatch()
		require.EqualValues(t, 0, b.Count())
	}

	require.NoError(t, d.Close())
}

func TestGetNoCache(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		CacheSize: 0,
		FS:        vfs.Default,
	})
	require.NoError(t, err)

	key := testMakeKey([]byte("a"))
	require.NoError(t, d.Set(key, []byte("aa"), nil))
	require.NoError(t, d.Flush())
	verifyGet(t, d, key, []byte("aa"))

	require.NoError(t, d.Close())
}

func TestLogData(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)

	defer func() {
		require.NoError(t, d.Close())
	}()

	require.NoError(t, d.LogData([]byte("foo"), Sync))
	require.NoError(t, d.LogData([]byte("bar"), Sync))
}

func TestDeleteGet(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	key := testMakeKey([]byte("key"))
	val := []byte("val")

	require.NoError(t, d.Set(key, val, nil))
	verifyGet(t, d, key, val)

	key2 := testMakeKey([]byte("key2"))
	val2 := []byte("val2")

	require.NoError(t, d.Set(key2, val2, nil))
	verifyGet(t, d, key2, val2)

	require.NoError(t, d.Delete(key2, nil))
	verifyGetNotFound(t, d, key2)
}

func TestDeleteFlush(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	key := testMakeKey([]byte("key"))
	valFirst := []byte("first")
	valSecond := []byte("second")
	key2 := testMakeKey([]byte("key2"))
	val2 := []byte("val2")

	require.NoError(t, d.Set(key, valFirst, nil))
	require.NoError(t, d.Set(key2, val2, nil))
	require.NoError(t, d.Flush())

	require.NoError(t, d.Set(key, valSecond, nil))
	require.NoError(t, d.Delete(key2, nil))
	require.NoError(t, d.Set(key2, val2, nil))
	require.NoError(t, d.Flush())

	require.NoError(t, d.Delete(key, nil))
	require.NoError(t, d.Delete(key2, nil))
	require.NoError(t, d.Flush())

	verifyGetNotFound(t, d, key)
	verifyGetNotFound(t, d, key2)
}

func TestUnremovableDelete(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	key := testMakeKey([]byte("key"))
	valFirst := []byte("valFirst")
	valSecond := []byte("valSecond")

	require.NoError(t, d.Set(key, valFirst, nil))
	require.NoError(t, d.Set(key, valSecond, nil))
	require.NoError(t, d.Flush())

	verifyGet(t, d, key, valSecond)

	require.NoError(t, d.Delete(key, nil))
	require.NoError(t, d.Flush())
	verifyGetNotFound(t, d, key)
}

func TestMemTableReservation(t *testing.T) {
	opts := &Options{
		FS:           vfs.Default,
		MemTableSize: 256 << 10,
	}
	opts.EnsureDefaults()

	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, opts)
	require.NoError(t, err)

	checkReserved := func(expected int64) {
		t.Helper()
		if reserved := atomic.LoadInt64(&d.atomic.memTableReserved); expected != reserved {
			t.Fatalf("expected %d reserved, but found %d", expected, reserved)
		}
	}

	checkReserved(int64(opts.MemTableSize))
	if refs := d.mu.mem.queue[len(d.mu.mem.queue)-1].readerRefs.Load(); refs != 2 {
		t.Fatalf("expected 2 refs, but found %d", refs)
	}

	require.NoError(t, d.Flush())
	checkReserved(int64(opts.MemTableSize))

	require.NoError(t, d.Set([]byte("a"), nil, nil))

	iter := d.NewIter(nil)
	require.NoError(t, d.Flush())
	checkReserved(2 * int64(opts.MemTableSize))
	require.NoError(t, iter.Close())
	checkReserved(int64(opts.MemTableSize))

	require.NoError(t, d.Close())
}

func TestMemTableReservationLeak(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{FS: vfs.Default})
	require.NoError(t, err)

	d.mu.Lock()
	last := d.mu.mem.queue[len(d.mu.mem.queue)-1]
	last.readerRef()
	defer last.readerUnref()
	d.mu.Unlock()
	if err := d.Close(); err == nil {
		t.Fatalf("expected failure, but found success")
	} else if !strings.HasPrefix(err.Error(), "leaked memtable reservation:") {
		t.Fatalf("expected leaked memtable reservation, but found %+v", err)
	} else {
		t.Log(err.Error())
	}
}

func TestAsyncFlushDataAndWrite(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)

	d.Set(testMakeKey([]byte("a")), []byte("100"), nil)
	d.Set(testMakeKey([]byte("b")), []byte("200"), nil)

	var val []byte
	var closer func()
	for _, k := range [][]byte{[]byte("a"), []byte("b")} {
		val, closer, err = d.Get(testMakeKey([]byte(k)))
		fmt.Println(string(k), string(val))
		if closer != nil {
			closer()
		}
	}

	ch, err := d.AsyncFlush()
	<-ch

	d.Set(testMakeKey([]byte("c")), []byte("300"), nil)
	d.Set(testMakeKey([]byte("d")), []byte("400"), nil)

	for _, k := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")} {
		val, closer, err = d.Get(testMakeKey([]byte(k)))
		fmt.Println(string(k), string(val))
		if closer != nil {
			closer()
		}
	}

	require.NoError(t, err)
	require.NoError(t, d.Close())
}

func TestFlushEmpty(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)

	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())
}

func TestDBConcurrentCommitCompactFlush(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)

	for _, disableWAL := range []bool{false, true} {
		os.RemoveAll(dir)
		d, err := Open(dir, &Options{
			FS:         vfs.Default,
			DisableWAL: disableWAL,
		})
		require.NoError(t, err)

		const n = 100
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()
				_ = d.Set([]byte(fmt.Sprint(i)), nil, NoSync)
				var err error
				switch i % 2 {
				case 0:
					err = d.Flush()
				case 1:
					_, err = d.AsyncFlush()
				}
				require.NoError(t, err)
			}(i)
		}
		wg.Wait()

		require.NoError(t, d.Close())
	}
}

func TestDBApplyBatchNilDB(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)

	b1 := &Batch{}
	b1.Set([]byte("test"), nil, nil)

	b2 := &Batch{}
	b2.Apply(b1, nil)
	if b2.memTableSize != 0 {
		t.Fatalf("expected memTableSize to not be set")
	}
	require.NoError(t, d.Apply(b2, nil))
	if b1.memTableSize != b2.memTableSize {
		t.Fatalf("expected memTableSize %d, but found %d", b1.memTableSize, b2.memTableSize)
	}

	require.NoError(t, d.Close())
}

func TestDBApplyBatchMismatch(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	srcDB, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)

	dir1 := "./data-1"
	defer os.RemoveAll(dir1)
	applyDB, err := Open(dir1, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)

	b := srcDB.NewBatch()
	b.Set([]byte("test"), nil, nil)

	err = applyDB.Apply(b, nil)
	if err == nil || !strings.Contains(err.Error(), "bitalosdb: batch db mismatch:") {
		t.Fatalf("expected error, but found %v", err)
	}

	require.NoError(t, srcDB.Close())
	require.NoError(t, applyDB.Close())
}

func BenchmarkDelete(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	const keyCount = 10000
	var keys [keyCount][]byte
	for i := 0; i < keyCount; i++ {
		keys[i] = []byte(strconv.Itoa(rng.Int()))
	}
	val := bytes.Repeat([]byte("x"), 10)

	benchmark := func(b *testing.B) {
		d, err := Open(
			"./data",
			&Options{})
		if err != nil {
			b.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				b.Fatal(err)
			}
		}()

		b.StartTimer()
		for _, key := range keys {
			_ = d.Set(key, val, nil)
			_ = d.Delete(key, nil)
		}

		if err := d.Flush(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}

	b.Run("delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmark(b)
		}
	})
}

func BenchmarkNewIterReadAmp(b *testing.B) {
	for _, readAmp := range []int{1} {
		b.Run(strconv.Itoa(readAmp), func(b *testing.B) {
			opts := &Options{}

			d, err := Open("./data", opts)
			require.NoError(b, err)

			for i := 0; i < readAmp; i++ {
				require.NoError(b, d.Set([]byte("a"), []byte("b"), NoSync))
				require.NoError(b, d.Flush())
			}

			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				iter := d.NewIter(nil)
				b.StopTimer()
				require.NoError(b, iter.Close())
			}

			require.NoError(b, d.Close())
		})
	}
}

func verifyGet(t *testing.T, r Reader, key, expected []byte) {
	val, closer, err := r.Get(key)
	require.NoError(t, err)
	if !bytes.Equal(expected, val) {
		t.Logf("key %s expected %s, but got %s", string(key), expected, val)
	}
	closer()
}

func verifyGetNotFound(t *testing.T, r Reader, key []byte) {
	val, _, err := r.Get(key)
	if err != base.ErrNotFound {
		t.Fatalf("key %s expected nil, but got %s", string(key), val)
	}
}
