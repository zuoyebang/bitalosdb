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
	"strings"
	"sync"
	"testing"
	"time"

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

func TestBasicBitowerWrites(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	names := []string{
		"Alatar",
		"Gandalf",
		"Pallando",
		"Radagast",
		"Saruman",
		"Joe",
	}
	wantMap := map[string]string{}

	batchNew := func() *BatchBitower {
		return newBatchBitowerByIndex(db, int(testSlotId))
	}

	inBatch, batch, pending := false, batchNew(), [][]string(nil)
	set0 := func(k, v string) error {
		return db.Set(makeTestSlotKey([]byte(k)), []byte(v), nil)
	}
	del0 := func(k string) error {
		return db.Delete(makeTestSlotKey([]byte(k)), nil)
	}
	set1 := func(k, v string) error {
		batch.Set(makeTestSlotKey([]byte(k)), []byte(v), nil)
		return nil
	}
	del1 := func(k string) error {
		batch.Delete(makeTestSlotKey([]byte(k)), nil)
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
			inBatch, batch, set, del = true, batchNew(), set1, del1
		case "apply":
			if err := db.ApplyBitower(batch, NoSync); err != nil {
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
			key := makeTestSlotKey([]byte(name))
			g, closer, err := db.Get(key)
			if err != nil && err != ErrNotFound {
				t.Errorf("#%d %s: Get(%q): %v", i, tc, name, err)
				fail = true
			}
			_, err = db.Exist(key)
			if err != nil && err != ErrNotFound {
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
}

func TestEmptyMemtable(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	db, err := Open(dir, &Options{})
	require.NoError(t, err)

	checkEmpty := func(index int) {
		d := db.bitowers[index]
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
	}

	for i := 0; i < len(db.bitowers); i++ {
		checkEmpty(i)
	}

	require.NoError(t, db.Close())
}

func TestNotEmptyMemtable(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	db, err := Open(dir, &Options{})
	require.NoError(t, err)

	key := makeTestKey([]byte("a"))
	require.NoError(t, db.Set(key, key, nil))

	index := db.getBitowerIndexByKey(key)
	d := db.bitowers[index]
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
	require.NoError(t, db.Close())
}

func TestRandomWrites(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS:           vfs.Default,
		MemTableSize: 1 << 20,
	})
	require.NoError(t, err)

	keys := [64][]byte{}
	wants := [64]int{}
	for k := range keys {
		keys[k] = makeTestIntKey(k)
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

func TestGetNoCache(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		CacheSize: 0,
		FS:        vfs.Default,
	})
	require.NoError(t, err)

	key := makeTestKey([]byte("a"))
	require.NoError(t, d.Set(key, []byte("aa"), nil))
	require.NoError(t, d.Flush())
	require.NoError(t, verifyGet(d, key, []byte("aa")))
	require.NoError(t, d.Close())
}

func TestLogData(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	val := testRandBytes(10)
	for i := 0; i < 100; i++ {
		require.NoError(t, db.Set(makeTestIntKey(i), val, NoSync))
	}
	for i := range db.bitowers {
		require.NoError(t, db.LogData([]byte("foo"), i, Sync))
	}
	require.NoError(t, db.Close())

	db = openTestDB(testDirname, nil)
	for i := 0; i < 100; i++ {
		require.NoError(t, verifyGet(db, makeTestIntKey(i), val))
	}
	require.NoError(t, db.Close())
}

func TestDeleteGet(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)

	key := makeTestKey([]byte("key"))
	val := []byte("val")

	require.NoError(t, d.Set(key, val, nil))
	require.NoError(t, verifyGet(d, key, val))

	key2 := makeTestKey([]byte("key2"))
	val2 := []byte("val2")

	require.NoError(t, d.Set(key2, val2, nil))
	require.NoError(t, verifyGet(d, key2, val2))

	require.NoError(t, d.Delete(key2, nil))
	require.NoError(t, verifyGetNotFound(d, key2))

	require.NoError(t, d.Close())
}

func TestDeleteFlush(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	key := makeTestKey([]byte("key"))
	valFirst := []byte("first")
	valSecond := []byte("second")
	key2 := makeTestKey([]byte("key2"))
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

	require.NoError(t, verifyGetNotFound(d, key))
	require.NoError(t, verifyGetNotFound(d, key2))
}

func TestUnremovableDelete(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	key := makeTestKey([]byte("key"))
	valFirst := []byte("valFirst")
	valSecond := []byte("valSecond")

	require.NoError(t, d.Set(key, valFirst, nil))
	require.NoError(t, d.Set(key, valSecond, nil))
	require.NoError(t, d.Flush())

	require.NoError(t, verifyGet(d, key, valSecond))

	require.NoError(t, d.Delete(key, nil))
	require.NoError(t, d.Flush())
	require.NoError(t, verifyGetNotFound(d, key))
}

func TestAsyncFlush(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	db := openTestDB(testDirname, nil)
	flushed, err := db.AsyncFlush()
	require.NoError(t, err)
	if flushed != nil {
		t.Fatalf("empty flush flushed is not nil")
	}

	val := testRandBytes(100)
	for i := 0; i < 100; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, val, NoSync))
	}

	flushed, err = db.AsyncFlush()
	require.NoError(t, err)
	<-flushed

	for i := 0; i < 100; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, verifyGet(db, key, val))
	}

	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestAsyncFlushDataAndWrite(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)

	d.Set(makeTestKey([]byte("a")), []byte("100"), nil)
	d.Set(makeTestKey([]byte("b")), []byte("200"), nil)

	var val []byte
	var closer func()
	for _, k := range [][]byte{[]byte("a"), []byte("b")} {
		val, closer, err = d.Get(makeTestKey(k))
		if closer != nil {
			closer()
		}
	}

	require.NoError(t, d.Flush())

	d.Set(makeTestKey([]byte("c")), []byte("300"), nil)
	d.Set(makeTestKey([]byte("d")), []byte("400"), nil)

	for _, k := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")} {
		val, closer, err = d.Get(makeTestKey(k))
		fmt.Println(string(k), string(val))
		if closer != nil {
			closer()
		}
	}

	require.NoError(t, err)
	require.NoError(t, d.Close())
}

func TestFlushEmpty(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d, err := Open(dir, &Options{})
	require.NoError(t, err)

	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())
}

func TestDBConcurrentCommitCompactFlush(t *testing.T) {
	for _, disableWAL := range []bool{false, true} {
		t.Run(fmt.Sprintf("disableWAL=%t", disableWAL), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
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
					_ = d.Set(makeTestIntKey(i), nil, NoSync)
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
		})
	}
}

func TestDBApplyBatchMismatch(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	srcDB, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)

	dir1 := testDirname + "1"
	defer os.RemoveAll(dir1)
	os.RemoveAll(dir1)
	applyDB, err := Open(dir1, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)

	b := srcDB.NewBatch()
	b.Set(makeTestKey([]byte("test")), nil, nil)

	err = applyDB.Apply(b, nil)
	if err == nil || !strings.Contains(err.Error(), "batch db mismatch:") {
		t.Fatalf("expected error, but found %v", err)
	}

	require.NoError(t, srcDB.Close())
	require.NoError(t, applyDB.Close())
}

func TestMetaFlushBitable(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	testOptsUseBitable = true
	db := openTestDB(testDirname, nil)
	require.Equal(t, uint8(0), db.meta.meta.GetFieldFlushedBitable())
	require.Equal(t, false, db.isFlushedBitable())
	db.setFlushedBitable()
	require.Equal(t, uint8(1), db.meta.meta.GetFieldFlushedBitable())
	require.Equal(t, true, db.isFlushedBitable())
	require.NoError(t, db.Close())
	testOptsUseBitable = true
	db = openTestDB(testDirname, nil)
	require.Equal(t, uint8(1), db.meta.meta.GetFieldFlushedBitable())
	require.Equal(t, true, db.isFlushedBitable())
	require.NoError(t, db.Close())
}
