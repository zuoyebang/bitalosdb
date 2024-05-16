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

package bdb_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

var statsFlag = flag.Bool("stats", false, "show performance stats")

const pageSize = 4096

const pageHeaderSize = 16

type meta struct {
	magic    uint32
	version  uint32
	_        uint32
	_        uint32
	_        [16]byte
	_        uint64
	pgid     uint64
	_        uint64
	checksum uint64
}

func TestOpen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := bdb.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}

	if s := db.Path(); s != path {
		t.Fatalf("unexpected path: %s", s)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen_MultipleGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	const (
		instances  = 30
		iterations = 30
	)
	path := tempfile()
	defer os.RemoveAll(path)
	var wg sync.WaitGroup
	errCh := make(chan error, iterations*instances)
	for iteration := 0; iteration < iterations; iteration++ {
		for instance := 0; instance < instances; instance++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				db, err := bdb.Open(path, nil)
				if err != nil {
					errCh <- err
					return
				}
				if err := db.Close(); err != nil {
					errCh <- err
					return
				}
			}()
		}
		wg.Wait()
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("error from inside goroutine: %v", err)
		}
	}
}

func TestOpen_ErrPathRequired(t *testing.T) {
	_, err := bdb.Open("", nil)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestOpen_ErrNotExists(t *testing.T) {
	_, err := bdb.Open(filepath.Join(tempfile(), "bad-path"), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOpen_ErrInvalid(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(f, "this is not a bdb database"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := bdb.Open(path, nil); err != bdb.ErrInvalid {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestOpen_ErrVersionMismatch(t *testing.T) {
	if pageSize != os.Getpagesize() {
		t.Skip("page size mismatch")
	}

	db := MustOpenDB()
	path := db.Path()
	defer db.MustClose()

	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}

	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	meta0 := (*meta)(unsafe.Pointer(&buf[pageHeaderSize]))
	meta0.version++
	meta1 := (*meta)(unsafe.Pointer(&buf[pageSize+pageHeaderSize]))
	meta1.version++
	if err := os.WriteFile(path, buf, 0666); err != nil {
		t.Fatal(err)
	}

	if _, err := bdb.Open(path, nil); err != bdb.ErrVersionMismatch {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestOpen_ErrChecksum(t *testing.T) {
	if pageSize != os.Getpagesize() {
		t.Skip("page size mismatch")
	}

	db := MustOpenDB()
	path := db.Path()
	defer db.MustClose()

	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}

	buf, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	meta0 := (*meta)(unsafe.Pointer(&buf[pageHeaderSize]))
	meta0.pgid++
	meta1 := (*meta)(unsafe.Pointer(&buf[pageSize+pageHeaderSize]))
	meta1.pgid++
	if err := ioutil.WriteFile(path, buf, 0666); err != nil {
		t.Fatal(err)
	}

	if _, err := bdb.Open(path, nil); err != bdb.ErrChecksum {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestOpen_Size(t *testing.T) {
	db := MustOpenDB()
	path := db.Path()
	defer db.MustClose()

	pagesize := db.Info().PageSize

	if err := db.Update(func(tx *bdb.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("data"))
		for i := 0; i < 10000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%04d", i)), make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}
	sz := fileSize(path)
	if sz == 0 {
		t.Fatalf("unexpected new file size: %d", sz)
	}

	db0, err := bdb.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db0.Update(func(tx *bdb.Tx) error {
		if err := tx.Bucket([]byte("data")).Put([]byte{0}, []byte{0}); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db0.Close(); err != nil {
		t.Fatal(err)
	}
	newSz := fileSize(path)
	if newSz == 0 {
		t.Fatalf("unexpected new file size: %d", newSz)
	}

	if sz < newSz-5*int64(pagesize) {
		t.Fatalf("unexpected file growth: %d => %d", sz, newSz)
	}
}

func No_TestOpen_Size_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	db := MustOpenDB()
	path := db.Path()
	defer db.MustClose()

	pagesize := db.Info().PageSize

	var index uint64
	for i := 0; i < 10000; i++ {
		if err := db.Update(func(tx *bdb.Tx) error {
			b, _ := tx.CreateBucketIfNotExists([]byte("data"))
			for j := 0; j < 1000; j++ {
				if err := b.Put(u64tob(index), make([]byte, 50)); err != nil {
					t.Fatal(err)
				}
				index++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}
	sz := fileSize(path)
	if sz == 0 {
		t.Fatalf("unexpected new file size: %d", sz)
	} else if sz < (1 << 30) {
		t.Fatalf("expected larger initial size: %d", sz)
	}

	db0, err := bdb.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db0.Update(func(tx *bdb.Tx) error {
		return tx.Bucket([]byte("data")).Put([]byte{0}, []byte{0})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db0.Close(); err != nil {
		t.Fatal(err)
	}

	newSz := fileSize(path)
	if newSz == 0 {
		t.Fatalf("unexpected new file size: %d", newSz)
	}

	if sz < newSz-5*int64(pagesize) {
		t.Fatalf("unexpected file growth: %d => %d", sz, newSz)
	}
}

func TestOpen_Check(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := bdb.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.View(func(tx *bdb.Tx) error { return <-tx.Check() }); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = bdb.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *bdb.Tx) error { return <-tx.Check() }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen_FileTooSmall(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := bdb.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := int64(db.Info().PageSize)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	if err = os.Truncate(path, pageSize); err != nil {
		t.Fatal(err)
	}

	_, err = bdb.Open(path, nil)
	if err == nil || err.Error() != "file size too small" {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDB_Open_InitialMmapSize(t *testing.T) {
	path := tempfile()
	defer os.Remove(path)

	testWriteSize := 1 << 27

	opts := options.DefaultBdbOptions
	opts.InitialMmapSize = 1 << 30

	db, err := bdb.Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}

	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	wtx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	b, err := wtx.CreateBucket([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Put([]byte("foo"), make([]byte, testWriteSize))
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)

	go func() {
		err := wtx.Commit()
		done <- err
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Errorf("unexpected that the reader blocks writer")
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := rtx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Open_ReadOnly(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}

	f := db.f
	o := &options.BdbOptions{ReadOnly: true}
	readOnlyDB, err := bdb.Open(f, o)
	if err != nil {
		panic(err)
	}

	if !readOnlyDB.IsReadOnly() {
		t.Fatal("expect db in read only mode")
	}

	if err := readOnlyDB.View(func(tx *bdb.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if !bytes.Equal(value, []byte("bar")) {
			t.Fatal("expect value 'bar', got", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := readOnlyDB.Begin(true); err != bdb.ErrDatabaseReadOnly {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := readOnlyDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen_BigPage(t *testing.T) {
	pz := os.Getpagesize()

	opts := options.DefaultBdbOptions
	opts.PageSize = pz * 2
	db1 := MustOpenWithOption(opts)
	defer db1.MustClose()

	opts.PageSize = pz * 4
	db2 := MustOpenWithOption(opts)
	defer db2.MustClose()

	if db1sz, db2sz := fileSize(db1.f), fileSize(db2.f); db1sz >= db2sz {
		t.Errorf("expected %d < %d", db1sz, db2sz)
	}
}

func TestOpen_RecoverFreeList(t *testing.T) {
	opts := options.DefaultBdbOptions
	opts.NoFreelistSync = true
	db := MustOpenWithOption(opts)
	defer db.MustClose()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	wbuf := make([]byte, 8192)
	for i := 0; i < 100; i++ {
		s := fmt.Sprintf("%d", i)
		b, err := tx.CreateBucket([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Put([]byte(s), wbuf); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if tx, err = db.Begin(true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		s := fmt.Sprintf("%d", i)
		b := tx.Bucket([]byte(s))
		if b == nil {
			t.Fatal(err)
		}
		if err := b.Delete([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}

	db.MustReopen()
	freepages := db.Stats().FreePageN
	if freepages == 0 {
		t.Fatalf("no free pages on NoFreelistSync reopen")
	}
	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}

	db.o = options.DefaultBdbOptions
	db.MustReopen()
	freepages--
	if fp := db.Stats().FreePageN; fp < freepages {
		t.Fatalf("closed with %d free pages, opened with %d", freepages, fp)
	}
}

func TestDB_Begin_ErrDatabaseNotOpen(t *testing.T) {
	var db bdb.DB
	if _, err := db.Begin(false); err != bdb.ErrDatabaseNotOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDB_BeginRW(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	} else if tx == nil {
		t.Fatal("expected tx")
	}

	if tx.DB() != db.DB {
		t.Fatal("unexpected tx database")
	} else if !tx.Writable() {
		t.Fatal("expected writable tx")
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Concurrent_WriteTo(t *testing.T) {
	o := &options.BdbOptions{NoFreelistSync: false}
	db := MustOpenWithOption(o)
	defer db.MustClose()

	var wg sync.WaitGroup
	wtxs, rtxs := 5, 5
	wg.Add(wtxs * rtxs)
	f := func(tx *bdb.Tx) {
		defer wg.Done()
		f, err := ioutil.TempFile("", "bdb-")
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Duration(rand.Intn(20)+1) * time.Millisecond)
		tx.WriteTo(f)
		tx.Rollback()
		f.Close()
		snap := &DB{nil, f.Name(), o}
		snap.MustReopen()
		defer snap.MustClose()
		snap.MustCheck()
	}

	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx1.CreateBucket([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < wtxs; i++ {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Bucket([]byte("abc")).Put([]byte{0}, []byte{0}); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < rtxs; j++ {
			rtx, rerr := db.Begin(false)
			if rerr != nil {
				t.Fatal(rerr)
			}
			go f(rtx)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestDB_BeginRW_Closed(t *testing.T) {
	var db bdb.DB
	if _, err := db.Begin(true); err != bdb.ErrDatabaseNotOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDB_Close_PendingTx_RW(t *testing.T) { testDB_Close_PendingTx(t, true) }
func TestDB_Close_PendingTx_RO(t *testing.T) { testDB_Close_PendingTx(t, false) }

func testDB_Close_PendingTx(t *testing.T, writable bool) {
	db := MustOpenDB()

	tx, err := db.Begin(writable)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		err := db.Close()
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("error from inside goroutine: %v", err)
		}
		t.Fatal("database closed too early")
	default:
	}

	if writable {
		err = tx.Commit()
	} else {
		err = tx.Rollback()
	}
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("error from inside goroutine: %v", err)
		}
	default:
		t.Fatal("database did not close")
	}
}

func TestDB_Update(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("baz"), []byte("bat")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		if v := b.Get([]byte("foo")); v != nil {
			t.Fatalf("expected nil value, got: %v", v)
		}
		if v := b.Get([]byte("baz")); !bytes.Equal(v, []byte("bat")) {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Update_Closed(t *testing.T) {
	var db bdb.DB
	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != bdb.ErrDatabaseNotOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDB_Update_ManualCommit(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	var panicked bool
	if err := db.Update(func(tx *bdb.Tx) error {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()
		return nil
	}); err != nil {
		t.Fatal(err)
	} else if !panicked {
		t.Fatal("expected panic")
	}
}

func TestDB_Update_ManualRollback(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	var panicked bool
	if err := db.Update(func(tx *bdb.Tx) error {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}()
		return nil
	}); err != nil {
		t.Fatal(err)
	} else if !panicked {
		t.Fatal("expected panic")
	}
}

func TestDB_View_ManualCommit(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	var panicked bool
	if err := db.View(func(tx *bdb.Tx) error {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()
		return nil
	}); err != nil {
		t.Fatal(err)
	} else if !panicked {
		t.Fatal("expected panic")
	}
}

func TestDB_View_ManualRollback(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	var panicked bool
	if err := db.View(func(tx *bdb.Tx) error {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}()
		return nil
	}); err != nil {
		t.Fatal(err)
	} else if !panicked {
		t.Fatal("expected panic")
	}
}

func TestDB_Update_Panic(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Log("recover: update", r)
			}
		}()

		if err := db.Update(func(tx *bdb.Tx) error {
			if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
				t.Fatal(err)
			}
			panic("omg")
		}); err != nil {
			t.Fatal(err)
		}
	}()

	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		if tx.Bucket([]byte("widgets")) == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_View_Error(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.View(func(tx *bdb.Tx) error {
		return errors.New("xxx")
	}); err == nil || err.Error() != "xxx" {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDB_View_Panic(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Log("recover: view", r)
			}
		}()

		if err := db.View(func(tx *bdb.Tx) error {
			if tx.Bucket([]byte("widgets")) == nil {
				t.Fatal("expected bucket")
			}
			panic("omg")
		}); err != nil {
			t.Fatal(err)
		}
	}()

	if err := db.View(func(tx *bdb.Tx) error {
		if tx.Bucket([]byte("widgets")) == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Stats(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	stats := db.Stats()
	if stats.TxStats.PageCount != 2 {
		t.Fatalf("unexpected TxStats.PageCount: %d", stats.TxStats.PageCount)
	} else if stats.FreePageN != 0 {
		t.Fatalf("unexpected FreePageN != 0: %d", stats.FreePageN)
	} else if stats.PendingPageN != 2 {
		t.Fatalf("unexpected PendingPageN != 2: %d", stats.PendingPageN)
	}
}

func TestDB_Consistency(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := db.Update(func(tx *bdb.Tx) error {
			if err := tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar")); err != nil {
				t.Fatal(err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		if p, _ := tx.Page(0); p == nil {
			t.Fatal("expected page")
		} else if p.Type != "meta" {
			t.Fatalf("unexpected page type: %s", p.Type)
		}

		if p, _ := tx.Page(1); p == nil {
			t.Fatal("expected page")
		} else if p.Type != "meta" {
			t.Fatalf("unexpected page type: %s", p.Type)
		}

		if p, _ := tx.Page(2); p == nil {
			t.Fatal("expected page")
		} else if p.Type != "free" {
			t.Fatalf("unexpected page type: %s", p.Type)
		}

		if p, _ := tx.Page(3); p == nil {
			t.Fatal("expected page")
		} else if p.Type != "free" {
			t.Fatalf("unexpected page type: %s", p.Type)
		}

		if p, _ := tx.Page(4); p == nil {
			t.Fatal("expected page")
		} else if p.Type != "leaf" {
			t.Fatalf("unexpected page type: %s", p.Type)
		}

		if p, _ := tx.Page(5); p == nil {
			t.Fatal("expected page")
		} else if p.Type != "freelist" {
			t.Fatalf("unexpected page type: %s", p.Type)
		}

		if p, _ := tx.Page(6); p != nil {
			t.Fatal("unexpected page")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDBStats_Sub(t *testing.T) {
	var a, b bdb.Stats
	a.TxStats.PageCount = 3
	a.FreePageN = 4
	b.TxStats.PageCount = 10
	b.FreePageN = 14
	diff := b.Sub(&a)
	if diff.TxStats.PageCount != 7 {
		t.Fatalf("unexpected TxStats.PageCount: %d", diff.TxStats.PageCount)
	}

	if diff.FreePageN != 14 {
		t.Fatalf("unexpected FreePageN: %d", diff.FreePageN)
	}
}

func TestDB_Batch(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	n := 2
	ch := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			ch <- db.Batch(func(tx *bdb.Tx) error {
				return tx.Bucket([]byte("widgets")).Put(u64tob(uint64(i)), []byte{})
			})
		}(i)
	}

	for i := 0; i < n; i++ {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < n; i++ {
			if v := b.Get(u64tob(uint64(i))); v == nil {
				t.Errorf("key not found: %d", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Batch_Panic(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	var sentinel int
	var bork = &sentinel
	var problem interface{}
	var err error

	func() {
		defer func() {
			if p := recover(); p != nil {
				problem = p
			}
		}()
		err = db.Batch(func(tx *bdb.Tx) error {
			panic(bork)
		})
	}()

	if g, e := err, error(nil); g != e {
		t.Fatalf("wrong error: %v != %v", g, e)
	}
	if g, e := problem, bork; g != e {
		t.Fatalf("wrong error: %v != %v", g, e)
	}
}

func TestDB_BatchFull(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	const size = 3
	ch := make(chan error, size)
	put := func(i int) {
		ch <- db.Batch(func(tx *bdb.Tx) error {
			return tx.Bucket([]byte("widgets")).Put(u64tob(uint64(i)), []byte{})
		})
	}

	db.MaxBatchSize = size
	db.MaxBatchDelay = 1 * time.Hour

	go put(1)
	go put(2)

	time.Sleep(10 * time.Millisecond)

	select {
	case <-ch:
		t.Fatalf("batch triggered too early")
	default:
	}

	go put(3)

	for i := 0; i < size; i++ {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 1; i <= size; i++ {
			if v := b.Get(u64tob(uint64(i))); v == nil {
				t.Errorf("key not found: %d", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_BatchTime(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	const size = 1
	ch := make(chan error, size)
	put := func(i int) {
		ch <- db.Batch(func(tx *bdb.Tx) error {
			return tx.Bucket([]byte("widgets")).Put(u64tob(uint64(i)), []byte{})
		})
	}

	db.MaxBatchSize = 1000
	db.MaxBatchDelay = 0

	go put(1)

	for i := 0; i < size; i++ {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 1; i <= size; i++ {
			if v := b.Get(u64tob(uint64(i))); v == nil {
				t.Errorf("key not found: %d", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Compact(t *testing.T) {
	dbDir := "/tmp/000"
	os.RemoveAll(dbDir)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatal(err)
	}

	srcFile := path.Join(dbDir, "db.src")
	srcDb, err := bdb.Open(srcFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srcDb.Close()

	destFile := path.Join(dbDir, "db.dest")
	destDb, err := bdb.Open(destFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer destDb.Close()

	n := 10000
	tx, err := srcDb.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := tx.CreateBucketIfNotExists([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}

	commitCount := 0
	for i := 0; i < n; i++ {
		key := "test-" + strconv.FormatInt(int64(i), 10)
		value := "value-" + strconv.FormatInt(int64(i), 10)

		if err := bucket.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		commitCount++
		if commitCount > 0 && commitCount%200 == 0 {
			tx.Commit()
			commitCount = 0
			tx, _ = srcDb.Begin(true)
			bucket = tx.Bucket([]byte("widgets"))
		}
	}
	if commitCount > 0 {
		tx.Commit()
		commitCount = 0
	} else {
		tx.Rollback()
		commitCount = 0
	}

	tx, _ = srcDb.Begin(true)
	bucket = tx.Bucket([]byte("widgets"))
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			continue
		}
		key := "test-" + strconv.FormatInt(int64(i), 10)

		if err := bucket.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
		commitCount++
		if commitCount > 0 && commitCount%200 == 0 {
			tx.Commit()
			commitCount = 0
			tx, _ = srcDb.Begin(true)
			bucket = tx.Bucket([]byte("widgets"))
		}
	}
	if commitCount > 0 {
		tx.Commit()
		commitCount = 0
	} else {
		tx.Rollback()
		commitCount = 0
	}

	txMaxSize := int64(200)
	if err := bdb.Compact(destDb, srcDb, txMaxSize); err != nil {
		t.Fatal(err)
	}
	fmt.Println("compact end")
}

func ExampleDB_Update() {
	db, err := bdb.Open(tempfile(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value of 'foo' is: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleDB_View() {
	db, err := bdb.Open(tempfile(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("people"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("john"), []byte("doe")); err != nil {
			return err
		}
		if err := b.Put([]byte("susy"), []byte("que")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		v := tx.Bucket([]byte("people")).Get([]byte("john"))
		fmt.Printf("John's last name is %s.\n", v)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleDB_Begin() {
	db, err := bdb.Open(tempfile(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	if err = db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	}); err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}
	b := tx.Bucket([]byte("widgets"))
	if err = b.Put([]byte("john"), []byte("blue")); err != nil {
		log.Fatal(err)
	}
	if err = b.Put([]byte("abby"), []byte("red")); err != nil {
		log.Fatal(err)
	}
	if err = b.Put([]byte("zephyr"), []byte("purple")); err != nil {
		log.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		log.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		log.Fatal(err)
	}
	c := tx.Bucket([]byte("widgets")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("%s likes %s\n", k, v)
	}

	if err = tx.Rollback(); err != nil {
		log.Fatal(err)
	}

	if err = db.Close(); err != nil {
		log.Fatal(err)
	}
}

func BenchmarkDBBatchAutomatic(b *testing.B) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := make(chan struct{})
		var wg sync.WaitGroup

		for round := 0; round < 1000; round++ {
			wg.Add(1)

			go func(id uint32) {
				defer wg.Done()
				<-start

				h := fnv.New32a()
				buf := make([]byte, 4)
				binary.LittleEndian.PutUint32(buf, id)
				_, _ = h.Write(buf[:])
				k := h.Sum(nil)
				insert := func(tx *bdb.Tx) error {
					b := tx.Bucket([]byte("bench"))
					return b.Put(k, []byte("filler"))
				}
				if err := db.Batch(insert); err != nil {
					b.Error(err)
					return
				}
			}(uint32(round))
		}
		close(start)
		wg.Wait()
	}

	b.StopTimer()
	validateBatchBench(b, db)
}

func BenchmarkDBBatchSingle(b *testing.B) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := make(chan struct{})
		var wg sync.WaitGroup

		for round := 0; round < 1000; round++ {
			wg.Add(1)
			go func(id uint32) {
				defer wg.Done()
				<-start

				h := fnv.New32a()
				buf := make([]byte, 4)
				binary.LittleEndian.PutUint32(buf, id)
				_, _ = h.Write(buf[:])
				k := h.Sum(nil)
				insert := func(tx *bdb.Tx) error {
					b := tx.Bucket([]byte("bench"))
					return b.Put(k, []byte("filler"))
				}
				if err := db.Update(insert); err != nil {
					b.Error(err)
					return
				}
			}(uint32(round))
		}
		close(start)
		wg.Wait()
	}

	b.StopTimer()
	validateBatchBench(b, db)
}

func BenchmarkDBBatchManual10x100(b *testing.B) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		errCh := make(chan error, 10)

		for major := 0; major < 10; major++ {
			wg.Add(1)
			go func(id uint32) {
				defer wg.Done()
				<-start

				insert100 := func(tx *bdb.Tx) error {
					h := fnv.New32a()
					buf := make([]byte, 4)
					for minor := uint32(0); minor < 100; minor++ {
						binary.LittleEndian.PutUint32(buf, uint32(id*100+minor))
						h.Reset()
						_, _ = h.Write(buf[:])
						k := h.Sum(nil)
						b := tx.Bucket([]byte("bench"))
						if err := b.Put(k, []byte("filler")); err != nil {
							return err
						}
					}
					return nil
				}
				err := db.Update(insert100)
				errCh <- err
			}(uint32(major))
		}
		close(start)
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.StopTimer()
	validateBatchBench(b, db)
}

func validateBatchBench(b *testing.B, db *DB) {
	var rollback = errors.New("sentinel error to cause rollback")
	validate := func(tx *bdb.Tx) error {
		bucket := tx.Bucket([]byte("bench"))
		h := fnv.New32a()
		buf := make([]byte, 4)
		for id := uint32(0); id < 1000; id++ {
			binary.LittleEndian.PutUint32(buf, id)
			h.Reset()
			_, _ = h.Write(buf[:])
			k := h.Sum(nil)
			v := bucket.Get(k)
			if v == nil {
				b.Errorf("not found id=%d key=%x", id, k)
				continue
			}
			if g, e := v, []byte("filler"); !bytes.Equal(g, e) {
				b.Errorf("bad value for id=%d key=%x: %s != %q", id, k, g, e)
			}
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			b.Errorf("unexpected key: %x = %q", k, v)
		}
		return rollback
	}
	if err := db.Update(validate); err != nil && err != rollback {
		b.Error(err)
	}
}

type DB struct {
	*bdb.DB
	f string
	o *options.BdbOptions
}

func MustOpenDB() *DB {
	return MustOpenWithOption(nil)
}

func MustOpenWithOption(o *options.BdbOptions) *DB {
	f := tempfile()
	if o == nil {
		o = options.DefaultBdbOptions
	}

	freelistType := consts.BdbFreelistArrayType
	if env := os.Getenv(bdb.TestFreelistType); env == consts.BdbFreelistMapType {
		freelistType = consts.BdbFreelistMapType
	}
	o.FreelistType = freelistType

	db, err := bdb.Open(f, o)
	if err != nil {
		panic(err)
	}
	return &DB{
		DB: db,
		f:  f,
		o:  o,
	}
}

func (db *DB) Close() error {
	if *statsFlag {
		db.PrintStats()
	}

	db.MustCheck()

	defer os.Remove(db.Path())
	return db.DB.Close()
}

func (db *DB) MustClose() {
	if err := db.Close(); err != nil {
		panic(err)
	}
}

func (db *DB) MustReopen() {
	indb, err := bdb.Open(db.f, db.o)
	if err != nil {
		panic(err)
	}
	db.DB = indb
}

func (db *DB) PrintStats() {
	var stats = db.Stats()
	fmt.Printf("[db] %-20s %-20s %-20s\n",
		fmt.Sprintf("pg(%d/%d)", stats.TxStats.PageCount, stats.TxStats.PageAlloc),
		fmt.Sprintf("cur(%d)", stats.TxStats.CursorCount),
		fmt.Sprintf("node(%d/%d)", stats.TxStats.NodeCount, stats.TxStats.NodeDeref),
	)
	fmt.Printf("     %-20s %-20s %-20s\n",
		fmt.Sprintf("rebal(%d/%v)", stats.TxStats.Rebalance, truncDuration(stats.TxStats.RebalanceTime)),
		fmt.Sprintf("spill(%d/%v)", stats.TxStats.Spill, truncDuration(stats.TxStats.SpillTime)),
		fmt.Sprintf("w(%d/%v)", stats.TxStats.Write, truncDuration(stats.TxStats.WriteTime)),
	)
}

func (db *DB) MustCheck() {
	if err := db.Update(func(tx *bdb.Tx) error {
		var errors []error
		for err := range tx.Check() {
			errors = append(errors, err)
			if len(errors) > 10 {
				break
			}
		}

		if len(errors) > 0 {
			var path = tempfile()
			if err := tx.CopyFile(path, 0600); err != nil {
				panic(err)
			}

			fmt.Print("\n\n")
			fmt.Printf("consistency check failed (%d errors)\n", len(errors))
			for _, err := range errors {
				fmt.Println(err)
			}
			fmt.Println("")
			fmt.Println("db saved to:")
			fmt.Println(path)
			fmt.Print("\n\n")
			os.Exit(-1)
		}

		return nil
	}); err != nil && err != bdb.ErrDatabaseNotOpen {
		panic(err)
	}
}

func (db *DB) CopyTempFile() {
	path := tempfile()
	if err := db.View(func(tx *bdb.Tx) error {
		return tx.CopyFile(path, 0600)
	}); err != nil {
		panic(err)
	}
	fmt.Println("db copied to: ", path)
}

func tempfile() string {
	f, err := os.CreateTemp("", "bdb-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}

func trunc(b []byte, length int) []byte {
	if length < len(b) {
		return b[:length]
	}
	return b
}

func truncDuration(d time.Duration) string {
	return regexp.MustCompile(`^(\d+)(\.\d+)`).ReplaceAllString(d.String(), "$1")
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
