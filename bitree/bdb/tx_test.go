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

package bdb_test

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

func TestTx_Check_ReadOnly(t *testing.T) {
	db := MustOpenDB()
	defer db.Close()
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

	opts := options.DefaultBdbOptions
	opts.ReadOnly = true
	readOnlyDB, err := bdb.Open(db.f, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnlyDB.Close()

	tx, err := readOnlyDB.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	numChecks := 2
	errc := make(chan error, numChecks)
	check := func() {
		errc <- <-tx.Check()
	}

	for i := 0; i < numChecks; i++ {
		go check()
	}
	for i := 0; i < numChecks; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}

	tx.Rollback()
}

func TestTx_Commit_ErrTxClosed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.CreateBucket([]byte("foo")); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != bdb.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestTx_Rollback_ErrTxClosed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != bdb.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestTx_Commit_ErrTxNotWritable(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != bdb.ErrTxNotWritable {
		t.Fatal(err)
	}
	tx.Rollback()
}

// Ensure that a transaction can retrieve a cursor on the root bucket.
func TestTx_Cursor(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}

		if _, err := tx.CreateBucket([]byte("woojits")); err != nil {
			t.Fatal(err)
		}

		c := tx.Cursor()
		if k, v := c.First(); !bytes.Equal(k, []byte("widgets")) {
			t.Fatalf("unexpected key: %v", k)
		} else if v != nil {
			t.Fatalf("unexpected value: %v", v)
		}

		if k, v := c.Next(); !bytes.Equal(k, []byte("woojits")) {
			t.Fatalf("unexpected key: %v", k)
		} else if v != nil {
			t.Fatalf("unexpected value: %v", v)
		}

		if k, v := c.Next(); k != nil {
			t.Fatalf("unexpected key: %v", k)
		} else if v != nil {
			t.Fatalf("unexpected value: %v", k)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that creating a bucket with a read-only transaction returns an error.
func TestTx_CreateBucket_ErrTxNotWritable(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.View(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("foo"))
		if err != bdb.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_CreateBucket_ErrTxClosed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.CreateBucket([]byte("foo")); err != bdb.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestTx_Bucket(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		if tx.Bucket([]byte("widgets")) == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_Get_NotFound(t *testing.T) {
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
		if b.Get([]byte("no_such_key")) != nil {
			t.Fatal("expected nil value")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_CreateBucket(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		} else if b == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		if tx.Bucket([]byte("widgets")) == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_CreateBucketIfNotExists(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		if b, err := tx.CreateBucketIfNotExists([]byte("widgets")); err != nil {
			t.Fatal(err)
		} else if b == nil {
			t.Fatal("expected bucket")
		}

		if b, err := tx.CreateBucketIfNotExists([]byte("widgets")); err != nil {
			t.Fatal(err)
		} else if b == nil {
			t.Fatal("expected bucket")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		if tx.Bucket([]byte("widgets")) == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_CreateBucketIfNotExists_ErrBucketNameRequired(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte{}); err != bdb.ErrBucketNameRequired {
			t.Fatalf("unexpected error: %s", err)
		}

		if _, err := tx.CreateBucketIfNotExists(nil); err != bdb.ErrBucketNameRequired {
			t.Fatalf("unexpected error: %s", err)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_CreateBucket_ErrBucketExists(t *testing.T) {
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

	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != bdb.ErrBucketExists {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_CreateBucket_ErrBucketNameRequired(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket(nil); err != bdb.ErrBucketNameRequired {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_DeleteBucket(t *testing.T) {
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

	if err := db.Update(func(tx *bdb.Tx) error {
		if err := tx.DeleteBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		if tx.Bucket([]byte("widgets")) != nil {
			t.Fatal("unexpected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if v := b.Get([]byte("foo")); v != nil {
			t.Fatalf("unexpected phantom value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_DeleteBucket_ErrTxClosed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteBucket([]byte("foo")); err != bdb.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestTx_DeleteBucket_ReadOnly(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.View(func(tx *bdb.Tx) error {
		if err := tx.DeleteBucket([]byte("foo")); err != bdb.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_DeleteBucket_NotFound(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		if err := tx.DeleteBucket([]byte("widgets")); err != bdb.ErrBucketNotFound {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_ForEach_NoError(t *testing.T) {
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

		if err := tx.ForEach(func(name []byte, b *bdb.Bucket) error {
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that an error is returned when a tx.ForEach function returns an error.
func TestTx_ForEach_WithError(t *testing.T) {
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

		marker := errors.New("marker")
		if err := tx.ForEach(func(name []byte, b *bdb.Bucket) error {
			return marker
		}); err != marker {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_OnCommit(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	var x int
	if err := db.Update(func(tx *bdb.Tx) error {
		tx.OnCommit(func() { x += 1 })
		tx.OnCommit(func() { x += 2 })
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	} else if x != 3 {
		t.Fatalf("unexpected x: %d", x)
	}
}

func TestTx_OnCommit_Rollback(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	var x int
	if err := db.Update(func(tx *bdb.Tx) error {
		tx.OnCommit(func() { x += 1 })
		tx.OnCommit(func() { x += 2 })
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return errors.New("rollback this commit")
	}); err == nil || err.Error() != "rollback this commit" {
		t.Fatalf("unexpected error: %s", err)
	} else if x != 0 {
		t.Fatalf("unexpected x: %d", x)
	}
}

func TestTx_CopyFile(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	path := tempfile()
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
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		return tx.CopyFile(path, 0600)
	}); err != nil {
		t.Fatal(err)
	}

	db2, err := bdb.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := db2.View(func(tx *bdb.Tx) error {
		if v := tx.Bucket([]byte("widgets")).Get([]byte("foo")); !bytes.Equal(v, []byte("bar")) {
			t.Fatalf("unexpected value: %v", v)
		}
		if v := tx.Bucket([]byte("widgets")).Get([]byte("baz")); !bytes.Equal(v, []byte("bat")) {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}

type failWriterError struct{}

func (failWriterError) Error() string {
	return "error injected for tests"
}

type failWriter struct {
	After int
}

func (f *failWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	if n > f.After {
		n = f.After
		err = failWriterError{}
	}
	f.After -= n
	return n, err
}

func TestTx_CopyFile_Error_Meta(t *testing.T) {
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
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		return tx.Copy(&failWriter{})
	}); err == nil || err.Error() != "meta 0 copy: error injected for tests" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTx_CopyFile_Error_Normal(t *testing.T) {
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
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		return tx.Copy(&failWriter{3 * db.Info().PageSize})
	}); err == nil || err.Error() != "error injected for tests" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTx_Rollback(t *testing.T) {
	for _, isSyncFreelist := range []bool{false, true} {
		db, err := bdb.Open(tempfile(), nil)
		if err != nil {
			log.Fatal(err)
		}
		defer os.Remove(db.Path())
		db.NoFreelistSync = isSyncFreelist

		tx, err := db.Begin(true)
		if err != nil {
			t.Fatalf("Error starting tx: %v", err)
		}
		bucket := []byte("mybucket")
		if _, err := tx.CreateBucket(bucket); err != nil {
			t.Fatalf("Error creating bucket: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Error on commit: %v", err)
		}

		tx, err = db.Begin(true)
		if err != nil {
			t.Fatalf("Error starting tx: %v", err)
		}
		b := tx.Bucket(bucket)
		if err := b.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("Error on put: %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("Error on rollback: %v", err)
		}

		tx, err = db.Begin(false)
		if err != nil {
			t.Fatalf("Error starting tx: %v", err)
		}
		b = tx.Bucket(bucket)
		if v := b.Get([]byte("k")); v != nil {
			t.Fatalf("Value for k should not have been stored")
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Error on rollback: %v", err)
		}

	}
}

func TestTx_releaseRange(t *testing.T) {
	opts := options.DefaultBdbOptions
	opts.InitialMmapSize = os.Getpagesize() * 100
	db := MustOpenWithOption(opts)
	defer db.MustClose()

	bucket := "bucket"

	put := func(key, value string) {
		if err := db.Update(func(tx *bdb.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				t.Fatal(err)
			}
			return b.Put([]byte(key), []byte(value))
		}); err != nil {
			t.Fatal(err)
		}
	}

	del := func(key string) {
		if err := db.Update(func(tx *bdb.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				t.Fatal(err)
			}
			return b.Delete([]byte(key))
		}); err != nil {
			t.Fatal(err)
		}
	}

	getWithTxn := func(txn *bdb.Tx, key string) []byte {
		return txn.Bucket([]byte(bucket)).Get([]byte(key))
	}

	openReadTxn := func() *bdb.Tx {
		readTx, err := db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		return readTx
	}

	checkWithReadTxn := func(txn *bdb.Tx, key string, wantValue []byte) {
		value := getWithTxn(txn, key)
		if !bytes.Equal(value, wantValue) {
			t.Errorf("Wanted value to be %s for key %s, but got %s", wantValue, key, string(value))
		}
	}

	rollback := func(txn *bdb.Tx) {
		if err := txn.Rollback(); err != nil {
			t.Fatal(err)
		}
	}

	put("k1", "v1")
	rtx1 := openReadTxn()
	put("k2", "v2")
	hold1 := openReadTxn()
	put("k3", "v3")
	hold2 := openReadTxn()
	del("k3")
	rtx2 := openReadTxn()
	del("k1")
	hold3 := openReadTxn()
	del("k2")
	hold4 := openReadTxn()
	put("k4", "v4")
	hold5 := openReadTxn()

	rollback(hold1)
	rollback(hold2)
	rollback(hold3)
	rollback(hold4)
	rollback(hold5)

	put("k4", "v4")

	checkWithReadTxn(rtx1, "k1", []byte("v1"))
	checkWithReadTxn(rtx2, "k2", []byte("v2"))
	rollback(rtx1)
	rollback(rtx2)

	rtx7 := openReadTxn()
	checkWithReadTxn(rtx7, "k1", nil)
	checkWithReadTxn(rtx7, "k2", nil)
	checkWithReadTxn(rtx7, "k3", nil)
	checkWithReadTxn(rtx7, "k4", []byte("v4"))
	rollback(rtx7)
}

func ExampleTx_Rollback() {
	db, err := bdb.Open(tempfile(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		return tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
	}); err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}
	b := tx.Bucket([]byte("widgets"))
	if err := b.Put([]byte("foo"), []byte("baz")); err != nil {
		log.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		log.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value for 'foo' is still: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleTx_CopyFile() {
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

	toFile := tempfile()
	if err := db.View(func(tx *bdb.Tx) error {
		return tx.CopyFile(toFile, 0666)
	}); err != nil {
		log.Fatal(err)
	}
	defer os.Remove(toFile)

	db2, err := bdb.Open(toFile, nil)
	if err != nil {
		log.Fatal(err)
	}

	if err := db2.View(func(tx *bdb.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value for 'foo' in the clone is: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	if err := db2.Close(); err != nil {
		log.Fatal(err)
	}
}
