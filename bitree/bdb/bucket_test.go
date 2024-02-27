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
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/zuoyebang/bitalosdb/bitree/bdb"
)

func TestBucket_Get_NonExistent(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if v := b.Get([]byte("foo")); v != nil {
			t.Fatal("expected nil value")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Get_FromNode(t *testing.T) {
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
		if v := b.Get([]byte("foo")); !bytes.Equal(v, []byte("bar")) {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Get_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo")); err != nil {
			t.Fatal(err)
		}

		if tx.Bucket([]byte("widgets")).Get([]byte("foo")) != nil {
			t.Fatal("expected nil value")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Get_Capacity(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		return b.Put([]byte("key"), []byte("val"))
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		k, v := tx.Bucket([]byte("bucket")).Cursor().First()

		if len(k) != cap(k) {
			t.Fatalf("unexpected key slice capacity: %d", cap(k))
		} else if len(v) != cap(v) {
			t.Fatalf("unexpected value slice capacity: %d", cap(v))
		}

		k = append(k, []byte("123")...)
		v = append(v, []byte("123")...)
		_, _ = k, v

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Put(t *testing.T) {
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

		v := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if !bytes.Equal([]byte("bar"), v) {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Put_Repeat(t *testing.T) {
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
		if err := b.Put([]byte("foo"), []byte("baz")); err != nil {
			t.Fatal(err)
		}

		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if !bytes.Equal([]byte("baz"), value) {
			t.Fatalf("unexpected value: %v", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Put_Large(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	count, factor := 100, 200
	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < count; i++ {
			if err := b.Put([]byte(strings.Repeat("0", i*factor)), []byte(strings.Repeat("X", (count-i)*factor))); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 1; i < count; i++ {
			value := b.Get([]byte(strings.Repeat("0", i*factor)))
			if !bytes.Equal(value, []byte(strings.Repeat("X", (count-i)*factor))) {
				t.Fatalf("unexpected value: %v", value)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Put_VeryLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	n, batchN := 400000, 200000
	ksize, vsize := 8, 500

	db := MustOpenDB()
	defer db.MustClose()

	for i := 0; i < n; i += batchN {
		if err := db.Update(func(tx *bdb.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
			if err != nil {
				t.Fatal(err)
			}
			for j := 0; j < batchN; j++ {
				k, v := make([]byte, ksize), make([]byte, vsize)
				binary.BigEndian.PutUint32(k, uint32(i+j))
				if err := b.Put(k, v); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBucket_Put_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b0, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		if err := b0.Put([]byte("foo"), []byte("bar")); err != bdb.ErrIncompatibleValue {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Put_Closed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	b, err := tx.CreateBucket([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := b.Put([]byte("foo"), []byte("bar")); err != bdb.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestBucket_Put_ReadOnly(t *testing.T) {
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

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		if err := b.Put([]byte("foo"), []byte("bar")); err != bdb.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Delete(t *testing.T) {
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
		if err := b.Delete([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		if v := b.Get([]byte("foo")); v != nil {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Delete_Large(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 100; i++ {
			if err := b.Put([]byte(strconv.Itoa(i)), []byte(strings.Repeat("*", 1024))); err != nil {
				t.Fatal(err)
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 100; i++ {
			if err := b.Delete([]byte(strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 100; i++ {
			if v := b.Get([]byte(strconv.Itoa(i))); v != nil {
				t.Fatalf("unexpected value: %v, i=%d", v, i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Delete_FreelistOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db := MustOpenDB()
	defer db.MustClose()

	k := make([]byte, 16)
	for i := uint64(0); i < 10000; i++ {
		if err := db.Update(func(tx *bdb.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("0"))
			if err != nil {
				t.Fatalf("bucket error: %s", err)
			}

			for j := uint64(0); j < 1000; j++ {
				binary.BigEndian.PutUint64(k[:8], i)
				binary.BigEndian.PutUint64(k[8:], j)
				if err := b.Put(k, nil); err != nil {
					t.Fatalf("put error: %s", err)
				}
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("0"))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	stats := db.Stats()
	freePages := stats.FreePageN + stats.PendingPageN
	if freePages <= 0xFFFF {
		t.Fatalf("expected more than 0xFFFF free pages, got %v", freePages)
	}

	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}
	db.MustReopen()
	if reopenFreePages := db.Stats().FreePageN; freePages != reopenFreePages {
		t.Fatalf("expected %d free pages, got %+v", freePages, db.Stats())
	}
}

func TestBucket_Delete_NonExisting(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if _, err = b.CreateBucket([]byte("nested")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		if err := b.Delete([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		if b.Bucket([]byte("nested")) == nil {
			t.Fatal("nested bucket has been deleted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Nested(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		_, err = b.CreateBucket([]byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		if err := b.Put([]byte("bar"), []byte("0000")); err != nil {
			t.Fatal(err)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.MustCheck()

	if err := db.Update(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		if err := b.Put([]byte("bar"), []byte("xxxx")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.MustCheck()

	if err := db.Update(func(tx *bdb.Tx) error {
		var b = tx.Bucket([]byte("widgets"))
		for i := 0; i < 10000; i++ {
			if err := b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.MustCheck()

	if err := db.Update(func(tx *bdb.Tx) error {
		var b = tx.Bucket([]byte("widgets"))
		if err := b.Bucket([]byte("foo")).Put([]byte("baz"), []byte("yyyy")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.MustCheck()

	if err := db.View(func(tx *bdb.Tx) error {
		var b = tx.Bucket([]byte("widgets"))
		if v := b.Bucket([]byte("foo")).Get([]byte("baz")); !bytes.Equal(v, []byte("yyyy")) {
			t.Fatalf("unexpected value: %v", v)
		}
		if v := b.Get([]byte("bar")); !bytes.Equal(v, []byte("xxxx")) {
			t.Fatalf("unexpected value: %v", v)
		}
		for i := 0; i < 10000; i++ {
			if v := b.Get([]byte(strconv.Itoa(i))); !bytes.Equal(v, []byte(strconv.Itoa(i))) {
				t.Fatalf("unexpected value: %v", v)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Delete_Bucket(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.CreateBucket([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete([]byte("foo")); err != bdb.ErrIncompatibleValue {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Delete_ReadOnly(t *testing.T) {
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

	if err := db.View(func(tx *bdb.Tx) error {
		if err := tx.Bucket([]byte("widgets")).Delete([]byte("foo")); err != bdb.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Delete_Closed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	b, err := tx.CreateBucket([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("foo")); err != bdb.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestBucket_DeleteBucket_Nested(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		foo, err := widgets.CreateBucket([]byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		bar, err := foo.CreateBucket([]byte("bar"))
		if err != nil {
			t.Fatal(err)
		}
		if err := bar.Put([]byte("baz"), []byte("bat")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Bucket([]byte("widgets")).DeleteBucket([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_DeleteBucket_Nested2(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		foo, err := widgets.CreateBucket([]byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		bar, err := foo.CreateBucket([]byte("bar"))
		if err != nil {
			t.Fatal(err)
		}

		if err := bar.Put([]byte("baz"), []byte("bat")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		widgets := tx.Bucket([]byte("widgets"))
		if widgets == nil {
			t.Fatal("expected widgets bucket")
		}

		foo := widgets.Bucket([]byte("foo"))
		if foo == nil {
			t.Fatal("expected foo bucket")
		}

		bar := foo.Bucket([]byte("bar"))
		if bar == nil {
			t.Fatal("expected bar bucket")
		}

		if v := bar.Get([]byte("baz")); !bytes.Equal(v, []byte("bat")) {
			t.Fatalf("unexpected value: %v", v)
		}
		if err := tx.DeleteBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		if tx.Bucket([]byte("widgets")) != nil {
			t.Fatal("expected bucket to be deleted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_DeleteBucket_Large(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		foo, err := widgets.CreateBucket([]byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 1000; i++ {
			if err := foo.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%0100d", i))); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		if err := tx.DeleteBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Bucket_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if err := widgets.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if b := tx.Bucket([]byte("widgets")).Bucket([]byte("foo")); b != nil {
			t.Fatal("expected nil bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_CreateBucket_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if err := widgets.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if _, err := widgets.CreateBucket([]byte("foo")); err != bdb.ErrIncompatibleValue {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_DeleteBucket_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := widgets.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Bucket([]byte("widgets")).DeleteBucket([]byte("foo")); err != bdb.ErrIncompatibleValue {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Sequence(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		bkt, err := tx.CreateBucket([]byte("0"))
		if err != nil {
			t.Fatal(err)
		}

		if v := bkt.Sequence(); v != 0 {
			t.Fatalf("unexpected sequence: %d", v)
		}

		if err := bkt.SetSequence(1000); err != nil {
			t.Fatal(err)
		}

		if v := bkt.Sequence(); v != 1000 {
			t.Fatalf("unexpected sequence: %d", v)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		if v := tx.Bucket([]byte("0")).Sequence(); v != 1000 {
			t.Fatalf("unexpected sequence: %d", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_NextSequence(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		woojits, err := tx.CreateBucket([]byte("woojits"))
		if err != nil {
			t.Fatal(err)
		}

		if seq, err := widgets.NextSequence(); err != nil {
			t.Fatal(err)
		} else if seq != 1 {
			t.Fatalf("unexpecte sequence: %d", seq)
		}

		if seq, err := widgets.NextSequence(); err != nil {
			t.Fatal(err)
		} else if seq != 2 {
			t.Fatalf("unexpected sequence: %d", seq)
		}

		if seq, err := woojits.NextSequence(); err != nil {
			t.Fatal(err)
		} else if seq != 1 {
			t.Fatalf("unexpected sequence: %d", 1)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_NextSequence_Persist(t *testing.T) {
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
		if _, err := tx.Bucket([]byte("widgets")).NextSequence(); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		seq, err := tx.Bucket([]byte("widgets")).NextSequence()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else if seq != 2 {
			t.Fatalf("unexpected sequence: %d", seq)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_NextSequence_ReadOnly(t *testing.T) {
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

	if err := db.View(func(tx *bdb.Tx) error {
		_, err := tx.Bucket([]byte("widgets")).NextSequence()
		if err != bdb.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_NextSequence_Closed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	b, err := tx.CreateBucket([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.NextSequence(); err != bdb.ErrTxClosed {
		t.Fatal(err)
	}
}

func TestBucket_ForEach(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("0000")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("baz"), []byte("0001")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("bar"), []byte("0002")); err != nil {
			t.Fatal(err)
		}

		var index int
		if err := b.ForEach(func(k, v []byte) error {
			switch index {
			case 0:
				if !bytes.Equal(k, []byte("bar")) {
					t.Fatalf("unexpected key: %v", k)
				} else if !bytes.Equal(v, []byte("0002")) {
					t.Fatalf("unexpected value: %v", v)
				}
			case 1:
				if !bytes.Equal(k, []byte("baz")) {
					t.Fatalf("unexpected key: %v", k)
				} else if !bytes.Equal(v, []byte("0001")) {
					t.Fatalf("unexpected value: %v", v)
				}
			case 2:
				if !bytes.Equal(k, []byte("foo")) {
					t.Fatalf("unexpected key: %v", k)
				} else if !bytes.Equal(v, []byte("0000")) {
					t.Fatalf("unexpected value: %v", v)
				}
			}
			index++
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if index != 3 {
			t.Fatalf("unexpected index: %d", index)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_ForEach_ShortCircuit(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("bar"), []byte("0000")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("baz"), []byte("0000")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("0000")); err != nil {
			t.Fatal(err)
		}

		var index int
		if err := tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
			index++
			if bytes.Equal(k, []byte("baz")) {
				return errors.New("marker")
			}
			return nil
		}); err == nil || err.Error() != "marker" {
			t.Fatalf("unexpected error: %s", err)
		}
		if index != 2 {
			t.Fatalf("unexpected index: %d", index)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_ForEach_Closed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	b, err := tx.CreateBucket([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := b.ForEach(func(k, v []byte) error { return nil }); err != bdb.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestBucket_Put_EmptyKey(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte(""), []byte("bar")); err != bdb.ErrKeyRequired {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := b.Put(nil, []byte("bar")); err != bdb.ErrKeyRequired {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Put_KeyTooLarge(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(make([]byte, 32769), []byte("bar")); err != bdb.ErrKeyTooLarge {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Put_ValueTooLarge(t *testing.T) {
	if os.Getenv("DRONE") == "true" {
		t.Skip("not enough RAM for test")
	}

	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), make([]byte, bdb.MaxValueSize+1)); err != bdb.ErrValueTooLarge {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Stats_RandomFill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	} else if os.Getpagesize() != 4096 {
		t.Skip("invalid page size for test")
	}

	db := MustOpenDB()
	defer db.MustClose()

	var count int
	rand := rand.New(rand.NewSource(42))
	for _, i := range rand.Perm(1000) {
		if err := db.Update(func(tx *bdb.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("woojits"))
			if err != nil {
				t.Fatal(err)
			}
			b.FillPercent = 0.9
			for _, j := range rand.Perm(100) {
				index := (j * 10000) + i
				if err := b.Put([]byte(fmt.Sprintf("%d000000000000000", index)), []byte("0000000000")); err != nil {
					t.Fatal(err)
				}
				count++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	db.MustCheck()

	if err := db.View(func(tx *bdb.Tx) error {
		stats := tx.Bucket([]byte("woojits")).Stats()
		if stats.KeyN != 100000 {
			t.Fatalf("unexpected KeyN: %d", stats.KeyN)
		}

		if stats.BranchPageN != 98 {
			t.Fatalf("unexpected BranchPageN: %d", stats.BranchPageN)
		} else if stats.BranchOverflowN != 0 {
			t.Fatalf("unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		} else if stats.BranchInuse != 130984 {
			t.Fatalf("unexpected BranchInuse: %d", stats.BranchInuse)
		} else if stats.BranchAlloc != 401408 {
			t.Fatalf("unexpected BranchAlloc: %d", stats.BranchAlloc)
		}

		if stats.LeafPageN != 3412 {
			t.Fatalf("unexpected LeafPageN: %d", stats.LeafPageN)
		} else if stats.LeafOverflowN != 0 {
			t.Fatalf("unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		} else if stats.LeafInuse != 4742482 {
			t.Fatalf("unexpected LeafInuse: %d", stats.LeafInuse)
		} else if stats.LeafAlloc != 13975552 {
			t.Fatalf("unexpected LeafAlloc: %d", stats.LeafAlloc)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Stats_Small(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("whozawhats"))
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

	db.MustCheck()

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("whozawhats"))
		stats := b.Stats()
		if stats.BranchPageN != 0 {
			t.Fatalf("unexpected BranchPageN: %d", stats.BranchPageN)
		} else if stats.BranchOverflowN != 0 {
			t.Fatalf("unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		} else if stats.LeafPageN != 0 {
			t.Fatalf("unexpected LeafPageN: %d", stats.LeafPageN)
		} else if stats.LeafOverflowN != 0 {
			t.Fatalf("unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		} else if stats.KeyN != 1 {
			t.Fatalf("unexpected KeyN: %d", stats.KeyN)
		} else if stats.Depth != 1 {
			t.Fatalf("unexpected Depth: %d", stats.Depth)
		} else if stats.BranchInuse != 0 {
			t.Fatalf("unexpected BranchInuse: %d", stats.BranchInuse)
		} else if stats.LeafInuse != 0 {
			t.Fatalf("unexpected LeafInuse: %d", stats.LeafInuse)
		}

		if db.Info().PageSize == 4096 {
			if stats.BranchAlloc != 0 {
				t.Fatalf("unexpected BranchAlloc: %d", stats.BranchAlloc)
			} else if stats.LeafAlloc != 0 {
				t.Fatalf("unexpected LeafAlloc: %d", stats.LeafAlloc)
			}
		}

		if stats.BucketN != 1 {
			t.Fatalf("unexpected BucketN: %d", stats.BucketN)
		} else if stats.InlineBucketN != 1 {
			t.Fatalf("unexpected InlineBucketN: %d", stats.InlineBucketN)
		} else if stats.InlineBucketInuse != 16+16+6 {
			t.Fatalf("unexpected InlineBucketInuse: %d", stats.InlineBucketInuse)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Stats_EmptyBucket(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		if _, err := tx.CreateBucket([]byte("whozawhats")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	db.MustCheck()

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("whozawhats"))
		stats := b.Stats()
		if stats.BranchPageN != 0 {
			t.Fatalf("unexpected BranchPageN: %d", stats.BranchPageN)
		} else if stats.BranchOverflowN != 0 {
			t.Fatalf("unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		} else if stats.LeafPageN != 0 {
			t.Fatalf("unexpected LeafPageN: %d", stats.LeafPageN)
		} else if stats.LeafOverflowN != 0 {
			t.Fatalf("unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		} else if stats.KeyN != 0 {
			t.Fatalf("unexpected KeyN: %d", stats.KeyN)
		} else if stats.Depth != 1 {
			t.Fatalf("unexpected Depth: %d", stats.Depth)
		} else if stats.BranchInuse != 0 {
			t.Fatalf("unexpected BranchInuse: %d", stats.BranchInuse)
		} else if stats.LeafInuse != 0 {
			t.Fatalf("unexpected LeafInuse: %d", stats.LeafInuse)
		}

		if db.Info().PageSize == 4096 {
			if stats.BranchAlloc != 0 {
				t.Fatalf("unexpected BranchAlloc: %d", stats.BranchAlloc)
			} else if stats.LeafAlloc != 0 {
				t.Fatalf("unexpected LeafAlloc: %d", stats.LeafAlloc)
			}
		}

		if stats.BucketN != 1 {
			t.Fatalf("unexpected BucketN: %d", stats.BucketN)
		} else if stats.InlineBucketN != 1 {
			t.Fatalf("unexpected InlineBucketN: %d", stats.InlineBucketN)
		} else if stats.InlineBucketInuse != 16 {
			t.Fatalf("unexpected InlineBucketInuse: %d", stats.InlineBucketInuse)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Stats_Nested(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("foo"))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%02d", i)), []byte(fmt.Sprintf("%02d", i))); err != nil {
				t.Fatal(err)
			}
		}

		bar, err := b.CreateBucket([]byte("bar"))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			if err := bar.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}
		}

		baz, err := bar.CreateBucket([]byte("baz"))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			if err := baz.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	db.MustCheck()

	if err := db.View(func(tx *bdb.Tx) error {
		b := tx.Bucket([]byte("foo"))
		stats := b.Stats()
		if stats.BranchPageN != 0 {
			t.Fatalf("unexpected BranchPageN: %d", stats.BranchPageN)
		} else if stats.BranchOverflowN != 0 {
			t.Fatalf("unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		} else if stats.LeafPageN != 2 {
			t.Fatalf("unexpected LeafPageN: %d", stats.LeafPageN)
		} else if stats.LeafOverflowN != 0 {
			t.Fatalf("unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		} else if stats.KeyN != 122 {
			t.Fatalf("unexpected KeyN: %d", stats.KeyN)
		} else if stats.Depth != 3 {
			t.Fatalf("unexpected Depth: %d", stats.Depth)
		} else if stats.BranchInuse != 0 {
			t.Fatalf("unexpected BranchInuse: %d", stats.BranchInuse)
		}

		foo := 16
		foo += 101 * 16
		foo += 100*2 + 100*2
		foo += 3 + 16

		bar := 16
		bar += 11 * 16
		bar += 10 + 10
		bar += 3 + 16

		baz := 16
		baz += 10 * 16
		baz += 10 + 10

		if stats.LeafInuse != foo+bar+baz {
			t.Fatalf("unexpected LeafInuse: %d", stats.LeafInuse)
		}

		if db.Info().PageSize == 4096 {
			if stats.BranchAlloc != 0 {
				t.Fatalf("unexpected BranchAlloc: %d", stats.BranchAlloc)
			} else if stats.LeafAlloc != 8192 {
				t.Fatalf("unexpected LeafAlloc: %d", stats.LeafAlloc)
			}
		}

		if stats.BucketN != 3 {
			t.Fatalf("unexpected BucketN: %d", stats.BucketN)
		} else if stats.InlineBucketN != 1 {
			t.Fatalf("unexpected InlineBucketN: %d", stats.InlineBucketN)
		} else if stats.InlineBucketInuse != baz {
			t.Fatalf("unexpected InlineBucketInuse: %d", stats.InlineBucketInuse)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func No_TestBucket_Stats_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db := MustOpenDB()
	defer db.MustClose()

	var index int
	for i := 0; i < 100; i++ {
		if err := db.Update(func(tx *bdb.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 1000; i++ {
				if err := b.Put([]byte(strconv.Itoa(index)), []byte(strconv.Itoa(index))); err != nil {
					t.Fatal(err)
				}
				index++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	db.MustCheck()

	if err := db.View(func(tx *bdb.Tx) error {
		stats := tx.Bucket([]byte("widgets")).Stats()
		if stats.BranchPageN != 13 {
			t.Fatalf("unexpected BranchPageN: %d", stats.BranchPageN)
		} else if stats.BranchOverflowN != 0 {
			t.Fatalf("unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		} else if stats.LeafPageN != 1196 {
			t.Fatalf("unexpected LeafPageN: %d", stats.LeafPageN)
		} else if stats.LeafOverflowN != 0 {
			t.Fatalf("unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		} else if stats.KeyN != 100000 {
			t.Fatalf("unexpected KeyN: %d", stats.KeyN)
		} else if stats.Depth != 3 {
			t.Fatalf("unexpected Depth: %d", stats.Depth)
		} else if stats.BranchInuse != 25257 {
			t.Fatalf("unexpected BranchInuse: %d", stats.BranchInuse)
		} else if stats.LeafInuse != 2596916 {
			t.Fatalf("unexpected LeafInuse: %d", stats.LeafInuse)
		}

		if db.Info().PageSize == 4096 {
			if stats.BranchAlloc != 53248 {
				t.Fatalf("unexpected BranchAlloc: %d", stats.BranchAlloc)
			} else if stats.LeafAlloc != 4898816 {
				t.Fatalf("unexpected LeafAlloc: %d", stats.LeafAlloc)
			}
		}

		if stats.BucketN != 1 {
			t.Fatalf("unexpected BucketN: %d", stats.BucketN)
		} else if stats.InlineBucketN != 0 {
			t.Fatalf("unexpected InlineBucketN: %d", stats.InlineBucketN)
		} else if stats.InlineBucketInuse != 0 {
			t.Fatalf("unexpected InlineBucketInuse: %d", stats.InlineBucketInuse)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBucket_Put_Single(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	index := 0
	if err := quick.Check(func(items testdata) bool {
		db := MustOpenDB()
		defer db.MustClose()

		m := make(map[string][]byte)

		if err := db.Update(func(tx *bdb.Tx) error {
			if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
				t.Fatal(err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		for _, item := range items {
			if err := db.Update(func(tx *bdb.Tx) error {
				if err := tx.Bucket([]byte("widgets")).Put(item.Key, item.Value); err != nil {
					panic("put error: " + err.Error())
				}
				m[string(item.Key)] = item.Value
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if err := db.View(func(tx *bdb.Tx) error {
				i := 0
				for k, v := range m {
					value := tx.Bucket([]byte("widgets")).Get([]byte(k))
					if !bytes.Equal(value, v) {
						t.Logf("value mismatch [run %d] (%d of %d):\nkey: %x\ngot: %x\nexp: %x", index, i, len(m), []byte(k), value, v)
						db.CopyTempFile()
						t.FailNow()
					}
					i++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}

		index++
		return true
	}, qconfig()); err != nil {
		t.Error(err)
	}
}

func TestBucket_Put_Multiple(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	if err := quick.Check(func(items testdata) bool {
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
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				if err := b.Put(item.Key, item.Value); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := db.View(func(tx *bdb.Tx) error {
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				value := b.Get(item.Key)
				if !bytes.Equal(item.Value, value) {
					db.CopyTempFile()
					t.Fatalf("exp=%x; got=%x", item.Value, value)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		return true
	}, qconfig()); err != nil {
		t.Error(err)
	}
}

func TestBucket_Delete_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	if err := quick.Check(func(items testdata) bool {
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
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				if err := b.Put(item.Key, item.Value); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		for _, item := range items {
			if err := db.Update(func(tx *bdb.Tx) error {
				return tx.Bucket([]byte("widgets")).Delete(item.Key)
			}); err != nil {
				t.Fatal(err)
			}
		}

		if err := db.View(func(tx *bdb.Tx) error {
			if err := tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
				t.Fatalf("bucket should be empty; found: %06x", trunc(k, 3))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		return true
	}, qconfig()); err != nil {
		t.Error(err)
	}
}

func ExampleBucket_Put() {
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

func ExampleBucket_Delete() {
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

		value := b.Get([]byte("foo"))
		fmt.Printf("The value of 'foo' was: %s\n", value)

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Update(func(tx *bdb.Tx) error {
		return tx.Bucket([]byte("widgets")).Delete([]byte("foo"))
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.View(func(tx *bdb.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if value == nil {
			fmt.Printf("The value of 'foo' is now: nil\n")
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleBucket_ForEach() {
	db, err := bdb.Open(tempfile(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucket([]byte("animals"))
		if err != nil {
			return err
		}

		if err := b.Put([]byte("dog"), []byte("fun")); err != nil {
			return err
		}
		if err := b.Put([]byte("cat"), []byte("lame")); err != nil {
			return err
		}
		if err := b.Put([]byte("liger"), []byte("awesome")); err != nil {
			return err
		}

		if err := b.ForEach(func(k, v []byte) error {
			fmt.Printf("A %s is %s.\n", k, v)
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
