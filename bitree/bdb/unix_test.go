//go:build !windows
// +build !windows

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
	"fmt"
	"testing"

	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/base"

	"golang.org/x/sys/unix"
)

func TestMlock_DbOpen(t *testing.T) {
	skipOnMemlockLimitBelow(t, 32*1024)

	db := MustOpenWithOption(&base.BdbOptions{Mlock: true})
	defer db.MustClose()
}

func TestMlock_DbCanGrow_Small(t *testing.T) {
	skipOnMemlockLimitBelow(t, 32*1024)

	db := MustOpenWithOption(&base.BdbOptions{Mlock: true})
	defer db.MustClose()

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("bucket"))
		if err != nil {
			t.Fatal(err)
		}

		key := []byte("key")
		value := []byte("value")
		if err := b.Put(key, value); err != nil {
			t.Fatal(err)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

}

func TestMlock_DbCanGrow_Big(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	skipOnMemlockLimitBelow(t, 32*1024*1024)

	chunksBefore := 64
	chunksAfter := 64

	db := MustOpenWithOption(&base.BdbOptions{Mlock: true})
	defer db.MustClose()

	for chunk := 0; chunk < chunksBefore; chunk++ {
		insertChunk(t, db, chunk)
	}
	dbSize := fileSize(db.f)

	for chunk := 0; chunk < chunksAfter; chunk++ {
		insertChunk(t, db, chunksBefore+chunk)
	}
	newDbSize := fileSize(db.f)

	if newDbSize <= dbSize {
		t.Errorf("db didn't grow: %v <= %v", newDbSize, dbSize)
	}
}

func insertChunk(t *testing.T, db *DB, chunkId int) {
	chunkSize := 1024

	if err := db.Update(func(tx *bdb.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("bucket"))
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < chunkSize; i++ {
			key := []byte(fmt.Sprintf("key-%d-%d", chunkId, i))
			value := []byte("value")
			if err := b.Put(key, value); err != nil {
				t.Fatal(err)
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func skipOnMemlockLimitBelow(t *testing.T, memlockLimitRequest uint64) {
	var info unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_MEMLOCK, &info); err != nil {
		t.Fatal(err)
	}

	if info.Cur < memlockLimitRequest {
		t.Skip(fmt.Sprintf(
			"skipping as RLIMIT_MEMLOCK is unsufficient: %v < %v",
			info.Cur,
			memlockLimitRequest,
		))
	}
}
