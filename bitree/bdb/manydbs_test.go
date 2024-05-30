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

package bdb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func createDb(t *testing.T) (*DB, func()) {
	tempDirName, err := ioutil.TempDir("", "bdbmemtest")
	if err != nil {
		t.Fatalf("error creating temp dir: %v", err)
	}
	path := filepath.Join(tempDirName, "testdb.db")

	bdb, err := Open(path, nil)
	if err != nil {
		t.Fatalf("error creating bdb db: %v", err)
	}

	cleanup := func() {
		bdb.Close()
		os.RemoveAll(tempDirName)
	}

	return bdb, cleanup
}

func createAndPutKeys(t *testing.T) {
	t.Parallel()

	db, cleanup := createDb(t)
	defer cleanup()

	bucketName := []byte("bucket")

	for i := 0; i < 100; i++ {
		err := db.Update(func(tx *Tx) error {
			nodes, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}

			var key [16]byte
			rand.Read(key[:])
			if err := nodes.Put(key[:], nil); err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestManyDBs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("%d", i), createAndPutKeys)
	}
}
