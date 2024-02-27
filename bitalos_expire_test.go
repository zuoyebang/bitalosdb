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
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
)

func testOpenBitalosDBForExpire(dir string) (*BitalosDB, error) {
	opts := &Options{
		MemTableSize:                testMemTableSize,
		MemTableStopWritesThreshold: testMaxWriteBufferNumber,
		Verbose:                     true,
		LogTag:                      "[bitalosdb/test]",
		Logger:                      DefaultLogger,
		UseBithash:                  true,
		UseBitable:                  true,
		UseMapIndex:                 true,
		KeyHashFunc:                 base.DefaultKeyHashFunc,
		KvCheckExpireFunc:           base.TestKvCheckExpireFunc,
		KvTimestampFunc:             base.TestKvTimestampFunc,
	}
	_, err := os.Stat(dir)
	if nil != err && !os.IsExist(err) {
		err = os.MkdirAll(dir, 0775)
		if nil != err {
			return nil, err
		}
		opts.WALDir = ""
	}

	pdb, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &BitalosDB{
		db:   pdb,
		ro:   &IterOptions{},
		wo:   NoSync,
		opts: opts,
	}, nil
}

func TestBitalosdb_MemFlush_CheckExpire(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	bitalosDB, err := testOpenBitalosDBForExpire(testDirname)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	const randLen = 20
	const valLen = randLen + 9
	valBytes := testRandBytes(valLen)
	now := uint64(time.Now().UnixMilli())
	makeValue := func(i int) []byte {
		val := make([]byte, valLen, valLen)
		ttl := now
		if i%2 == 0 {
			ttl += 20000
		}
		val[0] = 1
		binary.BigEndian.PutUint64(val[1:9], ttl)
		copy(val[9:], valBytes)
		return val[:]
	}
	makeKey := func(i int) []byte {
		keyVersion := uint64(i/100 + 100)
		key := []byte(fmt.Sprintf("key_%d", i))
		return testMakeKey2(key, uint16(keyVersion), keyVersion)
	}

	count := 10000

	for i := 0; i < count; i++ {
		key := makeKey(i)
		value := makeValue(i)
		if err = bitalosDB.db.Set(key, value, bitalosDB.wo); err != nil {
			t.Error("set err:", err)
		}
	}

	time.Sleep(time.Second)
	require.NoError(t, bitalosDB.db.Flush())

	for i := 0; i < count; i++ {
		key := makeKey(i)
		value := makeValue(i)
		v, vcloser, err := bitalosDB.db.Get(key)

		if i%2 == 0 {
			require.NoError(t, err)
			require.Equal(t, value, v)
			vcloser()
		} else if err != ErrNotFound {
			t.Fatal("find not exist key", string(key))
		}
	}
}

func TestBitalosdb_Bitable_CheckExpire(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	bitalosDB, err := testOpenBitalosDBForExpire(testDirname)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bitalosDB.db.Close())
	}()

	const randLen = 20
	const valLen = randLen + 9
	valBytes := testRandBytes(valLen)
	now := uint64(time.Now().UnixMilli())
	count := 10000
	makeKey := func(i int) []byte {
		keyVersion := uint64(i/100 + 100)
		key := []byte(fmt.Sprintf("key_%d", i))
		return testMakeKey2(key, uint16(keyVersion), keyVersion)
	}
	makeValue := func(i int) []byte {
		val := make([]byte, valLen, valLen)
		ttl := now
		if i%5 == 0 {
			ttl = now + 2000
		} else {
			ttl = now + 30000
		}
		val[0] = 1
		binary.BigEndian.PutUint64(val[1:9], ttl)
		copy(val[9:], valBytes)
		return val[:]
	}

	writeData := func() {
		for i := 0; i < count; i++ {
			key := makeKey(i)
			value := makeValue(i)
			if err = bitalosDB.db.Set(key, value, bitalosDB.wo); err != nil {
				t.Error("set err:", err)
			}
		}
		require.NoError(t, bitalosDB.db.Flush())
	}

	readData := func() {
		for i := 0; i < count; i++ {
			key := makeKey(i)
			value := makeValue(i)
			v, vcloser, err := bitalosDB.db.Get(key)
			require.NoError(t, err)
			require.Equal(t, value, v)
			vcloser()
		}
	}

	readDeleteKV := func(jobId int) {
		fmt.Println("readDeleteKV", jobId)
		for i := 0; i < count; i++ {
			key := makeKey(i)
			value := makeValue(i)

			isExist := true
			if jobId == 2 && i%5 == 0 {
				isExist = false
			}

			v, vcloser, err := bitalosDB.db.Get(key)
			if isExist {
				require.NoError(t, err)
				require.Equal(t, value, v)
				vcloser()
			} else {
				if err != ErrNotFound || len(v) > 0 {
					t.Fatal("find expire key", string(key))
				}
			}
		}
	}

	writeData()
	readData()
	fmt.Println("CompactBitree 1-------------")
	bitalosDB.db.bf.CompactBitree(1)
	readData()

	readDeleteKV(1)
	writeData()
	time.Sleep(3 * time.Second)
	fmt.Println("CompactBitree 2-------------")
	bitalosDB.db.bf.CompactBitree(2)
	readDeleteKV(2)
}
