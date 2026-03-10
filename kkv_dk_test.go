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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKKVDKHash(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	d := openTestDB(dir)
	dataType := DataTypeDKHash
	sid := testSlotId
	kn := 10
	ts := uint64(time.Now().UnixMilli() + 1000*20)

	read := func(db *DB) {
		for i := 0; i < kn; i++ {
			key := makeTestIntKey(i)
			shardNum := uint32(i + 1)
			dt, timestamp, _, sz, _, sn, err1 := db.GetMeta(key, sid)
			require.NoError(t, err1)
			if i%2 == 0 {
				require.Equal(t, ts, timestamp)
			} else {
				require.Equal(t, uint64(0), timestamp)
			}
			require.Equal(t, shardNum, sn)
			require.Equal(t, dataType, dt)
			require.Equal(t, uint64(6), sz)
		}
	}

	for i := 0; i < kn; i++ {
		key := makeTestIntKey(i)
		shardNum := uint32(i + 1)
		require.NoError(t, d.DKCreate(key, sid, dataType, shardNum))

		if i%2 == 0 {
			n, _, err := d.ExpireAt(key, sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}

		incr := int64(0)
		for j := 0; j < 3; j++ {
			incr += int64(j + 1)
			n1, err1 := d.DKIncrBySize(key, sid, int64(j+1))
			require.NoError(t, err1)
			require.Equal(t, incr, n1)
		}
	}

	read(d)

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(time.Second * 2)

	read(d)
	require.NoError(t, d.Close())

	d = openTestDB(dir)
	read(d)
	require.NoError(t, d.Close())
}

func TestKKVDKSet(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	d := openTestDB(dir)
	dataType := DataTypeDKSet
	sid := testSlotId
	kn := 10
	ts := uint64(time.Now().UnixMilli() + 1000*20)

	read := func(db *DB) {
		for i := 0; i < kn; i++ {
			key := makeTestIntKey(i)
			shardNum := uint32(i + 1)
			dt, timestamp, _, sz, _, sn, err1 := db.GetMeta(key, sid)
			require.NoError(t, err1)
			if i%2 == 0 {
				require.Equal(t, ts, timestamp)
			} else {
				require.Equal(t, uint64(0), timestamp)
			}
			require.Equal(t, shardNum, sn)
			require.Equal(t, dataType, dt)
			require.Equal(t, uint64(6), sz)
		}
	}

	for i := 0; i < kn; i++ {
		key := makeTestIntKey(i)
		shardNum := uint32(i + 1)
		require.NoError(t, d.DKCreate(key, sid, dataType, shardNum))

		if i%2 == 0 {
			n, _, err := d.ExpireAt(key, sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}

		incr := int64(0)
		for j := 0; j < 3; j++ {
			incr += int64(j + 1)
			n1, err1 := d.DKIncrBySize(key, sid, int64(j+1))
			require.NoError(t, err1)
			require.Equal(t, incr, n1)
		}
	}

	read(d)

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(time.Second * 2)

	read(d)
	require.NoError(t, d.Close())

	d = openTestDB(dir)
	read(d)
	require.NoError(t, d.Close())
}
