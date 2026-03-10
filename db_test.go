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
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
	"github.com/stretchr/testify/require"
)

const (
	testDirname        = "./test-data"
	testSlotId  uint16 = 1
)

func makeTestIntKey(i int) []byte {
	return sortedkv.MakeSortedKey(i)
}

func makeTestIntValue(i int, v []byte) []byte {
	return []byte(fmt.Sprintf("%s_%d", v, i))
}

func makeTestKeySlotid(key []byte) uint16 {
	khash := hash.Fnv32(key)
	return uint16(khash & metaSlotNumMask)
}

func openTestDB(dir string) *DB {
	opts := &Options{
		Logger:              DefaultLogger,
		VectorTableCount:    1,
		VectorTableHashSize: 262144,
		GetNowTimestamp:     options.DefaultGetNowTimestamp,
		VmTableSize:         32 << 20,
		MemTableSize:        32 << 20,
		BitpageFlushSize:    10 << 20,
		BitpageSplitSize:    20 << 20,
		BithashTableSize:    10 << 20,
	}
	return openTestDBByOpts(dir, opts)
}

func openTestDB1(dir string, useMiniVi bool) *DB {
	opts := &Options{
		Logger:               DefaultLogger,
		VectorTableCount:     1,
		VectorTableHashSize:  262144,
		GetNowTimestamp:      options.DefaultGetNowTimestamp,
		VmTableSize:          32 << 20,
		MemTableSize:         32 << 20,
		BitpageFlushSize:     10 << 20,
		BitpageSplitSize:     20 << 20,
		BitpageDisableMiniVi: useMiniVi,
		BithashTableSize:     10 << 20,
	}
	return openTestDBByOpts(dir, opts)
}

func openTestDB2(dir string, count uint16, disableStoreKey bool) *DB {
	opts := &Options{
		Logger:              DefaultLogger,
		DisableStoreKey:     disableStoreKey,
		VectorTableCount:    count,
		VectorTableHashSize: 262144,
		GetNowTimestamp:     options.DefaultGetNowTimestamp,
		VmTableSize:         32 << 20,
		MemTableSize:        32 << 20,
		BitpageFlushSize:    10 << 20,
		BitpageSplitSize:    20 << 20,
		BithashTableSize:    10 << 20,
	}
	return openTestDBByOpts(dir, opts)
}

func openTestDBByOpts(dir string, opts *Options) *DB {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0775); err != nil {
			panic(err)
		}
	}

	db, err := Open(dir, opts)
	if err != nil {
		panic(err)
	}
	return db
}

func testGet(t *testing.T, d *DB, key []byte, slotId uint16, expVal []byte, expDt uint8, expTTL uint64) {
	value, dt, timestamp, closer, err := d.Get(key, slotId)
	if err != nil {
		t.Fatalf("get fail key:%s err:%s\n", string(key), err)
	} else if !bytes.Equal(expVal, value) {
		t.Fatalf("get fail val not eq key:%s exp:%s act:%s\n", string(key), string(expVal), string(value))
	} else if dt != expDt {
		t.Fatalf("get fail dt not eq key:%s exp:%d act:%d\n", string(key), expDt, dt)
	} else if expTTL != timestamp {
		t.Fatalf("get fail ttl not eq key:%s exp:%d act:%d\n", string(key), expTTL, timestamp)
	} else if closer != nil {
		closer()
	}
}

func testGetNotFound(t *testing.T, d *DB, key []byte, slotId uint16) {
	_, _, _, _, err := d.Get(key, slotId)
	if err != base.ErrNotFound {
		t.Errorf("find not exist key:%s\n", string(key))
	}
}

func TestDBOpen(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	for i := 0; i < 2; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, uint16(i), DataTypeString, 0, utils.FuncRandBytes(1024)))
	}
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	require.NoError(t, db.Close())
}

func TestDBOpenClose(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	fs := vfs.Default
	opts := &Options{
		FS: fs,
	}

	for _, startFromEmpty := range []bool{false, true} {
		for _, length := range []int{-1, 0, 1, 128, 256} {
			dirname := "/sharedDatabase"
			if startFromEmpty {
				dirname = "/startFromEmpty" + strconv.Itoa(length)
			}
			dirname = fs.PathJoin(dir, dirname)
			got, xxx := []byte(nil), ""
			if length >= 0 {
				xxx = strings.Repeat("x", length)
			}

			d0, err := Open(dirname, opts)
			if err != nil {
				t.Fatalf("sfe=%t, length=%d: Open #0: %v", startFromEmpty, length, err)
			}
			if length >= 0 {
				tmpk := []byte("key")
				err = d0.Set(tmpk, makeTestKeySlotid(tmpk), DataTypeString, 0, []byte(xxx))
				if err != nil {
					t.Fatalf("sfe=%t, length=%d: Set: %v", startFromEmpty, length, err)
				}
			}
			err = d0.Close()
			if err != nil {
				t.Fatalf("sfe=%t, length=%d: Close #0: %v", startFromEmpty, length, err)
			}

			d1, err1 := Open(dirname, opts)
			if err1 != nil {
				t.Errorf("sfe=%t, length=%d: Open #1: %v", startFromEmpty, length, err1)
				continue
			}
			if length >= 0 {
				var closer func()
				tmpk := []byte("key")
				got, _, _, closer, err = d1.Get(tmpk, makeTestKeySlotid(tmpk))
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Get: %v", startFromEmpty, length, err)
					continue
				}
				got = append([]byte(nil), got...)
				closer()
			}
			err = d1.Close()
			if err != nil {
				t.Errorf("sfe=%t, length=%d: Close #1: %v", startFromEmpty, length, err)
				continue
			}

			if length >= 0 && string(got) != xxx {
				t.Errorf("sfe=%t, length=%d: got value differs from set value", startFromEmpty, length)
				continue
			}
		}
	}
}

func TestDBExpireAt(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	nowTime := uint64(time.Now().UnixMilli())
	key := makeTestIntKey(100)
	ts := nowTime + 100*1000
	dt := DataTypeString
	sid := makeTestKeySlotid(key)
	shard := db.getVmShard(sid)

	n, _, err := db.ExpireAt(key, sid, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	var v []byte
	opCmd := func() {
		for i := 0; i < 2; i++ {
			v = utils.FuncRandBytes(100)
			require.NoError(t, db.Set(key, sid, dt, ts, v))
			testGet(t, db, key, sid, v, dt, ts)

			n, _, err = db.ExpireAt(key, sid, 0)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			n, _, err = db.ExpireAt(key, sid, 0)
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
			testGet(t, db, key, sid, v, dt, 0)

			n, _, err = db.ExpireAt(key, sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			shard.makeRoomForWrite(false, false)

			n, _, err = db.ExpireAt(key, sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			dataType, oldTs, err2 := db.GetTimestamp(key, sid)
			require.NoError(t, err2)
			require.Equal(t, dt, dataType)
			require.Equal(t, ts, oldTs)
			testGet(t, db, key, sid, v, dt, ts)

			v = utils.FuncRandBytes(100)
			require.NoError(t, db.Set(key, sid, dt, ts, v))
			testGet(t, db, key, sid, v, dt, ts)

			require.NoError(t, db.Flush(false))

			ts += 100000
			n, _, err = db.ExpireAt(key, sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			n, _, err = db.ExpireAt(key, sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			testGet(t, db, key, sid, v, dt, ts)

			n, _, err = db.ExpireAt(key, sid, 0)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			n, _, err = db.ExpireAt(key, sid, 0)
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
			testGet(t, db, key, sid, v, dt, 0)

			v = utils.FuncRandBytes(100)
			ts += 100000
			require.NoError(t, db.Set(key, sid, dt, ts, v))
			dataType, oldTs, err2 = db.GetTimestamp(key, sid)
			require.NoError(t, err2)
			require.Equal(t, dt, dataType)
			require.Equal(t, ts, oldTs)
			testGet(t, db, key, sid, v, dt, ts)

			ts += 100000
			n, _, err = db.ExpireAt(key, sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			n, _, err = db.ExpireAt(key, sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			testGet(t, db, key, sid, v, dt, ts)
		}
	}

	opCmd()
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	testGet(t, db, key, sid, v, dt, ts)
	opCmd()
	require.NoError(t, db.Close())
}

func TestDBComplexExpireAt(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	nowTime := uint64(time.Now().UnixMilli())
	key := makeTestIntKey(100)
	subKey1 := makeTestIntKey(200)
	subKey2 := makeTestIntKey(300)
	subKey3 := makeTestIntKey(400)
	ts := nowTime + 100*1000
	dt := DataTypeHash
	sid := makeTestKeySlotid(key)
	shard := db.getVmShard(sid)

	n, _, err := db.ExpireAt(key, sid, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = db.HMSet(key, sid, subKey1, subKey1, subKey2, subKey2)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	_, timestamp, version, _, _, size, _ := db.GetMeta(key, sid)
	require.Equal(t, uint64(0), timestamp)
	require.Equal(t, uint32(2), size)

	shard.makeRoomForWrite(false, false)

	n, _, err = db.ExpireAt(key, sid, ts)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	size, _ = db.GetMetaSize(key, sid)
	require.Equal(t, uint32(2), size)

	n, _, err = db.ExpireAt(key, sid, ts)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	size, _ = db.GetMetaSize(key, sid)
	require.Equal(t, uint32(2), size)

	require.NoError(t, db.Flush(false))

	n, _, err = db.ExpireAt(key, sid, ts)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	shard.makeRoomForWrite(false, false)

	n, err = db.HMSet(key, sid, subKey3, subKey3)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	ts += 100 * 1000
	n, _, err = db.ExpireAt(key, sid, ts)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	_, timestamp, version, _, _, size, _ = db.GetMeta(key, sid)
	require.NoError(t, err)
	require.Equal(t, ts, timestamp)
	require.Equal(t, uint32(3), size)

	subValue, closer, err1 := db.HGet(key, sid, subKey1)
	require.NoError(t, err1)
	require.Equal(t, subKey1, subValue)
	closer()

	dataType, oldTs, err2 := db.GetTimestamp(key, sid)
	require.NoError(t, err2)
	require.Equal(t, dt, dataType)
	require.Equal(t, ts, oldTs)

	n, _, err = db.ExpireAt(key, sid, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	dataType, oldTs, err2 = db.GetTimestamp(key, sid)
	require.NoError(t, err2)
	require.Equal(t, dt, dataType)
	require.Equal(t, uint64(0), oldTs)

	n, _, err = db.ExpireAt(key, sid, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	require.NoError(t, db.Flush(false))

	ts += 100 * 1000
	n, _, err = db.ExpireAt(key, sid, ts)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	dataType, oldTs, err2 = db.GetTimestamp(key, sid)
	require.NoError(t, err2)
	require.Equal(t, dt, dataType)
	require.Equal(t, ts, oldTs)

	ts1 := ts + 1000*1000
	n, _, err = db.ExpireAt(key, sid, ts1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	_, timestamp, version, _, _, size, _ = db.GetMeta(key, sid)
	require.Equal(t, ts1, timestamp)
	require.Equal(t, uint32(3), size)

	require.Equal(t, false, db.eliTask.existExpireKey(sid, version, ts))
	require.Equal(t, true, db.eliTask.existExpireKey(sid, version, ts1))

	require.NoError(t, db.Close())

	db = openTestDB(dir)
	subValue, closer, err = db.HGet(key, sid, subKey2)
	require.NoError(t, err)
	require.Equal(t, subKey2, subValue)
	closer()

	_, timestamp, version, _, _, size, _ = db.GetMeta(key, sid)
	require.Equal(t, ts1, timestamp)
	require.Equal(t, uint32(3), size)

	require.Equal(t, false, db.eliTask.existExpireKey(sid, version, 0))
	require.Equal(t, false, db.eliTask.existExpireKey(sid, version, ts))
	require.Equal(t, true, db.eliTask.existExpireKey(sid, version, ts1))

	ts = ts1 + 1000*1000
	n, _, err = db.ExpireAt(key, sid, ts)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	n, _, err = db.ExpireAt(key, sid, ts)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	subValue, closer, err = db.HGet(key, sid, subKey1)
	require.NoError(t, err)
	require.Equal(t, subKey1, subValue)
	subValue, closer, err = db.HGet(key, sid, subKey2)
	require.NoError(t, err)
	require.Equal(t, subKey2, subValue)
	closer()

	dataType, oldTs, err = db.GetTimestamp(key, sid)
	require.NoError(t, err)
	require.Equal(t, dt, dataType)
	require.Equal(t, ts, oldTs)

	n, _, err = db.ExpireAt(key, sid, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	n, _, err = db.ExpireAt(key, sid, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	dataType, oldTs, err = db.GetTimestamp(key, sid)
	require.NoError(t, err)
	require.Equal(t, dt, dataType)
	require.Equal(t, uint64(0), oldTs)

	ts1 = ts + 1000*1000
	n, _, err = db.ExpireAt(key, sid, ts1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	_, timestamp, version, _, _, size, _ = db.GetMeta(key, sid)
	require.Equal(t, ts1, timestamp)
	require.Equal(t, uint32(3), size)

	require.Equal(t, false, db.eliTask.existExpireKey(sid, version, ts))
	require.Equal(t, true, db.eliTask.existExpireKey(sid, version, ts1))

	require.NoError(t, db.Close())
}

func TestDBVtGC(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	slotId := testSlotId
	dt := DataTypeString
	keyNum := 1000
	expireTime := db.opts.GetNowTimestamp() + 2000
	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		var ts uint64
		if i%2 == 0 {
			ts = expireTime
		}
		require.NoError(t, db.Set(key, slotId, dt, ts, key))
	}

	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		var ts uint64
		if i%2 == 0 {
			ts = expireTime
		}
		testGet(t, db, key, slotId, key, dt, ts)
	}

	require.NoError(t, db.FlushVm())

	time.Sleep(4 * time.Second)
	db.doVtGC(true)

	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		if i%2 == 0 {
			testGetNotFound(t, db, key, slotId)
		} else {
			testGet(t, db, key, slotId, key, dt, 0)
		}
	}

	require.NoError(t, db.Close())
}

func TestDBRemoveSlot(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	value := utils.FuncRandBytes(1024)
	for i := 0; i < 5; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, uint16(i), DataTypeString, 0, value))
	}
	require.NoError(t, db.FlushVm())

	slotId := uint16(2)
	require.Equal(t, slotBitupleCreated, db.meta.slotsStatus[slotId])
	require.NoError(t, db.RemoveSlot(slotId))
	require.Equal(t, slotBitupleNotCreated, db.meta.slotsStatus[slotId])

	for i := 0; i < 5; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, uint16(i), DataTypeString, 0, value))
	}

	require.NoError(t, db.Close())

	db = openTestDB(dir)
	for i := 0; i < 5; i++ {
		key := makeTestIntKey(i)
		testGet(t, db, key, uint16(i), value, DataTypeString, 0)
	}
	require.Equal(t, slotBitupleCreated, db.meta.slotsStatus[slotId])
	slotId = uint16(3)
	require.Equal(t, slotBitupleCreated, db.meta.slotsStatus[slotId])
	require.NoError(t, db.RemoveSlot(slotId))
	require.Equal(t, slotBitupleNotCreated, db.meta.slotsStatus[slotId])

	value = utils.FuncRandBytes(1024)
	for i := 0; i < 5; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, uint16(i), DataTypeString, 0, value))
	}
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	for i := 0; i < 5; i++ {
		key := makeTestIntKey(i)
		testGet(t, db, key, uint16(i), value, DataTypeString, 0)
	}
	require.NoError(t, db.Close())
}

func TestDBBitupleOpen(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	for i := 0; i < 3; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, uint16(i), DataTypeString, 0, key))
	}
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	for i, created := range db.meta.slotsStatus {
		if i < 3 {
			require.Equal(t, slotBitupleCreated, created)
		} else {
			require.Equal(t, slotBitupleNotCreated, created)
		}
	}
	for i := 3; i < 6; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, uint16(i), DataTypeString, 0, key))
	}
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	for i, created := range db.meta.slotsStatus {
		if i < 6 {
			require.Equal(t, slotBitupleCreated, created)
		} else {
			require.Equal(t, slotBitupleNotCreated, created)
		}
	}
	require.NoError(t, db.Close())
}

func TestDBSetVmExpire(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	dt := DataTypeString
	ts := db.opts.GetNowTimestamp()
	for i := 0; i < 100; i++ {
		key := makeTestIntKey(i)
		if i%2 == 0 {
			require.NoError(t, db.Set(key, testSlotId, dt, ts+10000, key))
		} else {
			require.NoError(t, db.Set(key, testSlotId, dt, ts+1, key))
		}
	}
	time.Sleep(1 * time.Second)
	require.NoError(t, db.FlushVm())
	for i := 0; i < 100; i++ {
		key := makeTestIntKey(i)
		if i%2 == 0 {
			testGet(t, db, key, testSlotId, key, dt, ts+10000)
		} else {
			testGetNotFound(t, db, key, testSlotId)
		}
	}

	require.NoError(t, db.Close())
}

func testcase(caseFunc func([]bool)) {
	for _, params := range [][]bool{
		{true},
		{false},
	} {
		fmt.Printf("testcase params:%v\n", params)
		caseFunc(params)
	}
}

func TestDBWriteRead(t *testing.T) {
	testcase(func(params []bool) {
		dir := testDirname
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		db := openTestDB2(dir, 1, params[0])
		dt := DataTypeString
		slotId := testSlotId
		keyNum := 10000
		ts := db.opts.GetNowTimestamp() + 10000
		for i := 0; i < keyNum; i++ {
			key := makeTestIntKey(i)
			require.NoError(t, db.Set(key, slotId, dt, ts, key))
		}
		for i := 0; i < keyNum; i++ {
			key := makeTestIntKey(i)
			testGet(t, db, key, slotId, key, dt, ts)
		}
		require.NoError(t, db.FlushVm())
		for i := 0; i < keyNum; i++ {
			key := makeTestIntKey(i)
			testGet(t, db, key, slotId, key, dt, ts)
		}

		require.NoError(t, db.Close())

		db = openTestDB(dir)
		for i := 0; i < keyNum; i++ {
			key := makeTestIntKey(i)
			testGet(t, db, key, slotId, key, dt, ts)
		}

		require.NoError(t, db.Close())
	})
}

func TestDBVmVtableWrite(t *testing.T) {
	testcase(func(params []bool) {
		dir := testDirname
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		db := openTestDB2(dir, 1, params[0])
		db.opts.VmTableStopWritesThreshold = 2
		dt := DataTypeString
		slotId := testSlotId
		keyNum := 200
		vprefix := utils.FuncRandBytes(102400)

		ckDir := filepath.Join(dir, "ck")
		ckCloser, err := db.Checkpoint(ckDir)
		require.NoError(t, err)

		for i := 0; i < keyNum/2; i++ {
			key := makeTestIntKey(i)
			value := makeTestIntValue(i, vprefix)
			require.NoError(t, db.Set(key, slotId, dt, 0, value))
		}

		if ckCloser != nil {
			ckCloser()
		}

		for i := keyNum / 2; i < keyNum; i++ {
			key := makeTestIntKey(i)
			value := makeTestIntValue(i, vprefix)
			require.NoError(t, db.Set(key, slotId, dt, 0, value))
		}

		for i := 0; i < keyNum; i++ {
			key := makeTestIntKey(i)
			value := makeTestIntValue(i, vprefix)
			testGet(t, db, key, slotId, value, dt, 0)
		}

		require.NoError(t, db.Close())

		db = openTestDB2(dir, 1, params[0])
		for i := 0; i < keyNum; i++ {
			key := makeTestIntKey(i)
			value := makeTestIntValue(i, vprefix)
			testGet(t, db, key, slotId, value, dt, 0)
		}
		require.NoError(t, db.Close())
	})
}

func TestDBVmVtableOpen(t *testing.T) {
	testcase(func(params []bool) {
		dir := testDirname
		defer os.RemoveAll(dir)
		os.RemoveAll(dir)

		db := openTestDB2(dir, 1, params[0])
		db.opts.VmTableStopWritesThreshold = 2
		dt := DataTypeString
		slotId := testSlotId
		keyNum := 200
		vprefix := utils.FuncRandBytes(102400)

		ckDir := filepath.Join(dir, "ck")
		_, err := db.Checkpoint(ckDir)
		require.NoError(t, err)

		for i := 0; i < keyNum/2; i++ {
			key := makeTestIntKey(i)
			value := makeTestIntValue(i, vprefix)
			require.NoError(t, db.Set(key, slotId, dt, 0, value))
		}

		vmVtDir := base.MakeVmVtablepath(db.dirname, int(slotId))
		files, err := db.opts.FS.List(vmVtDir)
		for _, file := range files {
			fn := filepath.Join(vmVtDir, file)
			require.Equal(t, true, os2.IsExist(fn))
		}

		for i := range db.bituples {
			bt := db.getBitupleSafe(i)
			if bt != nil {
				require.NoError(t, bt.Close())
			}
		}
		db.meta.close()
		db.fileLock.Close()
		db.dataDir.Close()
		db.optspool.Close()

		db = openTestDB2(dir, 1, params[0])
		for _, file := range files {
			fn := filepath.Join(vmVtDir, file)
			require.Equal(t, true, os2.IsNotExist(fn))
		}
		require.NoError(t, db.Close())
	})
}

func TestDBCheckpoint(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	srcDir := filepath.Join(testDirname, "src")
	dstDir := filepath.Join(testDirname, "dst")
	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(dstDir, 0755)
	db := openTestDB(srcDir)
	keyNum := 10
	kkn := 1000
	timestamp := db.opts.GetNowTimestamp() + 100000 + 86400*120*1000
	keyList := sortedkv.MakeKVList(keyNum+10, 100)
	kkvList := sortedkv.MakeKKVList(keyNum, kkn, DataTypeHash, 10, 10)
	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		require.NoError(t, db.Set(kv.Key, makeTestKeySlotid(kv.Key), kv.DataType, timestamp, kv.Value))
	}
	for i := 0; i < keyNum; i++ {
		kv := kkvList[i]
		n, err := db.HMSet(kv.Key, makeTestKeySlotid(kv.Key), kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kkn), n)
	}

	require.NoError(t, db.Flush(false))
	time.Sleep(1 * time.Second)
	fmt.Println("DirDiskInfo1", db.DirDiskInfo())

	readData := func(d *DB) {
		for i := 0; i < keyNum; i++ {
			kv := keyList[i]
			testGet(t, d, kv.Key, makeTestKeySlotid(kv.Key), kv.Value, kv.DataType, timestamp)

			kv1 := kkvList[i]
			for j := 0; j < len(kv1.Kvs); j += 2 {
				v, vcloser, err := d.HGet(kv1.Key, makeTestKeySlotid(kv1.Key), kv1.Kvs[j])
				require.NoError(t, err)
				require.Equal(t, kv1.Kvs[j+1], v)
				vcloser()
			}
		}
	}
	readData(db)

	ckDir := filepath.Join(srcDir, "ck", "123")
	ckCloser, err := db.Checkpoint(ckDir)
	require.NoError(t, err)

	dstDir = filepath.Join(dstDir, "123")
	cmd := exec.Command("cp", "-rf", ckDir, dstDir)
	require.NoError(t, cmd.Run())

	db2 := openTestDB(dstDir)
	readData(db2)
	fmt.Println("DirDiskInfo2", db2.DirDiskInfo())
	require.NoError(t, db2.Close())

	ckCloser()

	for i := keyNum; i < keyNum+10; i++ {
		kv := keyList[i]
		require.NoError(t, db.Set(kv.Key, makeTestKeySlotid(kv.Key), kv.DataType, timestamp, kv.Value))
		testGet(t, db, kv.Key, makeTestKeySlotid(kv.Key), kv.Value, kv.DataType, timestamp)
	}

	require.NoError(t, db.Close())
}

func TestDBCheckpointAndVtGC(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)
	srcDir := filepath.Join(testDirname, "src")
	dstDir := filepath.Join(testDirname, "dst")
	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(dstDir, 0755)
	db := openTestDB(srcDir)
	keyNum := 10
	kkn := 1000
	timestamp := db.opts.GetNowTimestamp() + 100000
	keyList := sortedkv.MakeKVList(keyNum+10, 100)
	kkvList := sortedkv.MakeKKVList(keyNum, kkn, DataTypeHash, 10, 10)
	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		require.NoError(t, db.Set(kv.Key, makeTestKeySlotid(kv.Key), kv.DataType, timestamp, kv.Value))
	}
	for i := 0; i < keyNum; i++ {
		kv := kkvList[i]
		n, err := db.HMSet(kv.Key, makeTestKeySlotid(kv.Key), kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kkn), n)
	}

	readData := func(d *DB) {
		for i := 0; i < keyNum; i++ {
			kv := keyList[i]
			testGet(t, d, kv.Key, makeTestKeySlotid(kv.Key), kv.Value, kv.DataType, timestamp)

			kv1 := kkvList[i]
			for j := 0; j < len(kv1.Kvs); j += 2 {
				v, vcloser, err := d.HGet(kv1.Key, makeTestKeySlotid(kv1.Key), kv1.Kvs[j])
				require.NoError(t, err)
				require.Equal(t, kv1.Kvs[j+1], v)
				vcloser()
			}
		}
	}

	readData(db)

	ckDir := filepath.Join(srcDir, "ck", "123")
	ckCloser, err := db.Checkpoint(ckDir)
	require.NoError(t, err)

	db.doVtGC(true)

	readData(db)

	dstDir = filepath.Join(dstDir, "123")
	cmd := exec.Command("cp", "-rf", ckDir, dstDir)
	require.NoError(t, cmd.Run())

	db2 := openTestDB(dstDir)
	readData(db2)
	require.NoError(t, db2.Close())
	ckCloser()

	readData(db)
	for i := keyNum; i < keyNum+10; i++ {
		kv := keyList[i]
		require.NoError(t, db.Set(kv.Key, makeTestKeySlotid(kv.Key), kv.DataType, timestamp, kv.Value))
		testGet(t, db, kv.Key, makeTestKeySlotid(kv.Key), kv.Value, kv.DataType, timestamp)
	}

	require.NoError(t, db.Close())
}

func TestDBWriteReadConcurrency(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	var wKeyIndex, rKeyIndex, readCount atomic.Uint64
	var wg sync.WaitGroup
	opts := &Options{
		LogTag:                     "[test]",
		Logger:                     DefaultLogger,
		VectorTableCount:           1,
		VectorTableHashSize:        5 << 20,
		GetNowTimestamp:            options.DefaultGetNowTimestamp,
		MemTableSize:               32 << 20,
		VmTableSize:                128 << 20,
		VmTableStopWritesThreshold: 2,
		BitpageFlushSize:           10 << 20,
		BitpageSplitSize:           20 << 20,
	}
	require.NoError(t, os.MkdirAll(dir, 0775))
	db, err := Open(dir, opts)
	require.NoError(t, err)

	closeCh := make(chan struct{})
	runTime := time.Duration(120) * time.Second
	readConcurrency := 7
	wdConcurrency := 3
	timeNow := uint64(time.Now().UnixMilli())
	dataType := DataTypeString
	prefix := utils.FuncRandBytes(200)
	makeKey := func(i int) []byte {
		return []byte(fmt.Sprintf("key_%s_%d", prefix, i))
	}
	makeTimestamp := func(i int) uint64 {
		return timeNow + uint64(i)*10000
	}

	wg.Add(3)
	go func() {
		defer func() {
			wg.Done()
			fmt.Println("write goroutine exit writeCount=", wKeyIndex.Load())
		}()

		var wwg sync.WaitGroup
		for i := 0; i < wdConcurrency; i++ {
			wwg.Add(1)
			go func(index int) {
				defer wwg.Done()
				for {
					select {
					case <-closeCh:
						return
					default:
						j := int(wKeyIndex.Add(1))
						key := makeKey(j)
						require.NoError(t, db.Set(key, testSlotId, dataType, makeTimestamp(j), key))
						rKeyIndex.Add(1)
						sn := rand.Intn(100) + 10
						time.Sleep(time.Duration(sn) * time.Microsecond)
					}
				}
			}(i)
		}
		wwg.Wait()
	}()

	go func() {
		defer func() {
			defer wg.Done()
			fmt.Println("get read goroutine exit readCount=", readCount.Load())
		}()
		var rgwg sync.WaitGroup
		for i := 0; i < readConcurrency; i++ {
			rgwg.Add(1)
			go func(index int) {
				defer rgwg.Done()
				time.Sleep(3 * time.Second)
				for {
					select {
					case <-closeCh:
						return
					default:
						keyIndex := rKeyIndex.Load()
						if keyIndex < 10000 {
							time.Sleep(1 * time.Second)
							continue
						}
						j := rand.Intn(int(keyIndex-10000)) + 1
						key := makeKey(j)
						testGet(t, db, key, testSlotId, key, dataType, makeTimestamp(j))
						readCount.Add(1)
						sn := rand.Intn(10) + 1
						time.Sleep(time.Duration(sn) * time.Microsecond)
					}
				}
			}(i)
		}
		rgwg.Wait()
	}()

	go func() {
		ckDstDir := filepath.Join(testDirname, "ck")
		os.RemoveAll(ckDstDir)
		defer func() {
			wg.Done()
			os.RemoveAll(ckDstDir)
			fmt.Println("checkpoint goroutine exit...")
		}()

		for {
			select {
			case <-closeCh:
				return
			default:
				time.Sleep(5 * time.Second)
				ckCloser, err := db.Checkpoint(ckDstDir)
				require.NoError(t, err)
				fmt.Println("Checkpoint finish", time.Now())
				time.Sleep(20 * time.Second)
				ckCloser()
				fmt.Println("CheckpointEnd finish", time.Now())
				require.NoError(t, os.RemoveAll(ckDstDir))
			}
		}
	}()

	time.Sleep(runTime)
	close(closeCh)
	wg.Wait()

	fmt.Println("check all key start")
	end := int(wKeyIndex.Load())
	for i := 1; i < end; i++ {
		key := makeKey(i)
		testGet(t, db, key, testSlotId, key, dataType, makeTimestamp(i))
	}
	fmt.Println("check all key end")

	fmt.Printf("GetStats:%+v\n", db.GetStats())

	require.NoError(t, db.Close())
}

func TestDBScanCursor(t *testing.T) {
	for _, scanNum := range []int{3, 10, 13} {
		t.Run(fmt.Sprintf("scanNum=%d", scanNum), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			opts := &Options{
				Logger:           DefaultLogger,
				VectorTableCount: 2,
				GetNowTimestamp:  options.DefaultGetNowTimestamp,
				VmTableSize:      32 << 20,
				MemTableSize:     32 << 20,
			}

			db := openTestDBByOpts(dir, opts)
			dt := DataTypeString
			keyNum := 100
			getSlot := func(i int) uint16 {
				return uint16(i % 10)
			}

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				require.NoError(t, db.Set(key, getSlot(i), dt, 0, key))
			}

			require.NoError(t, db.FlushVm())

			var cursor []byte
			var res [][]byte
			keyMap := make(map[string]bool, keyNum)

			i := 1
			for {
				cursor, res, _ = db.Scan(cursor, scanNum, 0, func(string) bool { return true })
				i++
				for _, k := range res {
					keyMap[string(k)] = true
				}
				if len(res) == 0 || bytes.Equal(cursor, []byte("0")) {
					break
				}
			}

			for j := 0; j < keyNum; j++ {
				key := makeTestIntKey(j)
				v, ok := keyMap[string(key)]
				require.True(t, ok)
				require.Equal(t, true, v)
			}

			require.NoError(t, db.Close())
		})
	}
}

func TestDBScan(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	dt := DataTypeString
	ts := db.opts.GetNowTimestamp() + 10000
	keyNum := 100
	getSlot := func(i int) uint16 {
		return uint16(i % 10)
	}

	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, db.Set(key, getSlot(i), dt, ts, key))
	}

	require.NoError(t, db.FlushVm())

	cursor, res, _ := db.Scan(nil, keyNum+1, dt, func(string) bool { return true })
	require.Equal(t, keyNum, len(res))
	require.Equal(t, []byte("0"), cursor)

	keyMap := make(map[string]struct{}, len(res))
	for _, k := range res {
		keyMap[string(k)] = struct{}{}
	}

	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		_, ok := keyMap[string(key)]
		require.True(t, ok)
	}

	require.NoError(t, db.Close())
}
