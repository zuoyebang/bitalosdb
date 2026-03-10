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

	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestDBMeta(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	require.Equal(t, uint32(1), db.meta.tupleNextId)
	for i := range db.meta.tupleList {
		require.Equal(t, uint32(0), db.meta.tupleList[i])
	}
	tupleCount := db.opts.VectorTableCount
	num := metaFieldNumberGap + 100
	for i := 0; i < num; i++ {
		db.meta.getNextSeqNum()
	}
	for i := 0; i < num; i++ {
		db.meta.getNextKeyVersion()
	}
	db.meta.writeSlotBituple(100, slotBitupleCreated)
	sn := db.meta.getCurrentSeqNum()
	require.Equal(t, uint64(524388), sn)
	require.Equal(t, slotBitupleCreated, db.meta.slotsStatus[100])
	version := db.meta.getCurrentKeyVersion()
	require.Equal(t, uint64(524388), version)
	require.Equal(t, tupleCount, db.meta.getTupleCount())
	ts := uint64(time.Now().Unix())
	db.meta.setEliminateScanTs(ts)
	require.Equal(t, ts, db.meta.getEliminateScanTs())
	tnInit := db.meta.tupleNextId
	tn := tnInit
	tsr := db.meta.splitTupleShardRanges(4)
	db.meta.writeTupleShardRanges(tsr)
	require.Equal(t, uint32(5), db.meta.tupleNextId)
	require.Equal(t, uint16(4), db.meta.tupleCount)
	for _, mr := range tsr {
		for j := mr.start; j <= mr.end; j++ {
			require.Equal(t, tn, db.meta.tupleList[j])
		}
		tn++
	}
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	sn = db.meta.getCurrentSeqNum()
	require.Equal(t, uint64(1048576), sn)
	require.Equal(t, ts, db.meta.getEliminateScanTs())
	require.Equal(t, slotBitupleCreated, db.meta.slotsStatus[100])
	require.Equal(t, slotBitupleNotCreated, db.meta.slotsStatus[101])
	version = db.meta.getCurrentKeyVersion()
	require.Equal(t, uint64(1048576), version)
	require.Equal(t, uint32(5), db.meta.tupleNextId)
	require.Equal(t, uint16(4), db.meta.tupleCount)
	tn = tnInit
	for _, mr := range tsr {
		for j := mr.start; j <= mr.end; j++ {
			require.Equal(t, tn, db.meta.tupleList[j])
		}
		tn++
	}
	require.NoError(t, db.Close())
}

func TestDBMetaUpdateVersion2(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	dt := DataTypeString
	keyNum := 100000
	vprefix := utils.FuncRandBytes(10)

	db := openTestDB(dir)
	require.NoError(t, db.meta.writeVersion1File())
	require.Equal(t, metaVersion1, db.meta.header.version)
	require.Equal(t, uint16(1), db.meta.tupleCount)

	makeKeySlotId := func(key []byte) uint16 {
		khash := hash.Fnv32(key)
		return uint16(khash % 8)
	}

	for i := 0; i < keyNum/2; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		require.NoError(t, db.Set(key, makeKeySlotId(key), dt, 0, value))
	}
	for i := keyNum / 2; i < keyNum; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		n, err := db.HMSet(key, makeKeySlotId(key), key, value)
		require.Equal(t, int64(1), n)
		require.NoError(t, err)
	}

	getSeqNum1 := db.meta.getSeqNum()
	getKeyVersion1 := db.meta.getKeyVersion()
	getCurrentSeqNum1 := db.meta.getCurrentSeqNum()
	getCurrentKeyVersion1 := db.meta.getCurrentKeyVersion()
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	require.Equal(t, uint32(1), db.meta.tupleNextId)
	require.Equal(t, uint16(1), db.meta.tupleCount)
	require.Equal(t, metaVersion2, db.meta.header.version)

	getSeqNum2 := db.meta.getSeqNum()
	getKeyVersion2 := db.meta.getKeyVersion()
	if getCurrentSeqNum1 > getSeqNum2 {
		t.Fatalf("reopen db seqNum err new:%d old:%d", getSeqNum2, getCurrentSeqNum1)
	}
	if getCurrentKeyVersion1 > getKeyVersion2 {
		t.Fatalf("reopen db keyVersion err new:%d old:%d", getKeyVersion2, getCurrentKeyVersion1)
	}
	require.Equal(t, getSeqNum1+metaFieldNumberGap, getSeqNum2)
	require.Equal(t, getKeyVersion1+metaFieldNumberGap, getKeyVersion2)

	for i := 0; i < keyNum/2; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		testGet(t, db, key, makeKeySlotId(key), value, dt, 0)
	}
	for i := keyNum / 2; i < keyNum; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		v, closer, err := db.HGet(key, makeKeySlotId(key), key)
		require.NoError(t, err)
		require.Equal(t, value, v)
		closer()
	}
	require.NoError(t, db.Close())

	db = openTestDB2(dir, 4, false)
	require.Equal(t, uint32(5), db.meta.tupleNextId)
	require.Equal(t, uint16(4), db.meta.tupleCount)
	require.Equal(t, metaVersion2, db.meta.header.version)

	getSeqNum3 := db.meta.getSeqNum()
	getKeyVersion3 := db.meta.getKeyVersion()
	require.Equal(t, getSeqNum2+metaFieldNumberGap, getSeqNum3)
	require.Equal(t, getKeyVersion2+metaFieldNumberGap, getKeyVersion3)

	tsr := db.meta.splitTupleShardRanges(4)
	tn := uint32(1)
	for _, mr := range tsr {
		for j := mr.start; j <= mr.end; j++ {
			require.Equal(t, tn, db.meta.tupleList[j])
		}
		tn++
	}
	for i := 0; i < keyNum/2; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		testGet(t, db, key, makeKeySlotId(key), value, dt, 0)
	}
	for i := keyNum / 2; i < keyNum; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		v, closer, err := db.HGet(key, makeKeySlotId(key), key)
		require.NoError(t, err)
		require.Equal(t, value, v)
		closer()
	}
	require.NoError(t, db.Close())
}

func TestDBExpandTupleShard(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	dt := DataTypeString
	keyNum := 100000
	vprefix := utils.FuncRandBytes(10)
	ts := db.opts.GetNowTimestamp() + 10000

	makeKeySlotId := func(key []byte) uint16 {
		khash := hash.Fnv32(key)
		return uint16(khash % 8)
	}

	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		require.NoError(t, db.Set(key, makeKeySlotId(key), dt, ts, value))
	}
	require.NoError(t, db.Close())

	db = openTestDB2(dir, 4, false)
	require.Equal(t, uint32(5), db.meta.tupleNextId)
	require.Equal(t, uint16(4), db.meta.tupleCount)
	tsr := db.meta.splitTupleShardRanges(4)
	tn := uint32(1)
	for _, mr := range tsr {
		for j := mr.start; j <= mr.end; j++ {
			require.Equal(t, tn, db.meta.tupleList[j])
		}
		tn++
	}
	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		testGet(t, db, key, makeKeySlotId(key), value, dt, ts)
	}

	for i := keyNum; i < keyNum*2; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		require.NoError(t, db.Set(key, makeKeySlotId(key), dt, ts, value))
	}

	require.NoError(t, db.Close())

	db = openTestDB2(dir, 4, false)
	for i := 0; i < keyNum*2; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		testGet(t, db, key, makeKeySlotId(key), value, dt, ts)
	}
	require.NoError(t, db.Close())

}

func TestDBExpandTupleShardNoStoreKey(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB2(dir, 1, true)
	dt := DataTypeString
	keyNum := 100000
	vprefix := utils.FuncRandBytes(10)
	ts := db.opts.GetNowTimestamp() + 10000

	makeKeySlotId := func(key []byte) uint16 {
		khash := hash.Fnv32(key)
		return uint16(khash % 8)
	}

	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		require.NoError(t, db.Set(key, makeKeySlotId(key), dt, ts, value))
	}
	require.NoError(t, db.Close())

	db = openTestDB2(dir, 4, true)
	require.Equal(t, uint32(5), db.meta.tupleNextId)
	require.Equal(t, uint16(4), db.meta.tupleCount)
	tsr := db.meta.splitTupleShardRanges(4)
	tn := uint32(1)
	for _, mr := range tsr {
		for j := mr.start; j <= mr.end; j++ {
			require.Equal(t, tn, db.meta.tupleList[j])
		}
		tn++
	}
	for i := 0; i < keyNum; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		testGet(t, db, key, makeKeySlotId(key), value, dt, ts)
	}

	for i := keyNum; i < keyNum*2; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		require.NoError(t, db.Set(key, makeKeySlotId(key), dt, ts, value))
	}

	require.NoError(t, db.Close())

	db = openTestDB2(dir, 4, true)
	for i := 0; i < keyNum*2; i++ {
		key := makeTestIntKey(i)
		value := makeTestIntValue(i, vprefix)
		testGet(t, db, key, makeKeySlotId(key), value, dt, ts)
	}
	require.NoError(t, db.Close())
}
