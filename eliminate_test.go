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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestDBEliminateTask(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname)
	eli := db.eliTask
	scanTs1 := eli.scanTs
	eli.saveScanTimestamp(scanTs1 + eliminateInterval)
	scanTs2 := eli.scanTs
	require.Equal(t, scanTs1+eliminateInterval, scanTs2)
	require.Equal(t, scanTs1+eliminateInterval, db.meta.getEliminateScanTs())
	require.NoError(t, db.Close())

	db = openTestDB(testDirname)
	eli = db.eliTask
	require.Equal(t, scanTs2, eli.scanTs)
	ts := uint64(time.Now().UnixMilli()) + 300*1000
	newSec := utils.FmtUnixMilliToSec(ts)
	for i := 1000; i < 1010; i++ {
		require.NoError(t, eli.setExpireKey(1, newSec, uint64(i), InternalKeyKindSet))
	}
	for i := 1000; i < 1010; i++ {
		require.Equal(t, true, eli.existExpireKey(1, uint64(i), ts))
		if i%2 == 0 {
			require.NoError(t, eli.setExpireKey(1, newSec, uint64(i), InternalKeyKindDelete))
		}
	}

	db.FlushBitpage()
	time.Sleep(1 * time.Second)

	for i := 1000; i < 1010; i++ {
		var exist bool
		if i%2 != 0 {
			exist = true
		}
		require.Equal(t, exist, eli.existExpireKey(1, uint64(i), ts))
	}

	cnt := 0
	eli.scanTs = calcExpireKeyTime(newSec)
	iter := eli.newIter()
	for iter.First(); iter.Valid(); iter.Next() {
		cnt++
	}
	require.NoError(t, iter.Close())
	require.Equal(t, 5, cnt)
	require.NoError(t, db.Close())
}

func TestDBEliminateTaskScan(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)
	eli := db.eliTask
	ts1 := uint64(time.Now().UnixMilli()) - 1000
	ts2 := ts1 + 300*1000
	ts3 := ts1 - 300*1000
	ts4 := ts1 + 3000*1000
	kn := 300
	kkn := 10
	keyList := sortedkv.MakeKKVItemList(kn, kkn, 10, 10)
	count := len(keyList)

	writeKV := func(start, end int) {
		for i := start; i < end; i++ {
			item := keyList[i]
			var ts uint64
			if i%4 == 0 {
				ts = ts1
			} else if i%4 == 1 {
				ts = ts2
			} else if i%4 == 2 {
				ts = ts3
			} else {
				ts = ts4
			}
			khash := hash.Fnv32(item.Key)
			item.Sid = uint16(khash & 10)
			var n int64
			var err error
			switch item.DataType {
			case DataTypeHash:
				n, err = db.HMSet(item.Key, item.Sid, item.Kvs...)
			case DataTypeSet:
				n, err = db.SAdd(item.Key, item.Sid, item.Kvs...)
			case DataTypeZset:
				n, err = db.ZAdd(item.Key, item.Sid, item.Kvs...)
			case DataTypeList:
				n, err = db.ListPush(item.Key, item.Sid, true, false, item.Kvs...)
			}
			require.NoError(t, err)
			require.Equal(t, int64(kkn), n)
			n, _, err = db.ExpireAt(item.Key, item.Sid, ts)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			_, timestamp, version, _, _, size, _ := db.GetMeta(item.Key, item.Sid)
			require.Equal(t, ts, timestamp)
			require.Equal(t, uint32(kkn), size)
			item.Version = version
			item.Timestamp = timestamp
			require.Equal(t, true, eli.existExpireKey(item.Sid, item.Version, item.Timestamp))
		}
	}

	writeKV(0, count/3)
	require.NoError(t, db.FlushMemtable())
	db.FlushBitpage()
	time.Sleep(2 * time.Second)
	writeKV(count/3, count/3*2)
	require.NoError(t, db.FlushMemtable())
	writeKV(count/3*2, count)

	delNum := eli.scanEliminateData()
	require.Equal(t, uint64(1), eli.jobId)
	require.Equal(t, count/4, delNum)

	var exist bool
	for i, item := range keyList {
		if i%4 == 0 {
			exist = false
		} else {
			exist = true
		}
		isExist := eli.existExpireKey(item.Sid, item.Version, item.Timestamp)
		require.Equal(t, exist, isExist)

		dt := kkv.GetSiDataType(item.DataType)
		var lowerBound [kkv.SubKeyHeaderLength]byte
		var upperBound [kkv.SubKeyUpperBoundLength]byte
		kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, item.Version)
		kkv.EncodeSubKeyUpperBound(upperBound[:], dt, item.Version)
		opts := &options.IterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			SlotId:     uint32(item.Sid),
			DataType:   dt,
		}
		iter := db.NewKKVIterator(opts)
		require.Equal(t, true, iter.First())
		require.NoError(t, iter.Close())
	}

	tsEnd := calcExpireKeyTime(utils.FmtUnixMilliToSec(ts3 - 300*1000))
	db.eliTask.saveScanTimestamp(tsEnd)
	require.NoError(t, db.Close())

	db = openTestDB(dir)
	time.Sleep(2 * time.Second)
	require.Equal(t, uint64(2), db.eliTask.jobId)
	delNum = db.eliTask.scanEliminateData()
	require.Equal(t, 0, delNum)
	delNum = db.eliTask.scanEliminateData()
	require.Equal(t, count/4, delNum)
	require.Equal(t, uint64(4), db.eliTask.jobId)

	require.NoError(t, db.FlushMemtable())
	db.FlushBitpage()
	time.Sleep(2 * time.Second)

	for i, item := range keyList {
		isExist := db.eliTask.existExpireKey(item.Sid, item.Version, item.Timestamp)
		dt := kkv.GetSiDataType(item.DataType)
		var lowerBound [kkv.SubKeyHeaderLength]byte
		var upperBound [kkv.SubKeyUpperBoundLength]byte
		kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, item.Version)
		kkv.EncodeSubKeyUpperBound(upperBound[:], dt, item.Version)
		opts := &options.IterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			SlotId:     uint32(item.Sid),
			DataType:   dt,
		}
		iter := db.NewKKVIterator(opts)
		ok := iter.First()
		if i%4 == 3 {
			if !ok {
				fmt.Println("not ok", dt, item.Version, item.Sid)
			}
			require.Equal(t, true, ok)
			require.Equal(t, true, isExist)
		} else {
			require.Equal(t, false, ok)
			require.Equal(t, false, isExist)
		}
		require.NoError(t, iter.Close())
	}

	require.NoError(t, db.Close())
}
