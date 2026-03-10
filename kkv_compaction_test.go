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

	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/stretchr/testify/require"
)

func TestKKVCompact1(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	ksize := 20
	vsize := consts.KvSeparateSize * 2
	d := openTestDB(dir)
	keyNum := 2000
	kknNum := 10
	keyList := sortedkv.MakeKKVList(keyNum, kknNum, DataTypeHash, ksize, vsize)
	sid := uint16(1)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		n, err := d.HMSet(kv.Key, sid, kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kknNum), n)
	}

	require.NoError(t, d.FlushMemtable())

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		require.Equal(t, kv.Size, size)

		var subKeys [][]byte
		for j := 0; j < len(kv.Kvs); j += 2 {
			subKeys = append(subKeys, kv.Kvs[j])
			vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
			require.NoError(t, vserr)
			require.Equal(t, kv.Kvs[j+1], vs)
			vscloser()
		}
		subKeys = append(subKeys, nil)
		vs, vscloser, vserr := d.HMGet(kv.Key, sid, subKeys...)
		require.NoError(t, vserr)
		require.Equal(t, len(subKeys), len(vs))
		require.Equal(t, len(subKeys)-1, len(vscloser))
		for k := 0; k < len(subKeys); k++ {
			if k == len(subKeys)-1 {
				require.Equal(t, []byte(nil), vs[k])
			} else {
				require.Equal(t, kv.Kvs[k*2+1], vs[k])
			}
		}
		for i2 := range vscloser {
			vscloser[i2]()
		}
	}

	bithashDelNum := 0
	for i := 0; i < keyNum; i++ {
		if i%2 == 0 {
			continue
		}
		kv := keyList[i]
		for j := 0; j < len(kv.Kvs)/2; j += 2 {
			n, err := d.DeleteKKV(kv.Key, sid, kv.DataType, kv.Kvs[j])
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			bithashDelNum++
		}
	}

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	bt := d.getBitupleSafe(int(sid))
	bithashDelKey := bt.kkv.BithashStats().DelKeyTotal.Load()
	require.Equal(t, uint64(bithashDelNum), bithashDelKey)

	d.doBithashCompact(1, 0.2)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		if i%2 == 0 {
			require.Equal(t, kv.Size, size)
			for j := 0; j < len(kv.Kvs); j += 2 {
				vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
				require.NoError(t, vserr)
				require.Equal(t, kv.Kvs[j+1], vs)
				vscloser()
			}
		} else {
			require.Equal(t, kv.Size/2, size)
			for j := len(kv.Kvs) / 2; j < len(kv.Kvs); j += 2 {
				vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
				require.NoError(t, vserr)
				require.Equal(t, kv.Kvs[j+1], vs)
				vscloser()
			}
		}
	}

	require.NoError(t, d.Close())
}

func TestKKVCompact2(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	ksize := 20
	vsize := consts.KvSeparateSize * 2
	d := openTestDB(dir)
	keyNum := 2000
	kknNum := 10
	keyList := sortedkv.MakeKKVList(keyNum, kknNum, DataTypeHash, ksize, vsize)
	sid := uint16(1)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		n, err := d.HMSet(kv.Key, sid, kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kknNum), n)
	}

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		require.Equal(t, kv.Size, size)

		var subKeys [][]byte
		for j := 0; j < len(kv.Kvs); j += 2 {
			subKeys = append(subKeys, kv.Kvs[j])
			vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
			require.NoError(t, vserr)
			require.Equal(t, kv.Kvs[j+1], vs)
			vscloser()
		}
		subKeys = append(subKeys, nil)
		vs, vscloser, vserr := d.HMGet(kv.Key, sid, subKeys...)
		require.NoError(t, vserr)
		require.Equal(t, len(subKeys), len(vs))
		require.Equal(t, len(subKeys)-1, len(vscloser))
		for k := 0; k < len(subKeys); k++ {
			if k == len(subKeys)-1 {
				require.Equal(t, []byte(nil), vs[k])
			} else {
				require.Equal(t, kv.Kvs[k*2+1], vs[k])
			}
		}
		for i2 := range vscloser {
			vscloser[i2]()
		}
	}

	bithashDelNum := 0
	for i := 0; i < keyNum; i++ {
		if i%2 == 0 {
			continue
		}
		kv := keyList[i]
		for j := 0; j < len(kv.Kvs)/2; j += 2 {
			n, err := d.HMSet(kv.Key, sid, kv.Kvs[j], kv.Kvs[j+1])
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
			bithashDelNum++
		}
	}

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	bt := d.getBitupleSafe(int(sid))
	bithashDelKey := bt.kkv.BithashStats().DelKeyTotal.Load()
	require.Equal(t, uint64(bithashDelNum), bithashDelKey)

	d.doBithashCompact(1, 0.2)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		require.Equal(t, kv.Size, size)
		for j := 0; j < len(kv.Kvs); j += 2 {
			vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
			require.NoError(t, vserr)
			require.Equal(t, kv.Kvs[j+1], vs)
			vscloser()
		}
	}

	require.NoError(t, d.Close())
}

func TestKKVCompact3(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	ksize := 20
	vsize := consts.KvSeparateSize * 2
	d := openTestDB(dir)
	keyNum := 2000
	kknNum := 10
	keyList := sortedkv.MakeKKVList(keyNum, kknNum, DataTypeHash, ksize, vsize)
	sid := uint16(1)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		n, err := d.HMSet(kv.Key, sid, kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kknNum), n)
	}

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		require.Equal(t, kv.Size, size)

		var subKeys [][]byte
		for j := 0; j < len(kv.Kvs); j += 2 {
			subKeys = append(subKeys, kv.Kvs[j])
			vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
			require.NoError(t, vserr)
			require.Equal(t, kv.Kvs[j+1], vs)
			vscloser()
		}
		subKeys = append(subKeys, nil)
		vs, vscloser, vserr := d.HMGet(kv.Key, sid, subKeys...)
		require.NoError(t, vserr)
		require.Equal(t, len(subKeys), len(vs))
		require.Equal(t, len(subKeys)-1, len(vscloser))
		for k := 0; k < len(subKeys); k++ {
			if k == len(subKeys)-1 {
				require.Equal(t, []byte(nil), vs[k])
			} else {
				require.Equal(t, kv.Kvs[k*2+1], vs[k])
			}
		}
		for i2 := range vscloser {
			vscloser[i2]()
		}
	}

	bithashDelNum := 0
	for i := 0; i < keyNum; i++ {
		if i%2 == 0 {
			continue
		}
		kv := keyList[i]
		for j := 0; j < len(kv.Kvs)/2; j += 2 {
			n, err := d.DeleteKKV(kv.Key, sid, kv.DataType, kv.Kvs[j])
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			bithashDelNum++
		}
	}

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	bt := d.getBitupleSafe(int(sid))
	bithashDelKey := bt.kkv.BithashStats().DelKeyTotal.Load()
	require.Equal(t, uint64(bithashDelNum), bithashDelKey)

	d.doBithashCompact(1, 0.2)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		if i%2 == 0 {
			require.Equal(t, kv.Size, size)
			for j := 0; j < len(kv.Kvs); j += 2 {
				vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
				require.NoError(t, vserr)
				require.Equal(t, kv.Kvs[j+1], vs)
				vscloser()
			}
		} else {
			require.Equal(t, kv.Size/2, size)
			for j := len(kv.Kvs) / 2; j < len(kv.Kvs); j += 2 {
				vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
				require.NoError(t, vserr)
				require.Equal(t, kv.Kvs[j+1], vs)
				vscloser()
			}
		}

	}

	require.NoError(t, d.Close())
}

func TestKKVCompact4(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	ksize := 20
	vsize := consts.KvSeparateSize * 2
	d := openTestDB(dir)
	keyNum := 2000
	kknNum := 10
	keyList := sortedkv.MakeKKVList(keyNum, kknNum, DataTypeHash, ksize, vsize)
	sid := uint16(1)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		n, err := d.HMSet(kv.Key, sid, kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kknNum), n)
	}

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		require.Equal(t, kv.Size, size)

		var subKeys [][]byte
		for j := 0; j < len(kv.Kvs); j += 2 {
			subKeys = append(subKeys, kv.Kvs[j])
			vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
			require.NoError(t, vserr)
			require.Equal(t, kv.Kvs[j+1], vs)
			vscloser()
		}
		subKeys = append(subKeys, nil)
		vs, vscloser, vserr := d.HMGet(kv.Key, sid, subKeys...)
		require.NoError(t, vserr)
		require.Equal(t, len(subKeys), len(vs))
		require.Equal(t, len(subKeys)-1, len(vscloser))
		for k := 0; k < len(subKeys); k++ {
			if k == len(subKeys)-1 {
				require.Equal(t, []byte(nil), vs[k])
			} else {
				require.Equal(t, kv.Kvs[k*2+1], vs[k])
			}
		}
		for i2 := range vscloser {
			vscloser[i2]()
		}
	}

	bithashDelNum := 0
	for i := 0; i < keyNum; i++ {
		if i%2 == 0 {
			continue
		}
		kv := keyList[i]
		_, _, err := d.Delete(kv.Key, sid)
		require.NoError(t, err)
		bithashDelNum += kknNum
	}

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	bt := d.getBitupleSafe(int(sid))
	bithashDelKey := bt.kkv.BithashStats().DelKeyTotal.Load()
	require.Equal(t, uint64(bithashDelNum), bithashDelKey)

	d.doBithashCompact(1, 0.2)

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		if i%2 == 0 {
			require.NoError(t, err)
			require.Equal(t, kv.DataType, dt)
			require.Equal(t, kv.Size, size)
			for j := 0; j < len(kv.Kvs); j += 2 {
				vs, vscloser, vserr := d.HGet(kv.Key, sid, kv.Kvs[j])
				require.NoError(t, vserr)
				require.Equal(t, kv.Kvs[j+1], vs)
				vscloser()
			}
		} else {
			require.Equal(t, ErrNotFound, err)
		}
	}

	require.NoError(t, d.Close())
}

func TestKKVCompact5(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	d := openTestDB(dir)
	keyNum := 2000
	kknNum := 10
	vsize := consts.KvSeparateSize * 2
	keyList := sortedkv.MakeKKVList(keyNum, kknNum, DataTypeList, vsize, 0)
	sid := uint16(1)

	for i := 0; i < keyNum; i++ {
		item := keyList[i]
		n, err := d.ListPush(item.Key, sid, true, false, item.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kknNum), n)
	}

	require.NoError(t, d.FlushMemtable())

	for i := 0; i < keyNum; i++ {
		kv := keyList[i]
		dt, _, _, _, _, size, err := d.GetMeta(kv.Key, sid)
		require.NoError(t, err)
		require.Equal(t, kv.DataType, dt)
		require.Equal(t, kv.Size, size)
	}
	for _, item := range keyList {
		res, resCloser, err := d.ListRange(item.Key, sid, 0, -1)
		require.NoError(t, err)
		require.Equal(t, kknNum, len(res))
		for i := range res {
			require.Equal(t, item.Kvs[kknNum-1-i], res[i])
		}
		resCloser()
	}

	bithashDelNum := 0
	for i := 0; i < keyNum; i++ {
		if i%2 == 0 {
			continue
		}
		kv := keyList[i]
		for j := 0; j < len(kv.Kvs)/2; j++ {
			v, vcloser, err := d.ListPop(kv.Key, sid, false)
			require.NoError(t, err)
			require.Equal(t, kv.Kvs[j], v)
			vcloser()
			bithashDelNum++
		}
	}

	require.NoError(t, d.FlushMemtable())
	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	bt := d.getBitupleSafe(int(sid))
	bithashDelAct := bt.kkv.BithashStats().DelKeyTotal.Load()
	require.Equal(t, uint64(bithashDelNum), bithashDelAct)

	d.doBithashCompact(1, 0.2)

	for i := 0; i < keyNum; i++ {
		item := keyList[i]
		var kkn int
		if i%2 == 0 {
			kkn = int(item.Size)
		} else {
			kkn = int(item.Size / 2)
		}
		dt, _, _, _, _, size, err := d.GetMeta(item.Key, sid)
		require.NoError(t, err)
		require.Equal(t, item.DataType, dt)
		require.Equal(t, kkn, int(size))
		res, resCloser, e := d.ListRange(item.Key, sid, 0, -1)
		require.NoError(t, e)
		require.Equal(t, kkn, len(res))
		for j := range res {
			require.Equal(t, item.Kvs[kknNum-1-j], res[j])
		}
		resCloser()
	}

	require.NoError(t, d.Close())
}
