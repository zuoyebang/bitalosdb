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

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestKKVHash(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			ksize := 20
			vsize := 100
			d := openTestDB1(dir, useMiniVi)
			keyList := sortedkv.MakeKKVList(1, 10, DataTypeHash, ksize, vsize)
			kv := keyList[0]
			n, err := d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
			require.NoError(t, err)
			require.Equal(t, int64(10), n)

			readKKV := func() {
				for j := 0; j < len(kv.Kvs); j += 2 {
					vs, vscloser, vserr := d.HGet(kv.Key, kv.Sid, kv.Kvs[j])
					require.NoError(t, vserr)
					require.Equal(t, kv.Kvs[j+1], vs)
					vscloser()
					kv.Kvs[j+1] = utils.FuncRandBytes(vsize)
				}
			}

			readKKV()

			dt, timestamp, version, _, _, size, err := d.GetMeta(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, uint64(0), timestamp)
			require.Equal(t, kv.DataType, dt)
			require.Equal(t, uint32(10), size)

			n, _, err = d.ExpireAt(kv.Key, kv.Sid, 100)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			_, timestamp, _, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, uint64(100), timestamp)
			require.Equal(t, uint32(10), size)

			n, err = d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
			require.NoError(t, err)
			require.Equal(t, int64(10), n)
			dt, timestamp, _, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, uint64(0), timestamp)
			require.Equal(t, kv.DataType, dt)
			require.Equal(t, uint32(10), size)
			readKKV()

			n, err = d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
			readKKV()

			n, err = d.HDel(kv.Key, kv.Sid, kv.Kvs[0], kv.Kvs[2], []byte("notexistsubkey"))
			require.NoError(t, err)
			require.Equal(t, int64(2), n)
			_, _, vserr := d.HGet(kv.Key, kv.Sid, kv.Kvs[0])
			require.Equal(t, base.ErrNotFound, vserr)
			_, _, vserr = d.HGet(kv.Key, kv.Sid, kv.Kvs[2])
			require.Equal(t, base.ErrNotFound, vserr)

			n, err = d.HDel(kv.Key, kv.Sid, kv.Kvs[0], kv.Kvs[2])
			require.NoError(t, err)
			require.Equal(t, int64(0), n)

			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(time.Second * 2)

			n, err = d.HDel(kv.Key, kv.Sid, kv.Kvs[0], kv.Kvs[2])
			require.NoError(t, err)
			require.Equal(t, int64(0), n)

			n, err = d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
			require.NoError(t, err)
			require.Equal(t, int64(2), n)

			newTs := uint64(time.Now().UnixMilli() + 10000)
			_, _, err = d.ExpireAt(kv.Key, kv.Sid, newTs)
			require.NoError(t, err)
			_, timestamp, version, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, newTs, timestamp)
			require.Equal(t, uint32(10), size)
			require.NotEqual(t, kv.Version, version)
			readKKV()

			n, _, err = d.Delete(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)
			_, _, _, _, _, _, err = d.GetMeta(kv.Key, kv.Sid)
			require.Equal(t, base.ErrNotFound, err)
			for j := 0; j < len(kv.Kvs); j += 2 {
				skey, skeyCloser := kkv.EncodeSubKeyByPool(kv.Kvs[j], kv.DataType, version)
				exist := d.kkvExist(skey, kv.Sid)
				skeyCloser()
				require.Equal(t, false, exist)
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVHashHGetAll(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			ksize := 10
			vsize := 10
			d := openTestDB1(dir, useMiniVi)
			keyNum := 10000
			kkn := 20
			keyList := sortedkv.MakeSortedKKVList(keyNum, kkn, DataTypeHash, ksize, vsize, true)
			sid := uint16(1)

			for i := 0; i < keyNum/2; i++ {
				kv := keyList[i]

				n, err := d.HMSetX(kv.Key, sid, true, kv.Kvs...)
				require.Equal(t, ErrNotFound, err)
				require.Equal(t, int64(0), n)

				n, err = d.HMSet(kv.Key, sid, kv.Kvs[0], kv.Kvs[1])
				require.NoError(t, err)
				require.Equal(t, int64(1), n)
				n, err = d.HMSetX(kv.Key, sid, true, kv.Kvs[2:]...)
				require.NoError(t, err)
				require.Equal(t, int64(kkn-1), n)
			}

			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			for i := keyNum / 2; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.HMSet(kv.Key, sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(kkn), n)
			}

			readKV := func() {
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

					kvs, kvsCloser, aerr := d.HGetAll(kv.Key, sid)
					require.NoError(t, aerr)
					require.Equal(t, len(kv.Kvs), len(kvs))
					for j := 0; j < len(kv.Kvs); j += 2 {
						require.Equal(t, kv.Kvs[j], kvs[j])
						require.Equal(t, kv.Kvs[j+1], kvs[j+1])
					}
					kvsCloser()
				}
			}

			readKV()
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			readKV()
			require.NoError(t, d.Close())
		})
	}
}

func TestKKVHashWriteRead(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			ksize := 20
			vsize := 100
			d := openTestDB1(dir, useMiniVi)
			keyNum := 30
			keyList := sortedkv.MakeKKVList(keyNum, keyNum, DataTypeHash, ksize, vsize)

			writeKV := func(repeat bool) {
				expireTs := d.opts.GetNowTimestamp() + 1000
				for i := 0; i < keyNum; i++ {
					kv := keyList[i]
					n, err := d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
					require.NoError(t, err)
					if repeat && i%10 != 0 {
						require.Equal(t, int64(0), n)
					} else {
						require.Equal(t, int64(keyNum), n)
					}
					if i%2 == 0 {
						kv.Timestamp = 0
					}
					if i%10 == 0 {
						kv.Timestamp = expireTs
					}
					en, dt, e := d.ExpireAt(kv.Key, kv.Sid, kv.Timestamp)
					require.NoError(t, e)
					require.Equal(t, dt, kv.DataType)
					if kv.Timestamp == 0 {
						require.Equal(t, int64(0), en)
					} else {
						require.Equal(t, int64(1), en)
					}
				}
			}

			readKV := func(db *DB, i int) {
				kv := keyList[i]

				dt, timestamp, _, _, _, size, err := db.GetMeta(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, kv.Timestamp, timestamp)
				require.Equal(t, kv.DataType, dt)
				require.Equal(t, kv.Size, size)

				var subKeys [][]byte
				for j := 0; j < len(kv.Kvs); j += 2 {
					subKeys = append(subKeys, kv.Kvs[j])
					vs, vscloser, vserr := db.HGet(kv.Key, kv.Sid, kv.Kvs[j])
					if i%10 == 0 {
						require.Equal(t, []byte(nil), vs)
					} else {
						require.NoError(t, vserr)
						require.Equal(t, kv.Kvs[j+1], vs)
						vscloser()
					}
				}
				subKeys = append(subKeys, nil)
				vs, vscloser, vserr := db.HMGet(kv.Key, kv.Sid, subKeys...)
				require.NoError(t, vserr)
				require.Equal(t, len(subKeys), len(vs))
				if i%10 == 0 {
					require.Equal(t, 0, len(vscloser))
					for k := 0; k < len(subKeys); k++ {
						require.Equal(t, []byte(nil), vs[k])
					}
				} else {
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
			}

			writeKV(false)
			time.Sleep(1 * time.Second)
			for i := 0; i < keyNum; i++ {
				readKV(d, i)
			}

			require.NoError(t, d.FlushMemtable())
			writeKV(true)
			d.FlushBitpage()
			time.Sleep(2 * time.Second)
			writeKV(true)
			require.NoError(t, d.Close())
			time.Sleep(2 * time.Second)

			d1 := openTestDB1(dir, useMiniVi)
			for i := 0; i < keyNum; i++ {
				readKV(d1, i)
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				cnt := 0
				for j := 0; j < len(kv.Kvs); j += 2 {
					if cnt%2 == 0 {
						kv.Kvs[j] = utils.FuncRandBytes(len(kv.Kvs[j]))
						kv.Kvs[j+1] = utils.FuncRandBytes(vsize)
					} else {
						kv.Kvs[j+1] = utils.FuncRandBytes(vsize)
					}
					cnt++
				}
				n, err := d1.HMSet(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				if i%10 == 0 {
					require.Equal(t, int64(keyNum), n)
					kv.Size = uint32(n)
					kv.Timestamp = 0
				} else {
					require.Equal(t, int64(keyNum/2), n)
					kv.Size = uint32(keyNum) + uint32(n)
				}
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				dt, timestamp, _, _, _, size, err := d1.GetMeta(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, kv.Timestamp, timestamp)
				require.Equal(t, kv.DataType, dt)
				require.Equal(t, kv.Size, size)

				var subKeys [][]byte
				for j := 0; j < len(kv.Kvs); j += 2 {
					subKeys = append(subKeys, kv.Kvs[j])
					vs, vscloser, vserr := d1.HGet(kv.Key, kv.Sid, kv.Kvs[j])
					require.NoError(t, vserr)
					require.Equal(t, kv.Kvs[j+1], vs)
					vscloser()
				}
				subKeys = append(subKeys, nil)
				vs, vscloser, vserr := d1.HMGet(kv.Key, kv.Sid, subKeys...)
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

			require.NoError(t, d1.Close())
		})
	}
}

func TestKKVHashIter(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			d := openTestDB1(dir, useMiniVi)
			keyNum := 10
			skeyNum := 100

			keyList := sortedkv.MakeSortedKKVList(keyNum, skeyNum, DataTypeHash, 10, 10, true)
			for i := 0; i < keyNum/2; i++ {
				kv := keyList[i]
				n, err := d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}

			require.NoError(t, d.FlushMemtable())

			for i := keyNum / 2; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				size, err := d.GetMetaSize(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, uint32(skeyNum), size)

				kvs, kvsCloser, aerr := d.HGetAll(kv.Key, kv.Sid)
				require.NoError(t, aerr)
				for j := 0; j < len(kv.Kvs); j += 2 {
					require.Equal(t, kv.Kvs[j], kvs[j])
					require.Equal(t, kv.Kvs[j+1], kvs[j+1])
				}
				kvsCloser()
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.HDel(kv.Key, kv.Sid, kv.Kvs[50], kv.Kvs[70])
				require.NoError(t, err)
				require.Equal(t, int64(2), n)
			}

			require.NoError(t, d.FlushMemtable())

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				size, err := d.GetMetaSize(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, uint32(skeyNum-2), size)

				kvs, kvsCloser, aerr := d.HGetAll(kv.Key, kv.Sid)
				require.NoError(t, aerr)
				require.Equal(t, skeyNum-2, len(kvs)/2)
				ri := 0
				for j := 0; j < len(kv.Kvs); j += 2 {
					if j == 50 || j == 70 {
						j += 2
					}
					require.Equal(t, kv.Kvs[j], kvs[ri])
					require.Equal(t, kv.Kvs[j+1], kvs[ri+1])
					ri += 2
				}
				kvsCloser()
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVHIncrBy(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			d := openTestDB1(dir, useMiniVi)
			ksize := 20
			keyList := sortedkv.MakeKKVList(10, 10, DataTypeSet, ksize, 0)
			for i := range keyList {
				kv := keyList[i]
				for j := range kv.Kvs {
					cnt := int64(0)
					for k := int64(-10); k < 20; k++ {
						cnt += k
						n, err := d.HIncrBy(kv.Key, kv.Sid, kv.Kvs[j], k)
						require.NoError(t, err)
						require.Equal(t, cnt, n)
					}

					n, err := d.HIncrBy(kv.Key, kv.Sid, kv.Kvs[j], 0)
					require.NoError(t, err)
					require.Equal(t, cnt, n)
				}

				size, err := d.GetMetaSize(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, uint32(10), size)
			}

			require.NoError(t, d.FlushMemtable())

			for i := range keyList {
				kv := keyList[i]
				size, err := d.GetMetaSize(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, uint32(10), size)
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVHashHDel(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 1000
			skeyNum := 100
			keyList := sortedkv.MakeSortedKKVList(keyNum, skeyNum, DataTypeHash, 20, 0, false)
			for i := 0; i < keyNum/3; i++ {
				kv := keyList[i]
				n, err := d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			for i := keyNum / 3; i < keyNum/3*2; i++ {
				kv := keyList[i]
				n, err := d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}
			require.NoError(t, d.FlushMemtable())

			for i := keyNum / 3 * 2; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.HMSet(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				size, err := d.GetMetaSize(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, uint32(skeyNum), size)
			}

			delCnt := 10
			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				var fields [][]byte
				for j := 0; j < delCnt; j++ {
					fields = append(fields, kv.Kvs[j*2])
				}
				n, err := d.HDel(kv.Key, kv.Sid, fields...)
				require.NoError(t, err)
				require.Equal(t, int64(delCnt), n)
			}

			require.NoError(t, d.FlushMemtable())

			read := func() {
				for i := 0; i < keyNum; i++ {
					kv := keyList[i]
					size, err := d.GetMetaSize(kv.Key, kv.Sid)
					require.NoError(t, err)
					require.Equal(t, uint32(skeyNum-delCnt), size)

					kvs, kvsCloser, aerr := d.HGetAll(kv.Key, kv.Sid)
					require.NoError(t, aerr)
					require.Equal(t, skeyNum-delCnt, len(kvs)/2)
					for j := range kvs {
						require.Equal(t, kv.Kvs[delCnt*2+j], kvs[j])
					}
					kvsCloser()
				}
			}

			read()
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			read()

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				pn := skeyNum - delCnt
				var fields [][]byte
				for j := delCnt; j < skeyNum; j++ {
					fields = append(fields, kv.Kvs[j*2])
				}
				if i%2 == 0 {
					n, err := d.HDel(kv.Key, kv.Sid, fields...)
					require.NoError(t, err)
					require.Equal(t, int64(pn), n)
					exist, _ := d.Exist(kv.Key, kv.Sid)
					require.False(t, exist)
				} else {
					n, err := d.HDelX(kv.Key, kv.Sid, fields...)
					require.NoError(t, err)
					require.Equal(t, int64(pn), n)
					exist, _ := d.Exist(kv.Key, kv.Sid)
					require.True(t, exist)
				}
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVIterReadAmp(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(dir)

	for i := range db.memShard {
		require.Equal(t, uint64(0), db.memShard[i].iterSlowCount.Load())
	}

	num := (consts.IterSlowCountThreshold + 10) * len(db.memShard)
	kkn := 1
	keyList := sortedkv.MakeKKVList(num, kkn, DataTypeHash, 20, 20)
	count := len(keyList)
	for i := 0; i < count; i++ {
		kv := keyList[i]
		sid := kv.Sid % 8
		n, err := db.HMSet(kv.Key, sid, kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kkn), n)
		n, err = db.HDel(kv.Key, sid, kv.Kvs[0])
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}

	loop := consts.IterReadAmplificationThreshold + 1
	for i := range db.memShard {
		if i == 8 {
			continue
		}
		for j := 0; j < loop; j++ {
			it := db.NewKKVIterator(&IterOptions{SlotId: uint32(i)})
			for it.First(); it.Valid(); it.Last() {
			}
			require.NoError(t, it.Close())
			require.Equal(t, uint64(1+j), db.memShard[i].iterSlowCount.Load())
		}
	}

	db.doMemShardIterReadAmp(1)
	for i := range db.memShard {
		require.Equal(t, uint64(0), db.memShard[i].iterSlowCount.Load())
	}

	require.NoError(t, db.Close())
}
