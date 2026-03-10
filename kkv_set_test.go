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
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestKKVSet(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			kkn := 100
			ksize := 20
			keyList := sortedkv.MakeKKVList(1, kkn, DataTypeSet, ksize, 0)
			kv := keyList[0]

			_, err := d.SAddX(kv.Key, kv.Sid, true, kv.Kvs...)
			require.Equal(t, ErrNotFound, err)

			var n int64
			n, err = d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
			require.NoError(t, err)
			require.Equal(t, int64(kkn), n)

			readKKV := func() {
				for j := 0; j < len(kv.Kvs); j++ {
					mn, _ := d.SIsMember(kv.Key, kv.Sid, kv.Kvs[j])
					require.Equal(t, int64(1), mn)
				}
			}

			dt, timestamp, version, _, _, size, err1 := d.GetMeta(kv.Key, kv.Sid)
			require.NoError(t, err1)
			require.Equal(t, uint64(0), timestamp)
			require.Equal(t, kv.DataType, dt)
			require.Equal(t, uint32(kkn), size)
			readKKV()

			_, _, err = d.ExpireAt(kv.Key, kv.Sid, 100)
			require.NoError(t, err)
			_, timestamp, _, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, uint64(100), timestamp)
			require.Equal(t, uint32(kkn), size)

			n, err = d.SAdd(kv.Key, kv.Sid, kv.Kvs[0])
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			n, err = d.SAddX(kv.Key, kv.Sid, true, kv.Kvs[1:]...)
			require.NoError(t, err)
			require.Equal(t, int64(kkn-1), n)
			dt, timestamp, version, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, uint64(0), timestamp)
			require.Equal(t, kv.DataType, dt)
			require.Equal(t, uint32(kkn), size)

			n, err = d.SAddX(kv.Key, kv.Sid, true, kv.Kvs...)
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
			n, err = d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
			readKKV()

			n, err = d.SRem(kv.Key, kv.Sid, kv.Kvs[0], []byte("notexistsubkey"))
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			n, err = d.SRem(kv.Key, kv.Sid, kv.Kvs[0])
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
			mn, _ := d.SIsMember(kv.Key, kv.Sid, kv.Kvs[0])
			require.Equal(t, int64(0), mn)

			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			n, err = d.SRem(kv.Key, kv.Sid, kv.Kvs[1], []byte("notexistsubkey"))
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
			n, err = d.SRem(kv.Key, kv.Sid, kv.Kvs[1])
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
			mn, _ = d.SIsMember(kv.Key, kv.Sid, kv.Kvs[1])
			require.Equal(t, int64(0), mn)

			n, err = d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
			require.NoError(t, err)
			require.Equal(t, int64(2), n)
			readKKV()

			newTs := uint64(time.Now().UnixMilli() + 10000)
			_, _, err = d.ExpireAt(kv.Key, kv.Sid, newTs)
			require.NoError(t, err)
			dt, timestamp, version, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, newTs, timestamp)
			require.Equal(t, uint32(kkn), size)
			readKKV()

			n, _, err = d.Delete(kv.Key, kv.Sid)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			_, err = d.Exist(kv.Key, kv.Sid)
			require.Equal(t, base.ErrNotFound, err)
			for j := 0; j < len(kv.Kvs); j++ {
				skey, skeyCloser := kkv.EncodeSubKeyByPool(kv.Kvs[j], kv.DataType, version)
				exist := d.kkvExist(skey, kv.Sid)
				skeyCloser()
				require.Equal(t, false, exist)
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVSetWriteRead(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)

			ksize := 20
			keyNum := 6
			keyList := sortedkv.MakeKKVList(keyNum, keyNum, DataTypeSet, ksize, 0)

			writeKV := func(repeat bool) {
				expireTs := d.opts.GetNowTimestamp() + 1000
				for i := 0; i < keyNum; i++ {
					kv := keyList[i]
					n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
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
				for j := 0; j < len(kv.Kvs); j++ {
					subKeys = append(subKeys, kv.Kvs[j])
					exist, _ := db.SIsMember(kv.Key, kv.Sid, kv.Kvs[j])
					if i%10 == 0 {
						require.Equal(t, int64(0), exist)
					} else {
						require.Equal(t, int64(1), exist)
					}
				}
				subKeys = append(subKeys, nil)
				exists := db.kkvExistSubKeys(kv.Key, kv.Sid, kv.DataType, subKeys...)
				if i%10 == 0 {
					for k := 0; k < len(subKeys); k++ {
						require.Equal(t, false, exists[k])
					}
				} else {
					for k := 0; k < len(subKeys); k++ {
						if k == len(subKeys)-1 {
							require.Equal(t, false, exists[k])
						} else {
							require.Equal(t, true, exists[k])
						}
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
			time.Sleep(1 * time.Second)

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
				for j := range kv.Kvs {
					if j%2 == 0 {
						kv.Kvs[j] = utils.FuncRandBytes(len(kv.Kvs[j]))
					}
				}
				n, err := d1.SAdd(kv.Key, kv.Sid, kv.Kvs...)
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
				for j := 0; j < len(kv.Kvs); j++ {
					subKeys = append(subKeys, kv.Kvs[j])
					exist, _ := d1.SIsMember(kv.Key, kv.Sid, kv.Kvs[j])
					require.Equal(t, int64(1), exist)
				}
				subKeys = append(subKeys, nil)
				exists := d1.kkvExistSubKeys(kv.Key, kv.Sid, kv.DataType, subKeys...)
				for k := 0; k < len(subKeys); k++ {
					if k == len(subKeys)-1 {
						require.Equal(t, false, exists[k])
					} else {
						require.Equal(t, true, exists[k])
					}
				}
			}

			require.NoError(t, d1.Close())
		})
	}
}

func TestKKVSetSmembers(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 1000
			skeyNum := 100
			keyList := sortedkv.MakeSortedKKVList(keyNum, skeyNum, DataTypeSet, 20, 0, false)
			for i := 0; i < keyNum/3; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			for i := keyNum / 3; i < keyNum/3*2; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}
			require.NoError(t, d.FlushMemtable())

			for i := keyNum / 3 * 2; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				size, err := d.GetMetaSize(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, uint32(skeyNum), size)

				kvs, kvsCloser, aerr := d.SMembers(kv.Key, kv.Sid)
				require.NoError(t, aerr)
				require.Equal(t, len(kv.Kvs), len(kvs))
				for j := 0; j < len(kv.Kvs); j++ {
					require.Equal(t, kv.Kvs[j], kvs[j])
				}
				kvsCloser()
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.SRem(kv.Key, kv.Sid, kv.Kvs[50], kv.Kvs[70])
				require.NoError(t, err)
				require.Equal(t, int64(2), n)
			}

			require.NoError(t, d.FlushMemtable())

			read := func() {
				for i := 0; i < keyNum; i++ {
					kv := keyList[i]
					size, err := d.GetMetaSize(kv.Key, kv.Sid)
					require.NoError(t, err)
					require.Equal(t, uint32(skeyNum-2), size)

					kvs, kvsCloser, aerr := d.SMembers(kv.Key, kv.Sid)
					require.NoError(t, aerr)
					require.Equal(t, skeyNum-2, len(kvs))
					ri := 0
					for j := 0; j < len(kv.Kvs); j++ {
						if j == 50 || j == 70 {
							j++
						}
						require.Equal(t, kv.Kvs[j], kvs[ri])
						ri++
					}
					kvsCloser()
				}
			}

			read()
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			read()
			require.NoError(t, d.Close())
		})
	}
}

func TestKKVSetSpop(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 1000
			skeyNum := 100
			keyList := sortedkv.MakeSortedKKVList(keyNum, skeyNum, DataTypeSet, 20, 0, false)
			for i := 0; i < keyNum/3; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			for i := keyNum / 3; i < keyNum/3*2; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}
			require.NoError(t, d.FlushMemtable())

			for i := keyNum / 3 * 2; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				size, err := d.GetMetaSize(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, uint32(skeyNum), size)

				kvs, kvsCloser, aerr := d.SMembers(kv.Key, kv.Sid)
				require.NoError(t, aerr)
				require.Equal(t, len(kv.Kvs), len(kvs))
				for j := 0; j < len(kv.Kvs); j += 2 {
					require.Equal(t, kv.Kvs[j], kvs[j])
				}
				kvsCloser()
			}

			popCnt := 10
			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				pop, closer, err := d.SPop(kv.Key, kv.Sid, int64(popCnt))
				require.NoError(t, err)
				require.Equal(t, popCnt, len(pop))
				for j := range pop {
					require.Equal(t, kv.Kvs[j], pop[j])
				}
				closer()
			}

			require.NoError(t, d.FlushMemtable())

			read := func() {
				for i := 0; i < keyNum; i++ {
					kv := keyList[i]
					size, err := d.GetMetaSize(kv.Key, kv.Sid)
					require.NoError(t, err)
					require.Equal(t, uint32(skeyNum-popCnt), size)

					kvs, kvsCloser, aerr := d.SMembers(kv.Key, kv.Sid)
					require.NoError(t, aerr)
					require.Equal(t, skeyNum-popCnt, len(kvs))
					for j := range kvs {
						require.Equal(t, kv.Kvs[popCnt+j], kvs[j])
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
				pn := skeyNum - popCnt
				if i%2 == 0 {
					pop, closer, err := d.SPop(kv.Key, kv.Sid, int64(pn))
					require.NoError(t, err)
					require.Equal(t, pn, len(pop))
					for j := range pop {
						require.Equal(t, kv.Kvs[popCnt+j], pop[j])
					}
					closer()
					exist, _ := d.Exist(kv.Key, kv.Sid)
					require.False(t, exist)
				} else {
					pop, closer, err := d.SPopX(kv.Key, kv.Sid, int64(pn))
					require.NoError(t, err)
					require.Equal(t, pn, len(pop))
					for j := range pop {
						require.Equal(t, kv.Kvs[popCnt+j], pop[j])
					}
					closer()

					exist, _ := d.Exist(kv.Key, kv.Sid)
					require.True(t, exist)
				}

			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVSetSrem(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 1000
			skeyNum := 100
			keyList := sortedkv.MakeSortedKKVList(keyNum, skeyNum, DataTypeSet, 20, 0, false)
			for i := 0; i < keyNum/3; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			for i := keyNum / 3; i < keyNum/3*2; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}
			require.NoError(t, d.FlushMemtable())

			for i := keyNum / 3 * 2; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.SAdd(kv.Key, kv.Sid, kv.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(skeyNum), n)
			}

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				size, err := d.GetMetaSize(kv.Key, kv.Sid)
				require.NoError(t, err)
				require.Equal(t, uint32(skeyNum), size)

				kvs, kvsCloser, aerr := d.SMembers(kv.Key, kv.Sid)
				require.NoError(t, aerr)
				require.Equal(t, len(kv.Kvs), len(kvs))
				for j := 0; j < len(kv.Kvs); j += 2 {
					require.Equal(t, kv.Kvs[j], kvs[j])
				}
				kvsCloser()
			}

			delCnt := 10
			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.SRem(kv.Key, kv.Sid, kv.Kvs[:delCnt]...)
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

					kvs, kvsCloser, aerr := d.SMembers(kv.Key, kv.Sid)
					require.NoError(t, aerr)
					require.Equal(t, skeyNum-delCnt, len(kvs))
					for j := range kvs {
						require.Equal(t, kv.Kvs[delCnt+j], kvs[j])
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
				if i%2 == 0 {
					n, err := d.SRem(kv.Key, kv.Sid, kv.Kvs[delCnt:]...)
					require.NoError(t, err)
					require.Equal(t, int64(pn), n)
					exist, _ := d.Exist(kv.Key, kv.Sid)
					require.False(t, exist)
				} else {
					n, err := d.SRemX(kv.Key, kv.Sid, kv.Kvs[delCnt:]...)
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
