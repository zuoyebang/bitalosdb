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

func TestKKVListGetRangeIndex(t *testing.T) {
	size := int64(100)
	var start, stop int64
	var err error

	start, stop, err = getListRangeIndex(0, -101, size)
	require.Equal(t, base.ErrIndexOutOfRange, err)

	start, stop, err = getListRangeIndex(90, 80, size)
	require.Equal(t, base.ErrIndexOutOfRange, err)

	start, stop, err = getListRangeIndex(110, 110, size)
	require.Equal(t, base.ErrIndexOutOfRange, err)

	start, stop, err = getListRangeIndex(0, 90, size)
	require.NoError(t, err)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(90), stop)

	start, stop, err = getListRangeIndex(0, 190, size)
	require.NoError(t, err)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(99), stop)

	start, stop, err = getListRangeIndex(1, -10, size)
	require.NoError(t, err)
	require.Equal(t, int64(1), start)
	require.Equal(t, int64(90), stop)

	start, stop, err = getListRangeIndex(-20, -10, size)
	require.NoError(t, err)
	require.Equal(t, int64(80), start)
	require.Equal(t, int64(90), stop)

	start, stop, err = getListRangeIndex(-20, 20, size)
	require.Equal(t, base.ErrIndexOutOfRange, err)

	start, stop, err = getListRangeIndex(-120, -10, size)
	require.NoError(t, err)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(90), stop)

	start, stop, err = getListRangeIndex(20000, 50000, 100000)
	require.NoError(t, err)
	require.Equal(t, int64(20000), start)
	require.Equal(t, int64(50000), stop)
}

func TestKKVListPushPop(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			kn := 10
			kkn := 10
			keyList := sortedkv.MakeKKVList(kn, kkn, DataTypeList, 100, 0)

			for i, item := range keyList {
				if i%2 == 0 {
					n, err := d.ListPush(item.Key, item.Sid, true, false, item.Kvs...)
					require.NoError(t, err)
					require.Equal(t, int64(kkn), n)
					dt, timestamp, version, lindex, _, size, err1 := d.GetMeta(item.Key, item.Sid)
					require.NoError(t, err1)
					require.Equal(t, uint64(0), timestamp)
					require.Equal(t, item.DataType, dt)
					require.Equal(t, uint32(kkn), size)

					for j := 0; j < kkn; j++ {
						v, vcloser, e := d.ListIndex(item.Key, item.Sid, int64(j))
						require.NoError(t, e)
						require.Equal(t, item.Kvs[kkn-1-j], v)
						vcloser()
					}

					v, vcloser, err2 := d.ListPop(item.Key, item.Sid, true)
					require.NoError(t, err2)
					require.Equal(t, item.Kvs[kkn-1], v)
					vcloser()

					var skBuf [kkv.SubKeyListLength]byte
					kkv.EncodeListKey(skBuf[:], dt, version, lindex+1)
					_, _, err2 = d.kkvGetValue(skBuf[:], item.Sid)
					require.Equal(t, ErrNotFound, err2)

					kkv.EncodeListKey(skBuf[:], dt, version, lindex+2)
					v, vcloser, err2 = d.kkvGetValue(skBuf[:], item.Sid)
					require.NoError(t, err2)
					require.Equal(t, item.Kvs[kkn-2], v)
					vcloser()
				} else {
					n, err := d.ListPush(item.Key, item.Sid, false, false, item.Kvs...)
					require.NoError(t, err)
					require.Equal(t, int64(kkn), n)
					dt, timestamp, version, _, rindex, size, err1 := d.GetMeta(item.Key, item.Sid)
					require.NoError(t, err1)
					require.Equal(t, uint64(0), timestamp)
					require.Equal(t, item.DataType, dt)
					require.Equal(t, uint32(kkn), size)

					for j := 1; j <= kkn; j++ {
						v, vcloser, e := d.ListIndex(item.Key, item.Sid, int64(-j))
						require.NoError(t, e)
						require.Equal(t, item.Kvs[kkn-j], v)
						vcloser()
					}

					v, vcloser, err2 := d.ListPop(item.Key, item.Sid, false)
					require.NoError(t, err2)
					require.Equal(t, item.Kvs[kkn-1], v)
					vcloser()

					var skBuf [kkv.SubKeyListLength]byte
					kkv.EncodeListKey(skBuf[:], dt, version, rindex-1)
					_, _, err2 = d.kkvGetValue(skBuf[:], item.Sid)
					require.Equal(t, ErrNotFound, err2)

					kkv.EncodeListKey(skBuf[:], dt, version, rindex-2)
					v, vcloser, err2 = d.kkvGetValue(skBuf[:], item.Sid)
					require.NoError(t, err2)
					require.Equal(t, item.Kvs[kkn-2], v)
					vcloser()
				}
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVList(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			kn := 10
			kkn := 100
			keyList := sortedkv.MakeKKVList(kn, kkn, DataTypeList, 100, 0)
			checkPush := func(isPushLeft, isPopLeft bool) {
				for _, item := range keyList {
					for _, value := range item.Kvs {
						n, err := d.ListPush(item.Key, item.Sid, isPushLeft, false, value)
						require.NoError(t, err)
						require.Equal(t, int64(1), n)

						v, vcloser, err1 := d.ListPop(item.Key, item.Sid, isPopLeft)
						require.NoError(t, err1)
						require.Equal(t, value, v)
						vcloser()
					}
					exist, _ := d.Exist(item.Key, item.Sid)
					require.Equal(t, false, exist)
				}
			}

			checkPush(true, true)
			checkPush(true, false)
			checkPush(false, true)
			checkPush(false, false)

			checkPushExist := func(isPushLeft, isPopLeft bool) {
				for _, item := range keyList {
					rid := 0
					num := int64(0)
					for i := 0; i < kkn; i += 2 {
						num += 2
						if i == 0 {
							n, err := d.ListPush(item.Key, item.Sid, isPushLeft, true, item.Kvs[i])
							require.NoError(t, err)
							require.Equal(t, int64(0), n)
							n, err = d.ListPush(item.Key, item.Sid, isPushLeft, false, item.Kvs[i], item.Kvs[i+1])
							require.NoError(t, err)
							require.Equal(t, num, n)
						} else {
							n, err := d.ListPush(item.Key, item.Sid, isPushLeft, true, item.Kvs[i], item.Kvs[i+1])
							require.NoError(t, err)
							require.Equal(t, num, n)
						}

						v, vcloser, err1 := d.ListPop(item.Key, item.Sid, isPopLeft)
						require.NoError(t, err1)
						if isPushLeft == isPopLeft {
							require.Equal(t, item.Kvs[i+1], v)
						} else {
							require.Equal(t, item.Kvs[rid], v)
							rid++
						}
						num--
						vcloser()
					}

					dt, timestamp, _, _, _, size, err := d.GetMeta(item.Key, item.Sid)
					require.NoError(t, err)
					require.Equal(t, uint64(0), timestamp)
					require.Equal(t, item.DataType, dt)
					require.Equal(t, uint32(kkn/2), size)

					newTs := uint64(time.Now().UnixMilli() - 10000)
					_, _, err = d.ExpireAt(item.Key, item.Sid, newTs)
					require.NoError(t, err)
					_, timestamp, err = d.GetTimestamp(item.Key, item.Sid)
					require.NoError(t, err)
					require.Equal(t, newTs, timestamp)
				}
			}

			checkPushExist(true, false)
			checkPushExist(false, true)
			checkPushExist(true, true)
			checkPushExist(false, false)

			for _, item := range keyList {
				err := d.ListSet(item.Key, item.Sid, int64(kkn+100), nil)
				require.Equal(t, base.ErrNoSuchKey, err)

				n, err1 := d.ListPush(item.Key, item.Sid, true, false, item.Kvs...)
				require.NoError(t, err1)
				require.Equal(t, int64(kkn), n)

				for i := 0; i < kkn; i++ {
					v, vcloser, err2 := d.ListIndex(item.Key, item.Sid, int64(i))
					require.NoError(t, err2)
					require.Equal(t, item.Kvs[kkn-1-i], v)
					vcloser()
				}

				for i := 0; i < kkn; i++ {
					v, vcloser, err2 := d.ListIndex(item.Key, item.Sid, int64(-1-i))
					require.NoError(t, err2)
					require.Equal(t, item.Kvs[i], v)
					vcloser()
				}

				err = d.ListSet(item.Key, item.Sid, int64(kkn+100), nil)
				require.Equal(t, base.ErrIndexOutOfRange, err)

				dt, timestamp, _, _, _, size, err2 := d.GetMeta(item.Key, item.Sid)
				require.NoError(t, err2)
				require.Equal(t, uint64(0), timestamp)
				require.Equal(t, item.DataType, dt)
				require.Equal(t, uint32(kkn), size)

				for i := 0; i < kkn; i++ {
					newVal := utils.FuncRandBytes(10)
					err = d.ListSet(item.Key, item.Sid, int64(i), newVal)
					require.NoError(t, err)
					v, vcloser, err2 := d.ListIndex(item.Key, item.Sid, int64(i))
					require.NoError(t, err2)
					require.Equal(t, newVal, v)
					vcloser()
				}

				n, _, err = d.Delete(item.Key, item.Sid)
				require.NoError(t, err)
				require.Equal(t, int64(1), n)
				_, _, _, _, _, _, err2 = d.GetMeta(item.Key, item.Sid)
				require.Equal(t, base.ErrNotFound, err2)
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVListTrim(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			kn := 10
			kkn := 100
			keyList := sortedkv.MakeKKVList(kn, kkn, DataTypeList, 100, 0)
			for i, item := range keyList {
				n, err := d.ListPush(item.Key, item.Sid, true, false, item.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(kkn), n)

				dt, timestamp, version, lindex, rindex, size, err := d.GetMeta(item.Key, item.Sid)
				require.NoError(t, err)
				require.Equal(t, uint64(0), timestamp)
				require.Equal(t, item.DataType, dt)
				require.Equal(t, uint32(n), size)
				keyList[i].Version = version
				keyList[i].Lindex = lindex
				keyList[i].Rindex = rindex
			}

			for _, item := range keyList {
				res, resCloser, err := d.ListRange(item.Key, item.Sid, 0, int64(kkn))
				require.NoError(t, err)
				require.Equal(t, kkn, len(res))
				for i := range res {
					require.Equal(t, item.Kvs[kkn-1-i], res[i])
				}
				resCloser()
			}

			require.NoError(t, d.FlushMemtable())
			for _, item := range keyList {
				res, resCloser, err := d.ListRange(item.Key, item.Sid, 0, int64(kkn))
				require.NoError(t, err)
				require.Equal(t, kkn, len(res))
				for i := range res {
					require.Equal(t, item.Kvs[kkn-1-i], res[i])
				}
				resCloser()
			}

			for _, item := range keyList {
				err := d.ListTrim(item.Key, item.Sid, 2, int64(kkn/2))
				require.NoError(t, err)
				res, resCloser, err1 := d.ListRange(item.Key, item.Sid, 0, -1)
				require.NoError(t, err1)
				require.Equal(t, kkn/2-1, len(res))
				for i := range res {
					require.Equal(t, item.Kvs[kkn-3-i], res[i])
				}
				resCloser()
			}

			for i := range keyList {
				if i%2 == 0 {
					continue
				}
				require.NoError(t, d.ListTrim(keyList[i].Key, keyList[i].Sid, 100, 200))
				exist, err := d.Exist(keyList[i].Key, keyList[i].Sid)
				require.Equal(t, base.ErrNotFound, err)
				require.Equal(t, false, exist)
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVListIter(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			kn := 10000
			kkn := 20
			keyList := sortedkv.MakeSortedKKVList(kn, kkn, DataTypeList, 100, 0, true)
			for i := 0; i < kn/2; i++ {
				item := keyList[i]
				n, err := d.ListPush(item.Key, item.Sid, true, false, item.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(kkn), n)
			}

			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			for i := kn / 2; i < kn; i++ {
				item := keyList[i]
				n, err := d.ListPush(item.Key, item.Sid, true, false, item.Kvs...)
				require.NoError(t, err)
				require.Equal(t, int64(kkn), n)
			}

			for i := 0; i < kn; i++ {
				item := keyList[i]
				dt, timestamp, _, _, _, size, err1 := d.GetMeta(item.Key, item.Sid)
				require.NoError(t, err1)
				require.Equal(t, uint64(0), timestamp)
				require.Equal(t, item.DataType, dt)
				require.Equal(t, uint32(kkn), size)
			}

			for _, item := range keyList {
				res, resCloser, err := d.ListRange(item.Key, item.Sid, 0, int64(kkn))
				require.NoError(t, err)
				require.Equal(t, kkn, len(res))
				for i := range res {
					require.Equal(t, item.Kvs[kkn-1-i], res[i])
				}
				resCloser()

				res, resCloser, err = d.ListRange(item.Key, item.Sid, 0, -1)
				require.NoError(t, err)
				require.Equal(t, kkn, len(res))
				for i := range res {
					require.Equal(t, item.Kvs[kkn-1-i], res[i])
				}
				resCloser()

				res, resCloser, err = d.ListRange(item.Key, item.Sid, 2, int64(kkn/2))
				require.NoError(t, err)
				require.Equal(t, kkn/2-1, len(res))
				for i := range res {
					require.Equal(t, item.Kvs[kkn-3-i], res[i])
				}
				resCloser()
			}

			require.NoError(t, d.Close())
		})
	}
}
