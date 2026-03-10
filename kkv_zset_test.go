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
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestKKVZsetIncrBy(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	d := openTestDB(dir)
	keyNum := 10
	skeyNum := 10
	ksize := 20
	keyList := sortedkv.MakeKKVList(keyNum, skeyNum, DataTypeZset, ksize, 0)
	keyCnt := make(map[string]float64)
	for i := range keyList {
		kv := keyList[i]
		for j := 0; j < len(kv.Kvs); j += 2 {
			cnt := float64(0)
			for k := float64(-10); k < 10; k++ {
				incr := k + rand.Float64()
				cnt += incr
				n, err := d.ZIncrBy(kv.Key, kv.Sid, kv.Kvs[j], incr)
				require.NoError(t, err)
				require.Equal(t, cnt, n)
			}

			n, err := d.ZIncrBy(kv.Key, kv.Sid, kv.Kvs[j], float64(0))
			require.NoError(t, err)
			require.Equal(t, cnt, n)

			keyCnt[string(kv.Key)+string(kv.Kvs[j])] = cnt
		}

		size, err := d.GetMetaSize(kv.Key, kv.Sid)
		require.NoError(t, err)
		require.Equal(t, uint32(skeyNum), size)
	}

	require.NoError(t, d.FlushMemtable())

	for i := range keyList {
		kv := keyList[i]
		size, err := d.GetMetaSize(kv.Key, kv.Sid)
		require.NoError(t, err)
		require.Equal(t, uint32(10), size)
	}

	d.FlushBitpage()
	time.Sleep(2 * time.Second)

	for i := range keyList {
		kv := keyList[i]
		for j := 0; j < len(kv.Kvs); j += 2 {
			cnt := keyCnt[string(kv.Key)+string(kv.Kvs[j])]
			for k := float64(-10); k < 10; k++ {
				incr := k + rand.Float64()
				cnt += incr
				n, err := d.ZIncrBy(kv.Key, kv.Sid, kv.Kvs[j], incr)
				require.NoError(t, err)
				require.Equal(t, cnt, n)
			}
		}

		sort.Slice(kv.Kvs, func(i, j int) bool {
			return bytes.Compare(kv.Kvs[i], kv.Kvs[j]) < 0
		})
		cnt, err := d.ZLexCount(kv.Key, kv.Sid, kv.Kvs[0], kv.Kvs[len(kv.Kvs)-1], false, false)
		require.NoError(t, err)
		require.Equal(t, skeyNum, int(cnt))
	}

	require.NoError(t, d.Close())
}

func TestKKVZset(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	d := openTestDB(dir)
	ksize := 100
	siKeySize := kkv.SiKeyLength
	keyList := sortedkv.MakeKKVList(1, 10, DataTypeZset, siKeySize, ksize)
	kv := keyList[0]
	n, err := d.ZAdd(kv.Key, kv.Sid, kv.Kvs...)
	require.NoError(t, err)
	require.Equal(t, int64(10), n)

	readKKV := func() {
		for j := 0; j < len(kv.Kvs); j += 2 {
			score, vserr := d.ZScore(kv.Key, kv.Sid, kv.Kvs[j+1])
			require.NoError(t, vserr)
			require.Equal(t, kkv.DecodeZsetScore(kv.Kvs[j]), score)
			kv.Kvs[j] = utils.FuncRandBytes(siKeySize)
		}
	}

	readKKV()

	dt, timestamp, _, _, _, size, e := d.GetMeta(kv.Key, kv.Sid)
	require.NoError(t, e)
	require.Equal(t, uint64(0), timestamp)
	require.Equal(t, kv.DataType, dt)
	require.Equal(t, uint32(10), size)

	_, _, err = d.ExpireAt(kv.Key, kv.Sid, 100)
	require.NoError(t, err)
	_, timestamp, _, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
	require.NoError(t, err)
	require.Equal(t, uint64(100), timestamp)
	require.Equal(t, uint32(10), size)

	require.NoError(t, d.FlushMemtable())

	n, err = d.ZAdd(kv.Key, kv.Sid, kv.Kvs...)
	require.NoError(t, err)
	require.Equal(t, int64(10), n)
	dt, timestamp, _, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
	require.NoError(t, err)
	require.Equal(t, uint64(0), timestamp)
	require.Equal(t, kv.DataType, dt)
	require.Equal(t, uint32(10), size)
	readKKV()

	n, err = d.ZAdd(kv.Key, kv.Sid, kv.Kvs...)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	readKKV()

	d.FlushBitpage()
	time.Sleep(2 * time.Second)
	n, err = d.ZAdd(kv.Key, kv.Sid, kv.Kvs...)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	readKKV()

	n, err = d.ZRem(kv.Key, kv.Sid, kv.Kvs[1], kv.Kvs[3], []byte("notexistsubkey"))
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	_, vserr := d.ZScore(kv.Key, kv.Sid, kv.Kvs[1])
	require.Equal(t, base.ErrNotFound, vserr)
	_, vserr = d.ZScore(kv.Key, kv.Sid, kv.Kvs[3])
	require.Equal(t, base.ErrNotFound, vserr)

	n, err = d.ZAdd(kv.Key, kv.Sid, kv.Kvs...)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	newTs := uint64(time.Now().UnixMilli() + 10000)
	n, _, err = d.ExpireAt(kv.Key, kv.Sid, newTs)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	_, timestamp, _, _, _, size, err = d.GetMeta(kv.Key, kv.Sid)
	require.NoError(t, err)
	require.Equal(t, newTs, timestamp)
	require.Equal(t, uint32(10), size)
	readKKV()

	for j := 0; j < len(kv.Kvs); j += 2 {
		n, err = d.ZRem(kv.Key, kv.Sid, kv.Kvs[j+1])
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}

	exist, err1 := d.Exist(kv.Key, kv.Sid)
	require.Equal(t, false, exist)
	require.Equal(t, base.ErrNotFound, err1)

	require.NoError(t, d.Close())
}

func testZAdd(t *testing.T, d *DB, keyList []*sortedkv.KKVItem, kkn, start, end int) {
	for i := start; i < end; i++ {
		kv := keyList[i]
		n, err := d.ZAdd(kv.Key, kv.Sid, kv.Kvs...)
		require.NoError(t, err)
		require.Equal(t, int64(kkn), n)

		dt, timestamp, _, _, _, size, err1 := d.GetMeta(kv.Key, kv.Sid)
		require.NoError(t, err1)
		require.Equal(t, uint64(0), timestamp)
		require.Equal(t, kv.DataType, dt)
		require.Equal(t, uint32(kkn), size)
	}
}

func TestKKVZsetZRange(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 1000
			skeyNum := 100
			vsize := 20
			keyList := sortedkv.MakeSortedKKVList(keyNum, skeyNum, DataTypeZset, kkv.SiKeyLength, vsize, false)
			wn := 0

			checkRange := func(dels []int, start, stop int64) {
				for _, reverse := range []bool{false, true} {
					sz := int64(skeyNum)
					startIndex, stopIndex := zParseLimit(sz, start, stop, reverse)
					var resNum int
					if startIndex > stopIndex || startIndex >= sz || stopIndex < 0 {
						resNum = 0
					} else {
						resNum = int(stopIndex) - int(startIndex) + 1
						if resNum == skeyNum {
							resNum -= len(dels)
						}
					}
					for i := 0; i < wn; i++ {
						kv := keyList[i]
						res, closer, err := d.ZRange(kv.Key, kv.Sid, start, stop, reverse)
						require.NoError(t, err)
						require.Equal(t, resNum, len(res))
						if resNum == 0 {
							continue
						}
						ri := 0
						if !reverse {
							for j := int(startIndex) * 2; j <= int(stopIndex)*2; j += 2 {
								for _, del := range dels {
									if del == j {
										j += 2
									}
								}
								require.Equal(t, kkv.DecodeZsetScore(kv.Kvs[j]), res[ri].Score)
								require.Equal(t, kv.Kvs[j+1], res[ri].Member)
								ri++
							}
							closer()
						} else {
							for j := int(stopIndex) * 2; j < int(startIndex)*2; j -= 2 {
								for _, del := range dels {
									if del == j {
										j -= 2
									}
								}
								require.Equal(t, kkv.DecodeZsetScore(kv.Kvs[j]), res[ri].Score)
								require.Equal(t, kv.Kvs[j+1], res[ri].Member)
								ri++
							}
							closer()
						}
					}
				}
			}

			runCheckRange := func(dels []int) {
				checkRange(dels, 0, -1)
				checkRange(dels, 0, int64(skeyNum)+10)
				checkRange(dels, 0, 80)
				checkRange(dels, 10, 90)
				checkRange(dels, 5, 75)
				checkRange(dels, 50, 40)
				checkRange(dels, int64(skeyNum)+10, -1)
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			wn = keyNum / 3

			runCheckRange(nil)
			require.NoError(t, d.FlushMemtable())

			testZAdd(t, d, keyList, skeyNum, wn, (keyNum/3)*2)
			wn = (keyNum / 3) * 2

			runCheckRange(nil)
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			testZAdd(t, d, keyList, skeyNum, wn, keyNum)
			wn = keyNum

			runCheckRange(nil)
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			runCheckRange(nil)

			for i := 0; i < keyNum; i++ {
				kv := keyList[i]
				n, err := d.ZRem(kv.Key, kv.Sid, kv.Kvs[51], kv.Kvs[71])
				require.NoError(t, err)
				require.Equal(t, int64(2), n)
			}

			runCheckRange([]int{50, 70})

			for i := 0; i < keyNum; i++ {
				if i%2 == 0 {
					continue
				}
				n, _, err := d.Delete(keyList[i].Key, keyList[i].Sid)
				require.NoError(t, err)
				require.Equal(t, int64(1), n)
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZRangeOne(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 10000
			skeyNum := 1
			vsize := 20
			keyList := sortedkv.MakeSortedKKVList(keyNum, skeyNum, DataTypeZset, 0, vsize, false)

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)
			testZAdd(t, d, keyList, skeyNum, keyNum/3, (keyNum/3)*2)
			require.NoError(t, d.FlushMemtable())
			testZAdd(t, d, keyList, skeyNum, (keyNum/3)*2, keyNum)

			read := func(db *DB) {
				for _, reverse := range []bool{false, true} {
					for i := 0; i < keyNum; i++ {
						kv := keyList[i]
						res, closer, err := db.ZRange(kv.Key, kv.Sid, 0, -1, reverse)
						require.NoError(t, err)
						require.Equal(t, skeyNum, len(res))
						require.Equal(t, kkv.DecodeZsetScore(kv.Kvs[0]), res[0].Score)
						require.Equal(t, kv.Kvs[1], res[0].Member)
						closer()
					}
				}
			}

			read(d)
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			read(d)
			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZRangeByScore(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 1000
			skeyNum := 100
			scoreStep := 0.5
			wn := 0
			startScore := float64(100)
			endScore := float64(100) + float64(skeyNum-1)*0.5
			keyList := sortedkv.MakeSortedZsetList(keyNum, skeyNum, startScore, scoreStep)

			checkRange := func(min, max float64, leftClose bool, rightClose bool, offset int, count int) {
				for _, reverse := range []bool{false, true} {
					for i := 0; i < wn; i++ {
						kv := keyList[i]

						tss := min
						ted := max
						if min < startScore {
							tss = startScore
						} else if leftClose {
							tss += scoreStep
						}
						if max > endScore {
							ted = endScore
						} else if rightClose {
							ted -= scoreStep
						}
						zc := int((ted-tss)/scoreStep) + 1
						if tss > ted {
							zc = 0
						}
						cnt, err := d.ZCount(kv.Key, kv.Sid, min, max, leftClose, rightClose)
						require.NoError(t, err)
						require.Equal(t, zc, int(cnt))

						var ss float64
						if !reverse {
							res, closer, err := d.ZRangeByScore(kv.Key, kv.Sid, min, max, leftClose, rightClose, offset, count)
							require.NoError(t, err)
							require.Equal(t, count, len(res))
							if count == 0 {
								continue
							}
							if min < startScore {
								ss = startScore + float64(offset)*scoreStep
							} else {
								ss = min + float64(offset)*scoreStep
								if leftClose {
									ss += scoreStep
								}
							}
							for _, sp := range res {
								require.Equal(t, ss, sp.Score)
								ss += 0.5
							}
							closer()
						} else {
							res, closer, err := d.ZRevRangeByScore(kv.Key, kv.Sid, min, max, leftClose, rightClose, offset, count)
							require.NoError(t, err)
							require.Equal(t, count, len(res))
							if count == 0 {
								continue
							}
							if max > endScore {
								ss = endScore - float64(offset)*scoreStep
							} else {
								ss = max - float64(offset)*scoreStep
								if rightClose {
									ss -= scoreStep
								}
							}
							for _, sp := range res {
								require.Equal(t, ss, sp.Score)
								ss -= 0.5
							}
							closer()
						}
					}
				}
			}

			runCheckRange := func() {
				checkRange(startScore, math.MaxFloat64, false, false, 0, 10)
				checkRange(startScore, math.MaxFloat64, false, true, 0, 10)
				checkRange(-math.MaxFloat64, endScore, false, false, 0, 10)
				checkRange(-math.MaxFloat64, endScore, false, true, 0, 10)
				checkRange(startScore, endScore-1, false, false, 0, 10)
				checkRange(startScore, endScore-1, false, true, 0, 10)
				checkRange(startScore, endScore+10, false, true, 0, 10)
				checkRange(startScore, endScore+10, false, false, 0, 10)
				checkRange(startScore+2, startScore+12, true, false, 0, 10)
				checkRange(startScore+2, startScore+12, true, false, 2, 10)
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			wn = keyNum / 3

			runCheckRange()
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			testZAdd(t, d, keyList, skeyNum, wn, (keyNum/3)*2)
			wn = (keyNum / 3) * 2

			runCheckRange()
			require.NoError(t, d.FlushMemtable())

			testZAdd(t, d, keyList, skeyNum, wn, keyNum)
			wn = keyNum

			runCheckRange()
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			runCheckRange()

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZRevRangeByScore(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 100
			skeyNum := 100
			scoreStep := 0.5
			startScore := float64(100)
			gap := 4
			maxScore := startScore + float64(skeyNum/gap-1)*scoreStep
			keyList := sortedkv.MakeSortedZsetList2(keyNum, skeyNum, gap, startScore, scoreStep)

			checkRange := func(min, max float64, leftClose bool, rightClose bool, offset int, count int) {
				startPos := (skeyNum - offset - 1) * 2
				if max < maxScore {
					startPos -= int((maxScore-max)/scoreStep) * gap * 2
				}

				if rightClose {
					startPos -= gap * 2
				}

				for i := keyNum - 1; i >= 0; i-- {
					kv := keyList[i]
					res, closer, err := d.ZRevRangeByScore(kv.Key, kv.Sid, min, max, leftClose, rightClose, offset, count)
					require.NoError(t, err)
					require.Equal(t, count, len(res))
					pos := startPos
					for _, sp := range res {
						score := kkv.DecodeZsetScore(kv.Kvs[pos])
						member := kv.Kvs[pos+1]
						require.Equal(t, score, sp.Score)
						require.Equal(t, member, sp.Member)
						pos -= 2
					}
					closer()
				}
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum)
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			checkRange(startScore, math.MaxFloat64, false, false, 2, 10)
			checkRange(startScore, maxScore+1, false, false, 2, 50)
			checkRange(startScore, 111, false, false, 2, 50)
			checkRange(startScore, 109, false, true, 4, 50)

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZScore(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 100
			skeyNum := 100
			scoreStep := 0.5
			startScore := float64(100)
			endScore := float64(100) + float64(skeyNum-1)*0.5
			keyList := sortedkv.MakeSortedZsetList(keyNum, skeyNum, startScore, scoreStep)

			checkRange := func() {
				for i := 0; i < keyNum; i++ {
					kv := keyList[i]
					r := int64(0)
					k := 0
					for j := startScore; j < endScore+scoreStep; j += scoreStep {
						if j > endScore {
							rank, err := d.ZRank(kv.Key, kv.Sid, []byte(fmt.Sprintf("%d", skeyNum*100)), false)
							require.Equal(t, ErrZsetMemberNil, err)
							require.Equal(t, int64(0), rank)
						} else {
							rank, err := d.ZRank(kv.Key, kv.Sid, kv.Kvs[k+1], false)
							require.NoError(t, err)
							require.Equal(t, r, rank)
							r++
						}
						k += 2
					}
					r--
					k = 0
					for j := startScore; j <= endScore; j += scoreStep {
						rank, err := d.ZRank(kv.Key, kv.Sid, kv.Kvs[k+1], true)
						require.NoError(t, err)
						require.Equal(t, r, rank)
						r--
						k += 2
					}
				}
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)
			testZAdd(t, d, keyList, skeyNum, keyNum/3, (keyNum/3)*2)
			require.NoError(t, d.FlushMemtable())
			testZAdd(t, d, keyList, skeyNum, (keyNum/3)*2, keyNum)

			checkRange()
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			checkRange()
			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZRem(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	d := openTestDB(dir)
	keyNum := 10
	skeyNum := 100
	scoreStep := 0.5
	wn := 0
	startScore := float64(100)
	keyList := sortedkv.MakeSortedZsetList(keyNum, skeyNum, startScore, scoreStep)

	read := func(isDel bool) {
		for i := 0; i < keyNum; i++ {
			kv := keyList[i]
			for j := 0; j < skeyNum; j += 2 {
				member := kv.Kvs[j+1]
				score, err := d.ZScore(kv.Key, kv.Sid, member)
				if isDel && j%2 == 0 {
					require.Equal(t, ErrNotFound, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, kkv.DecodeZsetScore(kv.Kvs[j]), score)
				}
			}
		}
	}

	zrem := func() {
		for i := 0; i < keyNum; i++ {
			kv := keyList[i]
			for j := 0; j < skeyNum; j += 2 {
				if j%2 == 0 {
					dn, err := d.ZRem(kv.Key, kv.Sid, kv.Kvs[j+1])
					require.NoError(t, err)
					require.Equal(t, int64(1), dn)
				}
			}
		}
	}

	testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
	wn = keyNum / 3
	require.NoError(t, d.FlushMemtable())
	testZAdd(t, d, keyList, skeyNum, wn, (keyNum/3)*2)
	wn = (keyNum / 3) * 2
	d.FlushBitpage()
	time.Sleep(2 * time.Second)
	testZAdd(t, d, keyList, skeyNum, wn, keyNum)
	read(false)
	zrem()
	read(true)
	require.NoError(t, d.Close())

	d = openTestDB(dir)
	read(true)
	require.NoError(t, d.Close())
}

func TestKKVZsetZRemRangeByScore(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 10
			skeyNum := 100
			scoreStep := 0.5
			wn := 0
			startScore := float64(100)
			keyList := sortedkv.MakeSortedZsetList(keyNum, skeyNum, startScore, scoreStep)

			checkRange := func(start, stop int, min, max float64, leftClose bool, rightClose bool, skip int, isDel bool, delCount int) {
				for i := start; i < stop; i++ {
					kv := keyList[i]
					if isDel {
						rem, err := d.ZRemRangeByScore(kv.Key, kv.Sid, min, max, leftClose, rightClose)
						require.NoError(t, err)
						require.Equal(t, int64(delCount), rem)
					}
					res, closer, err := d.ZRange(kv.Key, kv.Sid, 0, -1, false)
					require.NoError(t, err)
					require.Equal(t, skeyNum-delCount, len(res))
					closer()
					k := 0
					for j, sp := range res {
						if j == skip {
							for h := 0; h < delCount; h++ {
								_, err = d.ZScore(kv.Key, kv.Sid, kv.Kvs[k+1])
								require.Equal(t, ErrNotFound, err)
								k += 2
							}
						}
						require.Equal(t, kkv.DecodeZsetScore(kv.Kvs[k]), sp.Score)
						require.Equal(t, kv.Kvs[k+1], sp.Member)
						k += 2
					}
				}
			}

			runCheckRange := func(isDel bool) {
				checkRange(0, keyNum/4, startScore+1, startScore+10, false, false, 2, isDel, 19)
				checkRange(keyNum/4, keyNum/2, startScore+10, startScore+15, true, false, 21, isDel, 10)
				checkRange(keyNum/2, keyNum/4*3, startScore+10, startScore+15, false, true, 20, isDel, 10)
				checkRange(keyNum/4*3, keyNum, startScore+10, startScore+15, true, true, 21, isDel, 9)
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			wn = keyNum / 3
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)
			testZAdd(t, d, keyList, skeyNum, wn, (keyNum/3)*2)
			wn = (keyNum / 3) * 2
			require.NoError(t, d.FlushMemtable())
			testZAdd(t, d, keyList, skeyNum, wn, keyNum)
			wn = keyNum

			runCheckRange(true)
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			runCheckRange(false)
			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZRemRangeByLex(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 100
			skeyNum := 100
			scoreStep := 0.5
			wn := 0
			startScore := float64(100)
			keyList := sortedkv.MakeSortedZsetList1(keyNum, skeyNum, startScore, scoreStep)

			checkRange := func(start, stop int, min, max float64, leftClose bool, rightClose bool, skip int, isDel bool, delCount int) {
				utils.Float64ToByteSort(min, nil)
				for i := start; i < stop; i++ {
					kv := keyList[i]
					if isDel {
						rem, err := d.ZRemRangeByLex(kv.Key, kv.Sid,
							utils.Float64ToByteSort(min, nil),
							utils.Float64ToByteSort(max, nil),
							leftClose, rightClose)
						require.NoError(t, err)
						require.Equal(t, int64(delCount), rem)
					}
					res, closer, err := d.ZRange(kv.Key, kv.Sid, 0, -1, false)
					require.NoError(t, err)
					require.Equal(t, skeyNum-delCount, len(res))
					closer()
					k := 0
					for j, sp := range res {
						if j == skip {
							for h := 0; h < delCount; h++ {
								_, err = d.ZScore(kv.Key, kv.Sid, kv.Kvs[k+1])
								require.Equal(t, ErrNotFound, err)
								k += 2
							}
						}
						require.Equal(t, kkv.DecodeZsetScore(kv.Kvs[k]), sp.Score)
						require.Equal(t, kv.Kvs[k+1], sp.Member)
						k += 2
					}
				}
			}

			runCheckRange := func(isDel bool) {
				checkRange(0, keyNum/4, startScore+1, startScore+10, false, false, 2, isDel, 19)
				checkRange(keyNum/4, keyNum/2, startScore+10, startScore+15, true, false, 21, isDel, 10)
				checkRange(keyNum/2, keyNum/4*3, startScore+10, startScore+15, false, true, 20, isDel, 10)
				checkRange(keyNum/4*3, keyNum, startScore+10, startScore+15, true, true, 21, isDel, 9)
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			wn = keyNum / 3
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)
			testZAdd(t, d, keyList, skeyNum, wn, (keyNum/3)*2)
			wn = (keyNum / 3) * 2
			require.NoError(t, d.FlushMemtable())
			testZAdd(t, d, keyList, skeyNum, wn, keyNum)

			runCheckRange(true)
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			runCheckRange(false)

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZRemRangeByRank(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 10
			skeyNum := 100
			scoreStep := 0.5
			wn := 0
			startScore := float64(100)
			keyList := sortedkv.MakeSortedZsetList(keyNum, skeyNum, startScore, scoreStep)

			checkRange := func(start, stop, min, max int, isDel bool) {
				if max > skeyNum {
					max = skeyNum
				}
				if min < 0 {
					min = 0
				}
				delCount := max - min + 1
				for i := start; i < stop; i++ {
					kv := keyList[i]
					if isDel {
						rem, err := d.ZRemRangeByRank(kv.Key, kv.Sid, int64(min), int64(max))
						require.NoError(t, err)
						require.Equal(t, int64(delCount), rem)
					}
					res, closer, err := d.ZRange(kv.Key, kv.Sid, 0, -1, false)
					require.NoError(t, err)
					require.Equal(t, skeyNum-delCount, len(res))
					closer()
					k := 0
					for j, sp := range res {
						if j == min {
							for h := 0; h < delCount; h++ {
								_, err = d.ZScore(kv.Key, kv.Sid, kv.Kvs[k+1])
								require.Equal(t, ErrNotFound, err)
								k += 2
							}
						}
						require.Equal(t, kkv.DecodeZsetScore(kv.Kvs[k]), sp.Score)
						require.Equal(t, kv.Kvs[k+1], sp.Member)
						k += 2
					}
				}
			}

			runCheckRange := func(isDel bool) {
				checkRange(0, keyNum/4, 2, 10, isDel)
				checkRange(keyNum/4, keyNum/2, 10, keyNum+10, isDel)
				checkRange(keyNum/2, keyNum/4*3, -10, 40, isDel)
				checkRange(keyNum/4*3, keyNum, -2, keyNum+2, isDel)
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			wn = keyNum / 3
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)
			testZAdd(t, d, keyList, skeyNum, wn, (keyNum/3)*2)
			wn = (keyNum / 3) * 2
			require.NoError(t, d.FlushMemtable())
			testZAdd(t, d, keyList, skeyNum, wn, keyNum)
			wn = keyNum

			runCheckRange(true)
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			runCheckRange(false)
			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZCount(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 100
			skeyNum := 100
			scoreStep := 0.5
			wn := 0
			startScore := float64(100)
			endScore := float64(100) + float64(skeyNum-1)*scoreStep
			keyList := sortedkv.MakeSortedZsetList(keyNum, skeyNum, startScore, scoreStep)

			checkRange := func(min, max float64, leftClose bool, rightClose bool, count int64) {
				for i := 0; i < wn; i++ {
					kv := keyList[i]
					cnt, err := d.ZCount(kv.Key, kv.Sid, min, max, leftClose, rightClose)
					require.NoError(t, err)
					require.Equal(t, count, cnt)
				}
			}

			runCheckRange := func() {
				count := int64(skeyNum)
				for i := startScore; i <= endScore; i += scoreStep {
					checkRange(i, endScore+scoreStep, false, false, count)
					checkRange(i, endScore+scoreStep, false, true, count)
					checkRange(i, endScore+scoreStep, true, false, count-1)
					checkRange(i, endScore+scoreStep, true, true, count-1)
					count--
				}
				j := float64(0)
				count = int64(skeyNum)
				for i := startScore; i <= endScore; i += scoreStep {
					if i > endScore-j*scoreStep {
						break
					}
					checkRange(i, endScore-j*scoreStep, false, false, count)
					checkRange(i, endScore-j*scoreStep, true, false, count-1)
					checkRange(i, endScore-j*scoreStep, false, true, count-1)
					checkRange(i, endScore-j*scoreStep, true, true, count-2)
					count -= 2
					j++
				}
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			wn = keyNum / 3

			runCheckRange()
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)
			testZAdd(t, d, keyList, skeyNum, wn, (keyNum/3)*2)
			wn = (keyNum / 3) * 2
			runCheckRange()
			require.NoError(t, d.FlushMemtable())
			testZAdd(t, d, keyList, skeyNum, wn, keyNum)
			wn = keyNum
			runCheckRange()
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			runCheckRange()
			require.NoError(t, d.Close())
		})
	}
}

func TestKKVZsetZRank(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d := openTestDB1(dir, useMiniVi)
			keyNum := 100
			skeyNum := 100
			scoreStep := 0.5
			wn := 0
			startScore := float64(100)
			endScore := float64(100) + float64(skeyNum-1)*0.5
			keyList := sortedkv.MakeSortedZsetList1(keyNum, skeyNum, startScore, scoreStep)

			checkRange := func() {
				for i := 0; i < wn; i++ {
					kv := keyList[i]
					r := int64(0)
					for j := startScore; j <= endScore+scoreStep; j += scoreStep {
						member := utils.Float64ToByteSort(j, nil)
						rank, err := d.ZRank(kv.Key, kv.Sid, member, false)
						if j > endScore {
							require.Equal(t, ErrZsetMemberNil, err)
							require.Equal(t, int64(0), rank)
						} else {
							require.NoError(t, err)
							require.Equal(t, r, rank)
							r++
						}
					}
					r--
					for j := startScore; j <= endScore; j += scoreStep {
						member := utils.Float64ToByteSort(j, nil)
						rank, err := d.ZRank(kv.Key, kv.Sid, member, true)
						require.NoError(t, err)
						require.Equal(t, r, rank)
						r--
					}
				}
			}

			testZAdd(t, d, keyList, skeyNum, 0, keyNum/3)
			wn = keyNum / 3
			checkRange()
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(2 * time.Second)

			testZAdd(t, d, keyList, skeyNum, wn, (keyNum/3)*2)
			wn = (keyNum / 3) * 2
			checkRange()
			require.NoError(t, d.FlushMemtable())

			testZAdd(t, d, keyList, skeyNum, wn, keyNum)
			wn = keyNum

			checkRange()
			require.NoError(t, d.Close())

			d = openTestDB1(dir, useMiniVi)
			checkRange()

			require.NoError(t, d.Close())
		})
	}
}
