// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitalosdb

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKKVBitmapSetBit(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	opts := &Options{
		Logger: DefaultLogger,
	}
	d := openTestDBByOpts(dir, opts)

	key := []byte("setbit64_key")
	for i := 0; i < 10000000; i++ {
		j := rand.Int31n(100000000)
		n, err := d.SetBit64(key, testSlotId, uint64(j), 1)
		require.NoError(t, err)
		n, err = d.GetBit64(key, testSlotId, uint64(j))
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}

	require.NoError(t, d.Close())
}

func TestKKVBitmapSetGet(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			d := openTestDB1(dir, useMiniVi)
			keyNum := 100
			maxOffset := bitmapFieldBits * 100
			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				for j := 0; j < maxOffset; j += bitmapFieldBits {
					n, err := d.SetBit64(key, keySlotId, uint64(j), 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
					n, err = d.GetBit64(key, keySlotId, uint64(j))
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}

				if i == keyNum/2 {
					require.NoError(t, d.FlushMemtable())
					d.FlushBitpage()
					time.Sleep(time.Second * 2)
				}

				if i == keyNum/3*2 {
					require.NoError(t, d.FlushMemtable())
					time.Sleep(time.Second)
				}
			}

			ts := uint64((time.Now().Unix() + 100) * 1000)
			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				dt, timestamp, _, bitSize, _, size, err := d.GetMeta(key, keySlotId)
				require.NoError(t, err)
				require.Equal(t, uint64(0), timestamp)
				require.Equal(t, DataTypeBitmap, dt)
				require.Equal(t, uint32(100), size)
				require.Equal(t, uint64(100), bitSize)

				count, err1 := d.BitCount64(key, keySlotId, 0, -1)
				require.NoError(t, err1)
				require.Equal(t, int64(bitSize), count)

				n, _, err2 := d.ExpireAt(key, keySlotId, ts)
				require.NoError(t, err2)
				require.Equal(t, int64(1), n)
				dt, timestamp, _, _, _, size, err = d.GetMeta(key, keySlotId)
				require.NoError(t, err)
				require.Equal(t, DataTypeBitmap, dt)
				require.Equal(t, ts, timestamp)
				require.Equal(t, uint32(100), size)

				for j := 0; j < maxOffset; j += bitmapFieldBits {
					n, err = d.GetBit64(key, keySlotId, uint64(j))
					require.NoError(t, err)
					require.Equal(t, int64(1), n)

					n, err = d.GetBit64(key, keySlotId, uint64(j+1))
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
				}
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVBitmapBitCount(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			d := openTestDB1(dir, useMiniVi)
			keyNum := 100
			setCount := 100

			randOffset := func(size int) []uint64 {
				dupMap := make(map[uint64]bool, size)
				offs := make([]uint64, 0, size)
				for i := 0; i < size; i++ {
					for {
						num := uint64(rand.Int63n(math.MaxInt))
						if _, ok := dupMap[num]; !ok {
							dupMap[num] = true
							offs = append(offs, num)
							break
						}
					}
				}
				return offs
			}

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)

				setOffsets := randOffset(setCount)
				for _, offset := range setOffsets {
					n, err := d.SetBit64(key, keySlotId, offset, 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
					n, err = d.GetBit64(key, keySlotId, offset)
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}

				if i == keyNum/2 {
					require.NoError(t, d.FlushMemtable())
					d.FlushBitpage()
					time.Sleep(time.Second * 2)
				}

				if i == keyNum/3*2 {
					require.NoError(t, d.FlushMemtable())
					time.Sleep(time.Second)
				}
			}

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				dt, timestamp, _, bitSize, _, _, err := d.GetMeta(key, keySlotId)
				require.NoError(t, err)
				require.Equal(t, uint64(0), timestamp)
				require.Equal(t, DataTypeBitmap, dt)
				require.Equal(t, uint64(setCount), bitSize)

				count, err1 := d.BitCount64(key, keySlotId, 0, math.MaxInt)
				require.NoError(t, err1)
				require.Equal(t, int64(bitSize), count)
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVBitmapBitCount1(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			d := openTestDB1(dir, useMiniVi)
			keyNum := 100
			fieldNum := 100
			var setOffsets []uint64
			for i := 1; i <= fieldNum; i++ {
				setOffsets = append(setOffsets,
					uint64(0+bitmapFieldBits*i),
					uint64(99+bitmapFieldBits*i),
					uint64(199+bitmapFieldBits*i),
					uint64(1999+bitmapFieldBits*i),
					uint64(bitmapFieldBits*(i+1)-1))
			}
			setCount := len(setOffsets)

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				for j := 0; j < len(setOffsets)/3; j++ {
					n, err := d.SetBit64(key, keySlotId, setOffsets[j], 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
					n, err = d.GetBit64(key, keySlotId, setOffsets[j])
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}
			}
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(time.Second * 2)

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				for j := len(setOffsets) / 3; j < len(setOffsets)/3*2; j++ {
					n, err := d.SetBit64(key, keySlotId, setOffsets[j], 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
					n, err = d.GetBit64(key, keySlotId, setOffsets[j])
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}
			}
			require.NoError(t, d.FlushMemtable())
			time.Sleep(time.Second)

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				for j := len(setOffsets) / 3 * 2; j < len(setOffsets); j++ {
					n, err := d.SetBit64(key, keySlotId, setOffsets[j], 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
					n, err = d.GetBit64(key, keySlotId, setOffsets[j])
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}
			}

			checkBitCount := func(key []byte, slotId uint16, begin, end int, exp int64) {
				count, err := d.BitCount64(key, slotId, begin, end)
				require.NoError(t, err)
				require.Equal(t, exp, count)
			}

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				dt, timestamp, _, bitSize, _, size, err := d.GetMeta(key, keySlotId)
				require.NoError(t, err)
				require.Equal(t, uint64(0), timestamp)
				require.Equal(t, DataTypeBitmap, dt)
				require.Equal(t, uint64(setCount), bitSize)
				require.Equal(t, uint32(fieldNum), size)

				checkBitCount(key, keySlotId, 100, 99, 0)
				checkBitCount(key, keySlotId, 99, bitmapFieldBits+98, 1)
				checkBitCount(key, keySlotId, 99, bitmapFieldBits+99, 2)
				checkBitCount(key, keySlotId, bitmapFieldBits+99, bitmapFieldBits+99, 1)
				checkBitCount(key, keySlotId, 100, bitmapFieldBits*2-1, 5)
				checkBitCount(key, keySlotId, bitmapFieldBits+100, bitmapFieldBits*2-1, 3)
				checkBitCount(key, keySlotId, 99, bitmapFieldBits*2, 6)
				checkBitCount(key, keySlotId, bitmapFieldBits+99, bitmapFieldBits*2, 5)
				checkBitCount(key, keySlotId, 100, bitmapFieldBits*2, 6)
				checkBitCount(key, keySlotId, bitmapFieldBits+100, bitmapFieldBits*2, 4)
				checkBitCount(key, keySlotId, bitmapFieldBits+90, bitmapFieldBits+1999, 3)
				checkBitCount(key, keySlotId, bitmapFieldBits+90, bitmapFieldBits+2100, 3)
				checkBitCount(key, keySlotId, bitmapFieldBits+99, bitmapFieldBits+1999, 3)
				checkBitCount(key, keySlotId, bitmapFieldBits+100, bitmapFieldBits+1999, 2)
				checkBitCount(key, keySlotId, bitmapFieldBits+100, bitmapFieldBits+2000, 2)
				checkBitCount(key, keySlotId, 100, bitmapFieldBits*2+2000, 9)
				checkBitCount(key, keySlotId, bitmapFieldBits+100, bitmapFieldBits*2+2000, 7)
				checkBitCount(key, keySlotId, bitmapFieldBits+99, bitmapFieldBits*2+2000, 8)
				checkBitCount(key, keySlotId, bitmapFieldBits+99, bitmapFieldBits*2+1999, 8)
				checkBitCount(key, keySlotId, 100, bitmapFieldBits*2+1999, 9)
				checkBitCount(key, keySlotId, bitmapFieldBits+100, bitmapFieldBits*2+1999, 7)
				checkBitCount(key, keySlotId, 100, bitmapFieldBits*3+2000, 14)
				checkBitCount(key, keySlotId, bitmapFieldBits+100, bitmapFieldBits*3+2000, 12)
				checkBitCount(key, keySlotId, 0, bitmapFieldBits*(fieldNum+1), int64(setCount))
				checkBitCount(key, keySlotId, 0, -1, int64(setCount))
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVBitmapBitPos64(t *testing.T) {
	for _, useMiniVi := range []bool{true, false} {
		t.Run(fmt.Sprintf("useMiniVi=%v", useMiniVi), func(t *testing.T) {
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)

			d := openTestDB1(dir, useMiniVi)
			keyNum := 50
			fieldNum := 100
			var setOffsets []uint64
			for i := 1; i <= fieldNum; i++ {
				setOffsets = append(setOffsets,
					uint64(10+bitmapFieldBits*i),
					uint64(99+bitmapFieldBits*i),
					uint64(199+bitmapFieldBits*i),
					uint64(1999+bitmapFieldBits*i),
					uint64(bitmapFieldBits*(i+1)-1))
			}

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				for j := 0; j < len(setOffsets)/3; j++ {
					n, err := d.SetBit64(key, keySlotId, setOffsets[j], 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
					n, err = d.GetBit64(key, keySlotId, setOffsets[j])
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}
			}
			require.NoError(t, d.FlushMemtable())
			d.FlushBitpage()
			time.Sleep(time.Second * 2)

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				for j := len(setOffsets) / 3; j < len(setOffsets)/3*2; j++ {
					n, err := d.SetBit64(key, keySlotId, setOffsets[j], 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
					n, err = d.GetBit64(key, keySlotId, setOffsets[j])
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}
			}
			require.NoError(t, d.FlushMemtable())
			time.Sleep(time.Second)

			for i := 0; i < keyNum; i++ {
				key := makeTestIntKey(i)
				keySlotId := makeTestKeySlotid(key)
				for j := len(setOffsets) / 3 * 2; j < len(setOffsets); j++ {
					n, err := d.SetBit64(key, keySlotId, setOffsets[j], 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
					n, err = d.GetBit64(key, keySlotId, setOffsets[j])
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}
			}

			checkBitPos := func(key []byte, slotId uint16, begin, end, on int, exp int64) {
				count, err := d.BitPos64(key, slotId, on, begin, end)
				require.NoError(t, err)
				require.Equal(t, exp, count)
			}

			key := makeTestIntKey(keyNum + 100)
			keySlotId := makeTestKeySlotid(key)
			checkBitPos(key, keySlotId, 1, 20, 1, -1)
			checkBitPos(key, keySlotId, 1, 20, 0, 0)
			checkBitPos(key, keySlotId, 100, 99, 1, -1)
			checkBitPos(key, keySlotId, 100, 99, 0, 0)

			for i := 0; i < keyNum; i++ {
				key = makeTestIntKey(i)
				keySlotId = makeTestKeySlotid(key)
				checkBitPos(key, keySlotId, 1, 20, 1, -1)
				checkBitPos(key, keySlotId, 1, 20, 0, -1)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits+9, 1, -1)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits+9, 0, bitmapFieldBits+1)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits+20, 1, bitmapFieldBits+10)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits+20, 0, bitmapFieldBits+1)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits*2+20, 1, bitmapFieldBits+10)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits*2+20, 0, bitmapFieldBits+1)
				checkBitPos(key, keySlotId, bitmapFieldBits+11, bitmapFieldBits+20, 1, -1)
				checkBitPos(key, keySlotId, bitmapFieldBits+11, bitmapFieldBits+20, 0, bitmapFieldBits+11)
				checkBitPos(key, keySlotId, bitmapFieldBits+2000, bitmapFieldBits*(fieldNum+1), 1, bitmapFieldBits*2-1)
				checkBitPos(key, keySlotId, bitmapFieldBits+2000, bitmapFieldBits*(fieldNum+1), 0, bitmapFieldBits+2000)
				checkBitPos(key, keySlotId, bitmapFieldBits+2000, bitmapFieldBits+4000, 1, -1)
				checkBitPos(key, keySlotId, bitmapFieldBits+100, bitmapFieldBits+4000, 0, bitmapFieldBits+100)
				checkBitPos(key, keySlotId, bitmapFieldBits*(fieldNum+1), bitmapFieldBits*(fieldNum+2), 1, -1)
				checkBitPos(key, keySlotId, bitmapFieldBits*(fieldNum+1), bitmapFieldBits*(fieldNum+2), 0, -1)
			}

			for i := 0; i < keyNum; i++ {
				key = makeTestIntKey(i)
				keySlotId = makeTestKeySlotid(key)
				for j := 0; j < 5; j++ {
					n, err := d.SetBit64(key, keySlotId, setOffsets[j], 0)
					require.NoError(t, err)
					require.Equal(t, int64(1), n)
				}
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits+9, 1, -1)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits*2+20, 1, bitmapFieldBits*2+10)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits*2+20, 0, bitmapFieldBits+1)
				checkBitPos(key, keySlotId, bitmapFieldBits, bitmapFieldBits*2+20, 0, bitmapFieldBits)
			}

			for i := 0; i < keyNum; i++ {
				key = makeTestIntKey(i)
				keySlotId = makeTestKeySlotid(key)
				for j := bitmapFieldBits; j < bitmapFieldBits*2; j++ {
					n, err := d.SetBit64(key, keySlotId, uint64(j), 1)
					require.NoError(t, err)
					require.Equal(t, int64(0), n)
				}
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits*2+20, 1, bitmapFieldBits+1)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits*2+20, 0, bitmapFieldBits*2)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits*2-10, 0, -1)
				checkBitPos(key, keySlotId, bitmapFieldBits+1, bitmapFieldBits+100, 0, -1)

			}

			require.NoError(t, d.Close())
		})
	}
}

func TestKKVBitPos64(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	d := openTestDB(dir)

	key := makeTestIntKey(100)
	keySlotId := makeTestKeySlotid(key)
	n, err := d.BitPos64(key, keySlotId, 1, 0, -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), n)
	n, err = d.BitPos64(key, keySlotId, 0, 0, -1)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	for i := 110; i <= 120; i++ {
		n, err = d.SetBit64(key, keySlotId, uint64(i), 1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	}

	n, err = d.SetBit64(key, keySlotId, 125, 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	cases := []struct {
		start, end int
		exp1, exp0 int64
	}{
		{0, -1, 110, 0},
		{109, 130, 110, 109},
		{109, 113, 110, 109},
		{110, 113, 110, -1},
		{110, 130, 110, 121},
		{109, 130, 110, 109},
		{119, 130, 119, 121},
		{129, 140, -1, -1},
		{119, -2, 119, 121},
		{-10, -1, -1, -1},
		{-1, -10, -1, -1},
	}

	for _, c := range cases {
		n, err = d.BitPos64(key, keySlotId, 1, c.start, c.end)
		require.NoError(t, err)
		require.Equal(t, c.exp1, n)

		n, err = d.BitPos64(key, keySlotId, 0, c.start, c.end)
		require.NoError(t, err)
		require.Equal(t, c.exp0, n)
	}

	require.NoError(t, d.Close())
}
