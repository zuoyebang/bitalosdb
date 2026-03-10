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

package vectortable

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorIterator(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(path, 0755)
	defer os.RemoveAll(path)

	type data struct {
		k    []byte
		v    []byte
		h, l uint64
		seq  uint64
		ts   uint64
		ver  uint64
		size uint32
		pre  uint64
		next uint64
		dt   uint8
	}

	var ver uint64
	var sz uint32

	c := 1 << 16
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		rdt := uint8(rand.Int31n(int32(kkv.DataTypeDKSet)) + 1)
		if rdt == kkv.DataTypeZsetIndex {
			rdt -= 1
		} else if rdt == kkv.DataTypeExpireKey {
			rdt -= 2
		}
		var d = data{
			k:   []byte(strconv.Itoa(i)),
			seq: uint64(rand.Uint32()),
			ts:  randTimestamp(),
			dt:  rdt,
		}

		switch d.dt {
		case kkv.DataTypeString:
			d.v = []byte("value" + strconv.Itoa(i))
		case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeHash, kkv.DataTypeZset, kkv.DataTypeSet, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			d.ver = ver
			d.size = sz
			if kkv.IsDataTypeList(d.dt) {
				d.pre = rand.Uint64()
				d.next = rand.Uint64()
			}
		}

		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}

	vt, err := newTestVectorTable(path, uint32(c), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	var errMap = make(map[int]error)
	for i := 0; i < c; i++ {
		err = vt.Set(m[i].k, m[i].h, m[i].l, m[i].seq, m[i].dt, m[i].ts, 0, m[i].ver, m[i].size, m[i].pre, m[i].next, m[i].v)
		if err != nil {
			t.Log("set err", i, err)
			errMap[i] = err
		}
	}
	vt.MSync()
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
				return
			default:
				r := rand.Intn(c)
				if m[r].dt == kkv.DataTypeString {
					vl, seq, dt, ts, _, cl, err := vt.GetValue(m[r].h, m[r].l)
					// skip not updated
					if seq > m[r].seq {
						continue
					}
					if err != nil {
						if err == base.ErrNotFound && errMap[r] != nil {
							continue
						}
						t.Error(err, r, m[r].h, m[r].l)
					} else {
						assert.Equal(t, m[r].v, vl)
						assert.Equal(t, m[r].seq, seq)
						assert.Equal(t, m[r].dt, dt)
						assert.Equal(t, m[r].ts, ts)
						if cl != nil {
							cl()
						}
					}
				}
			}
		}
	}()

	it := vt.NewIterator()

	var count int
	for {
		key, h, l, seqNum, dataType, timestamp, version, _, size, pre, next, value, final := it.Next()
		if final {
			assert.Equal(t, c, count)
			break
		}
		// skip not updated
		i, _ := strconv.Atoi(string(key))
		if seqNum > m[i].seq {
			continue
		}
		assert.Equal(t, m[i].k, key)
		assert.Equal(t, m[i].h, h)
		assert.Equal(t, m[i].l, l)
		assert.Equal(t, m[i].seq, seqNum)
		assert.Equal(t, m[i].dt, dataType)
		assert.Equal(t, m[i].ts, timestamp)
		assert.Equal(t, m[i].v, value)
		assert.Equal(t, ver, version)
		assert.Equal(t, sz, size)
		if kkv.IsDataTypeList(m[i].dt) {
			assert.Equal(t, m[i].pre, pre)
			assert.Equal(t, m[i].next, next)
		}
		count++
	}

	time.Sleep(2 * time.Second)

	close(ch)

	require.NoError(t, testCloseVectorTable(vt, false))
}

func TestVectorIteratorNoKey(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(path, 0755)
	defer os.RemoveAll(path)

	type data struct {
		k, v []byte
		h, l uint64
		seq  uint64
		ts   uint64
		ver  uint64
		size uint32
		pre  uint64
		next uint64
		dt   uint8
	}

	var ver uint64
	var sz uint32

	c := 1 << 16
	m := make(map[uint64]map[uint64]*data)
	for i := 0; i < c; i++ {
		rdt := uint8(rand.Int31n(int32(kkv.DataTypeDKSet)) + 1)
		if rdt == kkv.DataTypeZsetIndex {
			rdt -= 1
		} else if rdt == kkv.DataTypeExpireKey {
			rdt -= 2
		}
		var d = data{
			k:   []byte(strconv.Itoa(i)),
			seq: uint64(rand.Uint32()),
			ts:  randTimestamp(),
			dt:  rdt,
		}

		switch d.dt {
		case kkv.DataTypeString:
			d.v = []byte(strconv.Itoa(i))
		case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeHash, kkv.DataTypeZset, kkv.DataTypeSet, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			d.ver = ver
			d.size = sz
			if kkv.IsDataTypeList(d.dt) {
				d.pre = rand.Uint64()
				d.next = rand.Uint64()
			}
		}

		d.h, d.l = hash.MD5Uint64(d.k)
		if m[d.h] == nil {
			m[d.h] = map[uint64]*data{
				d.l: &d,
			}
		} else {
			m[d.h][d.l] = &d
		}

	}

	vt, err := newTestVectorTableNoEM(path, uint32(c), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	var errMap = make(map[uint64]map[uint64]error)
	for h, mh := range m {
		for l := range mh {
			err = vt.Set(m[h][l].k, m[h][l].h, m[h][l].l, m[h][l].seq, m[h][l].dt, m[h][l].ts, 0, m[h][l].ver, m[h][l].size, m[h][l].pre, m[h][l].next, m[h][l].v)
			if err != nil {
				t.Log("set err", h, l, err)
				if errMap[h] == nil {
					errMap[h] = map[uint64]error{
						l: err,
					}
				} else {
					errMap[h][l] = err
				}
			}
		}
	}
	vt.MSync()
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
				return
			default:
				r := rand.Intn(c)
				h, l := hash.MD5Uint64([]byte(strconv.Itoa(r)))
				if m[h][l].dt == kkv.DataTypeString {
					vl, seq, dt, ts, _, cl, err := vt.GetValue(m[h][l].h, m[h][l].l)
					// skip not updated
					if seq > m[h][l].seq {
						continue
					}
					if err != nil {
						if err == base.ErrNotFound && errMap[h][l] != nil {
							continue
						}
						t.Error(err, r, m[h][l].h, m[h][l].l)
					} else {
						assert.Equal(t, m[h][l].v, vl)
						assert.Equal(t, m[h][l].seq, seq)
						assert.Equal(t, m[h][l].dt, dt)
						assert.Equal(t, m[h][l].ts, ts)
						if cl != nil {
							cl()
						}
					}
				}
			}
		}
	}()

	it := vt.NewIterator()

	var count int
	for {
		_, h, l, seqNum, dataType, timestamp, version, _, size, pre, next, value, final := it.Next()
		if final {
			assert.Equal(t, c, count)
			break
		}
		// skip not updated
		if seqNum > m[h][l].seq {
			count++
			continue
		}
		assert.Equal(t, m[h][l].h, h)
		assert.Equal(t, m[h][l].l, l)
		assert.Equal(t, m[h][l].seq, seqNum)
		assert.Equal(t, m[h][l].dt, dataType)
		assert.Equal(t, m[h][l].ts, timestamp)
		assert.Equal(t, m[h][l].v, value)
		assert.Equal(t, ver, version)
		assert.Equal(t, sz, size)
		if kkv.IsDataTypeList(m[h][l].dt) {
			assert.Equal(t, m[h][l].pre, pre)
			assert.Equal(t, m[h][l].next, next)
		}
		count++
	}

	time.Sleep(2 * time.Second)

	close(ch)

	require.NoError(t, testCloseVectorTable(vt, false))
}

func TestVectorIteratorWitSlotNoKey(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(path, 0755)
	defer os.RemoveAll(path)

	type data struct {
		k, v []byte
		h, l uint64
		seq  uint64
		ts   uint64
		ver  uint64
		slot uint16
		size uint32
		pre  uint64
		next uint64
		dt   uint8
	}

	var ver uint64
	var sz uint32

	c := 1 << 16
	m := make(map[uint64]map[uint64]*data)
	for i := 0; i < c; i++ {
		rdt := uint8(rand.Int31n(int32(kkv.DataTypeDKSet)) + 1)
		if rdt == kkv.DataTypeZsetIndex {
			rdt -= 1
		} else if rdt == kkv.DataTypeExpireKey {
			rdt -= 2
		}
		var d = data{
			k:    []byte(strconv.Itoa(i)),
			seq:  uint64(rand.Uint32()),
			ts:   randTimestamp(),
			dt:   rdt,
			slot: 10,
		}

		switch d.dt {
		case kkv.DataTypeString:
			d.v = []byte(strconv.Itoa(i))
		case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeHash, kkv.DataTypeZset, kkv.DataTypeSet, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			d.ver = ver
			d.size = sz
			if kkv.IsDataTypeList(d.dt) {
				d.pre = rand.Uint64()
				d.next = rand.Uint64()
			}
		}

		d.h, d.l = hash.MD5Uint64(d.k)
		if m[d.h] == nil {
			m[d.h] = map[uint64]*data{
				d.l: &d,
			}
		} else {
			m[d.h][d.l] = &d
		}

	}

	vt, err := newTestVectorTableN(path, uint32(c), nil, false, true)
	if err != nil {
		t.Fatal(err)
	}
	var errMap = make(map[uint64]map[uint64]error)
	for h, mh := range m {
		for l := range mh {
			err = vt.Set(m[h][l].k, m[h][l].h, m[h][l].l, m[h][l].seq, m[h][l].dt, m[h][l].ts, m[h][l].slot, m[h][l].ver, m[h][l].size, m[h][l].pre, m[h][l].next, m[h][l].v)
			if err != nil {
				t.Log("set err", h, l, err)
				if errMap[h] == nil {
					errMap[h] = map[uint64]error{
						l: err,
					}
				} else {
					errMap[h][l] = err
				}
			}
		}
	}
	vt.MSync()
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
				return
			default:
				r := rand.Intn(c)
				h, l := hash.MD5Uint64([]byte(strconv.Itoa(r)))
				if m[h][l].dt == kkv.DataTypeString {
					vl, seq, dt, ts, _, cl, err := vt.GetValue(m[h][l].h, m[h][l].l)
					if seq > m[h][l].seq {
						continue
					}
					if err != nil {
						if err == base.ErrNotFound && errMap[h][l] != nil {
							continue
						}
						t.Error(err, r, m[h][l].h, m[h][l].l)
					} else {
						assert.Equal(t, m[h][l].v, vl)
						assert.Equal(t, m[h][l].seq, seq)
						assert.Equal(t, m[h][l].dt, dt)
						assert.Equal(t, m[h][l].ts, ts)
						if cl != nil {
							cl()
						}
					}
				}
			}
		}
	}()

	it := vt.NewIterator()

	var count int
	for {
		_, h, l, seqNum, dataType, timestamp, version, slot, size, pre, next, value, final := it.Next()
		if final {
			assert.Equal(t, c, count)
			break
		}

		if seqNum > m[h][l].seq {
			count++
			continue
		}
		assert.Equal(t, m[h][l].h, h)
		assert.Equal(t, m[h][l].l, l)
		assert.Equal(t, m[h][l].seq, seqNum)
		assert.Equal(t, m[h][l].dt, dataType)
		assert.Equal(t, m[h][l].ts, timestamp)
		assert.Equal(t, m[h][l].v, value)
		assert.Equal(t, ver, version)
		assert.Equal(t, m[h][l].slot, slot)
		assert.Equal(t, sz, size)
		if kkv.IsDataTypeList(m[h][l].dt) {
			assert.Equal(t, m[h][l].pre, pre)
			assert.Equal(t, m[h][l].next, next)
		}
		count++
	}

	time.Sleep(2 * time.Second)

	close(ch)

	require.NoError(t, testCloseVectorTable(vt, false))
}

func TestVectorIterator2(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	type data struct {
		k, v                          []byte
		h, l, seq, ts, ver, pre, next uint64
		size                          uint32
		dt                            uint8
	}

	var ver uint64 = 1000
	var sz uint32 = 1024

	c := 1 << 16
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		rdt := uint8(rand.Int31n(int32(kkv.DataTypeDKSet)) + 1)
		if rdt == kkv.DataTypeZsetIndex {
			rdt -= 1
		} else if rdt == kkv.DataTypeExpireKey {
			rdt -= 2
		}
		var d = data{
			k:   []byte(strconv.Itoa(i)),
			seq: uint64(rand.Uint32()),
			ts:  rand.Uint64() & maskLow56,
			dt:  rdt,
		}

		switch d.dt {
		case kkv.DataTypeString:
			d.v = []byte("value" + strconv.Itoa(i))
		case kkv.DataTypeList, kkv.DataTypeHash, kkv.DataTypeZset, kkv.DataTypeSet, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			d.ver = ver
			d.size = sz
			if kkv.IsDataTypeList(d.dt) {
				d.pre = rand.Uint64()
				d.next = rand.Uint64()
			}
		}

		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}

	vt, err := newTestVectorTable(path, uint32(c), []string{fmt.Sprintf("%s/vt.vti.0.0", path)}, true)
	if err != nil {
		t.Fatal(err)
	}
	var errMap = make(map[int]error)
	for i := 0; i < c; i++ {
		err = vt.Set(m[i].k, m[i].h, m[i].l, m[i].seq, m[i].dt, m[i].ts, 0, m[i].ver, m[i].size, m[i].pre, m[i].next, m[i].v)
		if err != nil {
			t.Log("set err", i, err)
			errMap[i] = err
		}
	}
	vt.MSync()
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
				return
			default:
				it := vt.NewIterator()

				var count int
				for {
					key, h, l, seqNum, dataType, timestamp, version, _, size, pre, next, value, final := it.Next()
					if final {
						assert.Equal(t, c, count)
						break
					}
					if err != nil {
						t.Error(it, err)
						return
					}
					i, _ := strconv.Atoi(string(key))
					assert.Equal(t, key, m[i].k)
					assert.Equal(t, h, m[i].h)
					assert.Equal(t, l, m[i].l)
					assert.Equal(t, seqNum, m[i].seq)
					assert.Equal(t, dataType, m[i].dt)
					assert.Equal(t, timestamp, m[i].ts)
					assert.Equal(t, value, m[i].v)
					assert.Equal(t, version, m[i].ver)
					assert.Equal(t, size, m[i].size)
					assert.Equal(t, pre, m[i].pre)
					assert.Equal(t, next, m[i].next)
					count++
				}
			}
		}
	}()

	for i := 0; i < 1000000; i++ {
		r := rand.Intn(c)
		if m[r].dt == kkv.DataTypeString {
			vl, seq, dt, ts, _, cl, err := vt.GetValue(m[r].h, m[r].l)
			if err != nil {
				if err == base.ErrNotFound && errMap[r] != nil {
					t.Logf("%s %s", errMap[r].Error(), " not found")
					continue
				}
				t.Error(err, r, string(m[r].k))
			} else {
				assert.Equal(t, m[r].v, vl)
				assert.Equal(t, m[r].seq, seq)
				assert.Equal(t, m[r].dt, dt)
				assert.Equal(t, m[r].ts, ts)
				if cl != nil {
					cl()
				}
			}
		}
	}

	close(ch)
}
