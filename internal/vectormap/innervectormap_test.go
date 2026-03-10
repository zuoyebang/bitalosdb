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

package vectormap

import (
	"bytes"
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInnerVectorMap(t *testing.T) {
	{
		vt := newTestInnerVectorMap(1<<5, 1<<20)
		testInnerVectorMap(t, vt)
	}
	{
		vt := newTestInnerVectorMapWithKey(1<<5, 1<<20)
		testInnerVectorMap(t, vt)
	}
}

func testInnerVectorMap(t *testing.T, ivm *InnerVectorMap) {
	vt := newTestInnerVectorMap(1<<5, 1<<20)
	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, 0, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a', 'b'}, 512)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, 0, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keylong")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, 0, kkv.DataTypeString, 0, 200)
	}

	{
		k := []byte("key")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, 0, kkv.DataTypeString, 0, 200)
	}

	vt.Close()
}

func TestVectorMapFull(t *testing.T) {
	maxMem := 1 << 10
	vt := newTestInnerVectorMap(simd.MaxAvgGroupLoad, maxMem)
	v := []byte("value")

	for i := 0; i < simd.MaxAvgGroupLoad+1; i++ {
		k := []byte("key" + strconv.Itoa(i))
		h, l := hash.MD5Uint64(k)
		err := vt.Set(k, h, l, 1, kkv.DataTypeString, 1, 0, 0, 0, 0, 0, v)
		if i < simd.MaxAvgGroupLoad {
			require.NoError(t, err)
		} else {
			require.Equal(t, base.ErrTableFull, err)
		}
	}
	k := []byte("key0")
	h, l := hash.MD5Uint64(k)
	vt.Delete(h, l, 2)
	vLong := bytes.Repeat([]byte{'a'}, maxMem)
	err := vt.Set(k, h, l, 3, kkv.DataTypeString, 1, 0, 0, 0, 0, 0, vLong)
	require.Equal(t, base.ErrTableFull, err)
	err = vt.Set(k, h, l, 3, kkv.DataTypeString, 1, 0, 0, 0, 0, 0, v)
	require.NoError(t, err)
}

func TestVectorMapList(t *testing.T) {
	maxMem := 1 << 10
	vt := newTestInnerVectorMap(simd.MaxAvgGroupLoad, maxMem)
	v := []byte("value")
	k := []byte("key")
	h, l := hash.MD5Uint64(k)
	err := vt.Set(k, h, l, 1, kkv.DataTypeList, 100000, 0, 1, 100, 150, 200, v)
	assert.NoError(t, err)

	seq, dt, ts, _, ver, size, pre, next, err := vt.GetMeta(h, l)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), seq)
	assert.Equal(t, kkv.DataTypeList, dt)
	assert.Equal(t, uint64(100000), ts)
	assert.Equal(t, uint64(1), ver)
	assert.Equal(t, uint32(100), size)
	assert.Equal(t, uint64(150), pre)
	assert.Equal(t, uint64(200), next)

	vt.Close()
}

//	func TestVectorMapReload_MetaReuse(t *testing.T) {
//		printCurrentFunctionName()
//		path, err := os.MkdirTemp(os.TempDir(), "vt")
//		if err != nil {
//			t.Fatal(err)
//		}
//		defer os.RemoveAll(path)
//
//		vt, err := newTestInnerVectorMap(path, 1, nil, true)
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		dh := vt.kvHolder.(*dataHolder)
//		fmt.Println(dh.emOffset)
//
//		{
//			k := []byte("key")
//			v := []byte("value")
//			h, l := hash.MD5Uint64(k)
//			testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
//			vt.Delete(h, l, 101)
//		}
//
//		{
//			k := []byte("key1")
//			v := []byte("value1")
//			h, l := hash.MD5Uint64(k)
//			testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 101)
//		}
//
//		{
//			k := []byte("keylong")
//			v := bytes.Repeat([]byte{'a'}, 1024)
//			h, l := hash.MD5Uint64(k)
//			testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 102)
//		}
//
//		{
//			k := []byte("key1")
//			h, l := hash.MD5Uint64(k)
//			testDeleteGet(t, vt, k, h, l, 103)
//		}
//
//		require.NoError(t, testCloseInnerVectorMap(vt, false))
//
//		fs, err := getFiles(path)
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		vt, err = newTestInnerVectorMap(path, 1, fs, true)
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		{
//			k := []byte("key")
//			v := []byte("value")
//			h, l := hash.MD5Uint64(k)
//			testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
//		}
//
//		{
//			k := []byte("keylong")
//			v := bytes.Repeat([]byte{'a'}, 1024)
//			h, l := hash.MD5Uint64(k)
//			testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
//		}
//
//		{
//			k := []byte("key")
//			v := []byte("value")
//			h, l := hash.MD5Uint64(k)
//			testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
//		}
//
//		{
//			k := []byte("key1")
//			v := []byte("value1")
//			h, l := hash.MD5Uint64(k)
//			testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 101)
//		}
//
//		{
//			k := []byte("keylong")
//			v := bytes.Repeat([]byte{'a'}, 1024)
//			h, l := hash.MD5Uint64(k)
//			testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 102)
//		}
//
//		{
//			k := []byte("key1")
//			h, l := hash.MD5Uint64(k)
//			testDeleteGet(t, vt, k, h, l, 103)
//		}
//
//		require.NoError(t, testCloseInnerVectorMap(vt, false))
//	}
func testSetGetNoKey(t *testing.T, vt *InnerVectorMap, k, v []byte, h, l uint64, dataType uint8, ttl, seq uint64) {
	vOld, seqOld, _, _, _, closer, err := vt.Get(h, l)
	voExpected := make([]byte, len(vOld))
	copy(voExpected, vOld)
	if closer != nil {
		closer()
	}

	err = vt.Set(k, h, l, seq, dataType, ttl, 0, 0, 0, 0, 0, v)
	if err != nil {
		t.Error(err)
		return
	}

	if seq <= seqOld {
		vo, seq1, _, _, _, closer, err := vt.Get(h, l)
		if err != nil {
			t.Error(err)
			return
		}

		if seqOld != seq1 {
			t.Error("seq not equal")
		}

		if !bytes.Equal(vo, voExpected) {
			t.Error("value not equal")
		}

		if closer != nil {
			closer()
		}

		_, ttlO, _, err := vt.GetTimestamp(h, l)
		if err != nil {
			t.Error(err)
		}
		if ttl != ttlO {
			t.Error("timestamp not equal")
		}

		seqO, ok := vt.Has(h, l)
		if !ok {
			t.Error("not found")
			return
		}
		if seqOld != seqO {
			t.Error("seq not equal")
		}
	} else {
		vo, _, _, _, _, closer, err := vt.Get(h, l)
		if err != nil {
			t.Error(err)
			return
		}

		if !bytes.Equal(vo, v) {
			t.Errorf("value not equal exp(%s) act(%s)", string(v), string(vo))
		}

		if closer != nil {
			closer()
		}

		_, ttlO, _, err := vt.GetTimestamp(h, l)
		if err != nil {
			t.Error(err)
		}
		if ttl != ttlO {
			t.Error("timestamp not equal")
		}

		seqO, ok := vt.Has(h, l)
		if !ok {
			t.Error("not found")
			return
		}
		if seq != seqO {
			t.Error("seq not equal")
		}
	}
}

func testSetGet(t *testing.T, vt *InnerVectorMap, k, v []byte, h, l uint64, slot uint16, dataType uint8, ttl, seq uint64) {
	vOld, seqOld, _, _, _, closer, err := vt.Get(h, l)
	voExpected := make([]byte, len(vOld))
	copy(voExpected, vOld)
	if closer != nil {
		closer()
	}

	err = vt.Set(k, h, l, seq, dataType, ttl, slot, 0, 0, 0, 0, v)
	if err != nil {
		t.Error(err)
		return
	}

	if seq <= seqOld {
		vo, seq1, _, _, _, closer, err := vt.Get(h, l)
		if err != nil {
			t.Error(err)
			return
		}

		if seqOld != seq1 {
			t.Error("seq not equal")
		}

		if !bytes.Equal(vo, voExpected) {
			t.Error("value not equal")
		}

		if closer != nil {
			closer()
		}

		_, ttlO, _, err := vt.GetTimestamp(h, l)
		if err != nil {
			t.Error(err)
		}
		if ttl != ttlO {
			t.Error("timestamp not equal")
		}

		seqO, ok := vt.Has(h, l)
		if !ok {
			t.Error("not found")
			return
		}
		if seqOld != seqO {
			t.Error("seq not equal")
		}
	} else {
		vo, _, _, _, _, closer, err := vt.Get(h, l)
		if err != nil {
			t.Error(err)
			return
		}

		if !bytes.Equal(vo, v) {
			t.Errorf("value not equal exp(%s) act(%s)", string(v), string(vo))
		}

		if closer != nil {
			closer()
		}

		_, ttlO, _, err := vt.GetTimestamp(h, l)
		if err != nil {
			t.Error(err)
		}
		if ttl != ttlO {
			t.Error("timestamp not equal")
		}

		seqO, ok := vt.Has(h, l)
		if !ok {
			t.Error("not found")
			return
		}
		if seq != seqO {
			t.Error("seq not equal")
		}
	}
}

func TestInnerVectorMapWithKey(t *testing.T) {
	vm := newTestInnerVectorMapWithKey(14, 10<<10)
	{
		var (
			k, v        = []byte("string"), []byte("value")
			seq  uint64 = 1
			ts   uint64 = randTimestamp()
			slot uint16 = 2
			ver  uint64 = 100
			sz   uint32 = 5
			p, n uint64 = 100, 200
		)
		h, l := hash.MD5Uint64(k)
		testSetGetStringWithKey(t, vm, k, h, l, seq, ts, slot, ver, sz, p, n, v)
	}

	{
		var (
			k, v        = []byte("list"), []byte("value")
			seq  uint64 = 1
			ts   uint64 = randTimestamp()
			slot uint16 = 2
			ver  uint64 = 100
			sz   uint32 = 5
			p, n uint64 = 100, 200
		)
		h, l := hash.MD5Uint64(k)
		testSetGetListWithKey(t, vm, k, h, l, seq, ts, slot, ver, sz, p, n, v)
	}

	{
		var (
			k, v        = []byte("bitmap"), []byte("value")
			seq  uint64 = 1
			ts   uint64 = randTimestamp()
			slot uint16 = 2
			ver  uint64 = 100
			sz   uint32 = 5
			p, n uint64 = 100, 200
		)
		h, l := hash.MD5Uint64(k)
		testSetGetBitmapWithKey(t, vm, k, h, l, seq, ts, slot, ver, sz, p, n, v)
	}

	{
		var (
			k, v        = []byte("dkhash"), []byte("value")
			seq  uint64 = 1
			ts   uint64 = randTimestamp()
			slot uint16 = 2
			ver  uint64 = 100
			sz   uint32 = 5
			p, n uint64 = 100, 200
		)
		h, l := hash.MD5Uint64(k)
		testSetGetDKHash(t, vm, k, h, l, seq, ts, slot, ver, sz, p, n, v)
	}

	{
		var (
			k, v        = []byte("dkset"), []byte("value")
			seq  uint64 = 1
			ts   uint64 = randTimestamp()
			slot uint16 = 2
			ver  uint64 = 100
			sz   uint32 = 5
			p, n uint64 = 100, 200
		)
		h, l := hash.MD5Uint64(k)
		testSetGetDKSet(t, vm, k, h, l, seq, ts, slot, ver, sz, p, n, v)
	}

	{
		var (
			k, v        = []byte("set"), []byte("value")
			seq  uint64 = 1
			dt          = kkv.DataTypeSet
			ts   uint64 = randTimestamp()
			slot uint16 = 2
			ver  uint64 = 100
			sz   uint32 = 5
			p, n uint64 = 100, 200
		)
		h, l := hash.MD5Uint64(k)
		testSetGetMetaWithKey(t, vm, k, h, l, seq, dt, ts, slot, ver, sz, p, n, v)
	}
}

func testSetGetStringWithKey(t *testing.T, vm *InnerVectorMap, k []byte, h, l, seq uint64, ts uint64,
	slot uint16, ver uint64, sz uint32, p, n uint64, v []byte) {
	err := vm.Set(k, h, l, seq, kkv.DataTypeString, ts, slot, ver, sz, p, n, v)
	assert.NoError(t, err)
	if key, value, seqNum, dataType, timestamp, slotId, version, size, pre, next, err := vm.GetAll(h, l); err == nil {
		assert.Equal(t, k, key)
		assert.Equal(t, v, value)
		assert.Equal(t, seq, seqNum)
		assert.Equal(t, kkv.DataTypeString, dataType)
		assert.Equal(t, ts, timestamp)
		assert.Equal(t, slot, slotId)
		assert.Equal(t, uint64(0), version)
		assert.Equal(t, uint32(0), size)
		assert.Equal(t, uint64(0), pre)
		assert.Equal(t, uint64(0), next)
	} else {
		assert.NoError(t, err)
	}
}

func testSetGetMetaWithKey(t *testing.T, vm *InnerVectorMap, k []byte, h, l, seq uint64, dt uint8, ts uint64,
	slot uint16, ver uint64, sz uint32, p, n uint64, v []byte) {
	err := vm.Set(k, h, l, seq, dt, ts, slot, ver, sz, p, n, v)
	assert.NoError(t, err)
	if key, value, seqNum, dataType, timestamp, slotId, version, size, pre, next, err := vm.GetAll(h, l); err == nil {
		if vm.storeKey {
			assert.Equal(t, k, key)
		}
		assert.Equal(t, 0, len(value))
		assert.Equal(t, seq, seqNum)
		assert.Equal(t, dt, dataType)
		assert.Equal(t, ts, timestamp)
		assert.Equal(t, slot, slotId)
		assert.Equal(t, ver, version)
		assert.Equal(t, sz, size)
		if dt == kkv.DataTypeList {
			assert.Equal(t, p, pre)
			assert.Equal(t, n, next)
		}
	} else {
		assert.NoError(t, err)
	}
}

func testSetGetListWithKey(t *testing.T, vm *InnerVectorMap, k []byte, h, l, seq uint64, ts uint64,
	slot uint16, ver uint64, sz uint32, p, n uint64, v []byte) {
	err := vm.Set(k, h, l, seq, kkv.DataTypeList, ts, slot, ver, sz, p, n, v)
	assert.NoError(t, err)
	if key, value, seqNum, dataType, timestamp, slotId, version, size, pre, next, err := vm.GetAll(h, l); err == nil {
		assert.Equal(t, k, key)
		assert.Equal(t, 0, len(value))
		assert.Equal(t, seq, seqNum)
		assert.Equal(t, kkv.DataTypeList, dataType)
		assert.Equal(t, ts, timestamp)
		assert.Equal(t, slot, slotId)
		assert.Equal(t, ver, version)
		assert.Equal(t, sz, size)
		assert.Equal(t, p, pre)
		assert.Equal(t, n, next)
	} else {
		assert.NoError(t, err)
	}
}

func testSetGetBitmapWithKey(t *testing.T, vm *InnerVectorMap, k []byte, h, l, seq uint64, ts uint64,
	slot uint16, ver uint64, sz uint32, p, n uint64, v []byte) {
	err := vm.Set(k, h, l, seq, kkv.DataTypeBitmap, ts, slot, ver, sz, p, n, v)
	assert.NoError(t, err)
	if key, value, seqNum, dataType, timestamp, slotId, version, size, pre, next, err := vm.GetAll(h, l); err == nil {
		assert.Equal(t, k, key)
		assert.Equal(t, 0, len(value))
		assert.Equal(t, seq, seqNum)
		assert.Equal(t, kkv.DataTypeBitmap, dataType)
		assert.Equal(t, ts, timestamp)
		assert.Equal(t, slot, slotId)
		assert.Equal(t, ver, version)
		assert.Equal(t, sz, size)
		assert.Equal(t, p, pre)
		assert.Equal(t, n, next)
	} else {
		assert.NoError(t, err)
	}
}

func testSetGetDKHash(t *testing.T, vm *InnerVectorMap, k []byte, h, l, seq uint64, ts uint64,
	slot uint16, ver uint64, sz uint32, p, n uint64, v []byte) {
	err := vm.Set(k, h, l, seq, kkv.DataTypeDKHash, ts, slot, ver, sz, p, n, v)
	assert.NoError(t, err)
	if key, value, seqNum, dataType, timestamp, slotId, version, size, pre, next, err := vm.GetAll(h, l); err == nil {
		assert.Equal(t, k, key)
		assert.Equal(t, 0, len(value))
		assert.Equal(t, seq, seqNum)
		assert.Equal(t, kkv.DataTypeDKHash, dataType)
		assert.Equal(t, ts, timestamp)
		assert.Equal(t, slot, slotId)
		assert.Equal(t, ver, version)
		assert.Equal(t, sz, size)
		assert.Equal(t, p, pre)
		assert.Equal(t, n, next)
	} else {
		assert.NoError(t, err)
	}
}

func testSetGetDKSet(t *testing.T, vm *InnerVectorMap, k []byte, h, l, seq uint64, ts uint64,
	slot uint16, ver uint64, sz uint32, p, n uint64, v []byte) {
	err := vm.Set(k, h, l, seq, kkv.DataTypeDKSet, ts, slot, ver, sz, p, n, v)
	assert.NoError(t, err)
	if key, value, seqNum, dataType, timestamp, slotId, version, size, pre, next, err := vm.GetAll(h, l); err == nil {
		assert.Equal(t, k, key)
		assert.Equal(t, 0, len(value))
		assert.Equal(t, seq, seqNum)
		assert.Equal(t, kkv.DataTypeDKSet, dataType)
		assert.Equal(t, ts, timestamp)
		assert.Equal(t, slot, slotId)
		assert.Equal(t, ver, version)
		assert.Equal(t, sz, size)
		assert.Equal(t, p, pre)
		assert.Equal(t, n, next)
	} else {
		assert.NoError(t, err)
	}
}

func newTestInnerVectorMap(hashSize uint32, maxMem int) *InnerVectorMap {
	op := &VectorMapOptions{
		HashSize: hashSize,
		MaxMem:   maxMem,
		Logger:   base.DefaultLogger,
	}
	data := make([]byte, maxMem)
	vm := NewInnerVectorMap(op, 1, hashSize)
	vm.NewDataHolder(data)
	return vm
}

func newTestInnerVectorMapWithKey(hashSize uint32, maxMem int) *InnerVectorMap {
	op := &VectorMapOptions{
		HashSize: hashSize,
		MaxMem:   maxMem,
		Logger:   base.DefaultLogger,
		StoreKey: true,
	}
	data := make([]byte, maxMem)
	vm := NewInnerVectorMap(op, 1, hashSize)
	vm.NewDataHolder(data)
	return vm
}

func testDeleteGet(t *testing.T, vt *InnerVectorMap, k []byte, h, l uint64, seq uint64) {
	vt.Delete(h, l, seq)
	_, _, _, _, _, _, err := vt.Get(h, l)
	if !errors.Is(err, base.ErrNotFound) {
		t.Errorf("find delete key(%s)", string(k))
		return
	}
}

func randTimestamp() uint64 {
	return uint64(time.Now().UnixMilli() + rand.Int63n(10000) + 10000)
}

func genBytesData(size, count int) (keys [][]byte) {
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	r := make([]byte, size*count)
	for i := range r {
		r[i] = letters[rand.Intn(len(letters))]
	}
	keys = make([][]byte, count)
	for i := range keys {
		keys[i] = r[:size]
		r = r[size:]
	}
	return
}

func randomBytes(length int) []byte {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return result
}
