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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
)

func TestVectorTableBase(t *testing.T) {
	{
		vt := newTestVM(true)
		testBase(t, vt)
	}
	{
		vt := newTestVM(false)
		testBase(t, vt)
	}
}

func testBase(t *testing.T, vt *VectorMap) {
	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testVMSetGet(t, vt, k, v, h, l, 0, 100)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a', 'b'}, 512)
		h, l := hash.MD5Uint64(k)
		testVMSetGet(t, vt, k, v, h, l, 0, 100)
	}

	{
		k := []byte("keylong")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testVMSetGet(t, vt, k, v, h, l, 0, 200)
	}

	{
		k := []byte("key")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testVMSetGet(t, vt, k, v, h, l, 0, 200)
	}

	{
		k := []byte("hash")
		h, l := hash.MD5Uint64(k)
		testVMSetGetMeta(t, vt, k, h, l, kkv.DataTypeHash, randTimestamp(), 200, 100, 1, 10, 10, 20)
	}

	{
		k := []byte("list")
		h, l := hash.MD5Uint64(k)
		testVMSetGetMeta(t, vt, k, h, l, kkv.DataTypeList, randTimestamp(), 200, 100, 1, 10, 10, 20)
	}

	{
		k := []byte("hash")
		h, l := hash.MD5Uint64(k)
		testVMSetGetMeta(t, vt, k, h, l, kkv.DataTypeHash, randTimestamp(), 400, 100, 2, 9, 11, 21)
	}

	{
		k := []byte("list")
		h, l := hash.MD5Uint64(k)
		testVMSetGetMeta(t, vt, k, h, l, kkv.DataTypeList, randTimestamp(), 400, 100, 2, 9, 11, 21)
	}

	testCloseVectorMap(vt)
}

func testCloseVectorMap(vt *VectorMap) {
	vt.Close(false)
}

func testVMSetGet(t *testing.T, vm *VectorMap, k, v []byte, h, l uint64, ttl, seq uint64) {
	vOld, seqOld, _, _, _, vCloser, err := vm.GetValue(h, l)
	voExpected := make([]byte, len(vOld))
	copy(voExpected, vOld)
	if vCloser != nil {
		vCloser()
	}

	err = vm.Set(k, h, l, seq, kkv.DataTypeString, ttl, 0, 0, 0, 0, 0, v)
	if err != nil {
		t.Error(err)
		return
	}

	if seq <= seqOld {
		vo, seq1, _, _, _, closer, err := vm.GetValue(h, l)
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

		_, ttlO, _, err := vm.GetTimestamp(h, l)
		if err != nil {
			t.Error(err)
		}
		if ttl != ttlO {
			t.Error("timestamp not equal")
		}

		seqO, ok := vm.Has(h, l)
		if !ok {
			t.Error("not found")
			return
		}
		if seqOld != seqO {
			t.Error("seq not equal")
		}
	} else {
		vo, _, _, _, _, closer, err := vm.GetValue(h, l)
		if err != nil {
			t.Error(err)
			return
		}

		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}

		if closer != nil {
			closer()
		}

		_, ttlO, _, err := vm.GetTimestamp(h, l)
		if err != nil {
			t.Error(err)
		}
		if ttl != ttlO {
			t.Error("timestamp not equal")
		}

		seqO, ok := vm.Has(h, l)
		if !ok {
			t.Error("not found")
			return
		}
		if seq != seqO {
			t.Error("seq not equal")
		}
	}
}

func testVMSetGetMeta(t *testing.T, vm *VectorMap, k []byte, h, l uint64, dataType uint8, ttl, seq uint64,
	slot uint16, version uint64, size uint32, pre, next uint64) {
	err := vm.Set(k, h, l, seq, dataType, ttl, slot, version, size, pre, next, nil)
	if err != nil {
		t.Error(err)
		return
	}

	seqNo, dt, ts, slt, ver, sz, p, n, err := vm.GetMeta(h, l)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, seq, seqNo)
	assert.Equal(t, dataType, dt)
	assert.Equal(t, ttl, ts)
	assert.Equal(t, slot, slt)
	assert.Equal(t, version, ver)
	assert.Equal(t, size, sz)
	if kkv.IsDataTypeList(dt) {
		assert.Equal(t, pre, p)
		assert.Equal(t, next, n)
	} else {
		assert.Equal(t, uint64(0), p)
		assert.Equal(t, uint64(0), n)
	}
}

func newTestVM(storeKey bool) *VectorMap {
	opt := &VectorMapOptions{
		HashSize: 1024,
		Shards:   1,
		MaxMem:   1 << 20,
		StoreKey: storeKey,
		Logger:   getLogger(),
	}
	vm := NewVectorMap(opt)

	arenaBuf := manual.New(opt.MaxMem)
	vm.NewData(arenaBuf)

	return vm
}

func getLogger() base.Logger {
	opt := options.InitOptionsPool()
	return opt.BaseOptions.Logger
}
