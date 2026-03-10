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
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorTableBase(t *testing.T) {
	printCurrentFunctionName()
	testBase(t, true, false, false)
	testBase(t, false, true, false)
	testBase(t, false, false, false)
	testBase(t, true, true, false)
	testBase(t, true, false, true)
	testBase(t, false, true, true)
	testBase(t, false, false, true)
	testBase(t, true, true, true)
}

func testBase(t *testing.T, storeKey, withSlot, withBuf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	vt, err := newTestInnerVectorTableBuf(path, 1, nil, storeKey, withSlot, withBuf)
	if err != nil {
		t.Fatal(err)
	}

	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keyB")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a', 'b'}, 512)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keylong")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 200)
		testDeleteGet(t, vt, k, h, l, 201)
	}

	{
		k := []byte("key")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 200)
		testDeleteGet(t, vt, k, h, l, 201)
	}

	{
		k := []byte("keyB")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 200)
		testDeleteGet(t, vt, k, h, l, 201)
	}

	{
		k := []byte("hash")
		h, l := hash.MD5Uint64(k)
		testSetGetMeta(t, vt, k, h, l, kkv.DataTypeHash, randTimestamp(), 200, 100, 1, 10, 10, 20)
	}

	{
		k := []byte("list")
		h, l := hash.MD5Uint64(k)
		testSetGetMeta(t, vt, k, h, l, kkv.DataTypeList, randTimestamp(), 200, 100, 1, 10, 10, 20)
	}

	{
		k := []byte("hash")
		h, l := hash.MD5Uint64(k)
		testSetGetMeta(t, vt, k, h, l, kkv.DataTypeHash, randTimestamp(), 400, 100, 2, 9, 11, 21)
		testDeleteGet(t, vt, k, h, l, 401)
	}

	{
		k := []byte("list")
		h, l := hash.MD5Uint64(k)
		testSetGetMeta(t, vt, k, h, l, kkv.DataTypeList, randTimestamp(), 400, 100, 2, 9, 11, 21)
		testDeleteGet(t, vt, k, h, l, 401)
	}

	metaTail := vt.kvHolder.GetHeader().metaTail.GetOffset()

	{
		k := []byte("hash")
		h, l := hash.MD5Uint64(k)
		testSetGetMeta(t, vt, k, h, l, kkv.DataTypeHash, randTimestamp(), 500, 100, 1, 10, 10, 20)
	}

	{
		k := []byte("list")
		h, l := hash.MD5Uint64(k)
		testSetGetMeta(t, vt, k, h, l, kkv.DataTypeList, randTimestamp(), 500, 100, 1, 10, 10, 20)
	}

	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 300)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a', 'b'}, 512)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 300)
	}

	assert.Equal(t, metaTail, vt.kvHolder.GetHeader().metaTail.GetOffset())

	require.NoError(t, testCloseInnerVectorTable(vt, false))
}

func TestVectorTable(t *testing.T) {
	printCurrentFunctionName()
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	vt, err := newTestInnerVectorTable(path, 1, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}

	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a', 'b'}, 512)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keylong")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 200)
	}

	{
		k := []byte("key")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 200)
	}

	require.NoError(t, testCloseInnerVectorTable(vt, false))
}

func TestVectorTableReload(t *testing.T) {
	printCurrentFunctionName()
	testVectorTableReload(t, true, false, true)
	testVectorTableReload(t, false, true, true)
	testVectorTableReload(t, true, true, true)
	testVectorTableReload(t, false, false, true)
	testVectorTableReload(t, true, false, false)
	testVectorTableReload(t, false, true, false)
	testVectorTableReload(t, true, true, false)
	testVectorTableReload(t, false, false, false)
}

func testVectorTableReload(t *testing.T, storeKey, withSlot, withBuf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	vt, err := newTestInnerVectorTableBuf(path, 1, nil, storeKey, withSlot, withBuf)
	if err != nil {
		t.Fatal(err)
	}

	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("key1")
		v := []byte("value1")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 101)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 102)
	}

	{
		k := []byte("key1")
		h, l := hash.MD5Uint64(k)
		testDeleteGet(t, vt, k, h, l, 103)
	}

	require.NoError(t, testCloseInnerVectorTable(vt, false))

	fs, err := getFiles(path)
	if err != nil {
		t.Fatal(err)
	}

	vt, err = newTestInnerVectorTable(path, 1, fs, storeKey, withSlot)
	if err != nil {
		t.Fatal(err)
	}

	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'b'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	require.NoError(t, testCloseInnerVectorTable(vt, false))
}

func TestVectorTableReload_MetaReuse(t *testing.T) {
	printCurrentFunctionName()
	testVectorTableReload_MetaReuse(t, true, false, true)
	testVectorTableReload_MetaReuse(t, false, true, true)
	testVectorTableReload_MetaReuse(t, true, true, true)
	testVectorTableReload_MetaReuse(t, false, false, true)
	testVectorTableReload_MetaReuse(t, true, false, false)
	testVectorTableReload_MetaReuse(t, false, true, false)
	testVectorTableReload_MetaReuse(t, true, true, false)
	testVectorTableReload_MetaReuse(t, false, false, false)
}

func testVectorTableReload_MetaReuse(t *testing.T, storeKey, withSlot, withBuf bool) {

	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	vt, err := newTestInnerVectorTableBuf(path, 1, nil, storeKey, withSlot, withBuf)
	if err != nil {
		t.Fatal(err)
	}

	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
		vt.Delete(h, l, 101)
	}

	{
		k := []byte("key1")
		v := []byte("value1")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 101)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 102)
	}

	{
		k := []byte("key1")
		h, l := hash.MD5Uint64(k)
		testDeleteGet(t, vt, k, h, l, 103)
	}

	require.NoError(t, testCloseInnerVectorTable(vt, false))

	fs, err := getFiles(path)
	if err != nil {
		t.Fatal(err)
	}

	vt, err = newTestInnerVectorTable(path, 1, fs, storeKey, withSlot)
	if err != nil {
		t.Fatal(err)
	}

	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("key")
		v := []byte("value")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 100)
	}

	{
		k := []byte("key1")
		v := []byte("value1")
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 101)
	}

	{
		k := []byte("keylong")
		v := bytes.Repeat([]byte{'a'}, 1024)
		h, l := hash.MD5Uint64(k)
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, 0, 102)
	}

	{
		k := []byte("key1")
		h, l := hash.MD5Uint64(k)
		testDeleteGet(t, vt, k, h, l, 103)
	}

	require.NoError(t, testCloseInnerVectorTable(vt, false))
}

func getFiles(path string) ([]string, error) {
	files := make([]string, 0)
	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if !d.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func TestVTRehash(t *testing.T) {
	printCurrentFunctionName()
	testVTRehash(t, false, true, true)
	testVTRehash(t, true, false, true)
	testVTRehash(t, true, true, true)
	testVTRehash(t, false, false, true)
	testVTRehash(t, false, true, false)
	testVTRehash(t, true, false, false)
	testVTRehash(t, true, true, false)
	testVTRehash(t, false, false, false)
}

func testVTRehash(t *testing.T, storeKey, withSlot, withBuf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	type data struct {
		k    []byte
		h, l uint64
		tm   uint64
	}
	c := 1 << 10
	m := make(map[int]*data, c)
	tm := randTimestamp()
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		d.tm = tm
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 1024)

	vt, err := newTestInnerVectorTableBuf(path, 1, []string{fmt.Sprintf("%s/vt.vti.0.0", path)},
		storeKey, withSlot, withBuf)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, uint64(m[i].tm), 0, 0, 0, 0, 0, v)
	}

	vt.kvHolder.sync()
	for i := 0; i < c; i++ {
		vo, _, _, _, _, cl, _ := vt.Get(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}

	require.NoError(t, testCloseInnerVectorTable(vt, false))
}

func TestVectorTableReloadAfterRehash(t *testing.T) {
	printCurrentFunctionName()
	testVectorTableReloadAfterRehash(t, false, true, true)
	testVectorTableReloadAfterRehash(t, true, false, true)
	testVectorTableReloadAfterRehash(t, true, true, true)
	testVectorTableReloadAfterRehash(t, false, false, true)
	testVectorTableReloadAfterRehash(t, false, true, false)
	testVectorTableReloadAfterRehash(t, true, false, false)
	testVectorTableReloadAfterRehash(t, true, true, false)
	testVectorTableReloadAfterRehash(t, false, false, false)
}

func testVectorTableReloadAfterRehash(t *testing.T, storeKey, withSlot, withBuf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	type data struct {
		k    []byte
		h, l uint64
	}
	c := 1 << 10
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 1024)
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor

	vt, err := newTestInnerVectorTableBuf(path, 1, []string{fmt.Sprintf("%s/vt.vti.0.0", path)},
		storeKey, withSlot, withBuf)
	require.NoError(t, err)
	tm := randTimestamp()
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 0, 0, 0, 0, v)
	}

	require.NoError(t, testCloseInnerVectorTable(vt, false))

	fs, err := getFiles(path)
	require.NoError(t, err)
	vt, err = newTestInnerVectorTable(path, 1, fs, storeKey, withSlot)
	require.NoError(t, err)

	for i := 0; i < c; i++ {
		vo, _, _, _, _, cl, _ := vt.Get(m[i].h, m[i].l)
		require.Equal(t, v, vo)
		if cl != nil {
			cl()
		}
	}

	require.NoError(t, testCloseInnerVectorTable(vt, true))
}

// TODO
//func TestVTRehashCo(t *testing.T) {
//	printCurrentFunctionName()
//	path, err := os.MkdirTemp(os.TempDir(), "vt")
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer os.RemoveAll(path)
//
//	type data struct {
//		k    []byte
//		h, l uint64
//		tm   uint64
//	}
//	c := 1 << 16
//	m := make(map[int]*data, c)
//	tm := randTimestamp()
//	for i := 0; i < c; i++ {
//		var d = data{
//			k: []byte("key" + strconv.Itoa(i)),
//		}
//		d.h, d.l = hash.MD5Uint64(d.k)
//		d.tm = tm
//		m[i] = &d
//	}
//	v := bytes.Repeat([]byte{'a'}, 1024)
//
//	vt, err := newTestInnerVectorTable(path, 1, []string{fmt.Sprintf("%s/vt.vti.0.0", path)}, true, false)
//	if err != nil {
//		t.Fatal(err)
//	}
//	var wg sync.WaitGroup
//	wg.Add(2)
//	f := func(s, e int) {
//		defer wg.Done()
//		for i := s; i < e; i++ {
//			_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, m[i].tm, 0, 0, 0, 0, 0, v)
//			vt.kvHolder.sync()
//		}
//		fmt.Println("set done:", s, e)
//	}
//
//	f(0, c/2)
//	f(c/2, c)
//
//	find := func() {
//		for i := 0; i < c; i++ {
//			vo, _, _, _, _, cl, er := vt.Get(m[i].h, m[i].l)
//			if er == nil {
//				if !bytes.Equal(vo, v) {
//					t.Error("value not equal")
//				}
//				if cl != nil {
//					cl()
//				}
//			} else {
//				assert.Equal(t, base.ErrNotFound, er)
//			}
//		}
//	}
//
//	go find()
//	go find()
//	go find()
//
//	wg.Wait()
//	vt.kvHolder.sync()
//	for i := 0; i < c; i++ {
//		vo, _, _, _, _, cl, er := vt.Get(m[i].h, m[i].l)
//		if er != nil {
//			t.Error(er)
//		}
//		if !bytes.Equal(vo, v) {
//			t.Error("value not equal")
//		}
//		if cl != nil {
//			cl()
//		}
//	}
//
//	vt.Close(false)
//}

func TestBenchmark(t *testing.T) {
	printCurrentFunctionName()
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	fmt.Println(path)
	type data struct {
		k    []byte
		h, l uint64
	}
	c := 1 << 20
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 1024)

	vt, err := newTestInnerVectorTable(path, uint32(c), []string{fmt.Sprintf("%s/vt.vti.0.0", path)}, true, false)
	if err != nil {
		t.Fatal(err)
	}
	tm := randTimestamp()
	ts := time.Now()
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 0, 0, 0, 0, v)
	}
	fmt.Println(time.Since(ts))

	vt.kvHolder.sync()

	ts1 := time.Now()
	for i := 0; i < c; i++ {
		vo, _, _, _, _, cl, err := vt.Get(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal key", string(m[i].k), err)
		}
	}
	fmt.Println(time.Since(ts1))

	vt.Close(false)
}

func TestInnerVectorTable_Set(t *testing.T) {
	printCurrentFunctionName()
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	vt, err := newTestInnerVectorTable(path, 1, []string{fmt.Sprintf("%s/vt.vti.0.0", path)}, true, false)
	if err != nil {
		t.Fatal(err)
	}

	k := []byte("key")
	h, l := hash.MD5Uint64(k)
	vShort := []byte("value")
	vEqual := []byte("Equal")
	vEx := []byte("ExValue")
	vContract := []byte("Ct")
	vLong := bytes.Repeat([]byte{'a'}, 1024)
	vLongB := bytes.Repeat([]byte{'b'}, 1024)

	tm := randTimestamp()

	{
		v := vShort
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, tm, 1000)
	}

	{
		v := vEqual
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, tm+200, 2000)
	}

	{
		v := vEx
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, tm+300, 3000)
	}

	{
		v := vContract
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, tm+400, 4000)
	}

	{
		v := vLong
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, tm+500, 5000)
	}

	{
		v := vLongB
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, tm+600, 6000)
	}

	{
		v := vShort
		testSetGet(t, vt, k, v, h, l, kkv.DataTypeString, tm+700, 7000)
	}

	require.NoError(t, testCloseInnerVectorTable(vt, false))
}

func TestInnerVectorTable_Checkpoint2(t *testing.T) {
	printCurrentFunctionName()
	testInnerVectorTable_Checkpoint2(t, false, true, true)
	testInnerVectorTable_Checkpoint2(t, true, false, true)
	testInnerVectorTable_Checkpoint2(t, false, false, true)
	testInnerVectorTable_Checkpoint2(t, true, true, true)
	testInnerVectorTable_Checkpoint2(t, false, true, false)
	testInnerVectorTable_Checkpoint2(t, true, false, false)
	testInnerVectorTable_Checkpoint2(t, false, false, false)
	testInnerVectorTable_Checkpoint2(t, true, true, false)
}

func testInnerVectorTable_Checkpoint2(t *testing.T, storeKey, withSlot, withBuf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	type data struct {
		k    []byte
		h, l uint64
	}
	c := 10
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 1024)

	vt, err := newTestInnerVectorTableBuf(path, 1, []string{fmt.Sprintf("%s/vt.vti.0.0", path)},
		storeKey, withSlot, withBuf)
	require.NoError(t, err)
	tm := randTimestamp()
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 0, 0, 0, 0, v)
	}

	pathCpt, err := os.MkdirTemp(os.TempDir(), "vtCpt")
	require.NoError(t, err)
	defer os.RemoveAll(pathCpt)
	require.NoError(t, vt.Checkpoint(vfs.Default, pathCpt))
	fs, err := getFiles(pathCpt)
	require.NoError(t, err)

	vt1, err := newTestInnerVectorTable(pathCpt, 1, fs, storeKey, withSlot)
	require.NoError(t, err)

	for i := 0; i < c; i++ {
		vo, _, _, _, _, cl, _ := vt1.Get(m[i].h, m[i].l)
		require.Equal(t, v, vo)
		if cl != nil {
			cl()
		}
	}

	require.NoError(t, testCloseInnerVectorTable(vt1, false))
	require.NoError(t, testCloseInnerVectorTable(vt, true))
}

func TestInnerVectorTable_Checkpoint(t *testing.T) {
	printCurrentFunctionName()
	testInnerVectorTable_Checkpoint(t, false, true, true)
	testInnerVectorTable_Checkpoint(t, true, false, true)
	testInnerVectorTable_Checkpoint(t, false, false, true)
	testInnerVectorTable_Checkpoint(t, true, true, true)
	testInnerVectorTable_Checkpoint(t, false, true, false)
	testInnerVectorTable_Checkpoint(t, true, false, false)
	testInnerVectorTable_Checkpoint(t, false, false, false)
	testInnerVectorTable_Checkpoint(t, true, true, false)
}

func testInnerVectorTable_Checkpoint(t *testing.T, storeKey, withSlot, withBuf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	type data struct {
		k    []byte
		h, l uint64
	}
	c := 2
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)

		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 10)

	vt, err := newTestVectorTableBufN(path, 1, []string{fmt.Sprintf("%s/vt.vti.0.0", path)},
		storeKey, withSlot, withBuf)
	if err != nil {
		t.Fatal(err)
	}
	tm := randTimestamp()
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 0, 0, 0, 0, v)
	}
	pathCpt, err := os.MkdirTemp(os.TempDir(), "vtCpt")
	require.NoError(t, err)
	defer os.RemoveAll(pathCpt)
	require.NoError(t, vt.Checkpoint(vfs.Default, pathCpt))
	fs, err := getFiles(pathCpt)
	require.NoError(t, err)

	vt1, err := newTestVectorTableN(pathCpt, 1, fs, storeKey, withSlot)
	require.NoError(t, err)

	for i := 0; i < c; i++ {
		vo, _, _, _, _, cl, e := vt1.GetValue(m[i].h, m[i].l)
		if e != nil {
			t.Error("get err", e)
		} else if !bytes.Equal(vo, v) {
			t.Errorf("value not equal k(%s) exp(%s) act(%s)", string(m[i].k), string(v), string(vo))
		} else if cl != nil {
			cl()
		}
	}

	require.NoError(t, testCloseVectorTable(vt1, true))
	require.NoError(t, testCloseVectorTable(vt, true))
}

func TestInnerVectorTable_Expired(t *testing.T) {
	expireManagerDurationTmp := expireManagerDuration
	expireManagerPeriodsTmp := expireManagerPeriods
	sizeExpireManagerTmp := sizeExpireManager
	expireManagerDurationMSTmp := expireManagerDurationMS

	defer func() {
		expireManagerDuration = expireManagerDurationTmp
		expireManagerPeriods = expireManagerPeriodsTmp
		sizeExpireManager = sizeExpireManagerTmp
		expireManagerDurationMS = expireManagerDurationMSTmp
	}()

	expireManagerDuration = 2                                       // per n hour
	expireManagerPeriods = uint32(1 * (24 / expireManagerDuration)) // per 2 hour
	sizeExpireManager = (expireManagerPeriods+2)<<3 + 16
	expireManagerDurationMS = expireManagerDuration * 3600 * 1000 // debug

	printCurrentFunctionName()
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	vt, err := newTestVectorTable(path, 1, []string{fmt.Sprintf("%s/vt.vti.0.0", path)}, true)
	require.NoError(t, err)

	type data struct {
		k    []byte
		h, l uint64
	}
	c := 28
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte(fmt.Sprintf("key_%4d", i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 10230) // lk+k+v = 10240

	now := vt.options.GetNowTimestamp()
	for i := 0; i < c; i++ {
		n := now + uint64(i*expireManagerDurationMS/2)
		vt.Set(m[i].k, m[i].h, m[i].l, 1, kkv.DataTypeString, n, 0, 0, 0, 0, 0, v)
	}

	for i := 0; i < c; i++ {
		n := now + uint64(i*expireManagerDurationMS/2)
		vt.GC(false, n)
	}
}

func testSetGet(t *testing.T, vt *innerVectorTable, k, v []byte, h, l uint64, dataType uint8, ttl, seq uint64) {
	vOld, seqOld, _, _, _, closer, _ := vt.Get(h, l)
	voExpected := make([]byte, len(vOld))
	copy(voExpected, vOld)
	if closer != nil {
		closer()
	}

	err := vt.Set(k, h, l, seq, dataType, ttl, 0, 0, 0, 0, 0, v)
	if err != nil {
		t.Error(err)
		return
	}
	vt.kvHolder.sync()

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

func testSetGetMeta(t *testing.T, vt *innerVectorTable, k []byte, h, l uint64, dataType uint8, ttl, seq uint64,
	slot uint16, version uint64, size uint32, pre, next uint64) {
	err := vt.Set(k, h, l, seq, dataType, ttl, slot, version, size, pre, next, nil)
	if err != nil {
		t.Error(err)
		return
	}
	vt.kvHolder.sync()

	seqNo, dt, ts, slt, ver, sz, p, n, err := vt.GetMeta(h, l)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, seq, seqNo)
	assert.Equal(t, dataType, dt)
	assert.Equal(t, ttl, ts)
	if vt.opts.WithSlot {
		assert.Equal(t, slot, slt)
	} else {
		assert.Equal(t, uint16(0), slt)
	}
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

func testDeleteGet(t *testing.T, vt *innerVectorTable, k []byte, h, l uint64, seq uint64) {
	vt.Delete(h, l, seq)
	_, _, _, _, _, _, err := vt.Get(h, l)
	if !errors.Is(err, base.ErrNotFound) {
		t.Errorf("find delete key(%s), err: %s", string(k), err)
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
