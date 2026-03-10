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
	"os"
	"runtime"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
	"github.com/stretchr/testify/require"

	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/stretchr/testify/assert"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
)

func printCurrentFunctionName() {
	pc, _, _, _ := runtime.Caller(1)
	f := runtime.FuncForPC(pc)
	if f != nil {
		fmt.Println("Current function name:", f.Name())
	}
}

func newTestVectorTableN(path string, hashSize uint32, openFiles []string, storeKey, withSlot bool) (*VectorTable, error) {
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	vtOpts := &options.VectorTableOptions{
		Index:         1,
		Options:       opt.BaseOptions,
		Dirname:       path,
		Filename:      "vt",
		HashSize:      hashSize,
		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
		ReleaseFunc: func(fns []string) {
			opt.BaseOptions.DeleteFilePacer.AddFiles(fns)
		},
		CheckExpireFunc: func(slotId uint16, hi, lo uint64) bool {
			return false
		},
		StoreKey: storeKey,
		WithSlot: withSlot,
	}
	if len(openFiles) > 0 {
		vtOpts.OpenFiles = openFiles
	}
	return NewVectorTable(vtOpts)
}

func newTestVectorTableBufN(path string, hashSize uint32, openFiles []string, storeKey, withSlot, buf bool) (*VectorTable, error) {
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	vtOpts := &options.VectorTableOptions{
		Index:         1,
		Options:       opt.BaseOptions,
		Dirname:       path,
		Filename:      "vt",
		HashSize:      hashSize,
		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
		ReleaseFunc: func(fns []string) {
			opt.BaseOptions.DeleteFilePacer.AddFiles(fns)
		},
		CheckExpireFunc: func(slotId uint16, hi, lo uint64) bool {
			return false
		},
		StoreKey:    storeKey,
		WithSlot:    withSlot,
		WriteBuffer: buf,
	}
	if len(openFiles) > 0 {
		vtOpts.OpenFiles = openFiles
	}
	return NewVectorTable(vtOpts)
}

func newTestVectorTableExpiredBuf(path string, hashSize uint32, openFiles []string, storeKey, withSlot, buf bool) (*VectorTable, error) {
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	vtOpts := &options.VectorTableOptions{
		Index:         1,
		Options:       opt.BaseOptions,
		Dirname:       path,
		Filename:      "vt",
		HashSize:      hashSize,
		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
		ReleaseFunc: func(fns []string) {
			opt.BaseOptions.DeleteFilePacer.AddFiles(fns)
		},
		CheckExpireFunc: func(slotId uint16, hi, lo uint64) bool {
			return true
		},
		StoreKey:    storeKey,
		WithSlot:    withSlot,
		WriteBuffer: buf,
	}
	if len(openFiles) > 0 {
		vtOpts.OpenFiles = openFiles
	}
	return NewVectorTable(vtOpts)
}

func newTestVectorTableIndexN(path string, index int, hashSize uint32, openFiles []string, storeKey, withSlot bool) (*VectorTable, error) {
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	vtOpts := &options.VectorTableOptions{
		Index:         index,
		Options:       opt.BaseOptions,
		Dirname:       path,
		Filename:      strconv.Itoa(index),
		HashSize:      hashSize,
		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
		ReleaseFunc: func(fns []string) {
			for _, fn := range fns {
				os.Remove(fn)
			}
		},
		CheckExpireFunc: func(slotId uint16, hi, lo uint64) bool {
			return true
		},
		StoreKey:    storeKey,
		WithSlot:    withSlot,
		GCThreshold: 0.2,
	}
	if len(openFiles) > 0 {
		vtOpts.OpenFiles = openFiles
	}
	return NewVectorTable(vtOpts)
}

func newTestVectorTable(path string, hashSize uint32, openFiles []string, storeKey bool) (*VectorTable, error) {
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	vtOpts := &options.VectorTableOptions{
		Index:         1,
		Options:       opt.BaseOptions,
		Dirname:       path,
		Filename:      "0",
		HashSize:      hashSize,
		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
		ReleaseFunc: func(fns []string) {
			opt.BaseOptions.DeleteFilePacer.AddFiles(fns)
		},
		StoreKey: storeKey,
	}
	if len(openFiles) > 0 {
		vtOpts.OpenFiles = openFiles
	}
	return NewVectorTable(vtOpts)
}

func newTestVectorTableNoEM(path string, hashSize uint32, openFiles []string, storeKey bool) (*VectorTable, error) {
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	vtOpts := &options.VectorTableOptions{
		Index:         1,
		Options:       opt.BaseOptions,
		Dirname:       path,
		Filename:      "vt",
		HashSize:      hashSize,
		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
		ReleaseFunc: func(fns []string) {
			opt.BaseOptions.DeleteFilePacer.AddFiles(fns)
		},
		StoreKey:            storeKey,
		DisableExpireManage: true,
	}
	if len(openFiles) > 0 {
		vtOpts.OpenFiles = openFiles
	}
	return NewVectorTable(vtOpts)
}

func newTestInnerVectorTable(path string, hashSize uint32, openFiles []string, storeKey, withSlot bool) (*innerVectorTable, error) {
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	vtOpts := &options.VectorTableOptions{
		Index:         1,
		Options:       opt.BaseOptions,
		Dirname:       path,
		Filename:      "vt",
		HashSize:      hashSize,
		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
		ReleaseFunc: func(fns []string) {
			opt.BaseOptions.DeleteFilePacer.AddFiles(fns)
		},
		StoreKey: storeKey,
		WithSlot: withSlot,
	}
	if len(openFiles) > 0 {
		vtOpts.OpenFiles = openFiles
	}
	return newInnerVectorTable(vtOpts, vtOpts.HashSize)
}

func newTestInnerVectorTableBuf(path string, hashSize uint32, openFiles []string, storeKey, withSlot, withBuf bool) (*innerVectorTable, error) {
	opt := options.InitTestOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	vtOpts := &options.VectorTableOptions{
		Index:         1,
		Options:       opt.BaseOptions,
		Dirname:       path,
		Filename:      "vt",
		HashSize:      hashSize,
		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
		ReleaseFunc: func(fns []string) {
			opt.BaseOptions.DeleteFilePacer.AddFiles(fns)
		},
		StoreKey:    storeKey,
		WithSlot:    withSlot,
		WriteBuffer: withBuf,
	}
	if len(openFiles) > 0 {
		vtOpts.OpenFiles = openFiles
	}
	return newInnerVectorTable(vtOpts, vtOpts.HashSize)
}

func testCloseInnerVectorTable(vt *innerVectorTable, isFree bool) error {
	opts := vt.opts
	if err := vt.Close(isFree); err != nil {
		return err
	}
	opts.DeleteFilePacer.Close()
	return nil
}

func testCloseVectorTable(vt *VectorTable, isFree bool) error {
	opts := vt.options
	if err := vt.Close(isFree); err != nil {
		return err
	}
	opts.DeleteFilePacer.Close()
	return nil
}

func TestVectorTable_Set(t *testing.T) {
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
	c := 1 << 10
	c2 := 2 << 10
	m := make(map[int]*data, c2)
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 1024)

	{

		vt, err := newTestVectorTableN(path, uint32(c), nil, true, true)
		if err != nil {
			t.Fatal(err)
		}
		t1 := time.Now()
		tm := randTimestamp()
		for i := 0; i < c; i++ {
			_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 0, 0, 0, 0, v)
		}
		fmt.Println(time.Since(t1))
		vt.MSync()

		for i := 0; i < c; i++ {
			testVTSetGet(t, vt, m[i].k, v, m[i].h, m[i].l, tm+2000, 20)
		}

		require.NoError(t, testCloseVectorTable(vt, true))
	}

	{

		vt, err := newTestVectorTableN(path, uint32(c), nil, true, false)
		if err != nil {
			t.Fatal(err)
		}
		t1 := time.Now()
		tm := randTimestamp()
		for i := 0; i < c; i++ {
			_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 0, 0, 0, 0, v)
		}
		fmt.Println(time.Since(t1))
		vt.MSync()

		for i := 0; i < c; i++ {
			testVTSetGet(t, vt, m[i].k, v, m[i].h, m[i].l, tm+2000, 20)
		}

		require.NoError(t, testCloseVectorTable(vt, true))
	}

	{

		vt, err := newTestVectorTableN(path, uint32(c), nil, false, true)
		if err != nil {
			t.Fatal(err)
		}
		t1 := time.Now()
		tm := randTimestamp()
		for i := 0; i < c; i++ {
			_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 0, 0, 0, 0, v)
		}
		fmt.Println(time.Since(t1))
		vt.MSync()

		for i := 0; i < c; i++ {
			testVTSetGet(t, vt, m[i].k, v, m[i].h, m[i].l, tm+2000, 20)
		}

		require.NoError(t, testCloseVectorTable(vt, true))
	}

	{

		vt, err := newTestVectorTableN(path, uint32(c), nil, false, false)
		if err != nil {
			t.Fatal(err)
		}
		t1 := time.Now()
		tm := randTimestamp()
		for i := 0; i < c; i++ {
			_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 0, 0, 0, 0, v)
		}
		fmt.Println(time.Since(t1))
		vt.MSync()

		for i := 0; i < c; i++ {
			testVTSetGet(t, vt, m[i].k, v, m[i].h, m[i].l, tm+2000, 20)
		}

		require.NoError(t, testCloseVectorTable(vt, true))
	}

}

func TestVectorTable_SetGC(t *testing.T) {
	printCurrentFunctionName()
	fs := "trace.out"
	fi, _ := os.Stat(fs)
	if fi != nil {
		os.Remove(fs)
	}
	defer os.Remove(fs)

	f, err := os.OpenFile(fs, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// 设置跟踪器
	err = trace.Start(f)
	if err != nil {
		panic(err)
	}
	defer trace.Stop()

	testVectorTable_SetGC(t, true, false, true)
	testVectorTable_SetGC(t, true, true, true)
	testVectorTable_SetGC(t, false, true, true)
	testVectorTable_SetGC(t, false, false, true)
	testVectorTable_SetGC(t, true, false, false)
	testVectorTable_SetGC(t, true, true, false)
	testVectorTable_SetGC(t, false, true, false)
	testVectorTable_SetGC(t, false, false, false)
}

func testVectorTable_SetGC(t *testing.T, storeKey bool, withSlot, buf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	c := 1 << 20
	c2 := 2 << 20
	defer os.RemoveAll(path)
	vt, err := newTestVectorTableBufN(path, uint32(c/2), nil, storeKey, withSlot, buf)
	if err != nil {
		t.Fatal(err)
	}

	type data struct {
		k    []byte
		h, l uint64
	}

	m := make(map[int]*data, c2)
	for i := 0; i < c2; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 1024)
	tm := randTimestamp() + 20000
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 100, 0, 0, 0, v)
	}

	dCount := c * 4 / 5
	for i := 0; i < dCount; i++ {
		_ = vt.Delete(m[i].h, m[i].l, 1)
	}

	vt.MSync()

	vt.stable.gcFileSize = 1 << 12
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for i := c; i < c+c/2; i++ {
			_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 100, 0, 0, 0, v)
		}
		fmt.Println("go get done")
		wg.Done()
	}()
	time.Sleep(100 * time.Nanosecond)

	go func() {
		fmt.Println("GC start")
		vt.GC(true)
		wg.Done()
	}()

	for i := c + c/2; i < c2; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 100, 0, 0, 0, v)
	}
	fmt.Println("Set Done")
	wg.Wait()
	fmt.Println("GC done")
	sz, _ := vt.Size()
	assert.Equal(t, uint32(c2-dCount), sz)
}

func TestVectorTable_GC(t *testing.T) {
	printCurrentFunctionName()

	testVectorTable_GC(t, true, false, true)
	testVectorTable_GC(t, true, true, true)
	testVectorTable_GC(t, false, true, true)
	testVectorTable_GC(t, false, false, true)
	testVectorTable_GC(t, true, false, false)
	testVectorTable_GC(t, true, true, false)
	testVectorTable_GC(t, false, true, false)
	testVectorTable_GC(t, false, false, false)
}

func testVectorTable_GC(t *testing.T, storeKey bool, withSlot, buf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	c := 1 << 20
	c2 := 2 << 20
	defer os.RemoveAll(path)
	vt, err := newTestVectorTableBufN(path, uint32(c/2), nil, storeKey, withSlot, buf)
	if err != nil {
		t.Fatal(err)
	}

	type data struct {
		k    []byte
		h, l uint64
	}

	m := make(map[int]*data, c2)
	for i := 0; i < c2; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 1024)
	tm := randTimestamp() + 20000
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 100, 0, 0, 0, v)
	}

	vt.MSync()
	for i := 0; i < c; i++ {
		vo, _, _, _, _, cl, _ := vt.GetValue(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}

	dCount := c * 4 / 5
	for i := 0; i < dCount; i++ {
		_ = vt.Delete(m[i].h, m[i].l, 1)
	}

	vt.MSync()

	vt.stable.gcFileSize = 1 << 12
	vt.GC(false)

	for i := dCount; i < c; i++ {
		vo, _, _, _, _, cl, _ := vt.GetValue(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}

	for i := 0; i < dCount; i++ {
		_ = vt.Delete(m[i].h, m[i].l, 1)
	}

	s := func(st, end int) {
		for i := st; i < end; i++ {
			err := vt.Set(m[i].k, m[i].h, m[i].l, 3, kkv.DataTypeString, tm+40000, 0, 100, 0, 0, 0, v)
			if err != nil {
				fmt.Println("set err:", i, err)
			}
		}
	}
	vt.MSync()
	//var wg sync.WaitGroup
	//wg.Add(2)
	//go func() {
	//	defer wg.Done()
	//	s(0, dCount)
	//}()
	//go func() {
	//	defer wg.Done()
	//	s(c, c2)
	//}()

	s(0, dCount)
	s(c, c2)
	vt.MSync()

	for i := dCount; i < c; i++ {
		vo, _, _, _, _, cl, err := vt.GetValue(m[i].h, m[i].l)
		if err != nil {
			t.Error("get err:", i, err)
			if cl != nil {
				cl()
			}
			continue
		}

		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal:", i, "vo", string(vo), "v:", string(v))
			vo, _, _, _, _, cl, _ := vt.GetValue(m[i].h, m[i].l)
			if cl != nil {
				cl()
			}
			fmt.Println("value:", string(vo))
		}
	}

	//wg.Wait()

	for i := 0; i < c2; i++ {
		vo, _, _, _, _, cl, err := vt.GetValue(m[i].h, m[i].l)
		if err != nil {
			t.Error("get err:", i, err)
			if cl != nil {
				cl()
			}
			break
		}

		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal:", i)
		}
	}

	require.NoError(t, testCloseVectorTable(vt, false))
}

func TestVectorTable_ExpireGCDebug(t *testing.T) {
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
	c := 28
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 10240)

	vt, err := newTestVectorTable(path, 1, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	now := vt.options.GetNowTimestamp()
	for i := 0; i < c; i++ {
		vt.Set(m[i].k, m[i].h, m[i].l, 1, kkv.DataTypeString, now+uint64(i)*3600000, 0, 0, 0, 0, 0, v)
	}

	for i := 0; i < c; i++ {
		vt.GC(false, now+uint64(i)*3600000+1)
	}

	require.NoError(t, testCloseVectorTable(vt, false))
}

func TestVectorTable_ExpireGC(t *testing.T) {
	printCurrentFunctionName()
	testVectorTable_ExpireGC(t, false, true, true)
	testVectorTable_ExpireGC(t, true, false, true)
	testVectorTable_ExpireGC(t, true, true, true)
	testVectorTable_ExpireGC(t, false, false, true)
	testVectorTable_ExpireGC(t, true, false, false)
	testVectorTable_ExpireGC(t, false, true, false)
	testVectorTable_ExpireGC(t, true, true, false)
	testVectorTable_ExpireGC(t, false, false, false)

}

func testVectorTable_ExpireGC(t *testing.T, storeKey, withSlot, buf bool) {
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
	c := 1 << 18
	m := make(map[int]*data, c)
	for i := 0; i < c; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}

	v := utils.FuncRandBytes(10240)

	vt, err := newTestVectorTableExpiredBuf(path, 1, nil, storeKey, withSlot, buf)
	if err != nil {
		t.Fatal(err)
	}

	vt.stable.kvHolder.GetVDataM().maxFileSize = 1 << 30

	now := vt.options.GetNowTimestamp()
	tm := now + 1000 // - uint64(expireManagerDurationMS)
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 100, 0, 0, 0, v)
	}

	needGC := vt.rwVT.needGC(tm + uint64(expireManagerDurationMS))
	assert.Equal(t, true, needGC)

	oldEM := vt.rwVT.kvHolder.GetEM()

	require.NoError(t, testCloseVectorTable(vt, false))

	// test load em.

	fs, err := getFiles(path)
	require.NoError(t, err)
	vt, err = newTestVectorTableExpiredBuf(path, 1, fs, storeKey, withSlot, buf)
	require.NoError(t, err)

	needGC = vt.rwVT.needGC(tm + uint64(expireManagerDurationMS))
	assert.Equal(t, true, needGC)

	// count not load.
	assert.Equal(t, oldEM.minExTime, vt.rwVT.kvHolder.GetEM().minExTime)
	assert.Equal(t, oldEM.startTime, vt.rwVT.kvHolder.GetEM().startTime)
	assert.Equal(t, len(oldEM.list), len(vt.rwVT.kvHolder.GetEM().list))
	for i := 0; i < len(oldEM.list); i++ {
		assert.Equal(t, oldEM.list[i], vt.rwVT.kvHolder.GetEM().list[i])
	}

	vt.GC(false, tm+uint64(expireManagerDurationMS))
	assert.Equal(t, false, vt.rwVT.needGC(tm+uint64(expireManagerDurationMS)))
	size, _ := vt.Size()
	assert.Equal(t, uint32(0), size)

	require.NoError(t, testCloseVectorTable(vt, false))
}

func TestVTSetGet(t *testing.T) {
	printCurrentFunctionName()
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}

	vt, err := newTestVectorTable(path, uint32(10), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	k := []byte("test")
	h, l := hash.MD5Uint64(k)
	testVTSetGet(t, vt, k, []byte("value"), h, l, randTimestamp(), 100)
	require.NoError(t, testCloseVectorTable(vt, false))
}

func TestVTReloadMerge(t *testing.T) {
	printCurrentFunctionName()
	testVTReloadMerge(t, true, false, true)
	testVTReloadMerge(t, false, true, true)
	testVTReloadMerge(t, true, true, true)
	testVTReloadMerge(t, false, false, true)
	testVTReloadMerge(t, true, false, false)
	testVTReloadMerge(t, false, true, false)
	testVTReloadMerge(t, true, true, false)
	testVTReloadMerge(t, false, false, false)
}

func testVTReloadMerge(t *testing.T, storeK, withSlot, buf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	type data struct {
		k    []byte
		h, l uint64
	}
	c := 1 << 18
	c2 := 2 << 18
	m := make(map[int]*data, c2)
	for i := 0; i < c2; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	v := bytes.Repeat([]byte{'a'}, 1024)
	vt, err := newTestVectorTableBufN(path, 1, nil, storeK, withSlot, buf)
	if err != nil {
		t.Fatal(err)
	}
	tm := randTimestamp() + 20000
	for i := 0; i < c; i++ {
		_ = vt.Set(m[i].k, m[i].h, m[i].l, 0, kkv.DataTypeString, tm, 0, 100, 0, 0, 0, v)
	}
	vt.MSync()

	for i := 0; i < c; i++ {
		vo, _, _, _, _, cl, _ := vt.GetValue(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}

	dCount := c * 2 / 3
	for i := 0; i < dCount; i++ {
		_ = vt.Delete(m[i].h, m[i].l, 1)
	}

	vt.forceGC()

	require.NoError(t, testCloseVectorTable(vt, false))

	path1, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("path1:", path1)
	defer os.RemoveAll(path1)
	vt1, err := newTestVectorTableN(path1, 1, nil, storeK, withSlot)
	if err != nil {
		t.Fatal(err)
	}
	tm = randTimestamp() + 40000
	for i := c; i < c2; i++ {
		_ = vt1.Set(m[i].k, m[i].h, m[i].l, 100, kkv.DataTypeString, tm, 0, 200, 0, 0, 0, v)
	}

	vt1.MSync()

	for i := c; i < c2; i++ {
		vo, _, _, _, _, cl, _ := vt1.GetValue(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}
	require.NoError(t, testCloseVectorTable(vt1, false))

	fs, err := getFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fs {
		nf := strings.Replace(f, path, path1, 1)
		if err := vfs.Default.Link(f, nf); err != nil {
			t.Fatal(err)
		}
	}

	fs1, err := getFiles(path1)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("1 fs1:", fs1)

	vt2, err := newTestVectorTableN(path1, 1, fs1, storeK, withSlot)
	if err != nil {
		t.Fatal(err)
	}
	vt2.MSync()

	for i := dCount; i < c; i++ {
		vo, _, _, _, _, cl, err := vt2.GetValue(m[i].h, m[i].l)
		if err != nil {
			t.Error(err)
		}
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}
	for i := c; i < c2; i++ {
		vo, _, _, _, _, cl, _ := vt2.GetValue(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal", i, len(vo))
		}
	}

	require.NoError(t, testCloseVectorTable(vt2, false))

	fs2, err := getFiles(path1)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("2 fs1:", fs2)
	vt3, err := newTestVectorTableN(path1, 1, fs2, storeK, withSlot)
	if err != nil {
		t.Fatal(err)
	}

	for i := dCount; i < c; i++ {
		vo, _, _, _, _, cl, _ := vt3.GetValue(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}
	for i := c; i < c2; i++ {
		vo, _, _, _, _, cl, _ := vt3.GetValue(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}

	require.NoError(t, testCloseVectorTable(vt3, false))
}

func TestVTReloadPanic(t *testing.T) {
	printCurrentFunctionName()
	testVTReloadPanic(t, true, false)
	testVTReloadPanic(t, false, true)
	testVTReloadPanic(t, true, true)
	testVTReloadPanic(t, false, false)
}

func testVTReloadPanic(t *testing.T, storeKey, withSlot bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	for i := 0; ; i++ {
		if err := tReload(t, i, path, storeKey, withSlot); err == nil {
			break
		}
	}
}

func tReload(t *testing.T, sign int, path string, storeKey, withSlot bool) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.New(e.(string))
		}
	}()

	vt1, err := newTestVectorTableIndexN(path, 0, 1, []string{"0.vti.0.0"}, storeKey, withSlot)

	if err != nil {
		fmt.Println(err)
	}

	key := []byte("test")
	val := bytes.Repeat([]byte("a"), 1024)
	h, l := hash.MD5Uint64(key)

	_, _, _, _, _, _, gErr := vt1.GetValue(h, l)
	fmt.Println(sign, gErr)

	vt1.Set(key, h, l, uint64(sign+1), kkv.DataTypeString, randTimestamp(), 0, 0, 0, 0, 0, val)

	if sign == 0 {
		panic("0")
	}

	if sign == 1 {
		vt1.SyncHeader()
	}

	require.NoError(t, testCloseVectorTable(vt1, false))
	return nil
}

func TestVectorTableMultiFile(t *testing.T) {
	printCurrentFunctionName()
	testVectorTableMultifile(t, false, true, true)
	testVectorTableMultifile(t, true, false, true)
	testVectorTableMultifile(t, true, true, true)
	testVectorTableMultifile(t, false, false, true)
	testVectorTableMultifile(t, true, true, false)
	testVectorTableMultifile(t, false, true, false)
	testVectorTableMultifile(t, false, false, false)
	testVectorTableMultifile(t, true, false, false)
}

func testVectorTableMultifile(t *testing.T, storeKey, withSlot, buf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	type data struct {
		k    []byte
		h, l uint64
	}
	c := 1 << 8
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

	vt, err := newTestVectorTableBufN(path, uint32(c/2), []string{fmt.Sprintf("%s/vt.vti.0.0", path)},
		storeKey, withSlot, buf)
	if err != nil {
		t.Fatal(err)
	}
	if vt.options.StoreKey {
		vt.stable.kvHolder.GetKDataM().maxFileSize = 1 << 10
	}
	vt.stable.kvHolder.GetVDataM().maxFileSize = 1 << 13
	tm := randTimestamp()
	// set get
	for i := 0; i < c; i++ {
		testVTSetGet(t, vt, m[i].k, v, m[i].h, m[i].l, tm, uint64(i+1))
	}
	// update get
	v = bytes.Repeat([]byte{'b'}, 1024)
	for i := 0; i < c; i++ {
		testVTSetGet(t, vt, m[i].k, v, m[i].h, m[i].l, tm, uint64(2*(i+1)))
	}
	metaSize := vt.stable.kvHolder.GetHeader().metaTail.GetOffset()

	for i := 0; i < c; i++ {
		testVTDeleteGet(t, vt, m[i].k, m[i].h, m[i].l, uint64(3*(i+1)))
	}

	// reuse meta
	v = bytes.Repeat([]byte{'c'}, 1024)
	for i := c - 1; i >= 0; i-- {
		testVTSetGet(t, vt, m[i].k, v, m[i].h, m[i].l, tm, uint64(4*(i+1)))
	}
	require.Equal(t, metaSize, vt.stable.kvHolder.GetHeader().metaTail.GetOffset())

	// change data type
	for i := 0; i < c; i++ {
		testVTSetGetMeta(t, vt, m[i].k, m[i].h, m[i].l, tm, kkv.DataTypeHash, uint64(5*(i+1)), 200, 1, 0, 0)
	}

	for i := 0; i < c; i++ {
		testVTSetGetMeta(t, vt, m[i].k, m[i].h, m[i].l, tm, kkv.DataTypeList, uint64(5*(i+1)), 200, 1, 2, 10)
	}

	for i := 0; i < c; i++ {
		testVTSetGetMeta(t, vt, m[i].k, m[i].h, m[i].l, tm, kkv.DataTypeBitmap, uint64(5*(i+1)), 200, 1, 2, 10)
	}

	for i := 0; i < c; i++ {
		testVTSetGetMeta(t, vt, m[i].k, m[i].h, m[i].l, tm, kkv.DataTypeDKSet, uint64(5*(i+1)), 200, 1, 2, 10)
	}

	require.NoError(t, testCloseVectorTable(vt, true))
}

func TestDebugSet(t *testing.T) {
	c := 1 << 18
	c2 := 2 << 18
	type data struct {
		k    []byte
		h, l uint64
	}
	m := make(map[int]*data, c2)
	for i := c; i < c2; i++ {
		var d = data{
			k: []byte("key" + strconv.Itoa(i)),
		}
		d.h, d.l = hash.MD5Uint64(d.k)
		m[i] = &d
	}
	path1, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	v := bytes.Repeat([]byte{'a'}, 1024)
	opt := options.InitOptionsPool()
	opt.BaseOptions.Compressor = compress.SnappyCompressor
	defer os.RemoveAll(path1)
	vt1, err := newTestVectorTable(path1, 1, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	tm := randTimestamp() + 40000
	var metaTail uint64
	for i := c; i < c2; i++ {
		metaTail = vt1.stable.kvHolder.GetHeader().metaTail.GetOffset()
		_ = vt1.Set(m[i].k, m[i].h, m[i].l, 100, kkv.DataTypeString, tm, 0, 200, 0, 0, 0, v)
		if metaTail >= vt1.stable.kvHolder.GetHeader().metaTail.GetOffset() {
			fmt.Println("=======ErrMetaTail:======", i)
		}
	}
	fmt.Println("")
	vt1.MSync()

	for i := c; i < c2; i++ {
		vo, _, _, _, _, cl, _ := vt1.GetValue(m[i].h, m[i].l)
		if cl != nil {
			cl()
		}
		if !bytes.Equal(vo, v) {
			t.Error("value not equal")
		}
	}
	require.NoError(t, testCloseVectorTable(vt1, false))
}

func TestVTSetGetMeta(t *testing.T) {
	printCurrentFunctionName()
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}

	vt, err := newTestVectorTable(path, uint32(10), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	tm := randTimestamp()
	k := []byte("test")
	h, l := hash.MD5Uint64(k)
	testVTSetGetMeta(t, vt, k, h, l, tm, kkv.DataTypeHash, 100, 200, 1, 0, 0)
	testVTSetGetMeta(t, vt, k, h, l, tm, kkv.DataTypeSet, 100, 200, 1, 0, 0)
	testVTSetGetMeta(t, vt, k, h, l, tm, kkv.DataTypeZset, 100, 200, 1, 0, 0)
	testVTSetGetMeta(t, vt, k, h, l, tm, kkv.DataTypeList, 100, 200, 1, 5, 10)
	testVTSetGetMeta(t, vt, k, h, l, tm, kkv.DataTypeBitmap, 100, 200, 1, 5, 10)
	testVTSetGetMeta(t, vt, k, h, l, tm, kkv.DataTypeDKHash, 100, 200, 1, 5, 10)
	testVTSetGetMeta(t, vt, k, h, l, tm, kkv.DataTypeDKSet, 100, 200, 1, 5, 10)
	require.NoError(t, testCloseVectorTable(vt, false))
}

func TestVectorTable_GetMeta(t *testing.T) {
	printCurrentFunctionName()
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(path)

	vt, err := newTestVectorTable(path, uint32(10), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("test")
	h, l := hash.MD5Uint64(key)
	vt.Set(key, h, l, 1, kkv.DataTypeString, randTimestamp(), 0, 0, 0, 0, 0, []byte("test"))
	vt.GetMeta(h, l)
	require.NoError(t, testCloseVectorTable(vt, false))
}

//func TestChaos(t *testing.T) {
//	printCurrentFunctionName()
//	path, err := os.MkdirTemp(os.TempDir(), "vt")
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	defer os.RemoveAll(path)
//
//	opt := options.InitOptionsPool()
//	opt.BaseOptions.Compressor = compress.SnappyCompressor
//	vt, err := NewVectorTable(&options.VectorTableOptions{
//		Options:       opt.BaseOptions,
//		Dirname:       path,
//		Filename:      "vt",
//		HashSize:      uint32(1 << 20),
//		MmapBlockSize: opt.BaseOptions.TableMmapBlockSize,
//		ReleaseFunc: func(files []string) {
//			for _, file := range files {
//				err := os.Remove(file)
//				if err != nil {
//					fmt.Println("Remove:", file, err)
//				}
//			}
//		},
//	})
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	type data struct {
//		k    []byte
//		h, l uint64
//	}
//	c := 1 << 18
//	keys := genBytesData(100, c)
//	m := make(map[int]*data, c)
//	for i := 0; i < c; i++ {
//		var d = data{
//			k: keys[i],
//		}
//		d.h, d.l = hash.MD5Uint64(d.k)
//		m[i] = &d
//	}
//
//	ch := make(chan struct{})
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			vt.GetValue(rand.Uint64(), rand.Uint64())
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			vt.GetMeta(m[i].h, m[i].l)
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			vt.GetMeta(rand.Uint64(), rand.Uint64())
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			vt.GetValue(m[i].h, m[i].l)
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			dt := uint8(rand.Intn(7)) + 1
//			if dt == kkv.DataTypeBitmap {
//				dt = kkv.DataTypeString
//			}
//			err := vt.Set(m[i].k, m[i].h, m[i].l, rand.Uint64(), dt, vt.options.GetNowTimestamp()+1, 0, rand.Uint64(), rand.Uint32(), rand.Uint64(), rand.Uint64(), keys[i])
//			if err != nil {
//				t.Errorf("Set err: %s", err.Error())
//			}
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			dt := uint8(rand.Intn(7)) + 1
//			if dt == kkv.DataTypeBitmap {
//				dt = kkv.DataTypeString
//			}
//			// expire data
//			err := vt.Set(m[i].k, m[i].h, m[i].l, rand.Uint64(), dt,
//				vt.options.GetNowTimestamp()-uint64(expireManagerDurationMS), 0, // expire
//				rand.Uint64(), rand.Uint32(), rand.Uint64(), rand.Uint64(), keys[i])
//			if err != nil {
//				t.Errorf("Set err: %s", err.Error())
//			}
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			vt.Delete(m[i].h, m[i].l, rand.Uint64())
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			dt := uint8(rand.Intn(7)) + 1
//			if dt == kkv.DataTypeBitmap {
//				dt = kkv.DataTypeString
//			}
//			vt.SetTimestamp(m[i].h, m[i].l, rand.Uint64(), vt.options.GetNowTimestamp()+1, dt)
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			vt.SetSize(m[i].h, m[i].l, rand.Uint64(), rand.Uint32())
//		}
//	}()
//
//	go func() {
//		tk := time.NewTicker(time.Minute)
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			select {
//			case <-tk.C:
//				err := vt.GC(false)
//				if err != nil {
//					t.Errorf("GC err: %s", err.Error())
//				}
//			default:
//			}
//		}
//	}()
//
//	go func() {
//		tk := time.NewTicker(30 * time.Second)
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			select {
//			case <-tk.C:
//				path_ck, err := os.MkdirTemp(os.TempDir(), "vt")
//				if err != nil {
//					t.Fatal(err)
//				}
//				fmt.Println("Checkpoint!")
//				for {
//					err = vt.Checkpoint(vt.options.FS, path_ck)
//					if err == nil {
//						break
//					}
//				}
//				fmt.Println("Checkpoint done!")
//				os.RemoveAll(path_ck)
//			default:
//			}
//		}
//	}()
//
//	go func() {
//		tk := time.NewTicker(15 * time.Second)
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			select {
//			case <-tk.C:
//				it := vt.NewIterator()
//				vt.MSync()
//				for {
//					_, _, _, _, _, _, _, _, _, _, _, _, final := it.Next()
//					if final {
//						break
//					}
//				}
//			default:
//			}
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			dt := uint8(rand.Intn(7)) + 1
//			if dt == kkv.DataTypeBitmap {
//				dt = kkv.DataTypeString
//			}
//			vt.Set(m[i].k, m[i].h, m[i].l, rand.Uint64(), dt, vt.options.GetNowTimestamp()+1, 0, rand.Uint64(), rand.Uint32(), rand.Uint64(), rand.Uint64(), keys[i])
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			i := rand.Intn(c)
//			dt := uint8(rand.Intn(7)) + 1
//			if dt == kkv.DataTypeBitmap {
//				dt = kkv.DataTypeString
//			}
//			vt.Set(m[i].k, m[i].h, m[i].l, rand.Uint64(), dt, vt.options.GetNowTimestamp()+1, 0, rand.Uint64(), rand.Uint32(), rand.Uint64(), rand.Uint64(), keys[i])
//		}
//	}()
//
//	go func() {
//		for {
//			select {
//			case <-ch:
//				return
//			default:
//			}
//			dt := uint8(rand.Intn(7)) + 1
//			if dt == kkv.DataTypeBitmap {
//				dt = kkv.DataTypeString
//			}
//			k := randomBytes(rand.Intn(100))
//			h, l := hash.MD5Uint64(k)
//
//			vt.Set(k, h, l, rand.Uint64(), dt, randTimestamp(), 0, rand.Uint64(), rand.Uint32(), rand.Uint64(), rand.Uint64(), k)
//		}
//	}()
//
//	time.Sleep(5 * time.Minute)
//	ch <- struct{}{}
//	time.Sleep(time.Second)
//}

func TestMetaOver4G(t *testing.T) {
	testMetaOver4G(t, true, false)
	testMetaOver4G(t, false, true)
	testMetaOver4G(t, false, false)
	testMetaOver4G(t, true, true)
}

func testMetaOver4G(t *testing.T, storeKey, withSlot bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	vt, err := newTestVectorTableN(path, 90000000, nil, storeKey, withSlot)
	if err != nil {
		t.Fatal(err)
	}
	idx := 0
	tm := randTimestamp()
	for {
		k := []byte(strconv.Itoa(idx))
		h, l := hash.MD5Uint64(k)
		vt.Set(k, h, l, 100, kkv.DataTypeList, tm, 0, 200, 1, 5, 10, nil)
		if len(vt.rwVT.kvHolder.GetMeta().data) > 4<<30 {
			break
		}
		idx++
	}

	for i := 0; i < 1<<10; i++ {
		k := []byte(strconv.Itoa(idx + i))
		h, l := hash.MD5Uint64(k)
		testVTSetGetMeta(t, vt, k, h, l, tm+2000, kkv.DataTypeList, 100, 200, 1, 5, 10)
	}

	idx += 1 << 10
	v := randomBytes(10)
	for j := 0; j < 1<<10; j++ {
		k := []byte(strconv.Itoa(idx + j))
		h, l := hash.MD5Uint64(k)
		testVTSetGet(t, vt, k, v, h, l, tm+2000, 20)
	}

	vt.Close(false)
}

func TestNewVectorTable(t *testing.T) {
	printCurrentFunctionName()
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(path)
	_, err = newTestVectorTable(path, 100, []string{"0_1.vtk.0", "0_1.vtv.0"}, true)
	if err == nil {
		t.Error("need return index file error")
	}
}

func testVTSetGet(t *testing.T, vt *VectorTable, k, v []byte, h, l uint64, ttl, seq uint64) {
	vOld, seqOld, _, _, _, vCloser, _ := vt.GetValue(h, l)
	voExpected := make([]byte, len(vOld))
	copy(voExpected, vOld)
	if vCloser != nil {
		vCloser()
	}

	err := vt.Set(k, h, l, seq, kkv.DataTypeString, ttl, 0, 0, 0, 0, 0, v)
	if err != nil {
		t.Error(err)
		return
	}
	vt.MSync()

	if seq <= seqOld {
		vo, seq1, _, _, _, closer, err := vt.GetValue(h, l)
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
		vo, _, _, _, _, closer, err := vt.GetValue(h, l)
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

func testVTSetGetMeta(t *testing.T, vt *VectorTable, k []byte, h, l uint64, ttl uint64, dataType uint8, seq uint64,
	version uint64, size uint32, pre, next uint64) {
	seqOld, _, _, _, verOld, sizeOld, preOld, nextOld, _ := vt.GetMeta(h, l)
	err := vt.Set(k, h, l, seq, dataType, ttl, 0, version, size, pre, next, nil)
	if err != nil {
		t.Error(err)
		return
	}

	if seq > seqOld {
		seqOld, verOld, sizeOld, preOld, nextOld = seq, version, size, pre, next
	}
	seqO, _, _, _, verO, sizeO, preO, nextO, err := vt.GetMeta(h, l)
	if err != nil {
		t.Error(err)
		return
	}

	if seqO != seqOld {
		t.Error("seq not equal")
	}

	if verO != verOld {
		t.Error("version not equal")
	}

	if sizeO != sizeOld {
		t.Error("size not equal")
	}

	if kkv.IsDataTypeList(dataType) {
		if preO != preOld {
			t.Error("pre not equal")
		}

		if nextO != nextOld {
			t.Error("next not equal")
		}
	}
}

func testVTDeleteGet(t *testing.T, vt *VectorTable, k []byte, h, l uint64, seq uint64) {
	vt.Delete(h, l, seq)
	_, _, _, _, _, _, err := vt.GetValue(h, l)
	if !errors.Is(err, base.ErrNotFound) {
		t.Errorf("find delete key(%s)", string(k))
		return
	}
}
