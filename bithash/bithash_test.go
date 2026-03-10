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

package bithash

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/list2"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/stretchr/testify/require"
)

const testdataDir = "test"

type testKvItem struct {
	k  []byte
	v  []byte
	fn FileNum
	sn uint64
}

func testGetBithashOpts(sz int) *options.BithashOptions {
	opts := &options.BithashOptions{
		Options: &options.Options{
			FS:              testFs,
			Logger:          base.DefaultLogger,
			Compressor:      compress.NoCompressor,
			DeleteFilePacer: options.NewDefaultDeletionFileLimiter(),
			BytesPerSync:    512 << 10,
		},
		TableMaxSize: sz,
		Index:        1,
	}
	return opts
}

func testBuildKV(num int) []testKvItem {
	var kvList []testKvItem
	for i := 0; i < num; i++ {
		kvList = append(kvList, testKvItem{
			k: []byte("bithash_testkey_" + strconv.Itoa(i)),
			v: utils.FuncRandBytes(2048)})
	}

	return kvList
}

func testOpenBithash() *Bithash {
	opts := testGetBithashOpts(1 << 20)
	_, err := os.Stat(testdataDir)
	if nil != err && !os.IsExist(err) {
		err = os.MkdirAll(testdataDir, 0775)
		if nil != err {
			panic(err)
		}
	}
	bithash, err := Open(testdataDir, opts)
	if err != nil {
		panic(err)
	}

	return bithash
}

func testOpenBithash_64MB() *Bithash {
	opts := testGetBithashOpts(64 << 20)
	_, err := os.Stat(testdataDir)
	if nil != err && !os.IsExist(err) {
		err = os.MkdirAll(testdataDir, 0775)
		if nil != err {
			panic(err)
		}
	}
	bithash, err := Open(testdataDir, opts)
	if err != nil {
		panic(err)
	}

	return bithash
}

func getSlotIndexBuf(key []byte) []byte {
	index := hash.Fnv32(key) % 1024
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, index)
	return buf
}

func testBithashClose(t *testing.T, b *Bithash) {
	require.NoError(t, b.Close())
	b.deleteFilePacer.Close()
}

func TestBithashOpen(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	b := testOpenBithash()
	require.Equal(t, 0, len(b.mufn.fnMap))
	key := []byte("key")
	ek := make([]byte, len(key)+4)
	pos := copy(ek, getSlotIndexBuf(key))
	copy(ek[pos:], key)
	khash := hash.Fnv32(ek)
	_, _, err := b.Get(ek, khash, 5)
	require.Equal(t, ErrBhFileNumZero, err)
	testBithashClose(t, b)
}

func testGetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func TestBithashCompactAndGet(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	seqNum := uint64(1)
	b := testOpenBithash()

	num := 2000
	kvList := testBuildKV(num)
	bhWriter, err := b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
		seqNum++
		if i > 1000 {
			break
		}
	}
	require.NoError(t, b.FlushFinish(bhWriter))
	bhWriter, err = b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
		seqNum++
	}
	require.NoError(t, b.FlushFinish(bhWriter))
	testBithashClose(t, b)

	b = testOpenBithash()
	bw, err1 := b.NewBithashWriter(true)
	require.NoError(t, err1)

	newFileNum := bw.GetFileNum()
	fileNums := make([]FileNum, 4)
	fileNums[0] = FileNum(1)
	fileNums[1] = FileNum(2)
	fileNums[2] = FileNum(3)
	fileNums[3] = FileNum(4)

	compactFile := func(fileNum FileNum) error {
		iter, err := b.NewTableIter(fileNum)
		if err != nil {
			return err
		}
		defer func() {
			require.NoError(t, iter.Close())
		}()
		i := 0
		for k, v, fn := iter.First(); iter.Valid(); k, v, fn = iter.Next() {
			if err = bw.AddIkey(k, v, hash.Fnv32(k.UserKey), fn); err != nil {
				return err
			}
			i++
		}
		return nil
	}

	for _, fn := range fileNums {
		err = compactFile(fn)
		require.NoError(t, err)
		b.SetFileNumMap(newFileNum, fn)
		b.RemoveTableFiles([]FileNum{fn})
	}

	require.NoError(t, bw.Finish())

	for _, item := range kvList {
		khash := hash.Fnv32(item.k)
		value, putBytePool, err := b.Get(item.k, khash, item.fn)
		require.NoError(t, err)
		if !bytes.Equal(item.v, value) {
			t.Fatalf("get fail key:%s exp:%d act:%d", string(item.k), len(item.v), len(value))
		}
		putBytePool()
	}

	testBithashClose(t, b)

	b = testOpenBithash()
	iter, err := b.NewTableIter(newFileNum)
	if err != nil {
		t.Fatal(err)
	}
	for k, _, fn := iter.First(); iter.Valid(); k, _, fn = iter.Next() {
		khash := hash.Fnv32(k.UserKey)
		v, pool, err := b.Get(k.UserKey, khash, fn)
		require.NoError(t, err)
		require.Equal(t, 2048, len(v))
		if pool != nil {
			pool()
		}
	}
	require.NoError(t, iter.Close())
	testBithashClose(t, b)
}

func TestBithashCompactIter(t *testing.T) {
	dir := testdataDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	seqNum := uint64(1)
	opts := testGetBithashOpts(512 << 20)
	require.NoError(t, os.MkdirAll(dir, 0775))
	b, err := Open(dir, opts)
	require.NoError(t, err)

	num := 102400
	kvList := testBuildKV(num)
	bhWriter, err1 := b.FlushStart()
	require.NoError(t, err1)
	for i, item := range kvList {
		kvList[i].sn = seqNum
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		require.NoError(t, err)
		seqNum++
	}
	bhWriter.compact = true
	require.NoError(t, b.FlushFinish(bhWriter))
	testBithashClose(t, b)

	b = testOpenBithash()
	fileNum := FileNum(1)
	iter, err2 := b.NewTableIter(fileNum)
	require.NoError(t, err2)

	i := 0
	for k, v, fn := iter.First(); iter.Valid(); k, v, fn = iter.Next() {
		item := kvList[i]
		require.Equal(t, item.k, k.UserKey)
		require.Equal(t, item.sn, k.SeqNum())
		require.Equal(t, item.v, v)
		require.Equal(t, fileNum, fn)
		i++
	}
	require.Equal(t, num, i)
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())

	testBithashClose(t, b)
}

func TestBithashCompactInterrupt(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	seqNum := uint64(1)
	b := testOpenBithash()

	num := 1200
	kvList := testBuildKV(num)
	bhWriter, err := b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
		seqNum++
	}
	require.NoError(t, b.FlushFinish(bhWriter))
	testBithashClose(t, b)

	b = testOpenBithash()
	bw, err := b.NewBithashWriter(true)
	require.NoError(t, err)
	newFileNum := bw.GetFileNum()
	fileNums := make([]FileNum, 2)
	fileNums[0] = FileNum(1)
	fileNums[1] = FileNum(2)
	compactFile := func(fileNum FileNum) error {
		iter, e := b.NewTableIter(fileNum)
		if e != nil {
			return e
		}
		defer func() {
			require.NoError(t, iter.Close())
		}()
		i := 0
		for k, v, fn := iter.First(); iter.Valid(); k, v, fn = iter.Next() {
			if e = bw.AddIkey(k, v, hash.Fnv32(k.UserKey), fn); e != nil {
				return e
			}
			i++
			if i == 100 {
				return nil
			}
		}
		return nil
	}

	for _, fn := range fileNums {
		err = compactFile(fn)
		if err != nil {
			t.Fatalf("compactFile fail fn:%d err:%s", fn, err)
		}
	}

	b = testOpenBithash()
	filename := MakeFilepath(b.fs, b.dirname, fileTypeTable, newFileNum)
	_, err = b.fs.Stat(filename)
	require.Equal(t, true, errors.Is(err, fs.ErrNotExist))
	_, ok := b.meta.mu.filesMeta[newFileNum]
	require.Equal(t, false, ok)
	testBithashClose(t, b)
}

func TestBithashWriterFlush(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	b := testOpenBithash()
	kvList := testBuildKV(10000)
	bhWriter, err := b.FlushStart()
	if err != nil {
		panic(err)
	}
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, 0, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		require.NoError(t, err)
	}
	require.NoError(t, b.FlushFinish(bhWriter))
	testBithashClose(t, b)
}

func TestBithashWriterConcurrencyFlush(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	var wg sync.WaitGroup
	b := testOpenBithash()
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			func() {
				gid := testGetGID()
				var kvList []testKvItem
				for i := 0; i < 1000; i++ {
					kvList = append(kvList, testKvItem{
						k: []byte(fmt.Sprintf("vip:uinfo:scancode:%d:%d", gid, i)),
						v: utils.FuncRandBytes(2048)},
					)
				}

				bhWriter, err := b.FlushStart()
				if err != nil {
					panic(err)
				}
				for i, item := range kvList {
					ik := base.MakeInternalKey(item.k, 0, InternalKeyKindSet)
					kvList[i].fn, err = bhWriter.Add(ik, item.v)
					require.NoError(t, err)
				}
				err = b.FlushFinish(bhWriter)
				if err != nil {
					panic(err)
				}

				for _, item := range kvList {
					khash := hash.Fnv32(item.k)
					value, putBytePool, err := b.Get(item.k, khash, item.fn)
					require.NoError(t, err)
					require.Equal(t, item.v, value)
					putBytePool()
				}
			}()
		}()
	}
	wg.Wait()
	testBithashClose(t, b)
}

func TestBithashFlushAndGet(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	b := testOpenBithash()
	kvList := testBuildKV(5000)
	bhWriter, err := b.FlushStart()
	if err != nil {
		panic(err)
	}
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, 0, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
	}
	err = b.FlushFinish(bhWriter)
	if err != nil {
		panic(err)
	}

	for _, item := range kvList {
		khash := hash.Fnv32(item.k)
		value, putBytePool, err := b.Get(item.k, khash, item.fn)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(value, item.v) {
			panic(fmt.Sprintf("check kv fail k=%s fm=%d", string(item.k), uint64(item.fn)))
		}
		putBytePool()
	}

	testBithashClose(t, b)
}

func TestBithashFlushCloseAndGet(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	kvList := testBuildKV(10000)
	b := testOpenBithash()
	bhWriter, err := b.FlushStart()
	if err != nil {
		panic(err)
	}
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, 0, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
	}
	if err = b.FlushFinish(bhWriter); err != nil {
		panic(err)
	}
	testBithashClose(t, b)

	b1 := testOpenBithash()
	for _, item := range kvList {
		khash := hash.Fnv32(item.k)
		value, putBytePool, err := b1.Get(item.k, khash, item.fn)
		require.NoError(t, err)
		require.Equal(t, item.v, value)
		putBytePool()
	}

	testBithashClose(t, b1)
}

func TestBithashWriteAndGet(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	seqNum := uint64(1)
	b := testOpenBithash()
	num := 2000

	kvList := testBuildKV(num)
	bhWriter, err := b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
		seqNum++
	}
	require.NoError(t, b.FlushFinish(bhWriter))

	for _, item := range kvList {
		khash := hash.Fnv32(item.k)
		value, putBytePool, err := b.Get(item.k, khash, item.fn)
		require.NoError(t, err)
		if !bytes.Equal(item.v, value) {
			t.Fatalf("key:%s\nexp:%s\nact:%s", string(item.k), string(item.v), string(value))
		}
		putBytePool()
	}

	testBithashClose(t, b)

	b = testOpenBithash()
	kvList = testBuildKV(num)
	bhWriter, err = b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
		seqNum++
	}
	err = b.FlushFinish(bhWriter)
	require.NoError(t, err)

	for _, item := range kvList {
		khash := hash.Fnv32(item.k)
		value, putBytePool, err := b.Get(item.k, khash, item.fn)
		require.NoError(t, err)
		if !bytes.Equal(item.v, value) {
			t.Fatalf("key:%s exp:%s act:%s", string(item.k), string(item.v), string(value))
		}
		putBytePool()
	}

	testBithashClose(t, b)

	b = testOpenBithash()
	testBithashClose(t, b)
}

func TestBithashWriteAndClose(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	seqNum := uint64(1)

	writeFunc := func() {
		b := testOpenBithash()
		num := 1900
		kvList := testBuildKV(num)
		bhWriter, err := b.FlushStart()
		require.NoError(t, err)
		for i, item := range kvList {
			ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
			kvList[i].fn, err = bhWriter.Add(ik, item.v)
			if err != nil {
				panic(err)
			}
			seqNum++
		}
		require.NoError(t, b.FlushFinish(bhWriter))
		testBithashClose(t, b)
		b = testOpenBithash()
		for _, item := range kvList {
			khash := hash.Fnv32(item.k)
			value, putBytePool, e := b.Get(item.k, khash, item.fn)
			if e != nil {
				t.Fatalf("get fail key:%s err:%s", string(item.k), err)
			}
			if !bytes.Equal(item.v, value) {
				t.Fatalf("get fail key:%s exp:%d act:%d", string(item.k), len(item.v), len(value))
			}
			putBytePool()
		}
		testBithashClose(t, b)
	}

	for i := 0; i < 10; i++ {
		writeFunc()
	}
}

func TestBithashKeyRepeat(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)

	seqNum := uint64(1)
	b := testOpenBithash()
	num := 500

	kvList := testBuildKV(num)
	bhWriter, err := b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
		seqNum++
	}
	err = b.FlushFinish(bhWriter)
	require.NoError(t, err)

	kvList = testBuildKV(num)
	bhWriter, err = b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		if err != nil {
			panic(err)
		}
		seqNum++
	}
	err = b.FlushFinish(bhWriter)
	require.NoError(t, err)

	for _, item := range kvList {
		khash := hash.Fnv32(item.k)
		value, putBytePool, err := b.Get(item.k, khash, item.fn)
		require.NoError(t, err)
		require.Equal(t, item.v, value)
		putBytePool()
	}

	testBithashClose(t, b)
}

func TestBithashKeyHashConflict(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)

	seqNum := uint64(1)
	b := testOpenBithash_64MB()

	conflictKeys := []string{
		"l41khazkyppk4sBj7BhdQxpfMGF2bKH9",
		"zZah6yQoo4ElihZfMVwragoejhuHaocb",
		"yBZrxusPKQdo1rKauI6rtOfs5tjbySx6",
		"l0asXWDhSamz5qncres4xJSsaUK2Jhtz",
		"wmoVNuhDBJOblKUS8wiSXNNmTjvcxrc7",
		"H7fyvszFYYZqM0NnmfmjRjPoslT1V4nu",
		"NplhsekvJnBm7gJHge5qsgJcqb68GCJu",
		"1gncjKqtxufeiqwGfdpVrJubtEabsOyl",
		"AqLMVOYwsi67FbCqHr2aivuoyKZH1eiW",
		"zSpkzpkG9xbvR4IgqNcfF24pdg351Any",
		"tk0F3dTRD8BGqaPAekliaDiZvRojoTk1",
		"dzurIotnFYUPynzW6V9DzyfdzTzs2chx",
	}

	var conflictKvList []testKvItem
	for _, k := range conflictKeys {
		conflictKvList = append(conflictKvList, testKvItem{
			k: []byte(k),
			v: utils.FuncRandBytes(2048),
		})
	}

	kvList := testBuildKV(100)
	bhWriter, err := b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		require.NoError(t, err)
		seqNum++
		if i == 50 {
			for j, v := range conflictKvList {
				ikey := base.MakeInternalKey(v.k, seqNum, InternalKeyKindSet)
				conflictKvList[j].fn, err = bhWriter.Add(ikey, v.v)
				require.NoError(t, err)
				seqNum++
			}
		}
	}

	err = b.FlushFinish(bhWriter)
	require.NoError(t, err)

	read := func() {
		for _, item := range kvList {
			value, putBytePool, err := b.Get(item.k, hash.Fnv32(item.k), item.fn)
			require.NoError(t, err)
			require.Equal(t, item.v, value)
			putBytePool()
		}
		for _, item := range conflictKvList {
			value, putBytePool, err := b.Get(item.k, hash.Fnv32(item.k), item.fn)
			require.NoError(t, err)
			require.Equal(t, item.v, value)
			putBytePool()
		}
	}

	read()

	bhWriter, err = b.FlushStart()
	require.NoError(t, err)
	bhWriter.compact = true
	err = b.FlushFinish(bhWriter)
	require.NoError(t, err)

	read()

	fm := b.meta.mu.filesMeta[FileNum(1)]
	require.Equal(t, uint32(112), fm.keyNum)
	require.Equal(t, uint32(12), fm.conflictKeyNum)
	testBithashClose(t, b)
}

func TestBithashOpenTableErrRebuild(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)

	b := testOpenBithash()
	seqNum := uint64(1)
	num := 1200
	kvList := testBuildKV(num)
	bhWriter, err := b.FlushStart()
	require.NoError(t, err)
	for i, item := range kvList {
		ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
		kvList[i].fn, err = bhWriter.Add(ik, item.v)
		require.NoError(t, err)
		seqNum++
	}
	require.NoError(t, b.FlushFinish(bhWriter))
	n, err1 := bhWriter.wr.writer.Write([]byte("panic"))
	require.NoError(t, err1)
	require.Equal(t, 5, n)
	require.NoError(t, bhWriter.wr.Flush())
	require.Equal(t, uint32(405072), bhWriter.wr.currentOffset)
	require.Equal(t, int64(405077), bhWriter.wr.fileStatSize())
	time.Sleep(2 * time.Second)
	testBithashClose(t, b)

	b = testOpenBithash()
	bhWriter, err = b.FlushStart()
	require.NoError(t, err)
	require.Equal(t, uint32(405072), bhWriter.wr.currentOffset)
	require.Equal(t, int64(405077), bhWriter.wr.fileStatSize())
	require.NoError(t, b.FlushFinish(bhWriter))
	time.Sleep(2 * time.Second)
	for _, item := range kvList {
		khash := hash.Fnv32(item.k)
		value, putBytePool, err2 := b.Get(item.k, khash, item.fn)
		require.NoError(t, err2)
		require.Equal(t, item.v, value)
		putBytePool()
	}
	testBithashClose(t, b)
}

func TestBithashMemSize(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	b := testOpenBithash_64MB()

	value := []byte("v")

	bhWriter, err := b.FlushStart()
	require.NoError(t, err)

	lastFn := FileNum(0)
	fileNum := make([]int, 200)
	totalNum := 20 << 20

	bt := time.Now()
	for i := 0; i < totalNum; i++ {
		key := []byte(fmt.Sprintf("bit_%d+hash_test%d+%d", i, i+512, i+1024))
		ik := base.MakeInternalKey(key, uint64(i), InternalKeyKindSet)
		fn, err := bhWriter.Add(ik, value)
		if err != nil {
			panic(err)
		}

		if fn != lastFn {
			fileNum[int(fn)] = i
			lastFn = fn
		}
	}
	require.NoError(t, b.FlushFinish(bhWriter))
	et := time.Since(bt)
	fmt.Printf("build index time cost = %v\n", et)

	bt = time.Now()
	for i := 0; i < totalNum; i++ {
		key := []byte(fmt.Sprintf("bit_%d+hash_test%d+%d", i, i+512, i+1024))
		for j := 1; j < len(fileNum); j++ {
			if i >= fileNum[j] && i < fileNum[j+1] || i >= fileNum[j] && fileNum[j+1] == 0 {
				khash := hash.Fnv32(key)
				v, putBytePool, err := b.Get(key, khash, FileNum(j))
				if err != nil {
					fmt.Printf("check kv fail k=%s fn=%d\n", key, j)
					break
				}
				if !bytes.Equal(value, v) {
					panic(fmt.Sprintf("check kv fail k=%s fn=%d\n", key, j))
				}
				putBytePool()
				break
			}
		}
	}
	et = time.Since(bt)
	fmt.Printf("scan index time cost = %v\n", et)

	printMemStats()
}

const MB = 1024 * 1024

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Map MEM: Alloc=%vMB; TotalAlloc=%vMB; SYS=%vMB; Mallocs=%v; Frees=%v; HeapAlloc=%vMB; HeapSys=%vMB; HeapIdle=%vMB; HeapReleased=%vMB; GCSys=%vMB; NextGC=%vMB; NumGC=%v; NumForcedGC=%v\n",
		m.Alloc/MB, m.TotalAlloc/MB, m.Sys/MB, m.Mallocs, m.Frees, m.HeapAlloc/MB, m.HeapSys/MB, m.HeapIdle/MB, m.HeapReleased/MB,
		m.GCSys/MB, m.NextGC/MB, m.NumGC, m.NumForcedGC)
}

func TestInitManifest(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	bithash := testOpenBithash()
	defer func() {
		require.NoError(t, bithash.Close())
	}()

	require.Equal(t, versionV1, bithash.meta.version)

	for fileNum, pos := range bithash.meta.mu.filesPos {
		fmt.Println("fileMeta scan: ", fileNum, pos)
		fileMeta := bithash.meta.getFileMetadata(fileNum)
		fmt.Println("fileMeta value: ", fileMeta)
	}
}

func TestBithashStats(t *testing.T) {
	defer os.RemoveAll(testdataDir)
	os.RemoveAll(testdataDir)
	seqNum := uint64(1)
	b := testOpenBithash()
	defer testBithashClose(t, b)

	num := 2000
	kvList := testBuildKV(num)

	writeFunc := func() {
		bhWriter, err := b.FlushStart()
		require.NoError(t, err)
		for i, item := range kvList {
			ik := base.MakeInternalKey(item.k, seqNum, InternalKeyKindSet)
			kvList[i].fn, err = bhWriter.Add(ik, item.v)
			if err != nil {
				panic(err)
			}
			seqNum++
		}
		err = b.FlushFinish(bhWriter)
		require.NoError(t, err)
	}

	deleteFunc := func() {
		for i, item := range kvList {
			if i%2 == 0 {
				b.Delete(item.fn)
			}
		}
	}

	writeFunc()
	deleteFunc()

	require.Equal(t, uint64(2000), b.stats.KeyTotal.Load())
	require.Equal(t, uint64(1000), b.stats.DelKeyTotal.Load())
}

func TestBithashReadFile(t *testing.T) {
	fn := FileNum(1)
	dir := ""
	if dir == "" {
		return
	}

	opts := &options.BithashOptions{
		Options: &options.Options{
			FS:         testFs,
			Logger:     base.DefaultLogger,
			Compressor: compress.SnappyCompressor,
		},
		TableMaxSize: 512 << 20,
		Index:        1,
	}

	b := &Bithash{
		dirname:      dir,
		fs:           opts.FS,
		tableMaxSize: opts.TableMaxSize,
		logger:       opts.Logger,
		compressor:   opts.Compressor,
		index:        opts.Index,
		bhtReaders:   sync.Map{},
		rwwWriters:   sync.Map{},
		stats:        &Stats{},
	}
	b.mufn.fnMap = make(map[FileNum]FileNum, 1<<10)
	b.mutw.mutableWriters = list2.NewStack()

	defer func() {
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	filename := MakeFilepath(b.fs, b.dirname, fileTypeTable, fn)
	f, err := b.fs.Open(filename)
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(b, f, FileReopenOpt{fs: b.fs, filename: filename, fileNum: fn, readOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	b.addReaders(r)
	b.mufn.fnMap[fn] = fn

	fmt.Println("dataBH", r.dataBH)
	fmt.Println("indexHashBH", r.indexHashBH)
	fmt.Println("conflictBH", r.conflictBH)
	fmt.Println("conflictBuf len", len(r.conflictBuf))
	fmt.Println("indexHash", r.indexHash.Size(), r.indexHash.Length())
}
