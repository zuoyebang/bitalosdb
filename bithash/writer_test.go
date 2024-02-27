// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/unsafe2"
)

func TestFixedWidthWrite(t *testing.T) {
	f0, err := testMemFs.Create("filename")
	if err != nil {
		panic(err)
	}
	for _, indexBH := range []BlockHandle{{4, 6}, {18, 3}, {9999, 8888}, {19923892, 2378237}, {6, 7}, {2893824823, 239924829}, {88, 35}} {
		buf2 := make([]byte, 20)
		encodeBlockHandle(buf2, indexBH)
		n, err := f0.Write(buf2)
		if err != nil {
			panic(err)
		}
		if n != 20 {
			fmt.Println("no equal")
		}
	}
	if err = f0.Close(); err != nil {
		panic(err)
	}

	f1, err := testMemFs.Open("filename")
	if err != nil {
		panic(err)
	}
	off := 0
	for i := 0; i < 7; i++ {
		buff := make([]byte, 20)
		_, err = f1.ReadAt(buff, int64(off))
		if err != nil {
			panic(err)
		}
		bbh := decodeBlockHandle(buff)
		fmt.Printf("index:%d,blockhandle:%+v", i, bbh)
		off += 20
	}
	if err = f1.Close(); err != nil {
		panic(err)
	}
}

func TestBlockFile(t *testing.T) {
	f0, err := testMemFs.Create("filename")
	if err != nil {
		panic(err)
	}
	makeIkey := func(s string) InternalKey {
		j := strings.Index(s, ":")
		seq, err := strconv.Atoi(s[j+1:])
		if err != nil {
			panic(err)
		}
		return base.MakeInternalKey([]byte(s[:j]), uint64(seq), InternalKeyKindSet)
	}
	var block []byte
	w := &blockWriter{}
	for k, e := range strings.Split(strings.TrimSpace("a:1,b:2,c:3,d:4"), ",") {
		w.add(makeIkey(e), []byte(fmt.Sprintf("hello"+strconv.Itoa(k+100))))
	}
	block = w.finish()
	n, err := f0.Write(block)
	if err != nil {
		panic(err)
	}
	if err := f0.Close(); err != nil {
		panic(err)
	}
	f1, err := testMemFs.Open("filename")
	if err != nil {
		panic(err)
	}
	buff := make([]byte, n)
	_, err = f1.ReadAt(buff, 0)
	if err != nil {
		panic(err)
	}
	iter, err := newBlockIter(bytes.Compare, buff)
	if err != nil {
		panic(err)
	}
	fi, fv := iter.First()
	fmt.Println("first:", fi, string(fv))
	si, sv := iter.Next()
	fmt.Println("second:", si, string(sv))
}

func TestWriteReadBlock(t *testing.T) {
	makeIkey := func(s string) InternalKey {
		j := strings.Index(s, ":")
		seq, err := strconv.Atoi(s[j+1:])
		if err != nil {
			panic(err)
		}
		return base.MakeInternalKey([]byte(s[:j]), uint64(seq), InternalKeyKindSet)
	}
	w := &blockWriter{}
	for k, e := range strings.Split(strings.TrimSpace("a:1,b:2,c:3,d:4"), ",") {
		w.add(makeIkey(e), []byte(fmt.Sprintf("hello"+strconv.Itoa(k+100))))
	}
	iter, err := newBlockIter(bytes.Compare, w.finish())
	if err != nil {
		panic(err)
	}
	fi, fv := iter.First()
	fmt.Println("first:", fi, string(fv))
	si, sv := iter.Next()
	fmt.Println("second:", si, string(sv))
}

func TestByteReuse(t *testing.T) {
	buf := make([]byte, 10)
	for _, indexBH := range []BlockHandle{{4, 6}, {18, 3}, {9999, 8888}, {19923892, 2378237}, {6, 7}, {2893824823, 239924829}, {88, 35}} {
		buf2 := make([]byte, 10)
		encodeBlockHandle(buf, indexBH)
		encodeBlockHandle(buf2, indexBH)
		if !bytes.Equal(buf, buf2) {
			fmt.Printf("buf1:%+v\nbuf2:%+v \n", buf, buf2)
		}
	}
	fmt.Println("done")
}

func TestWriterUpdateIndex(t *testing.T) {
	w := &Writer{
		indexHash:    make(map[uint32]*hashHandle, 1<<18),
		indexArray:   make([]hashHandle, 1<<18),
		conflictKeys: make(map[string]BlockHandle, 10),
	}

	key1 := []byte("key1")
	w.updateHash(base.MakeInternalKey(key1, 1, InternalKeyKindSet), 100, BlockHandle{1, 1})
	key2 := []byte("key2")
	w.updateHash(base.MakeInternalKey(key2, 1, InternalKeyKindSet), 200, BlockHandle{2, 2})
	require.Equal(t, "key1", string(w.indexHash[100].userKey))
	require.Equal(t, "key2", string(w.indexHash[200].userKey))
	require.Equal(t, uint32(1), w.indexHash[100].bh.Offset)
	require.Equal(t, uint32(1), w.indexHash[100].bh.Length)
	require.Equal(t, uint32(2), w.indexHash[200].bh.Offset)
	require.Equal(t, uint32(2), w.indexHash[200].bh.Length)
	require.Equal(t, 2, len(w.indexHash))
	require.Equal(t, 0, len(w.conflictKeys))

	w.updateHash(base.MakeInternalKey(key1, 2, InternalKeyKindSet), 100, BlockHandle{3, 3})
	require.Equal(t, uint32(3), w.indexHash[100].bh.Offset)
	require.Equal(t, uint32(3), w.indexHash[100].bh.Length)
	require.Equal(t, false, w.indexHash[100].conflict)

	key11 := []byte("key11")
	w.updateHash(base.MakeInternalKey(key11, 3, InternalKeyKindSet), 100, BlockHandle{4, 4})
	require.Equal(t, true, w.indexHash[100].conflict)
	require.Equal(t, 2, len(w.conflictKeys))
	require.Equal(t, uint32(3), w.conflictKeys[unsafe2.String(key1)].Offset)
	require.Equal(t, uint32(3), w.conflictKeys[unsafe2.String(key1)].Length)
	require.Equal(t, uint32(4), w.conflictKeys[unsafe2.String(key11)].Offset)
	require.Equal(t, uint32(4), w.conflictKeys[unsafe2.String(key11)].Length)

	key111 := []byte("key111")
	w.updateHash(base.MakeInternalKey(key111, 4, InternalKeyKindSet), 100, BlockHandle{5, 5})
	w.updateHash(base.MakeInternalKey(key1, 5, InternalKeyKindSet), 100, BlockHandle{6, 6})
	require.Equal(t, true, w.indexHash[100].conflict)
	require.Equal(t, 3, len(w.conflictKeys))
	require.Equal(t, uint32(6), w.conflictKeys[unsafe2.String(key1)].Offset)
	require.Equal(t, uint32(6), w.conflictKeys[unsafe2.String(key1)].Length)
	require.Equal(t, uint32(4), w.conflictKeys[unsafe2.String(key11)].Offset)
	require.Equal(t, uint32(4), w.conflictKeys[unsafe2.String(key11)].Length)
	require.Equal(t, uint32(5), w.conflictKeys[unsafe2.String(key111)].Offset)
	require.Equal(t, uint32(5), w.conflictKeys[unsafe2.String(key111)].Length)
}
