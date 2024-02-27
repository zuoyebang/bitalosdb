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

package bitalosdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/batchskl"
	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/stretchr/testify/require"
)

func TestBatch(t *testing.T) {
	type testCase struct {
		kind       InternalKeyKind
		key, value string
	}

	verifyTestCases := func(b *Batch, testCases []testCase) {
		r := b.Reader()

		for _, tc := range testCases {
			kind, k, v, ok := r.Next()
			if !ok {
				t.Fatalf("next returned !ok: test case = %v", tc)
			}
			key, value := string(k), string(v)
			if kind != tc.kind || key != tc.key || value != tc.value {
				t.Errorf("got (%d, %q, %q), want (%d, %q, %q)",
					kind, key, value, tc.kind, tc.key, tc.value)
			}
		}
		if len(r) != 0 {
			t.Errorf("reader was not exhausted: remaining bytes = %q", r)
		}
	}

	testCases := []testCase{
		{InternalKeyKindSet, "roses", "red"},
		{InternalKeyKindSet, "violets", "blue"},
		{InternalKeyKindDelete, "roses", ""},
		{InternalKeyKindSet, "", ""},
		{InternalKeyKindSet, "", "non-empty"},
		{InternalKeyKindDelete, "", ""},
		{InternalKeyKindSet, "grass", "green"},
		{InternalKeyKindSet, "grass", "greener"},
		{InternalKeyKindSet, "eleventy", strings.Repeat("!!11!", 100)},
		{InternalKeyKindDelete, "nosuchkey", ""},
		{InternalKeyKindSet, "binarydata", "\x00"},
		{InternalKeyKindSet, "binarydata", "\xff"},
		{InternalKeyKindLogData, "logdata", ""},
		{InternalKeyKindLogData, "", ""},
	}
	var b Batch
	for _, tc := range testCases {
		switch tc.kind {
		case InternalKeyKindSet:
			_ = b.Set([]byte(tc.key), []byte(tc.value), nil)
		case InternalKeyKindDelete:
			_ = b.Delete([]byte(tc.key), nil)
		case InternalKeyKindLogData:
			_ = b.LogData([]byte(tc.key), nil)
		}
	}
	verifyTestCases(&b, testCases)

	b.Reset()
	for _, tc := range testCases {
		key := []byte(tc.key)
		value := []byte(tc.value)
		switch tc.kind {
		case InternalKeyKindSet:
			d := b.SetDeferred(len(key), len(value))
			copy(d.Key, key)
			copy(d.Value, value)
			d.Finish()
		case InternalKeyKindDelete:
			d := b.DeleteDeferred(len(key))
			copy(d.Key, key)
			copy(d.Value, value)
			d.Finish()
		case InternalKeyKindLogData:
			_ = b.LogData([]byte(tc.key), nil)
		}
	}
	verifyTestCases(&b, testCases)
}

func TestBatchEmpty(t *testing.T) {
	var b Batch
	require.True(t, b.Empty())

	b.Set(nil, nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = Batch{}

	b.Delete(nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = Batch{}

	b.LogData(nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = Batch{}

	_ = b.Reader()
	require.True(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = Batch{}

	require.Equal(t, uint64(0), b.SeqNum())
	require.True(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = Batch{}

	d, err := Open("./data", &Options{})
	require.NoError(t, err)
	defer d.Close()
	ib := newIndexedBatch(d, DefaultComparer)
	iter := ib.NewIter(nil)
	require.False(t, iter.First())
	iter2, err := iter.Clone()
	require.NoError(t, err)
	require.NoError(t, iter.Close())
	_, err = iter.Clone()
	require.True(t, err != nil)
	require.False(t, iter2.First())
	require.NoError(t, iter2.Close())
}

func TestBatchReset(t *testing.T) {
	dir := "./data"
	defer os.RemoveAll(dir)
	db, err := Open(dir, &Options{
		FS: vfs.Default,
	})
	require.NoError(t, err)
	defer db.Close()
	key := "test-key"
	value := "test-value"
	b := db.NewBatch()
	b.Set([]byte(key), []byte(value), nil)
	b.Delete([]byte(key), nil)
	b.setSeqNum(100)
	b.applied = 1
	b.commitErr = errors.New("test-error")
	b.commit.Add(1)
	require.Equal(t, uint32(2), b.Count())
	require.True(t, len(b.data) > 0)
	require.True(t, b.SeqNum() > 0)
	require.True(t, b.memTableSize > 0)
	require.NotEqual(t, b.deferredOp, DeferredBatchOp{})

	b.Reset()
	require.Equal(t, db, b.db)
	require.Equal(t, uint32(0), b.applied)
	require.Nil(t, b.commitErr)
	require.Equal(t, uint32(0), b.Count())
	require.Equal(t, batchHeaderLen, len(b.data))
	require.Equal(t, uint64(0), b.SeqNum())
	require.Equal(t, uint64(0), b.memTableSize)
	require.Equal(t, b.deferredOp, DeferredBatchOp{})

	var expected Batch
	expected.SetRepr(b.data)
	expected.db = db
	require.Equal(t, &expected, b)

	b.Set([]byte(key), []byte(value), nil)
	require.NoError(t, db.Apply(b, nil))
	v, closer, err := db.Get([]byte(key))
	require.NoError(t, err)
	defer closer()
	require.Equal(t, v, []byte(value))
}

func TestIndexedBatchReset(t *testing.T) {
	indexCount := func(sl *batchskl.Skiplist) int {
		count := 0
		iter := sl.NewIter(nil, nil)
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}
		return count
	}

	dir := "./data"
	defer os.RemoveAll(dir)
	db, err := Open(dir, &Options{})
	require.NoError(t, err)
	defer db.Close()
	b := newIndexedBatch(db, DefaultComparer)
	key := "test-key"
	value := "test-value"
	b.Set([]byte(key), []byte(value), nil)
	require.NotNil(t, b.index)
	require.Equal(t, 1, indexCount(b.index))

	b.Reset()
	require.NotNil(t, b.cmp)
	require.NotNil(t, b.formatKey)
	require.NotNil(t, b.abbreviatedKey)
	require.NotNil(t, b.index)

	count := func(ib *Batch) int {
		iter := ib.NewIter(nil)
		defer iter.Close()
		iter2, err := iter.Clone()
		require.NoError(t, err)
		defer iter2.Close()
		var count [2]int
		for i, it := range []*Iterator{iter, iter2} {
			for it.First(); it.Valid(); it.Next() {
				count[i]++
			}
		}
		require.Equal(t, count[0], count[1])
		return count[0]
	}
	contains := func(ib *Batch, key, value string) bool {
		iter := ib.NewIter(nil)
		defer iter.Close()
		iter2, err := iter.Clone()
		require.NoError(t, err)
		defer iter2.Close()
		var found [2]bool
		for i, it := range []*Iterator{iter, iter2} {
			for it.First(); it.Valid(); it.Next() {
				if string(it.Key()) == key &&
					string(it.Value()) == value {
					found[i] = true
				}
			}
		}
		require.Equal(t, found[0], found[1])
		return found[0]
	}

	b.Set([]byte(key), []byte(value), nil)
	require.Equal(t, 1, indexCount(b.index))
	require.Equal(t, 1, count(b))
	require.True(t, contains(b, key, value))
}

func TestFlushableBatchReset(t *testing.T) {
	var b Batch
	b.flushable = newFlushableBatch(&b, DefaultComparer)

	b.Reset()
	require.Nil(t, b.flushable)
}

func TestBatchIncrement(t *testing.T) {
	testCases := []uint32{
		0x00000000,
		0x00000001,
		0x00000002,
		0x0000007f,
		0x00000080,
		0x000000fe,
		0x000000ff,
		0x00000100,
		0x00000101,
		0x000001ff,
		0x00000200,
		0x00000fff,
		0x00001234,
		0x0000fffe,
		0x0000ffff,
		0x00010000,
		0x00010001,
		0x000100fe,
		0x000100ff,
		0x00020100,
		0x03fffffe,
		0x03ffffff,
		0x04000000,
		0x04000001,
		0x7fffffff,
		0xfffffffe,
	}
	for _, tc := range testCases {
		var buf [batchHeaderLen]byte
		binary.LittleEndian.PutUint32(buf[8:12], tc)
		var b Batch
		b.SetRepr(buf[:])
		b.count++
		got := binary.LittleEndian.Uint32(b.Repr()[8:12])
		want := tc + 1
		if got != want {
			t.Errorf("input=%d: got %d, want %d", tc, got, want)
		}
		_, count := ReadBatch(b.Repr())
		if got != want {
			t.Errorf("input=%d: got %d, want %d", tc, count, want)
		}
	}

	err := func() (err error) {
		defer func() {
			if v := recover(); v != nil {
				if verr, ok := v.(error); ok {
					err = verr
				}
			}
		}()
		var buf [batchHeaderLen]byte
		binary.LittleEndian.PutUint32(buf[8:12], 0xffffffff)
		var b Batch
		b.SetRepr(buf[:])
		b.count++
		b.Repr()
		return nil
	}()
	if err != ErrInvalidBatch {
		t.Fatalf("expected %v, but found %v", ErrInvalidBatch, err)
	}
}

func TestBatchOpDoesIncrement(t *testing.T) {
	var b Batch
	key := []byte("foo")
	value := []byte("bar")

	if b.Count() != 0 {
		t.Fatalf("new batch has a nonzero count: %d", b.Count())
	}

	_ = b.Set(key, value, nil)
	if b.Count() != 1 {
		t.Fatalf("expected count: %d, got %d", 1, b.Count())
	}

	var b2 Batch
	_ = b2.Set(key, value, nil)
	_ = b2.Delete(key, nil)
	if b2.Count() != 2 {
		t.Fatalf("expected count: %d, got %d", 2, b2.Count())
	}

	_ = b.Apply(&b2, nil)
	if b.Count() != 3 {
		t.Fatalf("expected count: %d, got %d", 3, b.Count())
	}

	_ = b.LogData([]byte("foobarbaz"), nil)
	if b.Count() != 3 {
		t.Fatalf("expected count: %d, got %d", 3, b.Count())
	}
}

func TestFlushableBatchBytesIterated(t *testing.T) {
	batch := newBatch(nil)
	for j := 0; j < 1000; j++ {
		key := make([]byte, 8+j%3)
		value := make([]byte, 7+j%5)
		batch.Set(key, value, nil)

		fb := newFlushableBatch(batch, DefaultComparer)

		var bytesIterated uint64
		it := fb.newFlushIter(nil, &bytesIterated)

		var prevIterated uint64
		for key, _ := it.First(); key != nil; key, _ = it.Next() {
			if bytesIterated < prevIterated {
				t.Fatalf("bytesIterated moved backward: %d < %d", bytesIterated, prevIterated)
			}
			prevIterated = bytesIterated
		}

		expected := fb.inuseBytes()
		if bytesIterated != expected {
			t.Fatalf("bytesIterated: got %d, want %d", bytesIterated, expected)
		}
	}
}

func TestEmptyFlushableBatch(t *testing.T) {
	fb := newFlushableBatch(newBatch(nil), DefaultComparer)
	it := newInternalIterAdapter(fb.newIter(nil))
	require.False(t, it.First())
}

func BenchmarkBatchSet(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newBatch(nil)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			batch.Set(key, value, nil)
		}
		batch.Reset()
	}

	b.StopTimer()
}

func BenchmarkIndexedBatchSet(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newIndexedBatch(nil, DefaultComparer)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			batch.Set(key, value, nil)
		}
		batch.Reset()
	}

	b.StopTimer()
}

func BenchmarkBatchSetDeferred(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newBatch(nil)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			deferredOp := batch.SetDeferred(len(key), len(value))

			copy(deferredOp.Key, key)
			copy(deferredOp.Value, value)

			deferredOp.Finish()
		}
		batch.Reset()
	}

	b.StopTimer()
}

func BenchmarkIndexedBatchSetDeferred(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newIndexedBatch(nil, DefaultComparer)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			deferredOp := batch.SetDeferred(len(key), len(value))

			copy(deferredOp.Key, key)
			copy(deferredOp.Value, value)

			deferredOp.Finish()
		}
		batch.Reset()
	}

	b.StopTimer()
}

func TestBatchMemTableSizeOverflow(t *testing.T) {
	opts := &Options{
		FS: vfs.Default,
	}
	opts.EnsureDefaults()
	dir := "./data"
	defer os.RemoveAll(dir)
	d, err := Open(dir, opts)
	require.NoError(t, err)

	bigValue := make([]byte, 1000)
	b := d.NewBatch()

	b.memTableSize = math.MaxUint32 - 50
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%05d", i)
		require.NoError(t, b.Set([]byte(k), bigValue, nil))
	}
	require.Greater(t, b.memTableSize, uint64(math.MaxUint32))
	require.NoError(t, b.Close())
	require.NoError(t, d.Close())
}
