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

	"github.com/stretchr/testify/require"
)

func TestBatchSetDelete(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	keyIndex := 0
	val := testRandBytes(100)

	for i := 0; i < 10; i++ {
		b := db.NewBatch()
		for j := 0; j < 100; j++ {
			key := makeTestIntKey(keyIndex)
			require.NoError(t, b.Set(key, val, nil))
			if keyIndex%10 == 0 {
				require.NoError(t, b.Delete(key, nil))
			}
			keyIndex++
		}
		require.NoError(t, b.Commit(NoSync))
		require.NoError(t, b.Close())
	}

	for i := 0; i < 1000; i++ {
		key := makeTestIntKey(i)
		if i%10 == 0 {
			require.NoError(t, verifyGetNotFound(db, key))
		} else {
			require.NoError(t, verifyGet(db, key, val))
		}
	}
}

func TestBatchDelete(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	keyIndex := 0
	val := testRandBytes(100)

	for i := 0; i < 10; i++ {
		b := db.NewBatch()
		for j := 0; j < 100; j++ {
			key := makeTestIntKey(keyIndex)
			require.NoError(t, b.Set(key, val, nil))
			keyIndex++
		}
		require.NoError(t, b.Commit(NoSync))
		require.NoError(t, b.Close())
	}

	for i := 0; i < 1000; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, verifyGet(db, key, val))
	}

	keyIndex = 0
	for i := 0; i < 10; i++ {
		b := db.NewBatch()
		for j := 0; j < 10; j++ {
			key := makeTestIntKey(keyIndex)
			require.NoError(t, b.Delete(key, nil))
			keyIndex++
		}
		require.NoError(t, b.Commit(NoSync))
		require.NoError(t, b.Close())
	}

	for i := 0; i < 1000; i++ {
		key := makeTestIntKey(i)
		if i < 100 {
			require.NoError(t, verifyGetNotFound(db, key))
		} else {
			require.NoError(t, verifyGet(db, key, val))
		}
	}
}

func TestBatchSetMultiValue(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	var val []byte
	keyIndex := 0
	val1 := testRandBytes(100)
	val2 := testRandBytes(10)
	val3 := testRandBytes(10)
	val = append(val, val1...)
	val = append(val, val2...)
	val = append(val, val3...)

	for i := 0; i < 10; i++ {
		b := db.NewBatch()
		for j := 0; j < 100; j++ {
			key := makeTestIntKey(keyIndex)
			require.NoError(t, b.SetMultiValue(key, val1, val2, val3))
			keyIndex++
		}
		require.NoError(t, b.Commit(NoSync))
		require.NoError(t, b.Close())
	}

	for i := 0; i < 1000; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, verifyGet(db, key, val))
	}
}

func TestBatchSetPrefixDeleteKey(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	defer func() {
		require.NoError(t, db.Close())
	}()

	b := db.NewBatch()
	for i := 0; i < 10; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, b.PrefixDeleteKeySet(key, nil))
	}
	require.NoError(t, b.Commit(NoSync))
	require.NoError(t, b.Close())

	for i := 0; i < 10; i++ {
		key := makeTestIntKey(i)
		require.NoError(t, verifyGet(db, key, []byte("")))
	}
}

func TestBatchCommitEmpty(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	db := openTestDB(testDirname, nil)
	defer db.Close()

	b := db.NewBatch()
	require.NoError(t, b.Commit(NoSync))
	require.NoError(t, b.Close())
}

func TestBatchBitower(t *testing.T) {
	type testCase struct {
		kind       InternalKeyKind
		key, value string
	}

	verifyTestCases := func(b *BatchBitower, testCases []testCase) {
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
	var b BatchBitower
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

func TestBatchBitowerEmpty(t *testing.T) {
	var b BatchBitower
	require.True(t, b.Empty())

	b.Set(nil, nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = BatchBitower{}

	b.Delete(nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = BatchBitower{}

	b.LogData(nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = BatchBitower{}

	_ = b.Reader()
	require.True(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
	b = BatchBitower{}

	require.Equal(t, uint64(0), b.SeqNum())
	require.True(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
}

func TestBatchBitowerCommitEmpty(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	db := openTestDB(testDirname, nil)
	defer db.Close()

	b := db.NewBatchBitower()
	require.NoError(t, b.Commit(nil))
	require.NoError(t, b.Close())
}

func TestBatchBitowerReset(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)
	db := openTestDB(testDirname, nil)
	defer db.Close()
	key := makeTestSlotKey([]byte("test-key"))
	value := []byte("test-value")
	b := db.NewBatchBitower()
	b.Set(key, value, nil)
	b.Delete(key, nil)
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
	var expected BatchBitower
	expected.SetRepr(b.data)
	expected.db = db
	b.Set(key, value, nil)
	require.NoError(t, db.ApplyBitower(b, nil))
	require.NoError(t, verifyGet(db, key, value))
}

func TestBatchBitowerIncrement(t *testing.T) {
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
		var b BatchBitower
		b.SetRepr(buf[:])
		b.count++
		got := binary.LittleEndian.Uint32(b.Repr()[8:12])
		want := tc + 1
		if got != want {
			t.Errorf("input=%d: got %d, want %d", tc, got, want)
		}
		_, count := ReadBatchBitower(b.Repr())
		if got != want {
			t.Errorf("input=%d: got %d, want %d", tc, count, want)
		}
	}
}

func TestBatchBitowerMemTableSizeOverflow(t *testing.T) {
	dir := testDirname
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	db := openTestDB(testDirname, nil)
	bigValue := make([]byte, 1000)
	b := db.NewBatchBitower()

	b.memTableSize = math.MaxUint32 - 50
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%05d", i)
		require.NoError(t, b.Set([]byte(k), bigValue, nil))
	}
	require.Greater(t, b.memTableSize, uint64(math.MaxUint32))
	require.NoError(t, b.Close())
	require.NoError(t, db.Close())
}

func BenchmarkBatchBitowerSet(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newBatchBitower(nil)

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

func BenchmarkBatchBitowerSetDeferred(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newBatchBitower(nil)

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
