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

package bitpage

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/base"

	"github.com/stretchr/testify/require"
)

const arenaSize = 1 << 20

type iterAdapter struct {
	*sklIterator
	key *internalKey
	val []byte
}

func newIterAdapter(iter *sklIterator) *iterAdapter {
	return &iterAdapter{
		sklIterator: iter,
	}
}

func (i *iterAdapter) update(key *internalKey, val []byte) bool {
	i.key = key
	i.val = val
	return i.key != nil
}

func (i *iterAdapter) String() string {
	return "iter-adapter"
}

func (i *iterAdapter) SeekGE(key []byte) bool {
	return i.update(i.sklIterator.SeekGE(key))
}

func (i *iterAdapter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) bool {
	return i.update(i.sklIterator.SeekPrefixGE(prefix, key, trySeekUsingNext))
}

func (i *iterAdapter) SeekLT(key []byte) bool {
	return i.update(i.sklIterator.SeekLT(key))
}

func (i *iterAdapter) First() bool {
	return i.update(i.sklIterator.First())
}

func (i *iterAdapter) Last() bool {
	return i.update(i.sklIterator.Last())
}

func (i *iterAdapter) Next() bool {
	return i.update(i.sklIterator.Next())
}

func (i *iterAdapter) Prev() bool {
	return i.update(i.sklIterator.Prev())
}

func (i *iterAdapter) Key() internalKey {
	return *i.key
}

func (i *iterAdapter) Value() []byte {
	return i.val
}

func (i *iterAdapter) Valid() bool {
	return i.key != nil
}

func makeIntKey(i int) internalKey {
	return internalKey{UserKey: []byte(fmt.Sprintf("%05d", i))}
}

func makeKey(s string) []byte {
	return []byte(s)
}

func makeIkey(s string) internalKey {
	return internalKey{UserKey: []byte(s)}
}

func makeInternalKey(key []byte, seqNum uint64, kind internalKeyKind) internalKey {
	return base.MakeInternalKey(key, seqNum, kind)
}

func makeValue(i int) []byte {
	return []byte(fmt.Sprintf("v%05d", i))
}

func makeinserterAdd(s *skl) func(key internalKey, value []byte) error {
	ins := &Inserter{}
	return func(key internalKey, value []byte) error {
		return ins.Add(s, key, value)
	}
}

func length(s *skl) int {
	count := 0

	it := newIterAdapter(s.NewIter(nil, nil))
	for valid := it.First(); valid; valid = it.Next() {
		count++
	}

	return count
}

func lengthRev(s *skl) int {
	count := 0

	it := newIterAdapter(s.NewIter(nil, nil))
	for valid := it.Last(); valid; valid = it.Prev() {
		count++
	}

	return count
}

func TestSkl_Empty(t *testing.T) {
	os.Remove(testPath)
	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()
	l, _ := newSkl(tbl, nil, true)
	it := newIterAdapter(l.NewIter(nil, nil))

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	key := makeKey("aaa")
	require.False(t, it.SeekGE(key))
	require.False(t, it.Valid())
}

func TestSkl_IsEmpty(t *testing.T) {
	os.Remove(testPath)
	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()
	l, _ := newSkl(tbl, nil, true)
	require.Equal(t, true, l.isEmpty())
	add := makeinserterAdd(l)
	add(base.MakeInternalKey([]byte("key1"), 1, internalKeyKindSet), makeValue(1))
	require.Equal(t, false, l.isEmpty())
}

func TestSkl_Seqnum(t *testing.T) {
	os.Remove(testPath)
	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()
	l, _ := newSkl(tbl, nil, true)
	it := newIterAdapter(l.NewIter(nil, nil))

	add := makeinserterAdd(l)

	// Try adding values.
	add(base.MakeInternalKey([]byte("key1"), 1, internalKeyKindSet), makeValue(1))
	add(base.MakeInternalKey([]byte("key12"), 3, internalKeyKindSet), makeValue(123))
	add(base.MakeInternalKey([]byte("key1"), 2, internalKeyKindSet), makeValue(2))
	add(base.MakeInternalKey([]byte("key12"), 2, internalKeyKindSet), makeValue(122))
	add(base.MakeInternalKey([]byte("key1"), 3, internalKeyKindSet), makeValue(3))
	add(base.MakeInternalKey([]byte("key12"), 1, internalKeyKindSet), makeValue(121))

	for it.First(); it.Valid(); it.Next() {
		fmt.Println(it.Key().String(), string(it.Value()))
	}
}

func TestSkl_Basic(t *testing.T) {
	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			os.Remove(testPath)
			tbl := testNewTable()
			defer func() {
				require.NoError(t, testCloseTable(tbl))
			}()
			l, _ := newSkl(tbl, nil, true)
			it := newIterAdapter(l.NewIter(nil, nil))

			add := l.Add
			if inserter {
				add = makeinserterAdd(l)
			}

			add(makeIkey("key1"), makeValue(1))
			add(makeIkey("key3"), makeValue(3))
			add(makeIkey("key2"), makeValue(2))

			require.True(t, it.SeekGE(makeKey("key")))
			require.True(t, it.Valid())
			require.NotEqual(t, "key", it.Key().UserKey)

			require.True(t, it.SeekGE(makeKey("key")))
			require.True(t, it.Valid())
			require.NotEqual(t, "key", it.Key().UserKey)

			require.True(t, it.SeekGE(makeKey("key1")))
			require.EqualValues(t, "key1", it.Key().UserKey)
			require.EqualValues(t, makeValue(1), it.Value())

			require.True(t, it.SeekGE(makeKey("key1")))
			require.EqualValues(t, "key1", it.Key().UserKey)
			require.EqualValues(t, makeValue(1), it.Value())

			require.True(t, it.SeekGE(makeKey("key2")))
			require.EqualValues(t, "key2", it.Key().UserKey)
			require.EqualValues(t, makeValue(2), it.Value())

			require.True(t, it.SeekGE(makeKey("key2")))
			require.EqualValues(t, "key2", it.Key().UserKey)
			require.EqualValues(t, makeValue(2), it.Value())

			require.True(t, it.SeekGE(makeKey("key3")))
			require.EqualValues(t, "key3", it.Key().UserKey)
			require.EqualValues(t, makeValue(3), it.Value())

			require.True(t, it.SeekGE(makeKey("key3")))
			require.EqualValues(t, "key3", it.Key().UserKey)
			require.EqualValues(t, makeValue(3), it.Value())

			key := makeIkey("a")
			key.SetSeqNum(1)
			add(key, nil)
			key.SetSeqNum(2)
			add(key, nil)

			require.True(t, it.SeekGE(makeKey("a")))
			require.True(t, it.Valid())
			require.EqualValues(t, "a", it.Key().UserKey)
			require.EqualValues(t, 2, it.Key().SeqNum())

			require.True(t, it.Next())
			require.True(t, it.Valid())
			require.EqualValues(t, "a", it.Key().UserKey)
			require.EqualValues(t, 1, it.Key().SeqNum())

			key = makeIkey("b")
			key.SetSeqNum(2)
			add(key, nil)
			//key.SetSeqNum(1)
			//add(key, nil)
			//in single refresh process, flushit is not feasible for a single kv to correspond to multiple seqnum

			require.True(t, it.SeekGE(makeKey("b")))
			require.True(t, it.Valid())
			require.EqualValues(t, "b", it.Key().UserKey)
			require.EqualValues(t, 2, it.Key().SeqNum())
		})
	}
}

func TestSkl_ConcurrentBasic(t *testing.T) {
	const n = 1000

	for _, inse := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inse), func(t *testing.T) {
			os.Remove(testPath)
			tbl := testNewTable()
			defer func() {
				require.NoError(t, testCloseTable(tbl))
			}()

			l, _ := newSkl(tbl, nil, true)
			l.testing = true

			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					if inse {
						var ins Inserter
						ins.Add(l, makeIntKey(i), makeValue(i))
					} else {
						l.Add(makeIntKey(i), makeValue(i))
					}
				}(i)
			}
			wg.Wait()

			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					it := newIterAdapter(l.NewIter(nil, nil))
					require.True(t, it.SeekGE(makeKey(fmt.Sprintf("%05d", i))))
					require.EqualValues(t, fmt.Sprintf("%05d", i), it.Key().UserKey)
				}(i)
			}
			wg.Wait()
			require.Equal(t, n, length(l))
			require.Equal(t, n, lengthRev(l))
		})
	}
}

func TestSkl_ConcurrentOneKey(t *testing.T) {
	const n = 100
	key := makeKey("thekey")
	ikey := makeIkey("thekey")

	for _, inse := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inse), func(t *testing.T) {
			os.Remove(testPath)
			tbl := testNewTable()
			defer func() {
				require.NoError(t, testCloseTable(tbl))
			}()
			l, _ := newSkl(tbl, nil, true)
			l.testing = true

			var wg sync.WaitGroup
			writeDone := make(chan struct{}, 1)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer func() {
						wg.Done()
						select {
						case writeDone <- struct{}{}:
						default:
						}
					}()

					if inse {
						var ins Inserter
						ins.Add(l, ikey, makeValue(i))
					} else {
						l.Add(ikey, makeValue(i))
					}
				}(i)
			}

			<-writeDone
			var sawValue atomic.Int32
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					it := newIterAdapter(l.NewIter(nil, nil))
					it.SeekGE(key)
					require.True(t, it.Valid())
					require.True(t, bytes.Equal(key, it.Key().UserKey))

					sawValue.Add(1)
					v, err := strconv.Atoi(string(it.Value()[1:]))
					require.NoError(t, err)
					require.True(t, 0 <= v && v < n)
				}()
			}
			wg.Wait()
			require.Equal(t, int32(n), sawValue.Load())
			require.Equal(t, 1, length(l))
			require.Equal(t, 1, lengthRev(l))
		})
	}
}

func TestSkl_Add(t *testing.T) {
	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			os.Remove(testPath)
			tbl := testNewTable()
			l, _ := newSkl(tbl, nil, true)
			it := newIterAdapter(l.NewIter(nil, nil))

			add := l.Add
			if inserter {
				add = makeinserterAdd(l)
			}

			err := add(internalKey{}, nil)
			require.Nil(t, err)
			require.True(t, it.SeekGE([]byte{}))
			require.EqualValues(t, []byte{}, it.Key().UserKey)
			require.EqualValues(t, []byte{}, it.Value())
			require.NoError(t, testCloseTable(tbl))

			tbl1 := testNewTable()
			l, _ = newSkl(tbl1, nil, true)
			it = newIterAdapter(l.NewIter(nil, nil))

			add = l.Add
			if inserter {
				add = makeinserterAdd(l)
			}

			err = add(makeIkey(""), []byte{})
			require.Nil(t, err)
			require.True(t, it.SeekGE([]byte{}))
			require.EqualValues(t, []byte{}, it.Key().UserKey)
			require.EqualValues(t, []byte{}, it.Value())

			err = add(makeIntKey(2), makeValue(2))
			require.Nil(t, err)
			require.True(t, it.SeekGE(makeKey("00002")))
			require.EqualValues(t, "00002", it.Key().UserKey)
			require.EqualValues(t, makeValue(2), it.Value())

			err = add(makeIntKey(1), makeValue(1))
			require.Nil(t, err)
			require.True(t, it.SeekGE(makeKey("00001")))
			require.EqualValues(t, "00001", it.Key().UserKey)
			require.EqualValues(t, makeValue(1), it.Value())

			err = add(makeIntKey(4), makeValue(4))
			require.Nil(t, err)
			require.True(t, it.SeekGE(makeKey("00004")))
			require.EqualValues(t, "00004", it.Key().UserKey)
			require.EqualValues(t, makeValue(4), it.Value())

			err = add(makeIntKey(3), makeValue(3))
			require.Nil(t, err)
			require.True(t, it.SeekGE(makeKey("00003")))
			require.EqualValues(t, "00003", it.Key().UserKey)
			require.EqualValues(t, makeValue(3), it.Value())

			err = add(makeIntKey(2), nil)
			require.Equal(t, ErrRecordExists, err)
			require.EqualValues(t, "00003", it.Key().UserKey)
			require.EqualValues(t, makeValue(3), it.Value())

			require.Equal(t, 5, length(l))
			require.Equal(t, 5, lengthRev(l))

			require.NoError(t, testCloseTable(tbl))
		})
	}
}

func TestSkl_ConcurrentAdd(t *testing.T) {
	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			const n = 100

			tbl := testNewTable()
			defer func() {
				require.NoError(t, testCloseTable(tbl))
			}()

			l, _ := newSkl(tbl, nil, true)
			l.testing = true

			start := make([]sync.WaitGroup, n)
			end := make([]sync.WaitGroup, n)

			for i := 0; i < n; i++ {
				start[i].Add(1)
				end[i].Add(2)
			}

			for f := 0; f < 2; f++ {
				go func(f int) {
					it := newIterAdapter(l.NewIter(nil, nil))
					add := l.Add
					if inserter {
						add = makeinserterAdd(l)
					}

					for i := 0; i < n; i++ {
						start[i].Wait()

						key := makeIntKey(i)
						if add(key, nil) == nil {
							require.True(t, it.SeekGE(key.UserKey))
							require.EqualValues(t, key, it.Key())
						}

						end[i].Done()
					}
				}(f)
			}

			for i := 0; i < n; i++ {
				start[i].Done()
				end[i].Wait()
			}

			require.Equal(t, n, length(l))
			require.Equal(t, n, lengthRev(l))
		})
	}
}

func TestSkl_IteratorNext(t *testing.T) {
	const n = 100
	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()
	l, _ := newSkl(tbl, nil, true)
	it := newIterAdapter(l.NewIter(nil, nil))

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	for i := n - 1; i >= 0; i-- {
		l.Add(makeIntKey(i), makeValue(i))
	}

	it.First()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		require.EqualValues(t, makeIntKey(i), it.Key())
		require.EqualValues(t, makeValue(i), it.Value())
		it.Next()
	}
	require.False(t, it.Valid())
}

func TestSkl_IteratorPrev(t *testing.T) {
	const n = 100
	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()
	l, _ := newSkl(tbl, nil, true)
	it := newIterAdapter(l.NewIter(nil, nil))

	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	var ins Inserter
	for i := 0; i < n; i++ {
		ins.Add(l, makeIntKey(i), makeValue(i))
	}

	it.Last()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.EqualValues(t, makeIntKey(i), it.Key())
		require.EqualValues(t, makeValue(i), it.Value())
		it.Prev()
	}
	require.False(t, it.Valid())
}

func TestSkl_IteratorSeekGEAndSeekPrefixGE(t *testing.T) {
	const n = 100
	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()
	l, _ := newSkl(tbl, nil, true)
	it := newIterAdapter(l.NewIter(nil, nil))

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())

	var ins Inserter
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		ins.Add(l, makeIntKey(v), makeValue(v))
	}

	require.True(t, it.SeekGE(makeKey("")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	require.True(t, it.SeekGE(makeKey("01000")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	require.True(t, it.SeekGE(makeKey("01005")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key().UserKey)
	require.EqualValues(t, "v01010", it.Value())

	require.True(t, it.SeekGE(makeKey("01010")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key().UserKey)
	require.EqualValues(t, "v01010", it.Value())

	require.False(t, it.SeekGE(makeKey("99999")))
	require.False(t, it.Valid())

	{
		require.True(t, it.SeekPrefixGE(makeKey("01000"), makeKey("01000"), false))
		require.True(t, it.Valid())
		require.EqualValues(t, "01000", it.Key().UserKey)
		require.EqualValues(t, "v01000", it.Value())

		require.True(t, it.SeekPrefixGE(makeKey("01000"), makeKey("01000"), true))
		require.True(t, it.Valid())
		require.EqualValues(t, "01000", it.Key().UserKey)
		require.EqualValues(t, "v01000", it.Value())

		require.True(t, it.SeekPrefixGE(makeKey("01020"), makeKey("01020"), true))
		require.True(t, it.Valid())
		require.EqualValues(t, "01020", it.Key().UserKey)
		require.EqualValues(t, "v01020", it.Value())

		require.True(t, it.SeekPrefixGE(makeKey("01200"), makeKey("01200"), true))
		require.True(t, it.Valid())
		require.EqualValues(t, "01200", it.Key().UserKey)
		require.EqualValues(t, "v01200", it.Value())

		require.True(t, it.SeekPrefixGE(makeKey("01100"), makeKey("01100"), true))
		require.True(t, it.Valid())
		require.EqualValues(t, "01200", it.Key().UserKey)
		require.EqualValues(t, "v01200", it.Value())

		require.True(t, it.SeekPrefixGE(makeKey("01100"), makeKey("01100"), false))
		require.True(t, it.Valid())
		require.EqualValues(t, "01100", it.Key().UserKey)
		require.EqualValues(t, "v01100", it.Value())
	}

	ins.Add(l, internalKey{}, nil)
	require.True(t, it.SeekGE([]byte{}))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)

	require.True(t, it.SeekGE(makeKey("")))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)
}

func TestIteratorSeekLT(t *testing.T) {
	const n = 100
	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()
	l, _ := newSkl(tbl, nil, true)
	it := newIterAdapter(l.NewIter(nil, nil))

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())

	var ins Inserter
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		ins.Add(l, makeIntKey(v), makeValue(v))
	}

	require.False(t, it.SeekLT(makeKey("")))
	require.False(t, it.Valid())

	require.False(t, it.SeekLT(makeKey("01000")))
	require.False(t, it.Valid())

	require.True(t, it.SeekLT(makeKey("01001")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	require.True(t, it.SeekLT(makeKey("01005")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	require.True(t, it.SeekLT(makeKey("01991")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key().UserKey)
	require.EqualValues(t, "v01990", it.Value())

	require.True(t, it.SeekLT(makeKey("99999")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key().UserKey)
	require.EqualValues(t, "v01990", it.Value())

	ins.Add(l, internalKey{}, nil)
	require.False(t, it.SeekLT([]byte{}))
	require.False(t, it.Valid())

	require.True(t, it.SeekLT(makeKey("\x01")))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)
}

func TestSkl_InvalidInternalKeyDecoding(t *testing.T) {
	tbl := testNewTable()
	defer func() {
		require.NoError(t, testCloseTable(tbl))
	}()

	l, _ := newSkl(tbl, nil, true)
	it := sklIterator{
		list: l,
	}

	nd, err := newRawNode(tbl, 1, 1, 1)
	require.Nil(t, err)

	it.nd = nd
	it.decodeKey()
	require.Nil(t, it.key.UserKey)
	require.Equal(t, uint64(internalKeyKindInvalid), it.key.Trailer)
}
