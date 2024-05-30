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

package bitalosdb

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/arenaskl"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"golang.org/x/exp/rand"
)

func (m *memTable) count() (n int) {
	x := newInternalIterAdapter(m.newIter(nil))
	for valid := x.First(); valid; valid = x.Next() {
		n++
	}
	if x.Close() != nil {
		return -1
	}
	return n
}

func (m *memTable) bytesIterated(t *testing.T) (bytesIterated uint64) {
	x := newInternalIterAdapter(m.newFlushIter(nil, &bytesIterated))
	var prevIterated uint64
	for valid := x.First(); valid; valid = x.Next() {
		if bytesIterated < prevIterated {
			t.Fatalf("bytesIterated moved backward: %d < %d", bytesIterated, prevIterated)
		}
		prevIterated = bytesIterated
	}
	if x.Close() != nil {
		return 0
	}
	return bytesIterated
}

func ikey(s string) InternalKey {
	return base.MakeInternalKey([]byte(s), 0, InternalKeyKindSet)
}

func testNewMemTable() *memTable {
	opts := &Options{}
	opts = opts.Clone().EnsureDefaults()
	return newMemTable(memTableOptions{Options: opts})
}

func TestMemTableBasic(t *testing.T) {
	m := testNewMemTable()
	if got, want := m.count(), 0; got != want {
		t.Fatalf("0.count: got %v, want %v", got, want)
	}

	getKey := func(key []byte) ([]byte, error) {
		v, exist, kind := m.get(key)
		if exist && kind == base.InternalKeyKindSet {
			return v, nil
		}
		return nil, ErrNotFound
	}

	v, err := getKey([]byte("cherry"))
	if string(v) != "" || err != ErrNotFound {
		t.Fatalf("1.get: got (%q, %v), want (%q, %v)", v, err, "", ErrNotFound)
	}
	m.set(ikey("cherry"), []byte("red"))
	m.set(ikey("peach"), []byte("yellow"))
	m.set(ikey("grape"), []byte("red"))
	m.set(ikey("grape"), []byte("green"))
	m.set(ikey("plum"), []byte("purple"))
	if got, want := m.count(), 4; got != want {
		t.Fatalf("2.count: got %v, want %v", got, want)
	}
	v, err = getKey([]byte("plum"))
	if string(v) != "purple" || err != nil {
		t.Fatalf("6.get: got (%q, %v), want (%q, %v)", v, err, "purple", error(nil))
	}
	v, err = getKey([]byte("lychee"))
	if string(v) != "" || err != ErrNotFound {
		t.Fatalf("7.get: got (%q, %v), want (%q, %v)", v, err, "", ErrNotFound)
	}
	s, x := "", newInternalIterAdapter(m.newIter(nil))
	for valid := x.SeekGE([]byte("mango")); valid; valid = x.Next() {
		s += fmt.Sprintf("%s/%s.", x.Key().UserKey, x.Value())
	}
	if want := "peach/yellow.plum/purple."; s != want {
		t.Fatalf("8.iter: got %q, want %q", s, want)
	}
	if err = x.Close(); err != nil {
		t.Fatalf("9.close: %v", err)
	}
	if err := m.set(ikey("apricot"), []byte("orange")); err != nil {
		t.Fatalf("12.set: %v", err)
	}
	if got, want := m.count(), 5; got != want {
		t.Fatalf("13.count: got %v, want %v", got, want)
	}
	if err := m.close(); err != nil {
		t.Fatalf("14.close: %v", err)
	}
}

func TestMemTableCount(t *testing.T) {
	m := testNewMemTable()
	for i := 0; i < 200; i++ {
		if j := m.count(); j != i {
			t.Fatalf("count: got %d, want %d", j, i)
		}
		m.set(InternalKey{UserKey: []byte{byte(i)}}, nil)
	}
	require.NoError(t, m.close())
}

func TestMemTableBytesIterated(t *testing.T) {
	m := testNewMemTable()
	for i := 0; i < 200; i++ {
		bytesIterated := m.bytesIterated(t)
		expected := m.inuseBytes()
		if bytesIterated != expected {
			t.Fatalf("bytesIterated: got %d, want %d", bytesIterated, expected)
		}
		m.set(InternalKey{UserKey: []byte{byte(i)}}, nil)
	}
	require.NoError(t, m.close())
}

func TestMemTableEmpty(t *testing.T) {
	m := testNewMemTable()
	if !m.empty() {
		t.Errorf("got !empty, want empty")
	}
	m.set(InternalKey{}, nil)
	if m.empty() {
		t.Errorf("got empty, want !empty")
	}
}

func TestMemTable1000Entries(t *testing.T) {
	const N = 1000
	m0 := testNewMemTable()
	for i := 0; i < N; i++ {
		k := ikey(strconv.Itoa(i))
		v := []byte(strings.Repeat("x", i))
		m0.set(k, v)
	}
	if got, want := m0.count(), 1000; got != want {
		t.Fatalf("count: got %v, want %v", got, want)
	}
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 3*N; i++ {
		j := r.Intn(N)
		k := []byte(strconv.Itoa(j))
		v, vexist, vkind := m0.get(k)
		require.Equal(t, true, vexist)
		require.Equal(t, base.InternalKeyKindSet, vkind)
		if len(v) != cap(v) {
			t.Fatalf("get: j=%d, got len(v)=%d, cap(v)=%d", j, len(v), cap(v))
		}
		var c uint8
		if len(v) != 0 {
			c = v[0]
		} else {
			c = 'x'
		}
		if len(v) != j || c != 'x' {
			t.Fatalf("get: j=%d, got len(v)=%d,c=%c, want %d,%c", j, len(v), c, j, 'x')
		}
	}
	wants := []string{
		"499",
		"5",
		"50",
		"500",
		"501",
		"502",
		"503",
		"504",
		"505",
		"506",
		"507",
	}
	x := newInternalIterAdapter(m0.newIter(nil))
	x.SeekGE([]byte(wants[0]))
	for _, want := range wants {
		if !x.Valid() {
			t.Fatalf("iter: next failed, want=%q", want)
		}
		if got := string(x.Key().UserKey); got != want {
			t.Fatalf("iter: got %q, want %q", got, want)
		}
		if k := x.Key().UserKey; len(k) != cap(k) {
			t.Fatalf("iter: len(k)=%d, cap(k)=%d", len(k), cap(k))
		}
		if v := x.Value(); len(v) != cap(v) {
			t.Fatalf("iter: len(v)=%d, cap(v)=%d", len(v), cap(v))
		}
		x.Next()
	}
	if err := x.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := m0.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func buildMemTable(b *testing.B) (*memTable, [][]byte) {
	m := testNewMemTable()
	var keys [][]byte
	var ikey InternalKey
	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		keys = append(keys, key)
		ikey = base.MakeInternalKey(key, 0, InternalKeyKindSet)
		if m.set(ikey, nil) == arenaskl.ErrArenaFull {
			break
		}
	}
	return m, keys
}

func BenchmarkMemTableIterSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := m.newIter(nil)
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter.SeekGE(keys[rng.Intn(len(keys))])
	}
}

func BenchmarkMemTableIterNext(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)
	_, _ = iter.First()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, _ := iter.Next()
		if key == nil {
			key, _ = iter.First()
		}
		_ = key
	}
}

func BenchmarkMemTableIterPrev(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)
	_, _ = iter.Last()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, _ := iter.Prev()
		if key == nil {
			key, _ = iter.Last()
		}
		_ = key
	}
}
