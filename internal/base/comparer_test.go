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

package base

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"golang.org/x/exp/rand"
)

func TestDefAppendSeparator(t *testing.T) {
	testCases := []struct {
		a, b, want string
	}{
		{"black", "blue", "blb"},
		{"green", "", "green"},
		{"", "2", ""},
		{"1", "2", "1"},
		{"1", "29", "2"},
		{"13", "19", "14"},
		{"13", "99", "2"},
		{"135", "19", "14"},
		{"1357", "19", "14"},
		{"1357", "2", "14"},
		{"13\xff", "14", "13\xff"},
		{"13\xff", "19", "14"},
		{"1\xff\xff", "19", "1\xff\xff"},
		{"1\xff\xff", "2", "1\xff\xff"},
		{"1\xff\xff", "9", "2"},
		{"", "", ""},
		{"1", "", "1"},
		{"11", "", "11"},
		{"11\xff", "", "11\xff"},
		{"1\xff", "", "1\xff"},
		{"1\xff\xff", "", "1\xff\xff"},
		{"\xff", "", "\xff"},
		{"\xff\xff", "", "\xff\xff"},
		{"\xff\xff\xff", "", "\xff\xff\xff"},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			got := string(DefaultComparer.Separator(nil, []byte(tc.a), []byte(tc.b)))
			if got != tc.want {
				t.Errorf("a, b = %q, %q: got %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestAbbreviatedKey(t *testing.T) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	randBytes := func(size int) []byte {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.Int() & 0xff)
		}
		return data
	}

	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = randBytes(rng.Intn(16))
	}
	sort.Slice(keys, func(i, j int) bool {
		return DefaultComparer.Compare(keys[i], keys[j]) < 0
	})

	for i := 1; i < len(keys); i++ {
		last := DefaultComparer.AbbreviatedKey(keys[i-1])
		cur := DefaultComparer.AbbreviatedKey(keys[i])
		cmp := DefaultComparer.Compare(keys[i-1], keys[i])
		if cmp == 0 {
			if last != cur {
				t.Fatalf("expected equal abbreviated keys: %x[%x] != %x[%x]",
					last, keys[i-1], cur, keys[i])
			}
		} else {
			if last > cur {
				t.Fatalf("unexpected abbreviated key ordering: %x[%x] > %x[%x]",
					last, keys[i-1], cur, keys[i])
			}
		}
	}
}

func BenchmarkAbbreviatedKey(b *testing.B) {
	rng := rand.New(rand.NewSource(1449168817))
	randBytes := func(size int) []byte {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.Int() & 0xff)
		}
		return data
	}
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = randBytes(8)
	}

	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		j := i % len(keys)
		sum += DefaultComparer.AbbreviatedKey(keys[j])
	}

	if testing.Verbose() {
		fmt.Println(sum)
	}
}
