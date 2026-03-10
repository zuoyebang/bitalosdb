// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashindex

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
)

func TestVectorIndex(t *testing.T) {
	path, _ := os.MkdirTemp(os.TempDir(), "vi")

	filePath := path + "/vi.bin"

	tests := []Case{
		{name: "test1", fnv: 1, off: 0},
		// fnv conflict
		{name: "test2", fnv: 2, off: 5},
		{name: "test3", fnv: 2, off: 10},
	}

	v, err := NewVectorIndex(&VectorIndexOptions{
		FilePath: filePath,
		HashSize: 16,
	})
	if err != nil {
		t.Fatal(err)
	}

	var expected = make(map[uint32]uint32, len(tests))

	for _, tt := range tests {
		expected[tt.fnv] = tt.off
		v.Add(tt.fnv, tt.off)
		off, err := v.Get(tt.fnv)
		assert.NoError(t, err)
		assert.Equal(t, tt.off, off)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := v.Get(tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, expected[tt.fnv], v)
		})
	}

	v.Close()

	v, err = NewVectorIndex(&VectorIndexOptions{
		FilePath: filePath,
		HashSize: 16,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := v.Get(tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, expected[tt.fnv], v)
		})
	}
}

type Case struct {
	name string
	fnv  uint32
	off  uint32
}

func TestVectorIndexData(t *testing.T) {
	tests := buildTestCase(100000)

	path, _ := os.MkdirTemp(os.TempDir(), "vi")
	filePath := path + "/vi.bin"

	v, err := NewVectorIndex(&VectorIndexOptions{
		FilePath: filePath,
		HashSize: 100000,
	})
	if err != nil {
		t.Fatal(err)
	}

	var expected = make(map[uint32]uint32, len(tests))

	for _, tt := range tests {
		expected[tt.fnv] = tt.off
		v.Add(tt.fnv, tt.off)
		off, err := v.Get(tt.fnv)
		assert.NoError(t, err)
		assert.Equal(t, tt.off, off)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := v.Get(tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, expected[tt.fnv], v)
		})
	}
	v.Close()
}

func TestVectorIndexLess(t *testing.T) {
	n := 15000
	tests := buildTestCase(n)

	path, _ := os.MkdirTemp(os.TempDir(), "vi")
	filePath := path + "/vi.bin"
	defer os.RemoveAll(path)
	var hashSz uint32 = 5000

	v, err := NewVectorIndex(&VectorIndexOptions{
		FilePath: filePath,
		HashSize: hashSz,
	})
	if err != nil {
		t.Fatal(err)
	}

	var expected = make(map[uint32]uint32, len(tests))

	for i := 0; i < n/3*2; i++ {
		tt := tests[i]
		expected[tt.fnv] = tt.off
		v.Add(tt.fnv, tt.off)
		off, err := v.Get(tt.fnv)
		assert.NoError(t, err)
		assert.Equal(t, expected[tt.fnv], off)
	}

	for i := 0; i < n/3*2; i++ {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			v, err := v.Get(tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, expected[tt.fnv], v)
		})
	}
	v.Close()

	// test reload
	v, err = NewVectorIndex(&VectorIndexOptions{
		FilePath: filePath,
		HashSize: 50,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n/3*2; i++ {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			v, err := v.Get(tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, expected[tt.fnv], v)
		})
	}

	for i := n / 3 * 2; i < n; i++ {
		tt := tests[i]
		expected[tt.fnv] = tt.off
		v.Add(tt.fnv, tt.off)
		off, err := v.Get(tt.fnv)
		assert.NoError(t, err)
		assert.Equal(t, expected[tt.fnv], off)
	}

	for i := n / 3 * 2; i < n; i++ {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			v, err := v.Get(tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, expected[tt.fnv], v)
		})
	}
	v.Close()

}

func buildTestCase(n int) []Case {
	cases := make([]Case, n)
	for i := 0; i < n; i++ {
		s := strconv.Itoa(i)
		cases[i].name = s
		cases[i].fnv = hash.Crc32([]byte(s))
		cases[i].off = uint32(i * 5)
	}
	return cases
}
