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

package vectorindex64

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"

	"github.com/stretchr/testify/assert"
)

type Case struct {
	name  string
	key   []byte
	fnv   uint64
	off32 uint32
	off64 uint64
	v96   []byte
	v128  []byte
}

func TestVectorIndex32(t *testing.T) {
	tests := buildTestCase(300)

	v := NewVectorIndexV32(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	for _, tt := range tests {
		v.Set(tt.key, tt.off32)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v = NewVectorIndexV32(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})
	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := v.Get(tt.key)
			assert.NoError(t, err)
			assert.Equal(t, tt.off32, val)
		})
	}
}

func TestVectorIndex32Update(t *testing.T) {
	tests := buildTestCase(1)

	v := NewVectorIndexV32(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	for _, tt := range tests {
		v.Set(tt.key, tt.off32)
	}

	v.Set(tests[0].key, 1000)

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v = NewVectorIndexV32(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})
	v.SetReader(b)

	val, err := v.Get(tests[0].key)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1000), val)
}

func TestVectorIndexV64(t *testing.T) {
	tests := buildTestCase64(300)

	v := NewVectorIndexV64(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	for _, tt := range tests {
		v.Set(tt.key, tt.off64)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v = NewVectorIndexV64(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})
	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := v.Get(tt.key)
			assert.NoError(t, err)
			assert.Equal(t, tt.off64, val)
		})
	}
}

func TestVectorIndexV96(t *testing.T) {
	tests := buildTestCase96(300)

	v := NewVectorIndexV96(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	var v96 [12]byte
	for _, tt := range tests {
		copy(v96[:], tt.v96[0:12])
		v.Set(tt.key, v96)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v = NewVectorIndexV96(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})
	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := v.Get(tt.key)
			assert.NoError(t, err)
			assert.Equal(t, tt.v96, val)
		})
	}
}

func TestVectorIndexV96Update(t *testing.T) {
	tests := buildTestCase96(1)

	v := NewVectorIndexV96(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	var v96 [12]byte
	for _, tt := range tests {
		copy(v96[:], tt.v96[0:12])
		v.Set(tt.key, v96)
	}

	v.Set(tests[0].key, [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v = NewVectorIndexV96(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})
	v.SetReader(b)

	val, err := v.Get(tests[0].key)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, val)
}

func TestVectorIndexV128(t *testing.T) {
	tests := buildTestCase128(300)

	v := NewVectorIndexV128(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	var v128 [16]byte
	for _, tt := range tests {
		copy(v128[:], tt.v128[0:16])
		v.Set(tt.key, v128)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v = NewVectorIndexV128(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})
	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := v.Get(tt.key)
			assert.NoError(t, err)
			assert.Equal(t, tt.v128, val)
		})
	}
}

func TestVectorIndexV128Update(t *testing.T) {
	tests := buildTestCase128(1)

	v := NewVectorIndexV128(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	var v128 [16]byte
	for _, tt := range tests {
		copy(v128[:], tt.v128[0:16])
		v.Set(tt.key, v128)
	}

	v.Set(tests[0].key, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v = NewVectorIndexV128(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})
	v.SetReader(b)

	val, err := v.Get(tests[0].key)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, val)
}

func TestVectorIndexData(t *testing.T) {
	tests := buildTestCase(10000)

	v := NewVectorIndexV32(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	for _, tt := range tests {
		v.Set(tt.key, tt.off32)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			off, err := v.Get(tt.key)
			assert.NoError(t, err)
			assert.Equal(t, tt.off32, off)
		})
	}
}

func TestVectorIndexData64(t *testing.T) {
	tests := buildTestCase64(10000)

	v := NewVectorIndexV64(&VectorIndexOptions{
		Logger: base.DefaultLogger,
	})

	for _, tt := range tests {
		v.Set(tt.key, tt.off64)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			off, err := v.Get(tt.key)
			assert.NoError(t, err)
			assert.Equal(t, tt.off64, off)
		})
	}
}

func buildTestCase(n int) []Case {
	cases := make([]Case, n)
	for i := 0; i < n; i++ {
		k := make([]byte, 8)
		s := strconv.Itoa(i)
		binary.LittleEndian.PutUint64(k, uint64(i))
		cases[i].name = s
		cases[i].key = k
		cases[i].off32 = uint32(i * 5)
	}
	return cases
}

func buildTestCase64(n int) []Case {
	cases := make([]Case, n)
	for i := 0; i < n; i++ {
		k := make([]byte, 8)
		s := strconv.Itoa(i)
		binary.LittleEndian.PutUint64(k, uint64(i))
		cases[i].name = s
		cases[i].key = k
		cases[i].off64 = uint64(i * 5)
	}
	return cases
}

func buildTestCase96(n int) []Case {
	cases := make([]Case, n)
	for i := 0; i < n; i++ {
		k := make([]byte, 8)
		s := strconv.Itoa(i)
		binary.LittleEndian.PutUint64(k, uint64(i))
		cases[i].name = s
		cases[i].key = k
		cases[i].v96 = make([]byte, 12)
		copy(cases[i].v96[0:8], k)
		copy(cases[i].v96[8:12], k)
	}
	return cases
}

func buildTestCase128(n int) []Case {
	cases := make([]Case, n)
	for i := 0; i < n; i++ {
		k := make([]byte, 8)
		s := strconv.Itoa(i)
		binary.LittleEndian.PutUint64(k, uint64(i))
		cases[i].name = s
		cases[i].key = k
		cases[i].v128 = make([]byte, 16)
		copy(cases[i].v128[0:8], k)
		copy(cases[i].v128[8:16], k)
	}
	return cases
}
