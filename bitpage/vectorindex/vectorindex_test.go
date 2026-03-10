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

package vectorindex

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
)

func TestVectorIndex(t *testing.T) {
	tests := buildConflict2TestCase(10)

	v := NewVectorIndex(&VectorIndexOptions{
		CompareKeyFunc: func(key []byte, off uint32) (v []byte, c int) {
			i := off / 5
			if off < tests[i].off {
				return nil, -1
			} else if off > tests[i].off {
				return nil, 1
			}
			return tests[i].key, bytes.Compare(key, tests[i].key)
		},
		CompareVerFunc: func(key []byte, off uint32) (c int) {
			i := off / 5
			if off < tests[i].off {
				return -1
			} else if off > tests[i].off {
				return 1
			}
			return bytes.Compare(key, tests[i].key)
		},
	})

	for _, tt := range tests {
		v.Add(tt.fnv, tt.off)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := v.GetValue(tt.key, tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, tt.key, val)
			off, err := v.GetOffset(tt.key, tt.fnv)
			assert.Equal(t, tt.off, off)
		})
	}
}

func TestVectorIndex_Conflict(t *testing.T) {
	tests := buildConflict10TestCase(30)

	v := NewVectorIndex(&VectorIndexOptions{
		CompareKeyFunc: func(key []byte, off uint32) (v []byte, c int) {
			i := off / 5
			if off < tests[i].off {
				return nil, -1
			} else if off > tests[i].off {
				return nil, 1
			}
			return tests[i].key, bytes.Compare(key, tests[i].key)
		},
		CompareVerFunc: func(key []byte, off uint32) (c int) {
			i := off / 5
			if off < tests[i].off {
				return -1
			} else if off > tests[i].off {
				return 1
			}
			return bytes.Compare(key, tests[i].key)
		},
	})

	for _, tt := range tests {
		v.Add(tt.fnv, tt.off)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := v.GetValue(tt.key, tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, tt.key, val)
			off, err := v.GetOffset(tt.key, tt.fnv)
			assert.Equal(t, tt.off, off)
		})
	}
}

type Case struct {
	name string
	key  []byte
	fnv  uint32
	off  uint32
}

func TestVectorIndexData(t *testing.T) {
	tests := buildTestCase(10000)

	v := NewVectorIndex(&VectorIndexOptions{
		CompareKeyFunc: func(key []byte, off uint32) (v []byte, c int) {
			i := off / 5
			if off < tests[i].off {
				return nil, -1
			} else if off > tests[i].off {
				return nil, 1
			}
			return tests[off/5].key, bytes.Compare(key, tests[i].key)
		},
		CompareVerFunc: func(key []byte, off uint32) (c int) {
			i := off / 5
			if off < tests[i].off {
				return -1
			} else if off > tests[i].off {
				return 1
			}
			return bytes.Compare(key, tests[i].key)
		},
	})

	for _, tt := range tests {
		v.Add(tt.fnv, tt.off)
	}

	var buf bytes.Buffer

	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := v.GetValue(tt.key, tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, tt.key, val)
			off, err := v.GetOffset(tt.key, tt.fnv)
			assert.Equal(t, tt.off, off)
		})
	}
}

func buildConflict2TestCase(n int) []Case {
	cases := make([]Case, n)
	for i := 0; i < n; i++ {
		s := fmt.Sprintf("test%06d", i)
		vi := strconv.Itoa(i % (n / 2))
		cases[i].name = s
		cases[i].key = []byte(s)
		cases[i].fnv = hash.Fnv32([]byte(vi))
		cases[i].off = uint32(i * 5)
	}
	return cases
}

func buildConflict10TestCase(n int) []Case {
	cases := make([]Case, n)
	for i := 0; i < n; i++ {
		s := fmt.Sprintf("test%06d", i)
		vi := strconv.Itoa(i % (n / 10))
		cases[i].name = s
		cases[i].key = []byte(s)
		cases[i].fnv = hash.Fnv32([]byte(vi))
		cases[i].off = uint32(i * 5)
	}
	return cases
}

func buildTestCase(n int) []Case {
	cases := make([]Case, n)
	for i := 0; i < n; i++ {
		s := strconv.Itoa(i)
		cases[i].name = s
		cases[i].key = []byte(s)
		cases[i].fnv = hash.Fnv32(cases[i].key)
		cases[i].off = uint32(i * 5)
	}
	return cases
}

func TestVectorIndexEdgeCases(t *testing.T) {
	tests := []Case{
		{
			name: "empty_key",
			key:  []byte{},
			fnv:  hash.Fnv32([]byte{}),
			off:  0,
		},
		{
			name: "max_offset",
			key:  []byte("max_offset"),
			fnv:  hash.Fnv32([]byte("max_offset")),
			off:  0x7fffffff,
		},
		{
			name: "special_chars",
			key:  []byte("!@#$%^&*()"),
			fnv:  hash.Fnv32([]byte("!@#$%^&*()")),
			off:  100,
		},
		{
			name: "unicode_chars",
			key:  []byte("你好世界"),
			fnv:  hash.Fnv32([]byte("你好世界")),
			off:  200,
		},
	}

	// 创建一个映射来存储offset到测试用例的映射关系
	offsetMap := make(map[uint32]Case)
	for _, tt := range tests {
		offsetMap[tt.off] = tt
	}

	v := NewVectorIndex(&VectorIndexOptions{
		CompareKeyFunc: func(key []byte, off uint32) (v []byte, c int) {
			tt, exists := offsetMap[off]
			if !exists {
				return nil, 1
			}
			return tt.key, bytes.Compare(key, tt.key)
		},
		CompareVerFunc: func(key []byte, off uint32) (c int) {
			tt, exists := offsetMap[off]
			if !exists {
				return 1
			}
			return bytes.Compare(key, tt.key)
		},
	})

	for _, tt := range tests {
		v.Add(tt.fnv, tt.off)
	}

	var buf bytes.Buffer
	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v.SetReader(b)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := v.GetValue(tt.key, tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, tt.key, val)
			off, err := v.GetOffset(tt.key, tt.fnv)
			assert.NoError(t, err)
			assert.Equal(t, tt.off, off)
		})
	}
}

func TestVectorIndexNotFound(t *testing.T) {
	tests := []Case{
		{
			name: "normal_key",
			key:  []byte("test_key"),
			fnv:  hash.Fnv32([]byte("test_key")),
			off:  100,
		},
	}

	v := NewVectorIndex(&VectorIndexOptions{
		CompareKeyFunc: func(key []byte, off uint32) (v []byte, c int) {
			return tests[0].key, bytes.Compare(key, tests[0].key)
		},
		CompareVerFunc: func(key []byte, off uint32) (c int) {
			return bytes.Compare(key, tests[0].key)
		},
	})

	for _, tt := range tests {
		v.Add(tt.fnv, tt.off)
	}

	var buf bytes.Buffer
	n, err := v.Serialize(&buf)
	assert.NoError(t, err)
	b := buf.Bytes()
	assert.Equal(t, n, len(b))

	v.SetReader(b)

	// 测试不存在的key
	nonExistKey := []byte("non_exist_key")
	nonExistHash := hash.Fnv32(nonExistKey)
	val, err := v.GetValue(nonExistKey, nonExistHash)
	assert.Error(t, err)
	assert.Equal(t, base.ErrNotFound, err)
	assert.Nil(t, val)

	off, err := v.GetOffset(nonExistKey, nonExistHash)
	assert.Error(t, err)
	assert.Equal(t, base.ErrNotFound, err)
	assert.Equal(t, uint32(0), off)
}
