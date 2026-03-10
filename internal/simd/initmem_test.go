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

package simd

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// const Empty = 0b10000000

func TestInitMem128(t *testing.T) {
	// t0 := time.Now()
	batch := 16
	c := 1 << 21
	bs := make([]byte, c*batch)

	empty := [16]byte{
		0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
		0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
	}
	t0 := time.Now()
	InitMem128(unsafe.Pointer(&bs[0]), unsafe.Pointer(&empty), c)

	fmt.Println(time.Since(t0))
	i := rand.Intn(c)
	assert.Equal(t, empty[:], bs[i*batch:(i+1)*batch])
	assert.Equal(t, empty[:], bs[(c-1)*batch:c*batch])
}
