// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arenaskl

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func newArena(n uint32) *Arena {
	return NewArena(make([]byte, n))
}

func TestArenaSizeOverflow(t *testing.T) {
	a := newArena(math.MaxUint32)

	offset, _, err := a.alloc(math.MaxUint16, 0, 0)
	require.Nil(t, err)
	require.Equal(t, uint32(1), offset)
	require.Equal(t, uint32(math.MaxUint16)+1, a.Size())

	_, _, err = a.alloc(math.MaxUint32, 0, 0)
	require.Equal(t, ErrArenaFull, err)
	require.Equal(t, uint32(math.MaxUint32), a.Size())

	_, _, err = a.alloc(math.MaxUint16, 0, 0)
	require.Equal(t, ErrArenaFull, err)
	require.Equal(t, uint32(math.MaxUint32), a.Size())
}
