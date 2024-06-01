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

package arenaskl

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/cache/lfucache/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/manual"

	"github.com/stretchr/testify/require"
)

func TestSkl_Rebuild(t *testing.T) {
	size := 10 << 20
	arenaBuf := manual.New(size)
	ar := NewArena(arenaBuf)
	skl := NewSkiplist(ar)

	num := 1000
	for i := 0; i < num; i++ {
		key := []byte(fmt.Sprintf("testSklKey_%d", i))
		value := []byte(fmt.Sprintf("testSklValue_%d", i))
		ikey := base.MakeInternalKey(key, uint64(num), 1)
		require.NoError(t, skl.Add(ikey, value))
	}

	newSkl := CloneSkiplist(ar, skl.Height())
	it := newSkl.NewIter(nil, nil)
	for i := 0; i < num; i++ {
		key := []byte(fmt.Sprintf("testSklKey_%d", i))
		value := []byte(fmt.Sprintf("testSklValue_%d", i))
		ik, v := it.SeekGE(key)
		if !bytes.Equal(ik.UserKey, key) {
			t.Fatalf("seek key not equal key:%s ik:%s", string(key), ik.String())
		}
		if !bytes.Equal(v, value) {
			t.Fatalf("seek value not equal key:%s value:%s v:%s", string(key), string(value), string(v))
		}
	}
	require.NoError(t, it.Close())
}
