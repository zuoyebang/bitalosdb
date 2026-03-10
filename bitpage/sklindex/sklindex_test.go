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

package sklindex

import (
	"bytes"
	"sort"
	"testing"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/stretchr/testify/require"
)

func TestFindKeyIndexPos(t *testing.T) {
	arri := make([]base.InternalKey, 0, 1<<6)
	arri = append(arri, base.InternalKey{UserKey: []byte("abc"), Trailer: 3}) //0
	arri = append(arri, base.InternalKey{UserKey: []byte("abc"), Trailer: 2}) //1
	arri = append(arri, base.InternalKey{UserKey: []byte("abc"), Trailer: 1}) //2

	arri = append(arri, base.InternalKey{UserKey: []byte("bds"), Trailer: 3}) //3
	arri = append(arri, base.InternalKey{UserKey: []byte("bds"), Trailer: 2}) //4
	arri = append(arri, base.InternalKey{UserKey: []byte("bds"), Trailer: 1}) //5

	arri = append(arri, base.InternalKey{UserKey: []byte("cqw"), Trailer: 1}) //6

	arri = append(arri, base.InternalKey{UserKey: []byte("dgf"), Trailer: 5}) //7
	arri = append(arri, base.InternalKey{UserKey: []byte("dgf"), Trailer: 4}) //8
	arri = append(arri, base.InternalKey{UserKey: []byte("dgf"), Trailer: 3}) //9
	arri = append(arri, base.InternalKey{UserKey: []byte("dgf"), Trailer: 2}) //10
	arri = append(arri, base.InternalKey{UserKey: []byte("dgf"), Trailer: 1}) //11

	arri = append(arri, base.InternalKey{UserKey: []byte("eff"), Trailer: 1}) //12
	arri = append(arri, base.InternalKey{UserKey: []byte("fqq"), Trailer: 1}) //13

	arri = append(arri, base.InternalKey{UserKey: []byte("gtt"), Trailer: 2}) //14
	arri = append(arri, base.InternalKey{UserKey: []byte("gtt"), Trailer: 1}) //15

	pos, foundKey := findKeyIndexPos(arri, base.InternalKey{UserKey: []byte("abc"), Trailer: 4})
	require.Equal(t, 0, pos)
	require.Equal(t, true, foundKey)

	pos, foundKey = findKeyIndexPos(arri, base.InternalKey{UserKey: []byte("cddd"), Trailer: 4})
	require.Equal(t, 6, pos)
	require.Equal(t, false, foundKey)

	pos, foundKey = findKeyIndexPos(arri, base.InternalKey{UserKey: []byte("cqw"), Trailer: 1})
	require.Equal(t, 6, pos)
	require.Equal(t, true, foundKey)

	pos, foundKey = findKeyIndexPos(arri, base.InternalKey{UserKey: []byte("dgf"), Trailer: 4})
	require.Equal(t, 8, pos)
	require.Equal(t, true, foundKey)

	pos, foundKey = findKeyIndexPos(arri, base.InternalKey{UserKey: []byte("dgf"), Trailer: 5})
	require.Equal(t, 7, pos)
	require.Equal(t, true, foundKey)

	pos, foundKey = findKeyIndexPos(arri, base.InternalKey{UserKey: []byte("fqq"), Trailer: 1})
	require.Equal(t, 13, pos)
	require.Equal(t, true, foundKey)

	pos, foundKey = findKeyIndexPos(arri, base.InternalKey{UserKey: []byte("gtt"), Trailer: 3})
	require.Equal(t, 14, pos)
	require.Equal(t, true, foundKey)

	n := 3
	m := len(arri)
	pos, foundKey = findKeyIndexPos(arri[n:m], base.InternalKey{UserKey: []byte("dgf"), Trailer: 6})
	pos += n
	require.Equal(t, 7, pos)
	require.Equal(t, true, foundKey)

	n = 1
	m = len(arri)
	pos, foundKey = findKeyIndexPos(arri[n:m], base.InternalKey{UserKey: []byte("ccw"), Trailer: 6})
	pos += n
	require.Equal(t, 6, pos)
	require.Equal(t, false, foundKey)
}

func findKeyIndexPos(arri []base.InternalKey, key base.InternalKey) (int, bool) {
	arrin := len(arri)
	if arrin == 0 {
		return -1, false
	}

	var foundKey bool
	pos := sort.Search(arrin, func(i int) bool {
		r := bytes.Compare(arri[i].UserKey, key.UserKey)
		if r != 0 {
			return r != -1
		} else {
			foundKey = true
			return arri[i].Trailer <= key.Trailer
		}
	})

	return pos, foundKey
}
