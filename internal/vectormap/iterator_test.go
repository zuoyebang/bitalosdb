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

package vectormap

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

func TestVM2Iterator(t *testing.T) {
	c := 1 << 10
	{
		vt := newTestInnerVectorMap(uint32(c), 100<<20)
		testVM2Iterator(t, vt, c)
		vt.Close()
	}

	{
		vt := newTestInnerVectorMapWithKey(uint32(c), 100<<20)
		testVM2Iterator(t, vt, c)
		vt.Close()
	}

}

func testVM2Iterator(t *testing.T, vt *InnerVectorMap, c int) {
	type info struct {
		k, v   []byte
		h      uint64
		l      uint64
		typ    uint8
		seqNum uint64
		ts     uint64
		ver    uint64
		size   uint32
		pre    uint64
		next   uint64
	}

	var m = make(map[int]*info, c)

	slot := uint16(rand.Uint32())

	for i := 0; i < c; i++ {
		vi := int(rand.Uint32())
		bs := []byte(strconv.Itoa(vi))
		h, l := hash.MD5Uint64(bs)
		var typ uint8
		if i%3 == 0 {
			typ = kkv.DataTypeString
		} else if i%3 == 1 {
			typ = kkv.DataTypeHash
		} else {
			typ = kkv.DataTypeList
		}
		m[vi] = &info{
			k:      bs,
			v:      bs,
			h:      h,
			l:      l,
			typ:    typ,
			seqNum: uint64(i + 1),
			ts:     randTimestamp(),
			ver:    uint64(i + 1),
			size:   uint32(vi),
			pre:    uint64(i),
			next:   uint64(i + 1),
		}
		if typ == kkv.DataTypeString {
			testSetGet(t, vt, m[vi].k, m[vi].v, m[vi].h, m[vi].l, slot, m[vi].typ, m[vi].ts, m[vi].seqNum)
		} else {
			testSetGetMetaWithKey(t, vt, m[vi].k, m[vi].h, m[vi].l, m[vi].seqNum, m[vi].typ, m[vi].ts, slot, m[vi].ver, m[vi].size, m[vi].pre, m[vi].next, nil)
		}
	}

	it := vt.NewIterator(nil)
	for {
		k, h, l, seqNum, dataType, ts, ver, slotId, size, pre, next, v, f := it.Next()
		if f {
			break
		}

		var vi int
		if dataType == kkv.DataTypeString {
			vi, _ = strconv.Atoi(string(v))
		} else {
			vi = int(size)
		}

		assert.Equal(t, m[vi].h, h)
		assert.Equal(t, m[vi].l, l)
		assert.Equal(t, m[vi].seqNum, seqNum)
		assert.Equal(t, m[vi].typ, dataType)
		assert.Equal(t, m[vi].ts, ts)
		assert.Equal(t, slot, slotId)
		switch dataType {
		case kkv.DataTypeString:
			if vt.storeKey {
				assert.Equal(t, m[vi].k, k)
			}
			assert.Equal(t, m[vi].v, v)
		case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
			assert.Equal(t, m[vi].ver, ver)
			assert.Equal(t, m[vi].size, size)
		case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
			assert.Equal(t, m[vi].ver, ver)
			assert.Equal(t, m[vi].size, size)
			assert.Equal(t, m[vi].pre, pre)
			assert.Equal(t, m[vi].next, next)
		}
		delete(m, vi)
	}
	assert.Equal(t, 0, len(m))
}
