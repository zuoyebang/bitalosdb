// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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

package bitpage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/hash"
)

func TestSkl_Arrat_Merging(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	var ikey internalKey

	valbuf := []byte("val4")
	addval, closer := base.EncodeInternalValue(valbuf, 1, internalKeyKindSetBithash)
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	type wtest struct {
		key  int
		seq  uint64
		kind internalKeyKind
	}

	type rtest struct {
		key   int
		seq   uint64
		exist bool
	}

	writeData := func(bp *Bitpage, pn PageNum, cases []wtest) {
		wr := bp.GetPageWriter(pn, nil)
		if wr == nil {
			t.Fatal("NewWriter fail")
		}

		for i := 0; i < len(cases); i++ {
			ikey = makeInternalKey(makeTestKey(cases[i].key), cases[i].seq, cases[i].kind)
			if cases[i].kind == internalKeyKindSet {
				if err := wr.Set(ikey, addval); err != nil {
					t.Fatal("set fail", ikey.String(), err)
				}
			} else {
				if err := wr.Set(ikey, nil); err != nil {
					t.Fatal("set fail", ikey.String(), err)
				}
			}
		}
		require.NoError(t, wr.FlushFinish())
	}

	readData := func(pg *page, cases []rtest) {
		for i := 0; i < len(cases); i++ {
			key := makeTestKey(cases[i].key)
			v, vexist, vcloser, _ := pg.get(key, hash.Crc32(key))
			if cases[i].exist {
				require.Equal(t, true, vexist)
				require.Equal(t, addval, v)
			} else {
				require.Equal(t, false, vexist)
			}
			if vcloser != nil {
				vcloser()
			}
		}
	}

	bp, err := testOpenBitpage(true)
	require.NoError(t, err)
	pn, err1 := bp.NewPage()
	require.NoError(t, err1)

	fmt.Printf("#####################writeData#####################\n")
	cases := make([]wtest, 0, 10)
	cases = append(cases, wtest{key: 0, seq: 1, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 0, seq: 2, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 0, seq: 3, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 0, seq: 4, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 1, seq: 5, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 1, seq: 6, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 2, seq: 7, kind: internalKeyKindDelete})
	cases = append(cases, wtest{key: 2, seq: 8, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 2, seq: 9, kind: internalKeyKindDelete})
	cases = append(cases, wtest{key: 2, seq: 10, kind: internalKeyKindDelete})
	cases = append(cases, wtest{key: 3, seq: 11, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 4, seq: 12, kind: internalKeyKindSet})
	cases = append(cases, wtest{key: 5, seq: 13, kind: internalKeyKindDelete})
	writeData(bp, pn, cases)

	rcases := make([]rtest, 0, 10)
	rcases = append(rcases, rtest{key: 0, seq: 4, exist: true})
	rcases = append(rcases, rtest{key: 1, seq: 6, exist: true})
	rcases = append(rcases, rtest{key: 2, seq: 10, exist: false})
	rcases = append(rcases, rtest{key: 3, seq: 11, exist: true})
	rcases = append(rcases, rtest{key: 4, seq: 12, exist: true})
	rcases = append(rcases, rtest{key: 5, seq: 13, exist: false})
	p := bp.GetPage(pn)
	readData(p, rcases)

	fmt.Printf("#####################flush#####################\n")
	require.NoError(t, p.flush(nil, ""))

	rcases2 := make([]rtest, 0, 10)
	rcases2 = append(rcases2, rtest{key: 0, seq: 1, exist: true})
	rcases2 = append(rcases2, rtest{key: 1, seq: 1, exist: true})
	rcases2 = append(rcases2, rtest{key: 2, seq: 1, exist: false})
	rcases2 = append(rcases2, rtest{key: 3, seq: 1, exist: true})
	rcases2 = append(rcases2, rtest{key: 4, seq: 1, exist: true})
	rcases2 = append(rcases2, rtest{key: 5, seq: 1, exist: false})
	p = bp.GetPage(pn)
	readData(p, rcases2)

	fmt.Printf("#####################writeData2#####################\n")
	cases2 := make([]wtest, 0, 10)
	cases2 = append(cases2, wtest{key: 0, seq: 14, kind: internalKeyKindSet})
	cases2 = append(cases2, wtest{key: 1, seq: 15, kind: internalKeyKindSet})
	cases2 = append(cases2, wtest{key: 2, seq: 16, kind: internalKeyKindSet})
	cases2 = append(cases2, wtest{key: 3, seq: 17, kind: internalKeyKindSet})
	cases2 = append(cases2, wtest{key: 4, seq: 18, kind: internalKeyKindSet})
	cases2 = append(cases2, wtest{key: 4, seq: 19, kind: internalKeyKindSet})
	writeData(bp, pn, cases2)

	rcases3 := make([]rtest, 0, 10)
	rcases3 = append(rcases3, rtest{key: 0, seq: 14, exist: true})
	rcases3 = append(rcases3, rtest{key: 1, seq: 15, exist: true})
	rcases3 = append(rcases3, rtest{key: 2, seq: 16, exist: true})
	rcases3 = append(rcases3, rtest{key: 3, seq: 17, exist: true})
	rcases3 = append(rcases3, rtest{key: 4, seq: 19, exist: true})
	rcases3 = append(rcases3, rtest{key: 5, seq: 1, exist: false})
	p = bp.GetPage(pn)
	readData(p, rcases3)

	fmt.Printf("#####################flush2#####################\n")
	require.NoError(t, p.flush(nil, ""))

	rcases4 := make([]rtest, 0, 10)
	rcases4 = append(rcases4, rtest{key: 0, seq: 1, exist: true})
	rcases4 = append(rcases4, rtest{key: 1, seq: 1, exist: true})
	rcases4 = append(rcases4, rtest{key: 2, seq: 1, exist: true})
	rcases4 = append(rcases4, rtest{key: 3, seq: 1, exist: true})
	rcases4 = append(rcases4, rtest{key: 4, seq: 1, exist: true})
	rcases4 = append(rcases4, rtest{key: 5, seq: 1, exist: false})
	p = bp.GetPage(pn)
	readData(p, rcases4)

	testCloseBitpage(t, bp)
}
