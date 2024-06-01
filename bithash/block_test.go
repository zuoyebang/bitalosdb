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

package bithash

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/base"
)

func TestBlockIter(t *testing.T) {
	w := blockWriter{restartInterval: 16}

	makeIkey := func(s string) InternalKey {
		j := strings.Index(s, ":")
		seqNum, err := strconv.Atoi(s[j+1:])
		if err != nil {
			panic(err)
		}
		return base.MakeInternalKey([]byte(s[:j]), uint64(seqNum), InternalKeyKindSet)
	}

	w.add(makeIkey("testkey:1"), nil)
	w.add(makeIkey("testkey:2"), nil)
	w.add(makeIkey("testkey:3"), nil)
	block := w.finish()

	iter, err := newBlockIter(bytes.Compare, block)
	require.NoError(t, err)
	defer iter.Close()
	key := []byte("testkey")
	var value []byte
	var fk *InternalKey
	for k, v := iter.SeekGE(key); iter.Valid(); k, v = iter.Next() {
		if bytes.Equal(k.UserKey, key) {
			fmt.Println("find Equal value", string(value))
			for ; iter.Valid() && bytes.Equal(k.UserKey, key); k, v = iter.Next() {
				value = v
				fk = k
			}
			break
		}
	}
	fmt.Println("find value", string(value), fk.String())
}

func TestBlockSameKey(t *testing.T) {
	w := blockWriter{restartInterval: 16}
	seqNum := uint64(1)
	skey := []byte("ip_b_39.144.177.163")
	w.add(base.MakeInternalKey(skey, seqNum, InternalKeyKindSet), skey)
	seqNum++
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("value_%d", i)
		w.add(base.MakeInternalKey([]byte(key), seqNum, InternalKeyKindSet), []byte(val))
		seqNum++
		if i == 5 {
			w.add(base.MakeInternalKey(skey, seqNum, InternalKeyKindSet), skey)
			seqNum++
		}
	}
	w.add(base.MakeInternalKey(skey, seqNum, InternalKeyKindSet), skey)
	seqNum++

	iter, err := newBlockIter(bytes.Compare, w.finish())
	require.NoError(t, err)
	defer iter.Close()

	var v []byte
	var k *InternalKey
	for k, v = iter.First(); iter.Valid(); k, v = iter.Next() {
		fmt.Println("find1 Equal value", k.String(), string(v))
	}
	fmt.Println("find end", k, v)
}
