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

package bitpage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/v2/internal/hash"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

type testKKVItem struct {
	Key      []byte
	IKey     *InternalKKVKey
	Value    []byte
	DataType uint8
	Version  uint64
	KeyHash  uint32
	Sn       uint64
}
type testSortedKKVItem []*testKKVItem

func (x testSortedKKVItem) Len() int { return len(x) }
func (x testSortedKKVItem) Less(i, j int) bool {
	return bytes.Compare(x[i].Key, x[j].Key) == -1
}
func (x testSortedKKVItem) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func testNewVAT(t *testing.T, exist bool) *vectorArrayTable {
	opts := testInitOpts()
	bpage := &Bitpage{
		dirname: testDir,
		opts:    opts,
	}
	testInitDir()
	pg := newPage(bpage, PageNum(1))
	at, err := newVectorArrayTable(pg, testTblPath, FileNum(1), exist)
	require.NoError(t, err)
	return at
}

func testMakeKKVAllList(kn, kkn, mn int, version uint64) testSortedKKVItem {
	var kvList, kvList1 testSortedKKVItem
	for i := 0; i < kn; i++ {
		for _, dt := range []uint8{
			kkv.DataTypeHash,
			kkv.DataTypeZset,
			kkv.DataTypeSet,
			kkv.DataTypeExpireKey,
			kkv.DataTypeList,
			kkv.DataTypeBitmap,
		} {
			switch dt {
			case kkv.DataTypeZset:
				kvList1 = testMakeZsetKeyList(1, kkn, mn, version)
			case kkv.DataTypeExpireKey:
				kvList1 = testMakeExpireKeyList(1, kkn, version)
			default:
				kvList1 = testMakeKKVList(1, kkn, dt, 100, version)
			}
			kvList = append(kvList, kvList1...)
			version++
		}
	}
	sort.Sort(kvList)
	return kvList
}

func testMakeHashConflictKKVList(kn, kkn int, dt uint8, vsize int) testSortedKKVItem {
	var kvList testSortedKKVItem
	var key []byte

	version := uint64(10000)
	count := kn * kkn
	cnt := 0
	for i := 0; i < kn; i++ {
		for j := 0; j < kkn; j++ {
			cnt++
			vi := strconv.Itoa(cnt % (count / 10))
			member := []byte(fmt.Sprintf("testMakeSubKey_%d", j))
			key = kkv.EncodeSubKey(member, dt, version)
			ikey := kkv.MakeInternalSetKey(key)
			kvList = append(kvList, &testKKVItem{
				Key:      key,
				KeyHash:  hash.Fnv32([]byte(vi)),
				IKey:     &ikey,
				Value:    utils.FuncRandBytes(vsize),
				Version:  version,
				DataType: dt,
			})
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func testMakeZsetKeyList(kn, kkn, mn int, version uint64) testSortedKKVItem {
	var kvList testSortedKKVItem
	if version == 0 {
		version = uint64(10000)
	}
	for i := 0; i < kn; i++ {
		for j := 1; j <= kkn; j++ {
			for k := 1; k <= mn; k++ {
				member := []byte(fmt.Sprintf("testMakeZsetKeyListKey_%d_%d", j, k))
				score := float64(j)
				key := kkv.EncodeZsetIndexKey(version, score, member)
				ikey1 := kkv.MakeInternalSetKey(key)
				kvList = append(kvList, &testKKVItem{
					Key:      key,
					IKey:     &ikey1,
					Version:  version,
					Value:    nil,
					DataType: kkv.DataTypeZsetIndex,
				})

				dataKey := make([]byte, kkv.SubKeyZsetLength)
				kkv.EncodeZsetDataKey(dataKey, version, member)
				ikey2 := kkv.MakeInternalSetKey(dataKey)
				kvList = append(kvList, &testKKVItem{
					Key:      dataKey,
					IKey:     &ikey2,
					Version:  version,
					Value:    kkv.EncodeZsetScore(float64(j)),
					DataType: kkv.DataTypeZset,
				})
			}
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func testMakeZsetIndexKeyList(kn, kkn, mn int, version uint64) testSortedKKVItem {
	var kvList testSortedKKVItem
	var keyPrefix [kkv.SubKeyHeaderLength]byte
	if version == 0 {
		version = uint64(10000)
	}
	dt := kkv.DataTypeZsetIndex
	for i := 0; i < kn; i++ {
		kkv.PutSubKeyHeader(keyPrefix[:], dt, version)
		for j := 1; j <= kkn; j++ {
			for k := 1; k <= mn; k++ {
				member := []byte(fmt.Sprintf("testMakeZsetKeyListKey_%d_%d", j, k))
				key := kkv.EncodeZsetIndexKey(version, float64(j), member)
				ikey := kkv.MakeInternalSetKey(key)
				kvList = append(kvList, &testKKVItem{
					Key:      key,
					IKey:     &ikey,
					Version:  version,
					DataType: dt,
				})
			}
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func testMakeKKVList(kn, kkn int, dt uint8, vsize int, version uint64) testSortedKKVItem {
	var kvList testSortedKKVItem
	var key, value []byte

	if version == 0 {
		version = 10000
	}
	for i := 0; i < kn; i++ {
		for j := 0; j < kkn; j++ {
			if kkv.IsDataTypeList(dt) {
				key = make([]byte, kkv.SubKeyListLength)
				kkv.EncodeListKey(key, dt, version, uint64(j))
			} else {
				member := []byte(fmt.Sprintf("testMakeSubKey_%d", j))
				key = kkv.EncodeSubKey(member, dt, version)
			}
			if dt == kkv.DataTypeSet || dt == kkv.DataTypeExpireKey || j%2 == 0 {
				value = nil
			} else {
				value = utils.FuncRandBytes(vsize)
			}

			ikey := kkv.MakeInternalSetKey(key)
			kvList = append(kvList, &testKKVItem{
				Key:      key,
				KeyHash:  hash.Fnv32(key),
				IKey:     &ikey,
				Value:    value,
				Version:  version,
				DataType: dt,
			})
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func testMakeExpireKeyList(kn, kkn int, version uint64) testSortedKKVItem {
	var kvList testSortedKKVItem
	var key []byte

	if version == 0 {
		version = 10000
	}
	for i := 0; i < kn; i++ {
		for j := 0; j < kkn; j++ {
			key = make([]byte, kkv.ExpireKeyLength)
			kkv.EncodeExpireKey(key, version, version+1, uint16(j+1))
			ikey := kkv.MakeInternalSetKey(key)
			kvList = append(kvList, &testKKVItem{
				Key:      key,
				IKey:     &ikey,
				Value:    nil,
				Version:  version,
				DataType: kkv.DataTypeExpireKey,
			})
		}
		version++
	}

	sort.Sort(kvList)
	return kvList
}

func TestVATHashWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	at := testNewVAT(t, false)
	kn := 100
	kkn := 100
	dataType := kkv.DataTypeHash
	kvList := testMakeKKVList(kn, kkn, dataType, 100, 0)
	keyNum := len(kvList)
	for i := 0; i < keyNum; i++ {
		_, err := at.writeItem(kvList[i].IKey, kvList[i].Value, kvList[i].KeyHash)
		require.NoError(t, err)
	}
	require.NoError(t, at.writeFinish(kvList[keyNum-1].Key))

	seekFunc := func(a *vectorArrayTable) {
		for j := 0; j < keyNum; j++ {
			skey := kvList[j].Key
			val, exist, _ := a.get(skey, hash.Fnv32(skey))
			require.Equal(t, true, exist)
			require.Equal(t, kvList[j].Value, val)
			exist, _ = a.exist(skey, hash.Fnv32(skey))
			require.Equal(t, true, exist)
		}

		for j := 0; j < keyNum; j += kkn {
			kv := kvList[j]
			var lowerBound [kkv.SubKeyHeaderLength]byte
			var upperBound [kkv.SubKeyUpperBoundLength]byte
			kkv.EncodeSubKeyLowerBound(lowerBound[:], dataType, kv.Version)
			kkv.EncodeSubKeyUpperBound(upperBound[:], dataType, kv.Version)
			opts := &iterOptions{
				DataType:   dataType,
				LowerBound: lowerBound[:],
				UpperBound: upperBound[:],
				IsTest:     true,
			}
			iter := a.newIter(opts)
			k := 0
			for key, val := iter.SeekGE(opts.LowerBound); key != nil; key, val = iter.Next() {
				require.Equal(t, kvList[j+k].Key, key.MakeUserKey())
				require.Equal(t, kvList[j+k].Value, val)
				k++
			}
			require.Equal(t, kkn, k)
			require.NoError(t, iter.Close())
		}

		iter := a.newIter(nil)
		i := 0
		for key, val := iter.First(); key != nil; key, val = iter.Next() {
			require.Equal(t, kvList[i].Key, key.MakeUserKey())
			require.Equal(t, kvList[i].Value, val)
			i++
		}
		require.NoError(t, iter.Close())
		require.Equal(t, keyNum, i)
	}

	seekFunc(at)
	maxKey := utils.CloneBytes(at.getMaxKey())
	require.NoError(t, at.close())

	at1 := testNewVAT(t, true)
	require.Equal(t, maxKey, at1.getMaxKey())
	seekFunc(at1)
	require.NoError(t, at1.close())
}

func TestVATHashConflictWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	at := testNewVAT(t, false)
	kn := 100
	kkn := 100
	dataType := kkv.DataTypeHash
	kvList := testMakeHashConflictKKVList(kn, kkn, dataType, 10)
	keyNum := len(kvList)
	for i := 0; i < keyNum; i++ {
		_, err := at.writeItem(kvList[i].IKey, kvList[i].Value, kvList[i].KeyHash)
		require.NoError(t, err)
	}
	require.NoError(t, at.writeFinish(kvList[keyNum-1].Key))

	seekFunc := func(a *vectorArrayTable) {
		for j := 0; j < keyNum; j++ {
			skey := kvList[j].Key
			val, exist, _ := a.get(skey, kvList[j].KeyHash)
			require.Equal(t, true, exist)
			require.Equal(t, kvList[j].Value, val)
			exist, _ = a.exist(skey, kvList[j].KeyHash)
			require.Equal(t, true, exist)
		}
	}

	seekFunc(at)
	maxKey := utils.CloneBytes(at.getMaxKey())
	require.NoError(t, at.close())

	at1 := testNewVAT(t, true)
	require.Equal(t, maxKey, at1.getMaxKey())
	seekFunc(at1)
	require.NoError(t, at1.close())
}

func TestVATSetWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	at := testNewVAT(t, false)
	kn := 1000
	kkn := 1000
	dataType := kkv.DataTypeSet
	kvList := testMakeKKVList(kn, kkn, dataType, 100, 0)
	keyNum := len(kvList)
	for i := 0; i < keyNum; i++ {
		_, err := at.writeItem(kvList[i].IKey, nil, kvList[i].KeyHash)
		require.NoError(t, err)
	}
	require.NoError(t, at.writeFinish(kvList[keyNum-1].Key))
	expMaxKey := kvList[keyNum-1].Key

	seekFunc := func(a *vectorArrayTable) {
		for j := 0; j < keyNum; j++ {
			skey := kvList[j].Key
			val, exist, _ := a.get(skey, hash.Fnv32(skey))
			require.Equal(t, true, exist)
			require.Equal(t, []byte(nil), val)
			exist, _ = a.exist(skey, hash.Fnv32(skey))
			require.Equal(t, true, exist)
		}

		if dataType == kkv.DataTypeZset {
			return
		}

		for j := 0; j < keyNum; j += kkn {
			kv := kvList[j]
			var lowerBound [kkv.SubKeyHeaderLength]byte
			var upperBound [kkv.SubKeyUpperBoundLength]byte
			kkv.EncodeSubKeyLowerBound(lowerBound[:], dataType, kv.Version)
			kkv.EncodeSubKeyUpperBound(upperBound[:], dataType, kv.Version)
			opts := &iterOptions{
				DataType:   dataType,
				LowerBound: lowerBound[:],
				UpperBound: upperBound[:],
				IsTest:     true,
			}
			iter := a.newIter(opts)
			k := 0
			for key, val := iter.SeekGE(opts.LowerBound); key != nil; key, val = iter.Next() {
				require.Equal(t, kvList[j+k].Key, key.MakeUserKey())
				require.Equal(t, []byte(nil), val)
				k++
			}
			require.Equal(t, kkn, k)
			require.NoError(t, iter.Close())
		}

		iter := a.newIter(nil)
		i := 0
		for key, val := iter.First(); key != nil; key, val = iter.Next() {
			require.Equal(t, kvList[i].Key, key.MakeUserKey())
			require.Equal(t, []byte(nil), val)
			i++
		}
		require.NoError(t, iter.Close())
		require.Equal(t, keyNum, i)
	}

	seekFunc(at)
	maxKey := utils.CloneBytes(at.getMaxKey())
	require.Equal(t, expMaxKey, maxKey)
	require.NoError(t, at.close())

	at1 := testNewVAT(t, true)
	require.Equal(t, maxKey, at1.getMaxKey())
	seekFunc(at1)
	require.NoError(t, at1.close())
}

func TestVATListWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	at := testNewVAT(t, false)
	kn := 100
	kkn := 1000
	dataType := kkv.DataTypeList
	kvList := testMakeKKVList(kn, kkn, dataType, 100, 0)
	keyNum := len(kvList)
	maxVersion := uint64(0)
	for i := 0; i < keyNum; i++ {
		_, err := at.writeItem(kvList[i].IKey, kvList[i].Value, 0)
		if maxVersion < kvList[i].Version {
			maxVersion = kvList[i].Version
		}
		require.NoError(t, err)
	}
	require.NoError(t, at.writeFinish(kvList[keyNum-1].Key))

	newOpts := func(ver, start, stop uint64) *iterOptions {
		var lowerBound, upperBound [kkv.SubKeyListLength]byte
		kkv.EncodeListKey(lowerBound[:], dataType, ver, start)
		kkv.EncodeListKey(upperBound[:], dataType, ver, stop)
		iterOpts := &iterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			DataType:   kkv.DataTypeList,
			IsTest:     true,
		}
		return iterOpts
	}

	maxVersion += 1

	seekFunc := func(a *vectorArrayTable) {
		for j := 0; j < keyNum; j++ {
			skey := kvList[j].Key
			val, exist, _ := a.get(skey, 0)
			require.Equal(t, true, exist)
			require.Equal(t, kvList[j].Value, val)
			exist, _ = a.exist(skey, 0)
			require.Equal(t, true, exist)
		}

		opts := newOpts(maxVersion, 0, 1)
		it := a.newIter(opts)
		ik, _ := it.SeekGE(opts.LowerBound)
		if ik != nil {
			t.Fatal("SeekGE maxVersion found")
		}
		ik, _ = it.SeekLT(opts.UpperBound)
		if ik != nil {
			t.Fatal("SeekLT maxVersion found")
		}
		require.NoError(t, it.Close())

		keyBuf := make([]byte, kkv.SubKeyListLength)
		kkv.EncodeListKey(keyBuf, dataType, 100, uint64(kkn+10))
		_, exist, _ := a.get(keyBuf, 0)
		require.Equal(t, false, exist)
		exist, _ = a.exist(keyBuf, 0)
		require.Equal(t, false, exist)

		for j := 0; j < keyNum; j += kkn {
			opts = newOpts(kvList[j].Version, 1, 5)
			it = a.newIter(opts)
			k := 1
			for key, val := it.SeekGE(opts.LowerBound); key != nil; key, val = it.Next() {
				require.Equal(t, kvList[j+k].Key, key.MakeUserKey())
				require.Equal(t, kvList[j+k].Value, val)
				k++
			}
			require.Equal(t, 5, k)
			require.NoError(t, it.Close())
		}

		iter := a.newIter(nil)
		i := 0
		for key, val := iter.First(); key != nil; key, val = iter.Next() {
			require.Equal(t, kvList[i].Key, key.MakeUserKey())
			require.Equal(t, kvList[i].Value, val)
			i++
		}
		require.NoError(t, iter.Close())
		require.Equal(t, keyNum, i)
	}

	seekFunc(at)

	maxKey := utils.CloneBytes(at.getMaxKey())
	header := at.header
	require.NoError(t, at.close())

	at1 := testNewVAT(t, true)
	require.Equal(t, maxKey, at1.getMaxKey())
	header1 := at1.header
	require.Equal(t, header.headerOffset, header1.headerOffset)
	require.Equal(t, header, header1)
	seekFunc(at1)
	require.NoError(t, at1.close())
}

func TestVATZindexWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	at := testNewVAT(t, false)
	kn := 1000
	kkn := 10
	mn := 5
	knum := kkn * mn
	dataType := kkv.DataTypeZsetIndex
	kvList := testMakeZsetIndexKeyList(kn, kkn, mn, 0)
	keyNum := len(kvList)
	maxVersion := uint64(0)
	for i := 0; i < keyNum; i++ {
		kv := kvList[i]
		_, err := at.writeItem(kv.IKey, kv.Value, 0)
		if maxVersion < kv.Version {
			maxVersion = kv.Version
		}
		require.NoError(t, err)
	}
	require.NoError(t, at.writeFinish(kvList[keyNum-1].Key))

	maxVersion += 1
	var maxVersionBuf [8]byte
	kkv.EncodeKeyVersion(maxVersionBuf[:], maxVersion)

	newOpts := func(ver uint64) *iterOptions {
		var lowerBound [kkv.SubKeyHeaderLength]byte
		var upperBound [kkv.SubKeyUpperBoundLength]byte
		kkv.EncodeSubKeyLowerBound(lowerBound[:], dataType, ver)
		kkv.EncodeSubKeyUpperBound(upperBound[:], dataType, ver)
		iterOpts := &iterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			DataType:   dataType,
			IsTest:     true,
		}
		return iterOpts
	}

	newIndexOpts := func(ver uint64, min, max float64) *iterOptions {
		var lowerBound [kkv.SiKeyLowerBoundLength]byte
		var upperBound [kkv.SiKeyUpperBoundLength]byte
		kkv.EncodeZsetIndexLowerBound(lowerBound[:], ver, min)
		kkv.EncodeZsetIndexUpperBound(upperBound[:], ver, max)
		iterOpts := &iterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			DataType:   dataType,
			IsTest:     true,
		}
		return iterOpts
	}

	seekFunc := func(a *vectorArrayTable) {
		opts := newOpts(maxVersion)
		it := a.newIter(opts)
		ik, val := it.SeekGE(maxVersionBuf[:])
		if ik != nil {
			t.Fatal("SeekGE maxVersion found")
		}

		ik, val = it.SeekLT(maxVersionBuf[:])
		if ik != nil {
			t.Fatal("SeekLT maxVersion found")
		}
		require.NoError(t, it.Close())

		for j := 0; j < keyNum; j += knum {
			opts = newOpts(kvList[j].Version)
			it = a.newIter(opts)
			k := 0
			for ik, val = it.SeekGE(opts.LowerBound); ik != nil; ik, val = it.Next() {
				require.Equal(t, kvList[j+k].Key, ik.MakeUserKey())
				require.Equal(t, kvList[j+k].Value, val)
				k++
			}
			require.Equal(t, knum, k)
			require.NoError(t, it.Close())
		}

		for j := keyNum - 1; j >= 0; j -= knum {
			opts = newOpts(kvList[j].Version)
			it = a.newIter(opts)
			k := 0
			for ik, val = it.SeekLT(opts.UpperBound); ik != nil; ik, val = it.Prev() {
				require.Equal(t, kvList[j-k].Key, ik.MakeUserKey())
				require.Equal(t, kvList[j-k].Value, val)
				k++
			}
			require.Equal(t, knum, k)
			require.NoError(t, it.Close())
		}

		for j := 0; j < keyNum; j += knum {
			opts = newIndexOpts(kvList[j].Version, 2, float64(kkn-1))
			it = a.newIter(opts)
			k := mn
			for ik, val = it.SeekGE(opts.LowerBound); ik != nil; ik, val = it.Next() {
				require.Equal(t, kvList[j+k].Key, ik.MakeUserKey())
				require.Equal(t, kvList[j+k].Value, val)
				k++
			}
			require.Equal(t, knum-mn, k)
			require.NoError(t, it.Close())
		}
	}

	seekFunc(at)
	maxKey := utils.CloneBytes(at.getMaxKey())

	var lowerBound [kkv.SubKeyHeaderLength]byte
	var upperBound [kkv.SubKeyUpperBoundLength]byte
	kkv.EncodeSubKeyLowerBound(lowerBound[:], kkv.DataTypeHash, maxVersion)
	kkv.EncodeSubKeyUpperBound(upperBound[:], kkv.DataTypeHash, maxVersion)
	opts := &iterOptions{
		DataType:   kkv.DataTypeHash,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		IsTest:     true,
	}
	it := at.newIter(opts)
	ik, _ := it.SeekGE(maxVersionBuf[:])
	if ik != nil {
		t.Fatal("SeekGE hash found")
	}
	require.NoError(t, it.Close())

	var lowerBound1, upperBound1 [kkv.SubKeyListLength]byte
	kkv.EncodeListKey(lowerBound1[:], kkv.DataTypeList, maxVersion, 0)
	kkv.EncodeListKey(upperBound1[:], kkv.DataTypeList, maxVersion, 100)
	opts = &iterOptions{
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
		DataType:   kkv.DataTypeList,
		IsTest:     true,
	}
	it = at.newIter(opts)
	ik, _ = it.SeekGE(maxVersionBuf[:])
	if ik != nil {
		t.Fatal("SeekGE list found")
	}
	require.NoError(t, it.Close())

	require.NoError(t, at.close())

	at1 := testNewVAT(t, true)
	require.Equal(t, maxKey, at1.getMaxKey())
	seekFunc(at1)
	require.NoError(t, at1.close())
}

func TestVATZsetWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	at := testNewVAT(t, false)
	kn := 1000
	kkn := 100
	mn := 10
	knum := kkn * mn
	kvList := testMakeZsetKeyList(kn, kkn, mn, 0)
	keyNum := len(kvList) - 1
	maxVersion := uint64(0)
	for i := 0; i < keyNum; i++ {
		kv := kvList[i]
		_, err := at.writeItem(kv.IKey, kv.Value, 0)
		if maxVersion < kv.Version {
			maxVersion = kv.Version
		}
		require.NoError(t, err)
	}
	require.NoError(t, at.writeFinish(kvList[keyNum-1].Key))

	maxVersion += 1
	var maxVersionBuf [8]byte
	kkv.EncodeKeyVersion(maxVersionBuf[:], maxVersion)

	newOpts := func(ver uint64) *iterOptions {
		var lowerBound [kkv.SubKeyHeaderLength]byte
		var upperBound [kkv.SubKeyUpperBoundLength]byte
		kkv.EncodeSubKeyLowerBound(lowerBound[:], kkv.DataTypeZsetIndex, ver)
		kkv.EncodeSubKeyUpperBound(upperBound[:], kkv.DataTypeZsetIndex, ver)
		iterOpts := &iterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			DataType:   kkv.DataTypeZsetIndex,
			IsTest:     true,
		}
		return iterOpts
	}

	newIndexOpts := func(ver uint64, min, max float64) *iterOptions {
		var lowerBound [kkv.SiKeyLowerBoundLength]byte
		var upperBound [kkv.SiKeyUpperBoundLength]byte
		kkv.EncodeZsetIndexLowerBound(lowerBound[:], ver, min)
		kkv.EncodeZsetIndexUpperBound(upperBound[:], ver, max)
		iterOpts := &iterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			DataType:   kkv.DataTypeZsetIndex,
			IsTest:     true,
		}
		return iterOpts
	}

	seekFunc := func(a *vectorArrayTable) {
		opts := newOpts(maxVersion)
		it := a.newIter(opts)
		ik, val := it.SeekGE(maxVersionBuf[:])
		if ik != nil {
			t.Fatal("SeekGE maxVersion found")
		}

		ik, val = it.SeekLT(maxVersionBuf[:])
		if ik != nil {
			t.Fatal("SeekLT maxVersion found")
		}
		require.NoError(t, it.Close())

		j := 0
		for j < keyNum {
			item := kvList[j]
			if item.DataType == kkv.DataTypeZsetIndex {
				opts = newOpts(item.Version)
				it = a.newIter(opts)
				k := 0
				for ik, val = it.SeekGE(opts.LowerBound); ik != nil; ik, val = it.Next() {
					require.Equal(t, kvList[j+k].Key, ik.MakeUserKey())
					require.Equal(t, kvList[j+k].Value, val)
					k++
				}
				if j+knum > keyNum {
					require.Equal(t, knum-1, k)
				} else {
					require.Equal(t, knum, k)
				}
				require.NoError(t, it.Close())

				opts = newIndexOpts(item.Version, 2, float64(kkn-1))
				it = a.newIter(opts)
				k = mn
				for ik, val = it.SeekGE(opts.LowerBound); ik != nil; ik, val = it.Next() {
					require.Equal(t, kvList[j+k].Key, ik.MakeUserKey())
					require.Equal(t, kvList[j+k].Value, val)
					k++
				}
				require.Equal(t, knum-mn, k)
				require.NoError(t, it.Close())

				j += knum
			} else {
				skey := item.Key
				v, exist, _ := a.get(skey, hash.Fnv32(skey))
				require.Equal(t, true, exist)
				require.Equal(t, item.Value, v)
				exist, _ = a.exist(skey, hash.Fnv32(skey))
				require.Equal(t, true, exist)
				j++
			}
		}

		j = keyNum - 1
		for j >= 0 {
			item := kvList[j]
			if item.DataType == kkv.DataTypeZsetIndex {
				opts = newOpts(item.Version)
				it = a.newIter(opts)
				k := 0
				for ik, val = it.SeekLT(opts.UpperBound); ik != nil; ik, val = it.Prev() {
					require.Equal(t, kvList[j-k].Key, ik.MakeUserKey())
					require.Equal(t, kvList[j-k].Value, val)
					k++
				}
				if j == keyNum-1 {
					require.Equal(t, knum-1, k)
					j -= knum - 1
				} else {
					require.Equal(t, knum, k)
					j -= knum
				}
				require.NoError(t, it.Close())
			} else {
				skey := item.Key
				v, exist, _ := a.get(skey, hash.Fnv32(skey))
				require.Equal(t, true, exist)
				require.Equal(t, item.Value, v)
				exist, _ = a.exist(skey, hash.Fnv32(skey))
				require.Equal(t, true, exist)
				j--
			}
		}
	}

	seekFunc(at)
	maxKey := utils.CloneBytes(at.getMaxKey())
	require.NoError(t, at.close())

	at1 := testNewVAT(t, true)
	require.Equal(t, maxKey, at1.getMaxKey())
	seekFunc(at1)
	require.NoError(t, at1.close())
}

func TestVATExpireWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	at := testNewVAT(t, false)
	kn := 1000
	kkn := 100
	dataType := kkv.DataTypeExpireKey
	kvList := testMakeExpireKeyList(kn, kkn, 0)
	keyNum := len(kvList)
	for i := 0; i < keyNum; i++ {
		_, err := at.writeItem(kvList[i].IKey, nil, 0)
		require.NoError(t, err)
	}
	require.NoError(t, at.writeFinish(kvList[keyNum-1].Key))
	expMaxKey := kvList[keyNum-1].Key

	seekFunc := func(a *vectorArrayTable) {
		for j := 0; j < keyNum; j++ {
			skey := kvList[j].Key
			val, exist, _ := a.get(skey, hash.Fnv32(skey))
			require.Equal(t, true, exist)
			require.Equal(t, []byte(nil), val)
			exist, _ = a.exist(skey, hash.Fnv32(skey))
			require.Equal(t, true, exist)
		}

		for j := 0; j < keyNum; j += kkn {
			kv := kvList[j]
			var lowerBound [kkv.SubKeyHeaderLength]byte
			var upperBound [kkv.SubKeyUpperBoundLength]byte
			kkv.EncodeSubKeyLowerBound(lowerBound[:], dataType, kv.Version)
			kkv.EncodeSubKeyUpperBound(upperBound[:], dataType, kv.Version)
			opts := &iterOptions{
				DataType:   dataType,
				LowerBound: lowerBound[:],
				UpperBound: upperBound[:],
				IsTest:     true,
			}
			iter := a.newIter(opts)
			k := 0
			for key, val := iter.SeekGE(opts.LowerBound); key != nil; key, val = iter.Next() {
				require.Equal(t, kvList[j+k].Key, key.MakeUserKey())
				require.Equal(t, []byte(nil), val)
				k++
			}
			require.Equal(t, kkn, k)
			require.NoError(t, iter.Close())
		}

		iter := a.newIter(nil)
		i := 0
		for key, val := iter.First(); key != nil; key, val = iter.Next() {
			require.Equal(t, kvList[i].Key, key.MakeUserKey())
			require.Equal(t, []byte(nil), val)
			i++
		}
		require.NoError(t, iter.Close())
		require.Equal(t, keyNum, i)
	}

	seekFunc(at)
	maxKey := utils.CloneBytes(at.getMaxKey())
	require.Equal(t, expMaxKey, maxKey)
	require.NoError(t, at.close())

	at1 := testNewVAT(t, true)
	require.Equal(t, maxKey, at1.getMaxKey())
	seekFunc(at1)
	require.NoError(t, at1.close())
}

func TestVATWriteRead(t *testing.T) {
	dir := testDir
	defer os.RemoveAll(dir)
	os.RemoveAll(dir)

	at := testNewVAT(t, false)
	kn := 700
	kkn := 100
	mn := 10
	knum := kkn * mn
	version := uint64(12345)
	kvList := testMakeKKVAllList(kn, kkn, mn, version)
	keyNum := len(kvList) - kkn
	for i := 0; i < keyNum; i++ {
		kv := kvList[i]
		_, err := at.writeItem(kv.IKey, kv.Value, kv.KeyHash)
		require.NoError(t, err)
	}
	require.NoError(t, at.writeFinish(kvList[keyNum-1].Key))

	maxVersionBuf := kkv.GetKeyVersion(kvList[keyNum].Key)
	maxVersion := binary.LittleEndian.Uint64(maxVersionBuf)

	newOpts := func(ver uint64, dt uint8) *iterOptions {
		var lowerBound [kkv.SubKeyHeaderLength]byte
		var upperBound [kkv.SubKeyUpperBoundLength]byte
		kkv.EncodeSubKeyLowerBound(lowerBound[:], dt, ver)
		kkv.EncodeSubKeyUpperBound(upperBound[:], dt, ver)
		iterOpts := &iterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			DataType:   dt,
			IsTest:     true,
		}
		return iterOpts
	}

	newListOpts := func(ver, start, stop uint64, dt uint8) *iterOptions {
		var lowerBound, upperBound [kkv.SubKeyListLength]byte
		kkv.EncodeListKey(lowerBound[:], dt, ver, start)
		kkv.EncodeListKey(upperBound[:], dt, ver, stop)
		iterOpts := &iterOptions{
			LowerBound: lowerBound[:],
			UpperBound: upperBound[:],
			DataType:   kkv.DataTypeList,
			IsTest:     true,
		}
		return iterOpts
	}

	readFunc := func(a *vectorArrayTable) {
		var opts *iterOptions
		var it InternalKKVIterator
		var addNum, j int
		for j < keyNum {
			kv := kvList[j]
			dt := kv.DataType
			keyVersion := kv.Version
			addNum = kkn
			switch dt {
			case kkv.DataTypeList:
				opts = newListOpts(maxVersion, 0, uint64(kkn), dt)
				it = a.newIter(opts)
				ik, _ := it.SeekGE(maxVersionBuf[:])
				require.Equal(t, nilInternalKKVKey, ik)
				ik, _ = it.SeekLT(maxVersionBuf[:])
				require.Equal(t, nilInternalKKVKey, ik)
				require.NoError(t, it.Close())
				opts = newListOpts(keyVersion, 0, uint64(kkn), dt)
				it = a.newIter(opts)
				k := 0
				for key, val := it.SeekGE(opts.LowerBound); key != nil; key, val = it.Next() {
					require.Equal(t, kvList[j+k].Key, key.MakeUserKey())
					require.Equal(t, kvList[j+k].Value, val)
					k++
				}
				require.Equal(t, addNum, k)
				require.NoError(t, it.Close())
			case kkv.DataTypeZsetIndex:
				opts = newOpts(maxVersion, dt)
				it = a.newIter(opts)
				ik, _ := it.SeekGE(maxVersionBuf[:])
				require.Equal(t, nilInternalKKVKey, ik)
				ik, _ = it.SeekLT(maxVersionBuf[:])
				require.Equal(t, nilInternalKKVKey, ik)
				require.NoError(t, it.Close())
				addNum = knum
				opts = newOpts(keyVersion, dt)
				it = a.newIter(opts)
				k := 0
				for key, val := it.SeekGE(opts.LowerBound); key != nil; key, val = it.Next() {
					require.Equal(t, kvList[j+k].Key, key.MakeUserKey())
					require.Equal(t, kvList[j+k].Value, val)
					k++
				}
				require.Equal(t, addNum, k)
				require.NoError(t, it.Close())
			default:
				opts = newOpts(keyVersion, dt)
				it = a.newIter(opts)
				k := 0
				for key, val := it.SeekGE(opts.LowerBound); key != nil; key, val = it.Next() {
					require.Equal(t, kvList[j+k].Key, key.MakeUserKey())
					require.Equal(t, kvList[j+k].Value, val)
					k++
				}
				if dt == kkv.DataTypeZset {
					addNum = knum
				}
				if k == 0 {
					fmt.Println("fail", keyVersion, dt)
				}
				require.Equal(t, addNum, k)
				require.NoError(t, it.Close())

				for k = 0; k < kkn; k++ {
					skey := kvList[j+k].Key
					val, exist, _ := a.get(skey, hash.Fnv32(skey))
					require.Equal(t, true, exist)
					require.Equal(t, kvList[j+k].Value, val)
					exist, _ = a.exist(skey, hash.Fnv32(skey))
					require.Equal(t, true, exist)
				}
			}
			j += addNum
		}
	}

	readFunc(at)
	maxKey := utils.CloneBytes(at.getMaxKey())
	header := at.header
	require.NoError(t, at.close())

	at1 := testNewVAT(t, true)
	require.Equal(t, maxKey, at1.getMaxKey())
	header1 := at1.header
	require.Equal(t, header, header1)
	readFunc(at1)
	require.NoError(t, at1.close())
}

//func TestReadVAT(t *testing.T) {
//	path := "1_1.vt"
//	file, err := os.OpenFile(path, os.O_RDONLY, consts.FileMode)
//	if err != nil {
//		panic(err)
//	}
//	defer file.Close()
//
//	fileStat, _ := file.Stat()
//	filesz := fileStat.Size()
//	readBuf := make([]byte, filesz)
//	n, err := file.ReadAt(readBuf, 0)
//	if err != nil {
//		panic(err)
//	} else if n != int(filesz) {
//		panic(n)
//	}
//
//	var keySize, valueSize uint32
//	offset := TblFileHeaderSize
//	var keyNum, hashNum, setNum, listNum, zsetNum uint32
//	for {
//		if offset >= int(filesz) {
//			break
//		}
//
//		if offset == 11518671 {
//			fmt.Println("offset:", offset)
//		}
//
//		h := readBuf[offset]
//		isValueNil := (h >> 7) == 1
//		dt := h & vatItemHeaderDtMask
//		if !kkv.IsDataType(dt) {
//			t.Fatalf("invalid dataType:%d offset:%d", dt, offset)
//		}
//
//		offset++
//		keySize = 0
//		valueSize = 0
//		if !kkv.IsDataTypeList(dt) {
//			kz, n1 := binary.Uvarint(readBuf[offset:])
//			offset += n1
//			keySize = uint32(kz)
//		}
//		if !isValueNil {
//			vz, n2 := binary.Uvarint(readBuf[offset:])
//			offset += n2
//			valueSize = uint32(vz)
//		}
//
//		switch dt {
//		case kkv.DataTypeZsetIndex:
//			zsetNum++
//		case kkv.DataTypeZset:
//			zsetNum += keySize
//		case kkv.DataTypeList, kkv.DataTypeBitmap:
//			listNum++
//		case kkv.DataTypeHash:
//			hashNum++
//		case kkv.DataTypeSet:
//			setNum++
//		default:
//		}
//
//		if dt == kkv.DataTypeZset {
//			offset += int(vatKeyVersionSize + keySize*vatZdataItemLength)
//			keyNum += keySize
//		} else {
//			offset += int(keySize + valueSize)
//			keyNum++
//		}
//	}
//
//	fmt.Println(keyNum, hashNum, setNum, listNum, zsetNum)
//}
