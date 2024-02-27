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

package bitalosdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

func TestBitalosdbCacheGetSet(t *testing.T) {
	defer os.RemoveAll(testDirname)

	for _, cacheType := range []int{consts.CacheTypeLfu, consts.CacheTypeLru} {
		fmt.Println("TestBitalosdbCacheGetSet cacheType=", cacheType)
		os.RemoveAll(testDirname)
		bitalosDB, err := openBitalosDBByCache(testDirname, cacheType, 0)
		require.NoError(t, err)

		num := int32(1000)
		keyIndex := int32(0)
		defaultVal := testRandBytes(2048)
		for keyIndex < num {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			if err = bitalosDB.db.Set(newKey, defaultVal, bitalosDB.wo); err != nil {
				t.Error("set err:", err)
			}
			keyIndex++
		}

		require.NoError(t, bitalosDB.db.Flush())
		if bitalosDB.db.cache != nil {
			t.Fatal("cache is not nil")
		}
		require.NoError(t, bitalosDB.db.Close())

		bitalosDB, err = openBitalosDBByCache(testDirname, cacheType, 128<<20)
		require.NoError(t, err)

		keyIndex = int32(0)
		for keyIndex < num {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			val, closer, e := bitalosDB.db.Get(newKey)
			require.NoError(t, e)
			require.Equal(t, defaultVal, val)
			keyIndex++
			if closer != nil {
				closer()
			}
		}

		keyIndex = int32(0)
		for keyIndex < num {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			cacheVal, cacheCloser, cacheFound := bitalosDB.db.GetCache(newKey)
			require.Equal(t, true, cacheFound)
			require.Equal(t, defaultVal, cacheVal)
			keyIndex++
			if cacheCloser != nil {
				cacheCloser()
			}
		}

		fmt.Println("cacheinfo", bitalosDB.db.CacheInfo())
		require.NoError(t, bitalosDB.db.Close())
	}
}

func TestBitalosdbCacheSet(t *testing.T) {
	defer os.RemoveAll(testDirname)

	for _, cacheType := range []int{consts.CacheTypeLfu, consts.CacheTypeLru} {
		fmt.Println("TestBitalosdbCacheSet cacheType=", cacheType)
		os.RemoveAll(testDirname)
		bitalosDB, err := openBitalosDBByCache(testDirname, cacheType, 128<<20)
		require.NoError(t, err)

		num := int32(1000)
		keyIndex := int32(0)
		val := testRandBytes(2048)
		for keyIndex < num {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			if err = bitalosDB.db.Set(newKey, val, bitalosDB.wo); err != nil {
				t.Error("set err:", err)
			}
			keyIndex++
		}

		require.NoError(t, bitalosDB.db.Flush())

		keyIndex = int32(0)
		for keyIndex < 10 {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			if err = bitalosDB.db.Delete(newKey, bitalosDB.wo); err != nil {
				t.Error("delete err:", err)
			}
			keyIndex++
		}

		require.NoError(t, bitalosDB.db.Flush())

		keyIndex = int32(0)
		for keyIndex < num {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			cacheVal, cacheCloser, cacheFound := bitalosDB.db.GetCache(newKey)
			if keyIndex < 10 {
				require.Equal(t, false, cacheFound)
			} else {
				require.Equal(t, true, cacheFound)
				require.Equal(t, cacheVal, val)
			}
			keyIndex++
			if cacheCloser != nil {
				cacheCloser()
			}
		}

		require.NoError(t, bitalosDB.db.Close())
	}
}

func TestBitalosdbCacheGetWhenFlushing(t *testing.T) {
	defer os.RemoveAll(testDirname)

	for _, cacheType := range []int{consts.CacheTypeLfu, consts.CacheTypeLru} {
		fmt.Println("TestBitalosdbCacheSet cacheType=", cacheType)
		os.RemoveAll(testDirname)
		bitalosDB, err := openBitalosDBByCache(testDirname, cacheType, 128<<20)
		require.NoError(t, err)

		num := int32(1000)
		keyIndex := int32(0)
		defaultVal := testRandBytes(2048)
		for keyIndex < num {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			if err = bitalosDB.db.Set(newKey, defaultVal, bitalosDB.wo); err != nil {
				t.Error("set err:", err)
			}
			keyIndex++
		}

		require.NoError(t, bitalosDB.db.Flush())

		bitalosDB.db.dbState.LockMemFlushing()

		keyIndex = int32(0)
		for keyIndex < num {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			value, vexist, vcloser := bitalosDB.db.bf.Get(newKey)
			require.Equal(t, defaultVal, value)
			require.Equal(t, true, vexist)
			keyIndex++
			if vcloser != nil {
				vcloser()
			}
		}

		bitalosDB.db.dbState.UnLockMemFlushing()

		keyIndex = int32(0)
		for keyIndex < num {
			newKey := []byte(fmt.Sprintf("key_%d", keyIndex))
			value, vexist, vcloser := bitalosDB.db.bf.Get(newKey)
			require.Equal(t, defaultVal, value)
			require.Equal(t, true, vexist)
			keyIndex++
			if vcloser != nil {
				vcloser()
			}
		}

		require.NoError(t, bitalosDB.db.Close())
	}
}
