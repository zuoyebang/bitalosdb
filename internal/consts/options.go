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

package consts

import (
	"path/filepath"
	"strconv"
)

const (
	BitableCacheSize                   int64 = 1 << 30
	BitableMemTableSize                int   = 64 << 20
	BitableMemTableStopWritesThreshold int   = 8
	BitableL0FileSize                  int64 = 256 << 20
	BitableL0CompactionFileThreshold   int   = 2
	BitableL0CompactionThreshold       int   = 2
	BitableL0StopWritesThreshold       int   = 128
	BitableLBaseMaxBytes               int64 = 1 << 30
	BitableMaxOpenFiles                int   = 5000
)

const (
	DefaultBytesPerSync                int   = 512 << 10
	DefaultMemTableSize                int   = 128 << 20
	DefaultMemTableStopWritesThreshold int   = 8
	DefaultCacheSize                   int64 = 128 << 20
	DefaultLruCacheShards              int   = 36
	DefaultLruCacheHashSize            int   = 8 * 1024 * DefaultLruCacheShards
	DefaultLfuCacheShards              int   = 256
)

const (
	BdbInitialSize int = 64 << 20
	BdbPageSize    int = 4 << 10
	BdbAllocSize   int = 16 << 20

	BdbFreelistArrayType = "array"
	BdbFreelistMapType   = "hashmap"
)

const (
	FlushDelPercentL1 float64 = 0.3
	FlushDelPercentL2 float64 = 0.5
	FlushDelPercentL3 float64 = 0.7
	FlushDelPercentL4 float64 = 0.9

	FlushItemCountL1 int = 1 << 20
	FlushItemCountL2 int = 2 << 20
	FlushItemCountL3 int = 3 << 20
)

const (
	BitpageLruCacheShards     int    = 8
	BitpageBlockCacheSize     int64  = 800 << 20
	BitpageBlockSize          uint32 = 32 << 10
	BitpageBlockCacheHashSize int    = 24 << 10
	BitpageBlockMinItemCount  int    = 40
	BitpageFlushSize          uint64 = 256 << 20
	BitpageSplitSize          uint64 = 592 << 20
	BitpageSplitNum           int    = 3
	BitpageInitMmapSize       int    = 2 << 30
)

const (
	MaxKeySize                    int   = 62 << 10
	KvSeparateSize                int   = 256
	BithashTableMaxSize           int   = 512 << 20
	CompactToBitableCiMaxSize     int   = 512 << 20
	UseBitableBitreeMaxSize       int64 = 24 << 30
	UseBitableForceCompactMaxSize int64 = 16 << 30
	BufioWriterBufSize            int   = 256 << 10
)

const (
	BitforestDefaultTreeNum      int = 8
	BitforestDefaultFlushBufSize int = 16 << 10
)

const (
	IterSlowCountThreshold         = 10000
	IterReadAmplificationThreshold = 10000
	DefaultDeletePercent           = 0.4
	MinCompactInterval             = 300
	DefaultCompactInterval         = 900
	BithashCompactInterval         = 1800
	DeletionFileInterval           = 4
)

const (
	CacheTypeLru int = 1 + iota
	CacheTypeLfu
)

const (
	BitreeFilePrefix  = "bitree."
	BitpageFilePrefix = "bitpage."
	BithashPathPrefix = "bithash."
	BitablePathPrefix = "bitable."
)

const FileMode = 0600

var (
	BdbBucketName = []byte("brt")
	BdbMaxKey     = []byte{0xff, 0xff, 0xff, 0xff}
)

func MakeBitreeFilePath(dir string, i int) string {
	return filepath.Join(dir, BitreeFilePrefix+strconv.Itoa(i))
}

func MakeBitpagePath(dir string, i int) string {
	return filepath.Join(dir, BitpageFilePrefix+strconv.Itoa(i))
}

func MakeBithashPath(dir string, i int) string {
	return filepath.Join(dir, BithashPathPrefix+strconv.Itoa(i))
}

func MakeBitablePath(dir string, i int) string {
	return filepath.Join(dir, BitablePathPrefix+strconv.Itoa(i))
}

func CheckFlushDelPercent(delPercent float64, inuse, size uint64) bool {
	if (delPercent > FlushDelPercentL1 && inuse > size/2) ||
		(delPercent > FlushDelPercentL2 && inuse > size/3) ||
		(delPercent > FlushDelPercentL3 && inuse > size/4) ||
		(delPercent > FlushDelPercentL4 && inuse > size/5) {
		return true
	}
	return false
}

func CheckFlushItemCount(itemCount int, inuse, size uint64) bool {
	if (itemCount > FlushItemCountL1 && inuse > (size*3/5)) ||
		(itemCount > FlushItemCountL2 && inuse > (size/2)) ||
		(itemCount > FlushItemCountL3 && inuse > (size*2/5)) {
		return true
	}
	return false
}
