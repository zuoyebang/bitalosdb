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

package bitalosdb

import (
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type IterOptions = options.IterOptions

var IterAll = IterOptions{IsAll: true}

type WriteOptions struct {
	Sync bool
}

var Sync = &WriteOptions{Sync: true}

var NoSync = &WriteOptions{Sync: false}

func (o *WriteOptions) GetSync() bool {
	return o == nil || o.Sync
}

type CompactEnv struct {
	StartHour     int
	EndHour       int
	DeletePercent float64
	BitreeMaxSize int64
	Interval      int
}

type BitableOptions = options.BitableOptions

type Options struct {
	BytesPerSync                int
	Comparer                    *Comparer
	DisableWAL                  bool
	EventListener               EventListener
	MemTableSize                int
	MemTableStopWritesThreshold int
	WALBytesPerSync             int
	WALDir                      string
	WALMinSyncInterval          func() time.Duration
	FS                          vfs.FS
	Logger                      Logger
	Id                          int
	Verbose                     bool
	LogTag                      string
	DataType                    string
	CompressionType             int
	DeleteFileInternal          int
	UseBithash                  bool
	UseBitable                  bool
	BitableOpts                 *options.BitableOptions
	AutoCompact                 bool
	CompactInfo                 CompactEnv
	CacheSize                   int64
	CacheType                   int
	CacheShards                 int
	CacheHashSize               int
	UseMapIndex                 bool
	UsePrefixCompress           bool
	UseBlockCompress            bool
	BlockCacheSize              int64
	FlushReporter               func(int)
	KeyHashFunc                 func([]byte) int
	KvCheckExpireFunc           func(int, []byte, []byte) bool
	KvTimestampFunc             func([]byte, uint8) (bool, uint64)
	IOWriteLoadThresholdFunc    func() bool
	KeyPrefixDeleteFunc         func([]byte) uint64

	private struct {
		logInit  bool
		optspool *options.OptionsPool
	}
}

func (o *Options) ensureOptionsPool(optspool *options.OptionsPool) *options.OptionsPool {
	if optspool == nil {
		optspool = options.InitDefaultsOptionsPool()
	}

	optspool.BaseOptions.Id = o.Id
	optspool.BaseOptions.FS = o.FS
	optspool.BaseOptions.Cmp = o.Comparer.Compare
	optspool.BaseOptions.Logger = o.Logger
	optspool.BaseOptions.BytesPerSync = o.BytesPerSync
	optspool.BaseOptions.Compressor = compress.SetCompressor(o.CompressionType)
	optspool.BaseOptions.UseBithash = o.UseBithash
	optspool.BaseOptions.UseBitable = o.UseBitable
	optspool.BaseOptions.UseMapIndex = o.UseMapIndex
	optspool.BaseOptions.UsePrefixCompress = o.UsePrefixCompress
	optspool.BaseOptions.UseBlockCompress = o.UseBlockCompress
	optspool.BaseOptions.KeyHashFunc = o.KeyHashFunc
	optspool.BaseOptions.BitpageBlockCacheSize = consts.BitpageDefaultBlockCacheSize
	if o.UseBlockCompress && o.BlockCacheSize > 0 {
		bitpageBlockCacheSize := o.BlockCacheSize / int64(consts.DefaultBitowerNum)
		if bitpageBlockCacheSize > consts.BitpageDefaultBlockCacheSize {
			optspool.BaseOptions.BitpageBlockCacheSize = bitpageBlockCacheSize
		}
	}
	if o.KvCheckExpireFunc != nil {
		optspool.BaseOptions.KvCheckExpireFunc = o.KvCheckExpireFunc
	}
	if o.KvTimestampFunc != nil {
		optspool.BaseOptions.KvTimestampFunc = o.KvTimestampFunc
	}
	if o.KeyPrefixDeleteFunc != nil {
		optspool.BaseOptions.KeyPrefixDeleteFunc = o.KeyPrefixDeleteFunc
	}

	if o.UseBitable {
		o.BitableOpts.Options = optspool.BaseOptions
		optspool.BitableOptions = o.BitableOpts
	}

	dflOpts := &base.DFLOption{
		Logger:         o.Logger,
		DeleteInterval: o.DeleteFileInternal,
	}
	if o.IOWriteLoadThresholdFunc != nil {
		dflOpts.IOWriteLoadThresholdCB = o.IOWriteLoadThresholdFunc
	} else {
		dflOpts.IOWriteLoadThresholdCB = options.DefaultIOWriteLoadThresholdFunc
	}
	optspool.BaseOptions.DeleteFilePacer.Run(dflOpts)

	return optspool
}

func (o *Options) EnsureDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	if o.BytesPerSync <= 0 {
		o.BytesPerSync = consts.DefaultBytesPerSync
	}
	if o.Comparer == nil {
		o.Comparer = DefaultComparer
	}
	if o.MemTableSize <= 0 {
		o.MemTableSize = consts.DefaultMemTableSize
	}
	o.MemTableSize = o.MemTableSize / consts.DefaultBitowerNum
	if o.MemTableStopWritesThreshold <= 0 {
		o.MemTableStopWritesThreshold = consts.DefaultMemTableStopWritesThreshold
	}
	if o.CacheSize <= 0 {
		o.CacheSize = 0
	} else if o.CacheSize > 0 {
		if o.CacheSize < consts.DefaultCacheSize {
			o.CacheSize = consts.DefaultCacheSize
		}
		if o.CacheType <= 0 || o.CacheType > consts.CacheTypeLfu {
			o.CacheType = consts.CacheTypeLfu
		}
		if o.CacheType == consts.CacheTypeLfu {
			o.CacheShards = consts.DefaultLfuCacheShards
		} else {
			o.CacheShards = consts.DefaultLruCacheShards
			if o.CacheHashSize <= 0 {
				o.CacheHashSize = consts.DefaultLruCacheHashSize
			}
		}
	}
	if o.DeleteFileInternal == 0 {
		o.DeleteFileInternal = consts.DeletionFileInterval
	}
	if !o.private.logInit {
		o.Logger = base.NewLogger(o.Logger, o.LogTag)
		if o.Verbose {
			o.EventListener = MakeLoggingEventListener(o.Logger)
		} else {
			o.EventListener.EnsureDefaults(o.Logger)
		}
		o.private.logInit = true
	}
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.CompactInfo.StartHour <= 0 {
		o.CompactInfo.StartHour = 0
	}
	if o.CompactInfo.EndHour <= 0 {
		o.CompactInfo.EndHour = 23
	}
	if o.CompactInfo.EndHour <= o.CompactInfo.StartHour {
		o.CompactInfo.EndHour = o.CompactInfo.StartHour + 1
	}
	if o.CompactInfo.EndHour > 23 {
		o.CompactInfo.EndHour = 23
	}
	if o.CompactInfo.DeletePercent < 0 || o.CompactInfo.DeletePercent >= 1 {
		o.CompactInfo.DeletePercent = consts.DefaultDeletePercent
	}
	if o.CompactInfo.BitreeMaxSize <= 0 {
		o.CompactInfo.BitreeMaxSize = consts.UseBitableBitreeMaxSize
	}
	if o.CompactInfo.Interval == 0 {
		o.CompactInfo.Interval = consts.DefaultCompactInterval
	}
	if o.CompactInfo.Interval < consts.MinCompactInterval {
		o.CompactInfo.Interval = consts.MinCompactInterval
	}
	if o.UseBitable {
		o.BitableOpts = options.DefaultBitableOptions
	}
	if o.IOWriteLoadThresholdFunc == nil {
		o.IOWriteLoadThresholdFunc = options.DefaultIOWriteLoadThresholdFunc
	}
	if o.KeyHashFunc == nil {
		o.KeyHashFunc = options.DefaultKeyHashFunc
	}

	return o
}

func (o *Options) Clone() *Options {
	n := &Options{}
	if o != nil {
		*n = *o
	}
	return n
}
