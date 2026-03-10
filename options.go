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
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

type IterOptions = options.IterOptions

type CompactEnv struct {
	StartHour            int
	EndHour              int
	BithashDeletePercent float64
	VtGCThreshold        float64
}

type Options struct {
	BytesPerSync                   int
	Comparer                       *Comparer
	FS                             vfs.FS
	Logger                         Logger
	CompressionType                int
	Verbose                        bool
	LogTag                         string
	DeleteFileInternal             int
	DisableStoreKey                bool
	VectorTableCount               uint16
	VectorTableHashSize            uint32
	VmTableSize                    int
	VmTableStopWritesThreshold     int
	MemTableSize                   int
	MemTableStopWritesThreshold    int
	GetNowTimestamp                func() uint64
	IOWriteLoadThresholdFunc       func() bool
	MaxKeySize                     int
	MaxSubKeySize                  int
	MaxValueSize                   int
	FlushPrefixDeleteKeyMultiplier int
	FlushFileLifetime              int
	BitpageFlushSize               uint64
	BitpageSplitSize               uint64
	BitpageDisableMiniVi           bool
	BitpageTaskWorkerNum           int
	BithashTableSize               int
	AutoCompact                    bool
	CompactInfo                    CompactEnv
	FlushReporter                  func(int)

	private struct {
		logInit  bool
		optspool *options.OptionsPool
	}
}

func (o *Options) ensureOptionsPool(optspool *options.OptionsPool) *options.OptionsPool {
	if optspool == nil {
		optspool = options.InitOptionsPool()
	}

	optspool.BaseOptions.FS = o.FS
	optspool.BaseOptions.Cmp = o.Comparer.Compare
	optspool.BaseOptions.Logger = o.Logger
	optspool.BaseOptions.Compressor = compress.SetCompressor(o.CompressionType)
	optspool.BaseOptions.BytesPerSync = o.BytesPerSync
	optspool.BaseOptions.FlushPrefixDeleteKeyMultiplier = o.FlushPrefixDeleteKeyMultiplier
	optspool.BaseOptions.FlushFileLifetime = o.FlushFileLifetime
	optspool.BaseOptions.BitpageFlushSize = o.BitpageFlushSize
	optspool.BaseOptions.BitpageSplitSize = o.BitpageSplitSize
	optspool.BaseOptions.BitpageDisableMiniVi = o.BitpageDisableMiniVi
	optspool.BithashOptions.TableMaxSize = o.BithashTableSize
	if o.GetNowTimestamp != nil {
		optspool.BaseOptions.GetNowTimestamp = o.GetNowTimestamp
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
		o.BytesPerSync = consts.BytesPerSyncDefault
	}
	if o.Comparer == nil {
		o.Comparer = DefaultComparer
	}
	if o.DeleteFileInternal == 0 {
		o.DeleteFileInternal = consts.DeletionFileInterval
	}
	if o.FS == nil {
		o.FS = vfs.Default
	}

	if o.VmTableSize <= 0 {
		o.VmTableSize = consts.VmTableSize
	}
	o.VmTableSize = o.VmTableSize / consts.MemIndexShardNum
	if o.VmTableStopWritesThreshold <= 0 {
		o.VmTableStopWritesThreshold = consts.VmTableStopWritesThreshold
	}
	if o.MemTableSize <= 0 {
		o.MemTableSize = consts.MemTableSize
	}
	o.MemTableSize = o.MemTableSize / consts.MemIndexShardNum
	if o.MemTableStopWritesThreshold <= 0 {
		o.MemTableStopWritesThreshold = consts.MemTableStopWritesThreshold
	}

	if o.VectorTableHashSize <= 0 {
		o.VectorTableHashSize = consts.VectorTableHashSizeDefault
	}
	if o.VectorTableCount == 0 {
		o.VectorTableCount = consts.VectorTableCountDefault
	} else if o.VectorTableCount > consts.VectorTableMaxShards {
		o.VectorTableCount = consts.VectorTableMaxShards
	}
	//o.VectorTableHashSize = o.VectorTableHashSize / uint32(o.VectorTableCount)

	if o.IOWriteLoadThresholdFunc == nil {
		o.IOWriteLoadThresholdFunc = options.DefaultIOWriteLoadThresholdFunc
	}

	if o.MaxKeySize == 0 {
		o.MaxKeySize = consts.MaxKeySizeDefault
	}
	if o.MaxSubKeySize == 0 {
		o.MaxSubKeySize = consts.MaxSubKeySizeDefault
	}
	if o.MaxValueSize == 0 {
		o.MaxValueSize = consts.MaxValueSizeDefault
	}

	if o.FlushPrefixDeleteKeyMultiplier == 0 {
		o.FlushPrefixDeleteKeyMultiplier = consts.DefaultFlushPrefixDeleteKeyMultiplier
	}
	if o.FlushFileLifetime == 0 {
		o.FlushFileLifetime = consts.DefaultFlushFileLifetime
	}

	if o.BithashTableSize == 0 {
		o.BithashTableSize = consts.BithashTableSize
	}

	if o.BitpageFlushSize == 0 {
		o.BitpageFlushSize = consts.BitpageFlushSize
	}
	if o.BitpageSplitSize == 0 {
		o.BitpageSplitSize = consts.BitpageSplitSize
	}

	if o.BitpageTaskWorkerNum <= 0 || o.BitpageTaskWorkerNum > consts.MemIndexShardNum {
		o.BitpageTaskWorkerNum = consts.BitpageTaskWorkerNum
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
	if o.CompactInfo.BithashDeletePercent < 0 || o.CompactInfo.BithashDeletePercent >= 1 {
		o.CompactInfo.BithashDeletePercent = consts.BithashDeletePercentDefault
	}
	if o.CompactInfo.VtGCThreshold < 0 || o.CompactInfo.VtGCThreshold >= 1 {
		o.CompactInfo.VtGCThreshold = consts.VectorTableGCThresholdDefault
	}

	if !o.private.logInit {
		o.Logger = base.NewLogger(o.Logger, o.LogTag)
		o.private.logInit = true
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
