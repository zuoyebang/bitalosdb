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

package options

import (
	"encoding/binary"
	"os"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bitask"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

var DefaultBitableOptions = &BitableOptions{
	MemTableSize:                consts.BitableMemTableSize,
	MemTableStopWritesThreshold: consts.BitableMemTableStopWritesThreshold,
	L0FileSize:                  consts.BitableL0FileSize,
	CacheSize:                   consts.BitableCacheSize,
	L0CompactionFileThreshold:   consts.BitableL0CompactionFileThreshold,
	L0CompactionThreshold:       consts.BitableL0CompactionThreshold,
	L0StopWritesThreshold:       consts.BitableL0StopWritesThreshold,
	LBaseMaxBytes:               consts.BitableLBaseMaxBytes,
	MaxOpenFiles:                consts.BitableMaxOpenFiles,
}

var DefaultBdbOptions = &BdbOptions{
	Options: &Options{
		Logger: base.DefaultLogger,
		Cmp:    base.DefaultComparer.Compare,
	},
	Timeout:      0,
	NoGrowSync:   false,
	FreelistType: consts.BdbFreelistArrayType,
}

var DefaultKeyHashFunc = func(k []byte) int {
	if len(k) < 2 {
		return int(k[0])
	}
	return int(binary.BigEndian.Uint16(k[0:2]))
}

var DefaultKvCheckExpireFunc = func(id int, k, v []byte) bool { return false }

var DefaultCheckExpireFunc = func(k, v []byte) bool { return false }

var DefaultKvTimestampFunc = func(v []byte, t uint8) (bool, uint64) {
	if t == 2 {
		return false, uint64(time.Now().UnixMilli())
	}
	return false, 0
}

var DefaultIOWriteLoadThresholdFunc = func() bool {
	return true
}

var DefaultKeyPrefixDeleteFunc = func(k []byte) uint64 {
	return 0
}

var TestKvCheckExpireFunc = func(id int, k, v []byte) bool {
	if len(v) == 0 {
		return false
	} else if uint8(v[0]) == 1 {
		timestamp := binary.BigEndian.Uint64(v[1:9])
		if timestamp == 0 {
			return false
		}
		if timestamp <= uint64(time.Now().UnixMilli()) {
			return true
		}
		return false
	}

	return false
}

var TestKvTimestampFunc = func(v []byte, t uint8) (bool, uint64) {
	if t == 2 {
		return false, uint64(time.Now().UnixMilli())
	}

	if uint8(v[0]) != 1 {
		return false, 0
	}

	return true, binary.BigEndian.Uint64(v[1:9])
}

var TestKeyHashFunc = func(k []byte) int {
	return int(binary.BigEndian.Uint16(k[0:2]))
}

var TestKeyPrefixDeleteFunc = func(k []byte) uint64 {
	if len(k) < 10 {
		return 0
	}
	return binary.LittleEndian.Uint64(k[2:10])
}

type CacheOptions struct {
	Size     int64
	Shards   int
	HashSize int
	Logger   base.Logger
}

type Options struct {
	Id                    int
	FS                    vfs.FS
	Cmp                   base.Compare
	Logger                base.Logger
	Compressor            compress.Compressor
	UseBithash            bool
	UseBitable            bool
	UseMapIndex           bool
	UsePrefixCompress     bool
	UseBlockCompress      bool
	BitpageBlockCacheSize int64
	BytesPerSync          int
	KvSeparateSize        int
	BitpageFlushSize      uint64
	BitpageSplitSize      uint64
	BitpageTaskPushFunc   func(*bitask.BitpageTaskData)
	DbState               *statemachine.DbStateMachine
	DeleteFilePacer       *base.DeletionFileLimiter
	KeyHashFunc           func([]byte) int
	KvCheckExpireFunc     func(int, []byte, []byte) bool
	KvTimestampFunc       func([]byte, uint8) (bool, uint64)
	KeyPrefixDeleteFunc   func([]byte) uint64
}

func (o *Options) KvCheckExpire(key, value []byte) bool {
	return o.KvCheckExpireFunc(o.Id, key, value)
}

type BdbOptions struct {
	*Options
	Index             int
	Timeout           time.Duration
	NoGrowSync        bool
	NoFreelistSync    bool
	FreelistType      string
	ReadOnly          bool
	MmapFlags         int
	InitialMmapSize   int
	PageSize          int
	NoSync            bool
	OpenFile          func(string, int, os.FileMode) (*os.File, error)
	Mlock             bool
	CheckPageSplitted func(uint32) bool
}

type BithashOptions struct {
	*Options
	TableMaxSize int
	Index        int
}

type BitpageOptions struct {
	*Options
	Index           int
	BithashDeleteCB func(uint32) error
	BitableDeleteCB func([]byte) error
	CheckExpireCB   func([]byte, []byte) bool
}

type BitableOptions struct {
	*Options
	Index                       int
	MemTableSize                int
	MemTableStopWritesThreshold int
	L0CompactionFileThreshold   int
	L0CompactionThreshold       int
	L0StopWritesThreshold       int
	LBaseMaxBytes               int64
	L0FileSize                  int64
	CacheSize                   int64
	MaxOpenFiles                int
	CheckExpireCB               func([]byte, []byte) bool
}

type BitreeOptions struct {
	*Options
	Index              int
	BdbOpts            *BdbOptions
	BitpageOpts        *BitpageOptions
	BithashOpts        *BithashOptions
	BitableOpts        *BitableOptions
	IsFlushedBitableCB func() bool
}

type OptionsPool struct {
	BaseOptions    *Options
	BitableOptions *BitableOptions
	BithashOptions *BithashOptions
	BitpageOptions *BitpageOptions
	BdbOptions     *BdbOptions
	BitreeOptions  *BitreeOptions
	DbState        *statemachine.DbStateMachine
}

func (o *OptionsPool) CloneBitreeOptions() *BitreeOptions {
	bropts := &BitreeOptions{}
	*bropts = *(o.BitreeOptions)
	bdbopts := &BdbOptions{}
	*bdbopts = *(o.BdbOptions)
	bpopts := &BitpageOptions{}
	*bpopts = *(o.BitpageOptions)
	bhopts := &BithashOptions{}
	*bhopts = *(o.BithashOptions)
	btopts := &BitableOptions{}
	*btopts = *(o.BitableOptions)

	bropts.BdbOpts = bdbopts
	bropts.BitpageOpts = bpopts
	bropts.BithashOpts = bhopts
	bropts.BitableOpts = btopts
	return bropts
}

func (o *OptionsPool) CloneBitpageOptions() *BitpageOptions {
	bpopts := &BitpageOptions{}
	*bpopts = *(o.BitpageOptions)
	return bpopts
}

func (o *OptionsPool) CloneBithashOptions() *BithashOptions {
	bhopts := &BithashOptions{}
	*bhopts = *(o.BithashOptions)
	return bhopts
}

func (o *OptionsPool) Close() {
	o.BaseOptions.DeleteFilePacer.Close()
}

func InitDefaultsOptionsPool() *OptionsPool {
	optspool := &OptionsPool{
		DbState: statemachine.NewDbStateMachine(),
	}

	optspool.BaseOptions = &Options{
		FS:                    vfs.Default,
		Cmp:                   base.DefaultComparer.Compare,
		Logger:                base.DefaultLogger,
		Compressor:            compress.NoCompressor,
		UseBithash:            true,
		UseBitable:            false,
		UseMapIndex:           true,
		UsePrefixCompress:     true,
		UseBlockCompress:      false,
		BitpageBlockCacheSize: consts.BitpageDefaultBlockCacheSize,
		BytesPerSync:          consts.DefaultBytesPerSync,
		KvSeparateSize:        consts.KvSeparateSize,
		BitpageFlushSize:      consts.BitpageFlushSize,
		BitpageSplitSize:      consts.BitpageSplitSize,
		DbState:               optspool.DbState,
		DeleteFilePacer:       NewDefaultDeletionFileLimiter(),
		KeyHashFunc:           DefaultKeyHashFunc,
		KvCheckExpireFunc:     DefaultKvCheckExpireFunc,
		KvTimestampFunc:       DefaultKvTimestampFunc,
		KeyPrefixDeleteFunc:   DefaultKeyPrefixDeleteFunc,
	}

	brOpts := &BitreeOptions{
		Options:            optspool.BaseOptions,
		IsFlushedBitableCB: func() bool { return false },
	}

	bdbOpts := &BdbOptions{
		Options:           optspool.BaseOptions,
		Timeout:           time.Second,
		InitialMmapSize:   consts.BdbInitialSize,
		NoSync:            true,
		NoGrowSync:        true,
		FreelistType:      consts.BdbFreelistMapType,
		PageSize:          consts.BdbPageSize,
		CheckPageSplitted: func(uint32) bool { return false },
	}

	bpOpts := &BitpageOptions{
		Options:         optspool.BaseOptions,
		BithashDeleteCB: func(uint32) error { return nil },
		BitableDeleteCB: func([]byte) error { return nil },
		CheckExpireCB:   DefaultCheckExpireFunc,
	}

	bhOpts := &BithashOptions{
		Options:      optspool.BaseOptions,
		TableMaxSize: consts.BithashTableMaxSize,
	}

	btOpts := DefaultBitableOptions
	btOpts.Options = optspool.BaseOptions
	btOpts.CheckExpireCB = DefaultCheckExpireFunc

	optspool.BdbOptions = bdbOpts
	optspool.BitpageOptions = bpOpts
	optspool.BitreeOptions = brOpts
	optspool.BithashOptions = bhOpts
	optspool.BitableOptions = btOpts

	return optspool
}

func InitTestDefaultsOptionsPool() *OptionsPool {
	optsPool := InitDefaultsOptionsPool()
	optsPool.BaseOptions.DeleteFilePacer.Run(nil)
	optsPool.BaseOptions.KeyPrefixDeleteFunc = TestKeyPrefixDeleteFunc
	return optsPool
}

func NewDefaultDeletionFileLimiter() *base.DeletionFileLimiter {
	dflOpts := &base.DFLOption{
		Logger:                 base.DefaultLogger,
		IOWriteLoadThresholdCB: DefaultIOWriteLoadThresholdFunc,
		DeleteInterval:         consts.DeletionFileInterval,
	}
	return base.NewDeletionFileLimiter(dflOpts)
}
