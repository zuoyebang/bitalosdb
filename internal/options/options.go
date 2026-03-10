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

package options

import (
	"os"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/bitask"
	"github.com/zuoyebang/bitalosdb/v2/internal/compress"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/statemachine"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

type IterOptions struct {
	LowerBound []byte
	UpperBound []byte
	Logger     base.Logger
	SlotId     uint32
	IsAll      bool
	DataType   uint8
	IsTest     bool
}

func (o *IterOptions) GetLowerBound() []byte {
	if o == nil {
		return nil
	}
	return o.LowerBound
}

func (o *IterOptions) GetUpperBound() []byte {
	if o == nil {
		return nil
	}
	return o.UpperBound
}

func (o *IterOptions) GetLogger() base.Logger {
	if o == nil || o.Logger == nil {
		return base.DefaultLogger
	}
	return o.Logger
}

func (o *IterOptions) GetSlotId() uint32 {
	if o == nil {
		return 0
	}
	return o.SlotId
}

func (o *IterOptions) IsGetAll() bool {
	return o == nil || o.IsAll
}

type CacheOptions struct {
	Size     int64
	Shards   int
	HashSize int
	Logger   base.Logger
}

type Options struct {
	Id                             int
	FS                             vfs.FS
	Cmp                            base.Compare
	Logger                         base.Logger
	Compressor                     compress.Compressor
	UseBithash                     bool
	BytesPerSync                   int
	KvSeparateSize                 int
	BitpageFlushSize               uint64
	BitpageSplitSize               uint64
	BitpageTaskPushFunc            func(*bitask.BitpageTaskData)
	BitpageDisableMiniVi           bool
	DbState                        *statemachine.DbStateMachine
	DeleteFilePacer                *base.DeletionFileLimiter
	KeyHashFunc                    func([]byte) int
	GetNowTimestamp                func() uint64
	FlushPrefixDeleteKeyMultiplier int
	FlushFileLifetime              int
	TableMmapBlockSize             uint32
}

func (o *Options) Clone() *Options {
	n := &Options{}
	if o != nil {
		*n = *o
	}
	return n
}

type BdbOptions struct {
	*Options
	Index           int
	Timeout         time.Duration
	NoGrowSync      bool
	NoFreelistSync  bool
	FreelistType    string
	ReadOnly        bool
	MmapFlags       int
	InitialMmapSize int
	AllocSize       int
	PageSize        int
	NoSync          bool
	OpenFile        func(string, int, os.FileMode) (*os.File, error)
	Mlock           bool
	CheckPageFreed  func(uint32) bool
}

type BithashOptions struct {
	*Options
	TableMaxSize int
	Index        int
}

type BitpageOptions struct {
	*Options
	Index           int
	SplitNum        int
	UseVi           bool
	BithashDeleteCB func(uint32) error
}

type BitreeOptions struct {
	*Options
	Index       int
	BdbOpts     *BdbOptions
	BitpageOpts *BitpageOptions
	BithashOpts *BithashOptions
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

var DefaultGetNowTimestamp = func() uint64 {
	return uint64(time.Now().UnixMilli())
}

var DefaultIOWriteLoadThresholdFunc = func() bool {
	return true
}

var DefaultKeyPrefixDeleteFunc = func(k []byte) uint64 {
	return kkv.DecodeKeyVersion(k)
}

func NewDefaultDeletionFileLimiter() *base.DeletionFileLimiter {
	dflOpts := &base.DFLOption{
		Logger:                 base.DefaultLogger,
		IOWriteLoadThresholdCB: DefaultIOWriteLoadThresholdFunc,
		DeleteInterval:         consts.DeletionFileInterval,
	}
	return base.NewDeletionFileLimiter(dflOpts)
}

type BitupleOptions struct {
	*Options
	BitreeOpts             *BitreeOptions
	Index                  int
	TupleCount             uint16
	VectorTableHashSize    uint32
	VectorTableGCThreshold float64
	DisableStoreKey        bool
	GetNextSeqNum          func() uint64
	CheckExpireFunc        func(slotId uint16, hi, lo uint64) bool
}

type VectorTableOptions struct {
	*Options
	Index               int
	Dirname             string
	Filename            string
	HashSize            uint32
	MmapBlockSize       uint32
	OpenFiles           []string
	ReleaseFunc         func([]string)
	CheckExpireFunc     func(slotId uint16, hi, lo uint64) bool
	DisableExpireManage bool
	StoreKey            bool
	WithSlot            bool
	WriteBuffer         bool
	GCFileSize          uint64
	GCThreshold         float64
}

func (o *VectorTableOptions) Clone() *VectorTableOptions {
	n := &VectorTableOptions{}
	if o != nil {
		*n = *o
	}
	return n
}

type OptionsPool struct {
	BaseOptions    *Options
	BitupleOptions *BitupleOptions
	BitreeOptions  *BitreeOptions
	BithashOptions *BithashOptions
	BitpageOptions *BitpageOptions
	BdbOptions     *BdbOptions
	DbState        *statemachine.DbStateMachine
}

func InitOptionsPool() *OptionsPool {
	optspool := &OptionsPool{
		DbState: statemachine.NewDbStateMachine(),
	}

	optspool.BaseOptions = &Options{
		FS:                             vfs.Default,
		Cmp:                            base.DefaultComparer.Compare,
		Logger:                         base.DefaultLogger,
		Compressor:                     compress.NoCompressor,
		BytesPerSync:                   consts.BytesPerSyncDefault,
		DeleteFilePacer:                NewDefaultDeletionFileLimiter(),
		GetNowTimestamp:                DefaultGetNowTimestamp,
		DbState:                        optspool.DbState,
		UseBithash:                     true,
		BitpageDisableMiniVi:           false,
		BitpageFlushSize:               consts.BitpageFlushSize,
		BitpageSplitSize:               consts.BitpageSplitSize,
		FlushPrefixDeleteKeyMultiplier: consts.DefaultFlushPrefixDeleteKeyMultiplier,
		FlushFileLifetime:              consts.DefaultFlushFileLifetime,
		TableMmapBlockSize:             consts.TableMmapBlockSizeDefault,
		KvSeparateSize:                 consts.KvSeparateSize,
	}

	btuOpts := &BitupleOptions{
		Options:             optspool.BaseOptions,
		TupleCount:          consts.VectorTableCountDefault,
		VectorTableHashSize: consts.VectorTableHashSizeDefault,
	}

	brOpts := &BitreeOptions{
		Options: optspool.BaseOptions,
	}

	bdbOpts := &BdbOptions{
		Options:         optspool.BaseOptions,
		Timeout:         time.Second,
		InitialMmapSize: consts.BdbInitialSize,
		AllocSize:       consts.BdbAllocSize,
		NoSync:          true,
		NoGrowSync:      true,
		Mlock:           false,
		FreelistType:    consts.BdbFreelistMapType,
		PageSize:        consts.BdbPageSize,
		CheckPageFreed:  func(uint32) bool { return false },
	}

	bpOpts := &BitpageOptions{
		Options:         optspool.BaseOptions,
		SplitNum:        consts.BitpageSplitNum,
		BithashDeleteCB: nil,
	}

	bhOpts := &BithashOptions{
		Options:      optspool.BaseOptions,
		TableMaxSize: consts.BithashTableSize,
	}

	optspool.BdbOptions = bdbOpts
	optspool.BitpageOptions = bpOpts
	optspool.BitreeOptions = brOpts
	optspool.BithashOptions = bhOpts
	optspool.BitupleOptions = btuOpts

	return optspool
}

func (o *OptionsPool) CloneBitupleOptions() *BitupleOptions {
	bropts := &BitreeOptions{}
	*bropts = *(o.BitreeOptions)
	bdbopts := &BdbOptions{}
	*bdbopts = *(o.BdbOptions)
	bpopts := &BitpageOptions{}
	*bpopts = *(o.BitpageOptions)
	bhopts := &BithashOptions{}
	*bhopts = *(o.BithashOptions)
	bropts.BdbOpts = bdbopts
	bropts.BitpageOpts = bpopts
	bropts.BithashOpts = bhopts

	btuopts := &BitupleOptions{}
	*btuopts = *(o.BitupleOptions)
	btuopts.BitreeOpts = bropts
	return btuopts
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

	bropts.BdbOpts = bdbopts
	bropts.BitpageOpts = bpopts
	bropts.BithashOpts = bhopts
	return bropts
}

func (o *OptionsPool) CloneBitpageOptions() *BitpageOptions {
	bpopts := &BitpageOptions{}
	*bpopts = *(o.BitpageOptions)
	return bpopts
}

func (o *OptionsPool) Close() {
	o.BaseOptions.DeleteFilePacer.Close()
}

func InitTestOptionsPool() *OptionsPool {
	optsPool := InitOptionsPool()
	optsPool.BaseOptions.DeleteFilePacer.Run(nil)
	return optsPool
}
