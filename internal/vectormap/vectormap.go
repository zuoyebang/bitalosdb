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
	"math"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
)

var _ base.VectorMemTable = &VectorMap{}

type Byte uint64

const (
	B  Byte = 1
	KB      = 1024 * B
	MB      = 1024 * KB
	GB      = 1024 * MB
	TB      = 1024 * GB
	PB      = 1024 * TB
)

const (
	MaxUint64 uint64 = 1<<64 - 1
)

type VectorMapOptions struct {
	HashSize uint32
	Shards   int
	MaxMem   int
	StoreKey bool
	Logger   base.Logger
}

type VectorMap struct {
	id           string
	shardsNum    int
	shardDataLen int
	shards       []*InnerVectorMap
	globalMask   uint64
	ds           []byte
	logger       base.Logger
}

func NewVectorMap(opts *VectorMapOptions) (vm *VectorMap) {
	vm = &VectorMap{
		logger: opts.Logger,
	}

	power := math.Ceil(math.Log2(float64(opts.Shards)))
	vm.shardsNum = int(math.Pow(2, power))
	globalMask := MaxUint64 >> (64 - uint32(power))
	c := uint32(math.Ceil(float64(opts.HashSize) / float64(vm.shardsNum)))

	vm.shards = make([]*InnerVectorMap, vm.shardsNum)
	vm.globalMask = globalMask
	vm.shardDataLen = opts.MaxMem / vm.shardsNum

	for i := range vm.shards {
		vm.shards[i] = NewInnerVectorMap(opts, i, c)
	}

	return vm
}

func (vm *VectorMap) NewData(buf []byte) {
	vm.ds = buf
	for i := range vm.shards {
		vm.shards[i].NewDataHolder(vm.ds[i*vm.shardDataLen : (i+1)*vm.shardDataLen])
	}
}

func (vm *VectorMap) Set(
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, slot uint16,
	version uint64, size uint32, pre, next uint64, value []byte,
) (err error) {
	return vm.slotAt(h).Set(key, h, l, seqNum, dataType, timestamp, slot, version, size, pre, next, value)
}

func (vm *VectorMap) Has(h, l uint64) (seqNum uint64, ok bool) {
	return vm.slotAt(h).Has(h, l)
}

func (vm *VectorMap) GetValue(h, l uint64) (
	value []byte, seqNum uint64, dataType uint8, timestamp uint64,
	slot uint16, closer func(), err error,
) {
	return vm.slotAt(h).Get(h, l)
}

func (vm *VectorMap) GetMeta(h, l uint64) (
	seqNum uint64, dataType uint8, timestamp uint64, slot uint16,
	version uint64, size uint32, pre, next uint64, err error,
) {
	return vm.slotAt(h).GetMeta(h, l)
}

func (vm *VectorMap) GetKeySize() (size uint32) {
	for _, s := range vm.shards {
		size += s.Size()
	}
	return
}

func (vm *VectorMap) Delete(h, l, seqNum uint64) (ok bool) {
	return vm.slotAt(h).Delete(h, l, seqNum)
}

func (vm *VectorMap) SetTimestamp(h, l uint64, seqNum, timestamp uint64, dt uint8) error {
	err := vm.slotAt(h).SetTimestamp(h, l, seqNum, timestamp, dt)
	return err
}

func (vm *VectorMap) GetTimestamp(h, l uint64) (seqNum, timestamp uint64, dt uint8, err error) {
	return vm.slotAt(h).GetTimestamp(h, l)
}

//go:inline
func (vm *VectorMap) slotAt(hi uint64) *InnerVectorMap {
	return vm.shards[hi%uint64(vm.shardsNum)]
}

func (vm *VectorMap) SetId(id string) {
	vm.id = id
}

func (vm *VectorMap) FreeData() {
	if vm.ds != nil {
		manual.Free(vm.ds)
		vm.ds = nil
		vm.logger.Infof("vectormap(%s) manual free success", vm.id)
	}
}

func (vm *VectorMap) Reset() {
	for i := range vm.shards {
		vm.shards[i].Reset()
	}
	vm.FreeData()
}

func (vm *VectorMap) DataSize() int {
	return len(vm.ds)
}

func (vm *VectorMap) MSync() error {
	return nil
}

func (vm *VectorMap) Close(isFree bool) error {
	for i := range vm.shards {
		vm.shards[i].Close()
	}
	vm.FreeData()
	return nil
}
