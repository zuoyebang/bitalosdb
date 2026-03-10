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

package vectortable

import (
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

type VectorIterator struct {
	g, s  uint32
	v     *VectorTable
	final bool
}

func (v *VectorTable) NewIterator() base.VectorTableIterator {
	iter := &VectorIterator{
		v: v,
	}
	return iter
}

func (it *VectorIterator) First() (
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, version uint64,
	slotId uint16, size uint32, pre, next uint64, value []byte, final bool) {
	it.g, it.s = 0, 0
	return it.Next()
}

func (it *VectorIterator) Next() (
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, version uint64,
	slotId uint16, size uint32, pre, next uint64, value []byte, final bool) {
	var err error
	var rwLockPos uint32

	it.v.stable.rehashLock.RLock()
	defer it.v.stable.rehashLock.RUnlock()

	for {
		if it.g >= uint32(len(it.v.stable.groups)) {
			final = true
			it.final = true
			return
		}

		rwLockPos = it.v.stable.getRwLockPos(it.g)
		it.v.stable.rwLock[rwLockPos].RLock()

		for {
			if it.v.stable.ctrl[it.g][it.s] != simd.Empty && it.v.stable.ctrl[it.g][it.s] != simd.Tombstone {
				hashHL, offset := it.v.stable.unmarshalItemMeta(it.v.stable.groups[it.g][it.s])
				imOffset := uint64(offset) << alignDisplacement
				var hashHH uint32
				l, hashHH = it.v.stable.kvHolder.getHashInfo(imOffset)
				h = uint64(hashHH)<<32 | uint64(hashHL)
				key, value, seqNum, dataType, timestamp, slotId, version, size, pre, next, err = it.v.stable.kvHolder.getKVCopy(imOffset)
				it.s++
				if err != nil {
					if it.v.stable.opts.Logger != nil {
						it.v.stable.opts.Logger.Errorf("VectorIterator: getKVCopy failed: %s", err.Error())
					}
					if it.s >= simd.GroupSize {
						it.s = 0
						break
					}
					continue
				}
				it.v.stable.rwLock[rwLockPos].RUnlock()
				if it.s >= simd.GroupSize {
					it.g++
					it.s = 0
				}
				return
			}
			it.s++
			if it.s >= simd.GroupSize {
				it.s = 0
				break
			}
		}
		it.v.stable.rwLock[rwLockPos].RUnlock()
		it.g++
	}
}

func (it *VectorIterator) SeekCursor(g, s uint32) (key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, version uint64,
	slotId uint16, size uint32, pre, next uint64, value []byte, final bool) {
	it.g, it.s = g, s
	return it.Next()
}
func (it *VectorIterator) GetCursor() (uint32, uint32, bool) {
	if it.final {
		return 0, 0, false
	}
	return it.g, it.s, true
}

func (it *VectorIterator) Close() error {
	it.g, it.s = 0, 0
	it.v = nil
	return nil
}

type innerVectorIterator struct {
	g, s  uint32
	v     *innerVectorTable
	final bool
}

func (v *innerVectorTable) NewIterator(opts *options.IterOptions) base.VectorTableIterator {
	iter := &innerVectorIterator{
		v: v,
	}
	return iter
}

func (it *innerVectorIterator) First() (
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, version uint64,
	slotId uint16, size uint32, pre, next uint64, value []byte, final bool) {
	it.g, it.s = 0, 0
	return it.Next()
}

func (it *innerVectorIterator) Next() (
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, version uint64,
	slotId uint16, size uint32, pre, next uint64, value []byte, final bool) {
	var err error
	var rwLockPos uint32

	it.v.rehashLock.RLock()
	defer it.v.rehashLock.RUnlock()

	for {
		if it.g >= uint32(len(it.v.groups)) {
			final = true
			it.final = true
			return
		}

		rwLockPos = it.v.getRwLockPos(it.g)
		it.v.rwLock[rwLockPos].RLock()

		for {
			if it.v.ctrl[it.g][it.s] != simd.Empty && it.v.ctrl[it.g][it.s] != simd.Tombstone {
				hashHL, offset := it.v.unmarshalItemMeta(it.v.groups[it.g][it.s])
				imOffset := uint64(offset) << alignDisplacement
				var hashHH uint32
				l, hashHH = it.v.kvHolder.getHashInfo(imOffset)
				h = uint64(hashHH)<<32 | uint64(hashHL)
				key, value, seqNum, dataType, timestamp, slotId, version, size, pre, next, err = it.v.kvHolder.getKVCopy(imOffset)
				it.s++
				if err != nil {
					if it.v.opts.Logger != nil {
						it.v.opts.Logger.Errorf("VectorIterator: getKVCopy failed: %s", err.Error())
					}
					if it.s >= simd.GroupSize {
						it.s = 0
						break
					}
					continue
				}
				it.v.rwLock[rwLockPos].RUnlock()
				if it.s >= simd.GroupSize {
					it.g++
					it.s = 0
				}
				return
			}
			it.s++
			if it.s >= simd.GroupSize {
				it.s = 0
				break
			}
		}
		it.v.rwLock[rwLockPos].RUnlock()
		it.g++
	}
}

func (it *innerVectorIterator) SeekCursor(g, s uint32) (
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, version uint64,
	slotId uint16, size uint32, pre, next uint64, value []byte, final bool,
) {
	it.g, it.s = g, s
	return it.Next()
}

func (it *innerVectorIterator) GetCursor() (uint32, uint32, bool) {
	if it.final {
		return 0, 0, false
	}
	return it.g, it.s, true
}

func (it *innerVectorIterator) Close() error {
	it.g, it.s = 0, 0
	it.v = nil
	return nil
}
