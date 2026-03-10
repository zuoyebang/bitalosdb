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
	"encoding/binary"
	"sync"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

const (
	rwLockSize = 2 << 10
	rwLockMask = rwLockSize - 1
)

type group [simd.GroupSize][8]byte

type InnerVectorMap struct {
	id         int
	ctrl       []simd.Metadata
	groups     []group
	groupSize  uint32
	resident   uint32
	dead       uint32
	limit      uint32
	memCap     int
	kvHolder   DataHolder
	putLock    sync.Mutex
	rwLock     []sync.RWMutex
	rwLockMask uint32
	storeKey   bool
	logger     base.Logger
}

func NewInnerVectorMap(o *VectorMapOptions, id int, sz uint32) *InnerVectorMap {
	groups := numGroups(sz)
	vm := &InnerVectorMap{
		id:         id,
		logger:     o.Logger,
		limit:      groups * simd.MaxAvgGroupLoad,
		memCap:     o.MaxMem,
		ctrl:       make([]simd.Metadata, groups),
		groups:     make([]group, groups),
		groupSize:  groups,
		rwLockMask: rwLockMask,
		rwLock:     make([]sync.RWMutex, rwLockSize),
		storeKey:   o.StoreKey,
	}

	for i := range vm.ctrl {
		for j := range vm.ctrl[i] {
			vm.ctrl[i][j] = simd.Empty
		}
	}

	return vm
}

func (v *InnerVectorMap) NewDataHolder(arenaBuf []byte) {
	if v.storeKey {
		v.kvHolder = NewDataHolder(arenaBuf)
	} else {
		v.kvHolder = NewDataHolderNokey(arenaBuf, v.logger)
	}
}

func (v *InnerVectorMap) Size() uint32 {
	return v.resident - v.dead
}

func (v *InnerVectorMap) Set(
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64,
	slot uint16, version uint64, size uint32, pre, next uint64, value []byte,
) (err error) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	v.putLock.Lock()
	defer v.putLock.Unlock()

	if v.resident >= v.limit {
		return base.ErrTableFull
	}

	g := simd.ProbeStart64(hi, len(v.groups))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].Lock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			if hashHL == hl {
				hashL, hashHH, ver, dt := v.kvHolder.getHashAndVersionDT(offset)
				if hashL == l && hashHH == hh {
					if v.kvHolder.checkSeq(offset, seqNum) {
						v.rwLock[rwLockPos].Unlock()
						return nil
					}

					if dataType != dt || ver != version {
						switch dataType {
						case kkv.DataTypeString:
							offset, err = v.kvHolder.set(h, l, key, value, dataType, timestamp, seqNum, slot)
						case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
							offset, err = v.kvHolder.setKKVMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size)
						case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
							offset, err = v.kvHolder.setKKVListMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size, pre, next)
						default:
							err = errors.Errorf("set unsupported datatype %d", dataType)
						}
						if err == nil {
							v.marshalItemMeta(v.groups[g][s][:], hl, offset)
							v.ctrl[g][s] = int8(lo)
						}
						v.rwLock[rwLockPos].Unlock()
						return err
					} else {
						switch dataType {
						case kkv.DataTypeString:
							err = v.kvHolder.update(offset, value, dataType, timestamp, seqNum, slot)
							if err != nil {
								if simd.MetaMatchEmpty(&v.ctrl[g]) != 0 {
									v.ctrl[g][s] = simd.Empty
									v.resident--
								} else {
									v.ctrl[g][s] = simd.Tombstone
									v.dead++
								}
							}
						case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
							v.kvHolder.updateKKVMeta(offset, dataType, timestamp, seqNum, slot, version, size)
						case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
							v.kvHolder.updateKKVListMeta(offset, dataType, timestamp, seqNum, slot, version, size, pre, next)
						default:
							err = errors.Errorf("set unsupported datatype %d", dataType)
						}
						v.rwLock[rwLockPos].Unlock()
						return err
					}
				}
			}
		}

		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			s := simd.NextMatch(&matches)
			var offset uint32
			switch dataType {
			case kkv.DataTypeString:
				offset, err = v.kvHolder.set(h, l, key, value, dataType, timestamp, seqNum, slot)
			case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
				offset, err = v.kvHolder.setKKVMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size)
			case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
				offset, err = v.kvHolder.setKKVListMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size, pre, next)
			default:
				err = errors.Errorf("unsupported datatype %d", dataType)
			}
			if err == nil {
				v.ctrl[g][s] = int8(lo)
				v.marshalItemMeta(v.groups[g][s][:], hl, offset)
				v.resident++
			}
			v.rwLock[rwLockPos].Unlock()
			return err
		}
		v.rwLock[rwLockPos].Unlock()
		g += 1
		if g >= uint32(len(v.groups)) {
			g = 0
		}
	}
}

func (v *InnerVectorMap) GetAll(h, l uint64) (
	key, value []byte, seqNum uint64, dataType uint8,
	timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, err error,
) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(offset)
				if hashL == l && hashHH == hh {
					key, value, seqNum, dataType, timestamp, slot, version, size, pre, next, err = v.kvHolder.getKV(offset)
					v.rwLock[rwLockPos].RUnlock()
					return
				}
			}
		}

		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			v.rwLock[rwLockPos].RUnlock()
			err = base.ErrNotFound
			return
		}
		v.rwLock[rwLockPos].RUnlock()
		g += 1
		if g >= uint32(len(v.groups)) {
			g = 0
		}
	}
}

func (v *InnerVectorMap) Get(h, l uint64) (
	value []byte, seqNum uint64, dataType uint8, timestamp uint64,
	slot uint16, closer func(), err error,
) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(offset)
				if hashL == l && hashHH == hh {
					value, seqNum, dataType, timestamp, slot, err = v.kvHolder.getValue(offset)
					v.rwLock[rwLockPos].RUnlock()
					return
				}
			}
		}

		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			v.rwLock[rwLockPos].RUnlock()
			err = base.ErrNotFound
			return
		}
		v.rwLock[rwLockPos].RUnlock()
		g += 1
		if g >= uint32(len(v.groups)) {
			g = 0
		}
	}
}

func (v *InnerVectorMap) GetMeta(h, l uint64) (
	seqNum uint64, dataType uint8, timestamp uint64, slot uint16,
	version uint64, size uint32, pre, next uint64, err error,
) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(offset)
				if hashL == l && hashHH == hh {
					seqNum, dataType, timestamp, slot, version, size, pre, next, err = v.kvHolder.getMeta(offset)
					v.rwLock[rwLockPos].RUnlock()
					return
				}
			}
		}
		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			v.rwLock[rwLockPos].RUnlock()
			err = base.ErrNotFound
			return
		}
		v.rwLock[rwLockPos].RUnlock()
		g += 1
		if g >= uint32(len(v.groups)) {
			g = 0
		}
	}
}

func (v *InnerVectorMap) Has(h, l uint64) (seqNum uint64, ok bool) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(offset)
				if hashL == l && hashHH == hh {
					seqNum = v.kvHolder.getSeqNum(offset)
					ok = true
					v.rwLock[rwLockPos].RUnlock()
					return
				}
			}
		}
		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			v.rwLock[rwLockPos].RUnlock()
			return seqNum, false
		}
		v.rwLock[rwLockPos].RUnlock()
		g += 1
		if g >= uint32(len(v.groups)) {
			g = 0
		}
	}
}

func (v *InnerVectorMap) Delete(h, l, seqNum uint64) (ok bool) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	v.putLock.Lock()
	defer v.putLock.Unlock()

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].Lock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(offset)
				if hashL == l && hashHH == hh {
					if !v.kvHolder.checkSeq(offset, seqNum) {
						if simd.MetaMatchEmpty(&v.ctrl[g]) != 0 {
							v.ctrl[g][s] = simd.Empty
							v.resident--
						} else {
							v.ctrl[g][s] = simd.Tombstone
							v.dead++
						}
					}
					ok = true
					v.rwLock[rwLockPos].Unlock()
					return
				}
			}
		}
		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			v.rwLock[rwLockPos].Unlock()
			ok = true
			return
		}
		v.rwLock[rwLockPos].Unlock()
		g += 1
		if g >= uint32(len(v.groups)) {
			g = 0
		}
	}
}

func (v *InnerVectorMap) SetTimestamp(h, l uint64, seqNum, ts uint64, datatype uint8) error {
	t := combine(ts, datatype)
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	v.putLock.Lock()
	defer v.putLock.Unlock()

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].Lock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			if hashHL == hl {
				hashL, hashHH, _, dt := v.kvHolder.getHashAndVersionDT(offset)
				if hashL == l && hashHH == hh && dt == datatype {
					if !v.kvHolder.checkSeq(offset, seqNum) {
						v.kvHolder.setTTL(offset, seqNum, t)
					}
					v.rwLock[rwLockPos].Unlock()
					return nil
				}
			}
		}
		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			v.rwLock[rwLockPos].Unlock()
			return base.ErrNotFound
		}
		v.rwLock[rwLockPos].Unlock()
		g += 1
		if g >= uint32(len(v.groups)) {
			g = 0
		}
	}
}

func (v *InnerVectorMap) GetTimestamp(h, l uint64) (seqNum, ttl uint64, dt uint8, err error) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(offset)
				if hashL == l && hashHH == hh {
					seqNum, ttl, dt = v.kvHolder.getTTL(offset)
					v.rwLock[rwLockPos].RUnlock()
					return
				}
			}
		}
		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			v.rwLock[rwLockPos].RUnlock()
			return 0, 0, 0, base.ErrNotFound
		}
		v.rwLock[rwLockPos].RUnlock()
		g += 1
		if g >= uint32(len(v.groups)) {
			g = 0
		}
	}
}

func (v *InnerVectorMap) Reset() {
	v.kvHolder = nil
	v.resident = 0
	v.dead = 0
	v.groups = nil
	v.groups = make([]group, v.groupSize)
	for i := range v.ctrl {
		for j := range v.ctrl[i] {
			v.ctrl[i][j] = simd.Empty
		}
	}
}

func (v *InnerVectorMap) Close() {
	v.ctrl = nil
	v.groups = nil
	v.kvHolder = nil
}

//go:inline
func combine(low56 uint64, high8 uint8) uint64 {
	return low56&maskLow56 | uint64(high8)<<56
}

//go:inline
func split(u uint64) (uint64, uint8) {
	return u & maskLow56, uint8(u >> 56)
}

//go:inline
func splitLow56(u uint64) uint64 {
	return u & maskLow56
}

//go:inline
func splitHigh8(u uint64) uint8 {
	return uint8(u >> 56)
}

func (v *InnerVectorMap) marshalItemMeta(buf []byte, hl uint32, itemOffset uint32) {
	binary.LittleEndian.PutUint32(buf[0:], hl)
	binary.LittleEndian.PutUint32(buf[4:], itemOffset)
	return
}

func (v *InnerVectorMap) unmarshalItemMeta(buf [8]byte) (hl uint32, itemOffset uint32) {
	hl = binary.LittleEndian.Uint32(buf[0:])
	itemOffset = binary.LittleEndian.Uint32(buf[4:])
	return
}

func (v *InnerVectorMap) getRwLockPos(n uint32) uint32 {
	return n & v.rwLockMask
}

func numGroups(n uint32) (groups uint32) {
	groups = (n + simd.MaxAvgGroupLoad - 1) / simd.MaxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}
