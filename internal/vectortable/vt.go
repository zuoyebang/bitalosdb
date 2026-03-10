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
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/humanize"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

var _ base.VectorMemTable = &VectorTable{}

type gcStatus struct {
	deleteCount  int
	expiredCount int
	expiredKSize int
	expiredVSize int
	writeKSize   int
	writeVSize   int
}

type VectorTable struct {
	options *options.VectorTableOptions
	stable  *innerVectorTable
	gc      *innerVectorTable
	rwVT    *innerVectorTable
	gcLock  sync.RWMutex
}

func (v *VectorTable) FreeData() {
	//TODO implement me
}

func (v *VectorTable) NewData(buf []byte) {
}

func (v *VectorTable) Reset() {
}

func (v *VectorTable) SyncHeader() {
	v.gcLock.RLock()
	v.rwVT.SyncHeader()
	v.gcLock.RUnlock()
}

func (v *VectorTable) SyncBuffer() error {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()

	return v.rwVT.syncBuffer()
}

func (v *VectorTable) MSync() error {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()

	return v.rwVT.sync()
}

func NewVectorTable(opts *options.VectorTableOptions) (*VectorTable, error) {
	vt := &VectorTable{
		options: opts,
	}
	var err error
	vt.stable, err = newInnerVectorTable(opts, opts.HashSize)
	if err != nil {
		return nil, err
	}
	vt.rwVT = vt.stable
	return vt, nil
}

func (v *VectorTable) Set(
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, slot uint16,
	version uint64, size uint32, pre, next uint64, value []byte,
) (err error) {
	v.gcLock.RLock()
	err = v.rwVT.Set(key, h, l, seqNum, dataType, timestamp, slot, version, size, pre, next, value)
	v.gcLock.RUnlock()
	return err
}

func (v *VectorTable) SetTest(ch chan struct{}, ci chan int,
	key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, slot uint16,
	version uint64, size uint32, pre, next uint64, value []byte,
) (err error) {
	v.gcLock.RLock()
	err = v.rwVT.SetTest(ch, ci, key, h, l, seqNum, dataType, timestamp, slot, version, size, pre, next, value)
	v.gcLock.RUnlock()
	return err
}

func (v *VectorTable) Has(h, l uint64) (seqNum uint64, ok bool) {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	if v.stable.kvHolder.GetStat() == StatGC {
		seqNum, ok = v.gc.Has(h, l)
		if ok {
			return seqNum, true
		} else {
			return v.stable.Has(h, l)
		}
	}
	return v.stable.Has(h, l)
}

func (v *VectorTable) GetValue(h, l uint64) (value []byte, seqNum uint64, dataType uint8, timestamp uint64, slot uint16, closer func(), err error) {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	if v.stable.kvHolder.GetStat() == StatGC {
		value, seqNum, dataType, timestamp, slot, closer, err = v.gc.Get(h, l)
		if err == nil || (err != base.ErrNotFound && err != io.EOF) {
			return
		}
	}
	return v.stable.Get(h, l)
}

func (v *VectorTable) GetMeta(h, l uint64) (seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, err error) {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	if v.stable.kvHolder.GetStat() == StatGC {
		seqNum, dataType, timestamp, slot, version, size, pre, next, err = v.gc.GetMeta(h, l)
		if err == nil || err != base.ErrNotFound {
			return
		}
	}
	return v.stable.GetMeta(h, l)
}

func (v *VectorTable) Size() (uint32, error) {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	if v.stable.kvHolder.GetStat() == StatGC {
		return 0, errors.New("vectortable: garbage collection in progress")
	}
	return v.stable.size(), nil
}

func (v *VectorTable) GetKeySize() uint32 {
	return v.stable.size()
}

func (v *VectorTable) Delete(h, l, seqNum uint64) (ok bool) {
	// TODO lock gc iterator
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	if v.stable.kvHolder.GetStat() == StatGC {
		ok = v.stable.Delete(h, l, seqNum)
		if v.gc != nil {
			v.gc.Delete(h, l, seqNum)
		}
		return
	}
	return v.stable.Delete(h, l, seqNum)
}

func (v *VectorTable) SetTimestamp(h, l uint64, seqNum, timestamp uint64, dt uint8) error {
	// TODO lock gc iterator
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	if v.stable.kvHolder.GetStat() == StatGC {
		e := v.stable.SetTimestamp(h, l, seqNum, timestamp, dt)
		if v.gc != nil {
			err := v.gc.SetTimestamp(h, l, seqNum, timestamp, dt)
			if err == nil {
				return nil
			}
		}
		return e
	}
	return v.stable.SetTimestamp(h, l, seqNum, timestamp, dt)
}

func (v *VectorTable) SetSize(h, l uint64, seqNum uint64, size uint32) error {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	if v.stable.kvHolder.GetStat() == StatGC {
		if v.gc != nil {
			err := v.gc.SetSize(h, l, seqNum, size)
			if err == nil {
				return nil
			}
		}
		return v.stable.SetSize(h, l, seqNum, size)
	}
	return v.stable.SetSize(h, l, seqNum, size)
}

func (v *VectorTable) GetTimestamp(h, l uint64) (seqNum, timestamp uint64, dt uint8, err error) {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	if v.stable.kvHolder.GetStat() == StatGC {
		seqNum, timestamp, dt, err = v.gc.GetTimestamp(h, l)
		if err == nil || err != base.ErrNotFound {
			return seqNum, timestamp, dt, err
		}
	}
	return v.stable.GetTimestamp(h, l)
}

func (v *VectorTable) Checkpoint(fs vfs.FS, destDir string) error {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()
	stat := v.stable.kvHolder.GetStat()
	if stat != StatNormal {
		return errors.Errorf("vectortable is not normal:%d", stat)
	}
	err := v.stable.Checkpoint(fs, destDir)
	return err
}

func (v *VectorTable) Rehash(force bool) error {
	return v.stable.needRehash(force)
}

func (v *VectorTable) GC(forceGC bool, testNow ...uint64) error {
	var now uint64
	if len(testNow) > 0 {
		now = testNow[0]
	} else {
		now = v.options.GetNowTimestamp()
	}
	if !forceGC && !v.stable.needGC(now) {
		v.stable.kvHolder.MergeExpired(now)
		return nil
	}
	v.stable.kvHolder.ClearExpired(now)
	return v.forceGC()
}

func (v *VectorTable) forceGC() error {
	v.gcLock.Lock()
	stat := v.stable.kvHolder.GetStat()
	if stat != StatNormal {
		v.gcLock.Unlock()
		return errors.Errorf("skip by stat exp normal act %d", stat)
	}

	v.stable.putLock.Lock()
	hdr := v.stable.kvHolder.GetHeader()
	items := hdr.resident - hdr.dead
	opts := v.stable.opts
	hashSize := hdr.groupNum * simd.GroupSize
	if items < hashSize/2 {
		hashSize = max(opts.HashSize, hashSize/2)
	}
	opts.OpenFiles = []string{
		buildVtIdxFileName(opts.Dirname, opts.Filename, fileExtIndex, hdr.dataSeq+1, 0),
	}
	var err error
	v.gc, err = newInnerVectorTable(opts, hashSize)
	if err != nil {
		v.stable.putLock.Unlock()
		v.gcLock.Unlock()
		return err
	}

	if err = v.stable.kvHolder.sync(); err != nil {
		v.stable.putLock.Unlock()
		v.gcLock.Unlock()
		return err
	}

	v.rwVT = v.gc
	v.stable.kvHolder.SetStat(StatGC)
	v.stable.putLock.Unlock()
	v.gcLock.Unlock()

	var gcStatus gcStatus
	var tMap = make(map[uint8]int, 10)

	start := time.Now()
	slotId := uint16(opts.Index)
	opts.Logger.Infof("[VTGC %d] vt:%s start slotId:%d forNum:%d items:%d oldHashSize:%d newHashSize:%d optHashSize:%d",
		opts.Index, opts.Filename, slotId, len(v.stable.ctrl)*simd.GroupSize, items, hdr.groupNum*simd.GroupSize, hashSize, opts.HashSize)

	rangeGroup := func(g int) {
		rwLockPos := v.stable.getRwLockPos(uint32(g))
		v.stable.rwLock[rwLockPos].RLock()
		defer v.stable.rwLock[rwLockPos].RUnlock()

		for s := range v.stable.ctrl[g] {
			c := v.stable.ctrl[g][s]
			if c == simd.Empty {
				continue
			}
			if c == simd.Tombstone {
				gcStatus.deleteCount++
				continue
			}
			hashHL, offset := v.stable.unmarshalItemMeta(v.stable.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			hashL, hashHH, key, value, seqNum, dataType, timestamp, slot, version, size, pre, next, kCloser, vCloser, err := v.stable.kvHolder.getKV(imOffset)
			if err != nil {
				opts.Logger.Errorf("[VTGC %d] vt:%s getKV key:%s error:%s", opts.Index, opts.Filename, string(key), err)
				continue
			}

			hashH := uint64(hashHH)<<32 | uint64(hashHL)
			if timestamp > 0 {
				nowT := opts.GetNowTimestamp()
				if timestamp <= nowT && (dataType != kkv.DataTypeString || opts.CheckExpireFunc(slotId, hashH, hashL)) {
					gcStatus.expiredCount++
					gcStatus.expiredKSize += len(key)
					gcStatus.expiredVSize += len(value)
					if kCloser != nil {
						kCloser()
					}
					if vCloser != nil {
						vCloser()
					}
					continue
				}
			}

			err = v.gc.Set(key, hashH, hashL, seqNum, dataType, timestamp, slot, version, size, pre, next, value)
			if err != nil {
				opts.Logger.Errorf("[VTGC %d] vt:%s set key:%s error:%s", opts.Index, opts.Filename, string(key), err)
			}
			gcStatus.writeKSize += len(key)
			gcStatus.writeVSize += len(value)
			tMap[dataType]++

			if kCloser != nil {
				kCloser()
			}
			if vCloser != nil {
				vCloser()
			}
		}
	}

	for g := range v.stable.ctrl {
		for opts.DbState.WaitVmTableHighPriority(opts.Index) {
			time.Sleep(2 * time.Second)
		}

		rangeGroup(g)
	}

	gcHdr := v.gc.kvHolder.GetHeader()
	cost := time.Since(start).Seconds()
	var oldKDataSize, newKDataSize humanize.FormattedString
	if opts.StoreKey {
		oldKDataSize = humanize.Uint64(v.stable.kvHolder.GetKDataM().TotalSize())
		newKDataSize = humanize.Uint64(v.gc.kvHolder.GetKDataM().TotalSize())
	} else {
		oldKDataSize = humanize.Uint64(0)
		newKDataSize = humanize.Uint64(0)
	}
	opts.Logger.Infof("[VTGC %d] vt:%s oldKeys:%d oldKDataSize:%s oldVDataSize:%s newKeys:%d newKDataSize:%s newVDataSize:%s delKeys:%d expKeys:%d exKSize:%d exVSize:%d wtKSize:%d wtVSize:%d wtCount:%v cost:%.3fs",
		opts.Index, opts.Filename,
		hdr.resident-hdr.dead,
		oldKDataSize,
		humanize.Uint64(v.stable.kvHolder.GetVDataM().TotalSize()),
		gcHdr.resident-gcHdr.dead,
		newKDataSize,
		humanize.Uint64(v.gc.kvHolder.GetVDataM().TotalSize()),
		gcStatus.deleteCount,
		gcStatus.expiredCount,
		gcStatus.expiredKSize,
		gcStatus.expiredVSize,
		gcStatus.writeKSize,
		gcStatus.writeVSize,
		tMap,
		cost)

	if err = v.gc.sync(); err != nil {
		opts.Logger.Errorf("[VTGC %d] vt:%s syncBuffer error:%s", opts.Index, opts.Filename, err)
	}

	v.gcLock.Lock()
	v.stable.kvHolder.SetStat(StatNormal)
	v.stable, v.gc = v.gc, v.stable
	v.rwVT = v.stable
	v.gcLock.Unlock()
	if err = v.gc.Close(true); err != nil {
		opts.Logger.Errorf("[VTGC %d] vt:%s gc close error:%s", opts.Index, opts.Filename, err)
	}
	v.gc = nil
	return nil
}

func (v *VectorTable) Close(isFree bool) error {
	v.gcLock.Lock()
	defer v.gcLock.Unlock()
	if v.stable.kvHolder.GetStat() != StatNormal {
		return errors.New("vector table is not normal")
	}
	err := v.stable.Close(isFree)
	return err
}

func (v *VectorTable) GetHeaderInfo() string {
	v.gcLock.RLock()
	defer v.gcLock.RUnlock()

	var gRatio float64
	h := v.stable.kvHolder.GetHeader()
	g, dKCap, dVCap, expCap, kDataSize, vDataSize, kvDataSize := v.stable.FileSize()
	if kvDataSize == 0 {
		gRatio = 0
	} else {
		gRatio = float64(g) / float64(kvDataSize)
	}

	em := v.stable.kvHolder.GetEM()
	startTime := em.getStartTime()
	minExTime := em.getMinExTime()

	sb := new(bytes.Buffer)
	fmt.Fprintf(sb, "ver:%d cap:%d groupNum:%d resident:%d dead:%d idxSeq:%d dataSeq:%d vFileIdx:%d kFileIdx:%d delKeys:%d kDataTail:%s vDataTail:%s ",
		h.ver, h.groupNum*simd.MaxAvgGroupLoad, h.groupNum,
		h.resident, h.dead, h.idxSeq, h.dataSeq,
		h.vFileIdx, h.kFileIdx, h.delKeys,
		humanize.Uint64(uint64(h.kDataTail)),
		humanize.Uint64(uint64(h.vDataTail)))
	fmt.Fprintf(sb, "indexSize:%s metaSize:%s kDataSize:%s vDataSize:%s kvDataSize:%s dKCap:%s dVCap:%s expCap:%s garbage:%s startTime:%d minExTime:%d gRatio:%.2f\n",
		humanize.Uint64(uint64(h.indexTail)),
		humanize.Uint64(uint64(h.metaTail)),
		humanize.Uint64(kDataSize),
		humanize.Uint64(vDataSize),
		humanize.Uint64(kvDataSize),
		humanize.Uint64(dKCap),
		humanize.Uint64(dVCap),
		humanize.Uint64(expCap),
		humanize.Uint64(g),
		startTime,
		minExTime,
		gRatio)
	return sb.String()
}
