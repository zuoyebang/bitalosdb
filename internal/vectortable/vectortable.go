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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/errors"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

const (
	rwLockSize                  = 2 << 10
	residentMaxSize             = 128 << 10
	gcThresholdTotalSize uint64 = 16 << 20
	gcThresholdGRatio           = 0.35
	rehashThresholdRatio        = 0.9
)

var ErrPanic = errors.New("panic")

type group [simd.GroupSize][8]byte

type innerVectorTable struct {
	ctrl       []simd.Metadata
	groups     []group
	limit      uint32
	kvHolder   DataHolder
	putLock    sync.RWMutex
	rehashLock sync.RWMutex
	rwLock     []sync.RWMutex
	rwLockMask uint32
	gcFileSize uint64
	gcGRatio   float64
	opts       *options.VectorTableOptions
}

type vtSeq struct {
	dataSeq uint32
	idxSeq  []uint32
}

func newInnerVt(opts *options.VectorTableOptions) *innerVectorTable {
	v := &innerVectorTable{
		gcFileSize: gcThresholdTotalSize,
		gcGRatio:   gcThresholdGRatio,
		opts:       opts,
		rwLockMask: rwLockSize - 1,
		rwLock:     make([]sync.RWMutex, rwLockSize),
	}
	if opts.GCThreshold > 0 {
		v.gcGRatio = opts.GCThreshold
	}
	if opts.GCFileSize > 0 {
		v.gcFileSize = opts.GCFileSize
	}
	return v
}

func newInnerVectorTable(opts *options.VectorTableOptions, newHashSize uint32) (vt *innerVectorTable, err error) {
	var idxSeq, dataSeq uint32
	needMerger := false
	var seqs []*vtSeq
	if len(opts.OpenFiles) > 0 {
		seqs, err = parseSeq(opts.OpenFiles)
		if err != nil {
			return nil, err
		}
		lSeq := len(seqs)
		if lSeq == 0 || len(seqs[0].idxSeq) == 0 {
			return nil, errors.Errorf("no index file, path:%s filename:%s openFiles:%v", opts.Dirname, opts.Filename, opts.OpenFiles)
		}
		var dFiles []string
		idxSeq = seqs[0].idxSeq[0]
		dataSeq = seqs[0].dataSeq
		if lSeq == 1 {
			for j := range seqs[0].idxSeq {
				if seqs[0].idxSeq[j] < idxSeq {
					idxSeq = seqs[0].idxSeq[j]
				}
			}
		} else {
			for i := range seqs {
				if seqs[i].dataSeq < dataSeq {
					tmpDataSeq := seqs[i].dataSeq
					tmpidxSeq := seqs[i].idxSeq[0]
					for j := range seqs[i].idxSeq {
						if seqs[i].idxSeq[j] < idxSeq {
							idxSeq = seqs[i].idxSeq[j]
						}
					}
					dfs, pass := checkIntegrity(opts, tmpDataSeq, tmpidxSeq)
					if !pass {
						dFiles = append(dFiles, dfs...)
						seqs = append(seqs[:i], seqs[i+1:]...)
						lSeq--
					} else {
						dataSeq = tmpDataSeq
						idxSeq = tmpidxSeq
					}
				}
			}
		}

		if lSeq > 1 || len(seqs[0].idxSeq) > 1 {
			needMerger = true
		}
		if len(dFiles) > 0 {
			opts.Logger.Errorf("panic integrity check failed files:%v", dFiles)
			opts.ReleaseFunc(dFiles)
		}
	}

	vt = newInnerVt(opts)
	var groupNum uint32
	if newHashSize > 0 {
		groupNum = numGroups(newHashSize)
	} else {
		groupNum = numGroups(opts.HashSize)
	}
	now := opts.GetNowTimestamp()

	if opts.StoreKey {
		if opts.WithSlot {
			vt.kvHolder, err = NewDataHolderSlot(opts, idxSeq, dataSeq, groupNum, now)
			if err != nil {
				return nil, err
			}
		} else {
			vt.kvHolder, err = NewDataHolder(opts, idxSeq, dataSeq, groupNum, now)
			if err != nil {
				return nil, err
			}
		}
	} else {
		if opts.WithSlot {
			vt.kvHolder, err = NewdataHolderSlotNoKey(opts, idxSeq, dataSeq, groupNum, now)
			if err != nil {
				return nil, err
			}
		} else {
			vt.kvHolder, err = NewDataHolderNokey(opts, idxSeq, dataSeq, groupNum, now)
			if err != nil {
				return nil, err
			}
		}
	}

	if vt.kvHolder.GetHeader().indexTail.GetOffset() == headerSize {
		vt.kvHolder.GetHeader().groupNum = groupNum
		slots := simd.GroupSize * uint64(vt.kvHolder.GetHeader().groupNum)
		offset, err := vt.kvHolder.GetIndex().alloc(9 * slots)
		if err != nil {
			return nil, errors.Wrapf(err, "index alloc fail,path:%s filename:%s files:%v", opts.Dirname, opts.Filename, opts.OpenFiles)
		}
		if offset != headerSize {
			return nil, errors.Errorf("unexpected path:%s filename:%s offset:%d", opts.Dirname, opts.Filename, offset)
		}
		ctrlOffset := headerSize

		vt.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&vt.kvHolder.GetIndex().data[ctrlOffset])))[:vt.kvHolder.GetHeader().groupNum]
		groupOffset := uint64(ctrlOffset) + slots
		vt.groups = (*(*[maxGroups]group)(unsafe.Pointer(&vt.kvHolder.GetIndex().data[groupOffset])))[:vt.kvHolder.GetHeader().groupNum]
		for i := range vt.ctrl {
			for j := range vt.ctrl[i] {
				vt.ctrl[i][j] = simd.Empty
			}
		}
		vt.limit = vt.kvHolder.GetHeader().groupNum * simd.MaxAvgGroupLoad
		vt.kvHolder.GetHeader().tags.setExpireManage(!opts.DisableExpireManage)
	} else {
		ctrlOffset := headerSize
		slots := simd.GroupSize * uint64(vt.kvHolder.GetHeader().groupNum)
		vt.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&vt.kvHolder.GetIndex().data[ctrlOffset])))[:vt.kvHolder.GetHeader().groupNum]
		vt.groups = (*(*[maxGroups]group)(unsafe.Pointer(&vt.kvHolder.GetIndex().data[uint64(ctrlOffset)+slots])))[:vt.kvHolder.GetHeader().groupNum]
		vt.limit = vt.kvHolder.GetHeader().groupNum * simd.MaxAvgGroupLoad
		vt.opts.DisableExpireManage = !vt.kvHolder.GetHeader().tags.isExpireManage()
	}

	merge := func() {
		var (
			tempIdxSeq, tempDataSeq uint32
			tmpVT                   *innerVectorTable
		)

		defer func() {
			if err := recover(); err != nil {
				indexFile, metaFile, kDataFile, vDataFile := buildVTFileName(opts.Dirname, opts.Filename, tempIdxSeq, tempDataSeq)
				vt.opts.Logger.Errorf("panic merge failed files:[%s, %s, %s, %s] err:%v", indexFile, metaFile, kDataFile, vDataFile, err)
				if err := tmpVT.Close(true); err != nil {
					vt.opts.Logger.Errorf("panic close vt failed, files:[%s, %s, %s, %s] err:%v", indexFile, metaFile, kDataFile, vDataFile, err)
				}
			}
		}()
		for i := range seqs {
			if seqs[i].dataSeq != dataSeq {
				tempIdxSeq = seqs[i].idxSeq[0]
				tempDataSeq = seqs[i].dataSeq
				tempOpts := vt.opts.Clone()
				tmpVT = newInnerVt(tempOpts)
				var iErr error
				if opts.StoreKey {
					if opts.WithSlot {
						tmpVT.kvHolder, iErr = NewDataHolderSlot(opts, tempIdxSeq, tempDataSeq, groupNum, now)
						if iErr != nil {
							opts.Logger.Errorf("merge NewDataHolder failed path:%s file:%s err:%s", opts.Dirname,
								opts.Filename+strconv.Itoa(int(tempIdxSeq))+"_"+strconv.Itoa(int(tempDataSeq)), err)
							tmpVT.Close(true)
							continue
						}
					} else {
						tmpVT.kvHolder, iErr = NewDataHolder(opts, tempIdxSeq, tempDataSeq, groupNum, now)
						if iErr != nil {
							opts.Logger.Errorf("merge NewDataHolder failed path:%s file:%s err:%s", opts.Dirname,
								opts.Filename+strconv.Itoa(int(tempIdxSeq))+"_"+strconv.Itoa(int(tempDataSeq)), err)
							tmpVT.Close(true)
							continue
						}
					}
				} else {
					if opts.WithSlot {
						tmpVT.kvHolder, iErr = NewdataHolderSlotNoKey(opts, tempIdxSeq, tempDataSeq, groupNum, now)
						if iErr != nil {
							opts.Logger.Errorf("merge NewDataHolder failed path:%s file:%s err:%s", opts.Dirname,
								opts.Filename+strconv.Itoa(int(tempIdxSeq))+"_"+strconv.Itoa(int(tempDataSeq)), err)
							tmpVT.Close(true)
							continue
						}
					} else {
						tmpVT.kvHolder, iErr = NewDataHolderNokey(opts, tempIdxSeq, tempDataSeq, groupNum, now)
						if iErr != nil {
							opts.Logger.Errorf("merge NewDataHolder failed path:%s file:%s err:%s", opts.Dirname,
								opts.Filename+strconv.Itoa(int(tempIdxSeq))+"_"+strconv.Itoa(int(tempDataSeq)), err)
							tmpVT.Close(true)
							continue
						}
					}
				}

				if tmpVT.kvHolder.GetHeader().indexTail.GetOffset() == headerSize {
					tmpVT.Close(true)
					continue
				} else {
					ctrlOffset := headerSize
					slots := simd.GroupSize * uint64(tmpVT.kvHolder.GetHeader().groupNum)
					tmpVT.ctrl = (*(*[maxGroups]simd.Metadata)(unsafe.Pointer(&tmpVT.kvHolder.GetIndex().data[ctrlOffset])))[:tmpVT.kvHolder.GetHeader().groupNum]
					tmpVT.groups = (*(*[maxGroups]group)(unsafe.Pointer(&tmpVT.kvHolder.GetIndex().data[uint64(ctrlOffset)+slots])))[:tmpVT.kvHolder.GetHeader().groupNum]

					tmpVT.limit = tmpVT.kvHolder.GetHeader().groupNum * simd.MaxAvgGroupLoad
					tmpVT.opts.DisableExpireManage = !tmpVT.kvHolder.GetHeader().tags.isExpireManage()
				}

				iter := tmpVT.NewIterator(nil)
				for {
					key, h, l, seqNum, dataType, timestamp, version, slot, size, pre, next, value, final := iter.Next()
					if final {
						break
					}
					if err = vt.Set(key, h, l, seqNum, dataType, timestamp, slot, version, size, pre, next, value); err != nil {
						continue
					}
				}

				if err := tmpVT.Close(true); err != nil {
					indexFile, metaFile, kDataFile, vDataFile := buildVTFileName(opts.Dirname, opts.Filename, tempIdxSeq, tempDataSeq)
					vt.opts.Logger.Errorf("panic close vt failed, files:[%s, %s, %s, %s] err:%v", indexFile, metaFile, kDataFile, vDataFile, err)
				}
			}
		}
	}

	if needMerger {
		merge()
	}

	vt.SyncHeader()
	return
}

func checkIntegrity(opts *options.VectorTableOptions, dataSeq, idxSeq uint32) (dFiles []string, pass bool) {
	idxFile, metaFile, kDataFile, vDataFile := buildVTFileName(opts.Dirname, opts.Filename, idxSeq, dataSeq)
	pass = true
	var exists []string
	if f, ok := existFile(opts.OpenFiles, opts.Dirname, idxFile); ok {
		exists = append(exists, f)
	} else {
		pass = pass && ok
	}
	if f, ok := existFile(opts.OpenFiles, opts.Dirname, metaFile); ok {
		exists = append(exists, f)
	} else {
		pass = pass && ok
	}
	if opts.StoreKey {
		if f, ok := existFile(opts.OpenFiles, opts.Dirname, kDataFile); ok {
			exists = append(exists, f)
		} else {
			pass = pass && ok
		}
	}
	if f, ok := existFile(opts.OpenFiles, opts.Dirname, vDataFile); ok {
		exists = append(exists, f)
	} else {
		pass = pass && ok
	}
	if !pass {
		return exists, pass
	}
	return
}

func existFile(fs []string, path, file string) (string, bool) {
	paths := strings.Split(file, "/")
	for _, f := range fs {
		oPath := strings.Split(f, "/")
		if strings.Contains(oPath[len(oPath)-1], paths[len(paths)-1]) {
			return fmt.Sprintf("%s/%s", strings.TrimSuffix(path, "/"), oPath[len(oPath)-1]), true
		}
	}
	return "", false
}

func (v *innerVectorTable) SyncHeader() {
	v.kvHolder.SyncHeader()
}

func (v *innerVectorTable) Set(key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, value []byte) (err error) {
	v.putLock.Lock()
	defer v.putLock.Unlock()

	if v.kvHolder.GetStat() != StatNormal {
		return base.ErrVTBusy
	}

	if v.kvHolder.GetHeader().resident >= v.limit {
		v.kvHolder.SetStat(StatRehashing)
		oldSize := v.kvHolder.GetHeader().groupNum
		startTime := time.Now()
		if errR := v.rehash(); errR != nil {
			v.opts.Logger.Errorf("[VTREHASH %d] vt:%s rehash fail err:%s", v.opts.Index, v.opts.Filename, errR)
			if v.size() >= v.kvHolder.GetHeader().groupNum*simd.GroupSize {
				return base.ErrVTFull
			}
		} else {
			v.opts.Logger.Infof("[VTREHASH %d] vt:%s rehash done oldSize:%d newSize:%d cost:%.3fs",
				v.opts.Index, v.opts.Filename, oldSize, v.kvHolder.GetHeader().groupNum, time.Since(startTime).Seconds())
		}

		v.kvHolder.SetStat(StatNormal)
	}

	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	g := simd.ProbeStart64(hi, len(v.groups))

	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].Lock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH, ver, dt := v.kvHolder.getHashAndVersionDT(imOffset)
				if hashL == l && hashHH == hh {
					if v.kvHolder.checkSeq(imOffset, seqNum) {
						v.rwLock[rwLockPos].Unlock()
						return nil
					}

					if dataType != dt || ver != version {
						v.ctrl[g][s] = simd.Tombstone
						v.kvHolder.GetHeader().dead++
						v.kvHolder.del(imOffset)
						switch dataType {
						case kkv.DataTypeString:
							imOffset, err = v.kvHolder.set(h, l, key, value, dataType, timestamp, seqNum, slot)
						case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
							imOffset, err = v.kvHolder.setKKVMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size)
						case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
							imOffset, err = v.kvHolder.setKKVListMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size, pre, next)
						default:
							err = errors.Errorf("set unsupported datatype %d", dataType)
						}
						if err == nil {
							alignOffset := uint32(imOffset >> alignDisplacement)
							v.marshalItemMeta(v.groups[g][s][:], hl, alignOffset)
							v.ctrl[g][s] = int8(lo)
							v.kvHolder.GetHeader().dead--
						}
						v.rwLock[rwLockPos].Unlock()
						return err
					} else {
						switch dataType {
						case kkv.DataTypeString:
							err = v.kvHolder.update(imOffset, value, dataType, timestamp, seqNum)
							if err != nil {
								if simd.MetaMatchEmpty(&v.ctrl[g]) != 0 {
									v.ctrl[g][s] = simd.Empty
									v.kvHolder.GetHeader().resident--
								} else {
									v.ctrl[g][s] = simd.Tombstone
									v.kvHolder.GetHeader().dead++
								}
								v.kvHolder.GetHeader().delKeys++
								v.kvHolder.del(imOffset)
							}
						case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
							v.kvHolder.updateKKVMeta(imOffset, dataType, timestamp, seqNum, version, size)
						case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
							v.kvHolder.updateKKVListMeta(imOffset, dataType, timestamp, seqNum, version, size, pre, next)
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
			var imOffset uint64
			switch dataType {
			case kkv.DataTypeString:
				imOffset, err = v.kvHolder.set(h, l, key, value, dataType, timestamp, seqNum, slot)
			case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
				imOffset, err = v.kvHolder.setKKVMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size)
			case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
				imOffset, err = v.kvHolder.setKKVListMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size, pre, next)
			default:
				err = errors.Errorf("unsupported datatype %d", dataType)
			}
			if err == nil {
				v.ctrl[g][s] = int8(lo)
				offset := uint32(imOffset >> alignDisplacement)
				v.marshalItemMeta(v.groups[g][s][:], hl, offset)
				v.kvHolder.GetHeader().resident++
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

func (v *innerVectorTable) SetTest(ch chan struct{}, ci chan int, key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, value []byte) (err error) {
	v.putLock.Lock()
	defer v.putLock.Unlock()
	if v.kvHolder.GetStat() != StatNormal {
		return base.ErrVTBusy
	}

	if v.kvHolder.GetHeader().resident >= v.limit {
		v.kvHolder.SetStat(StatRehashing)
		if err = v.rehashTest(ch); err == ErrPanic {
			return ErrPanic
		}
		v.kvHolder.SetStat(StatNormal)
	}

	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	g := simd.ProbeStart64(hi, len(v.groups))

	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].Lock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH, ver, dt := v.kvHolder.getHashAndVersionDT(imOffset)
				if hashL == l && hashHH == hh {
					if v.kvHolder.checkSeq(imOffset, seqNum) {
						v.rwLock[rwLockPos].Unlock()
						return nil
					}

					if dataType != dt || ver != version {
						v.ctrl[g][s] = simd.Tombstone
						v.kvHolder.GetHeader().dead++
						v.kvHolder.del(imOffset)
						switch dataType {
						case kkv.DataTypeString:
							imOffset, err = v.kvHolder.setDebug(h, l, key, value, dataType, timestamp, seqNum, slot, ci)
						case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
							imOffset, err = v.kvHolder.setKKVMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size)
						case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
							imOffset, err = v.kvHolder.setKKVListMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size, pre, next)
						default:
							err = errors.Errorf("unsupported datatype %d", dataType)
						}
						if err == nil {
							alignOffset := uint32(imOffset >> alignDisplacement)
							v.marshalItemMeta(v.groups[g][s][:], hl, alignOffset)
							v.ctrl[g][s] = int8(lo)
							v.kvHolder.GetHeader().dead--
						}
					} else {
						switch dataType {
						case kkv.DataTypeString:
							err = v.kvHolder.update(imOffset, value, dataType, timestamp, seqNum)
							if err != nil {
								if simd.MetaMatchEmpty(&v.ctrl[g]) != 0 {
									v.ctrl[g][s] = simd.Empty
									v.kvHolder.GetHeader().resident--
								} else {
									v.ctrl[g][s] = simd.Tombstone
									v.kvHolder.GetHeader().dead++
								}
								v.kvHolder.GetHeader().delKeys++
								v.kvHolder.del(imOffset)
							}
						case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
							v.kvHolder.updateKKVMeta(imOffset, dataType, timestamp, seqNum, version, size)
						case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
							v.kvHolder.updateKKVListMeta(imOffset, dataType, timestamp, seqNum, version, size, pre, next)
						default:
							err = errors.Errorf("unsupported datatype %d", dataType)
						}
					}
					v.rwLock[rwLockPos].Unlock()
					return err
				}
			}
		}

		matches = simd.MetaMatchEmpty(&v.ctrl[g])
		if matches != 0 {
			s := simd.NextMatch(&matches)
			var imOffset uint64
			switch dataType {
			case kkv.DataTypeString:
				imOffset, err = v.kvHolder.set(h, l, key, value, dataType, timestamp, seqNum, slot)
			case kkv.DataTypeHash, kkv.DataTypeSet, kkv.DataTypeZset:
				imOffset, err = v.kvHolder.setKKVMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size)
			case kkv.DataTypeList, kkv.DataTypeBitmap, kkv.DataTypeDKHash, kkv.DataTypeDKSet:
				imOffset, err = v.kvHolder.setKKVListMeta(h, l, key, dataType, timestamp, seqNum, slot, version, size, pre, next)
			default:
				err = errors.Errorf("unsupported datatype %d", dataType)
			}
			if err == nil {
				v.ctrl[g][s] = int8(lo)
				offset := uint32(imOffset >> alignDisplacement)
				v.marshalItemMeta(v.groups[g][s][:], hl, offset)
				v.kvHolder.GetHeader().resident++
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

func (v *innerVectorTable) Get(h, l uint64) (value []byte, seqNum uint64, dataType uint8, timestamp uint64, slot uint16, closer func(), err error) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	v.rehashLock.RLock()
	defer v.rehashLock.RUnlock()

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(imOffset)
				if hashL == l && hashHH == hh {
					value, seqNum, dataType, timestamp, slot, closer, err = v.kvHolder.getValue(imOffset)
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

func (v *innerVectorTable) GetMeta(h, l uint64) (
	seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64,
	size uint32, pre, next uint64, err error,
) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	v.rehashLock.RLock()
	defer v.rehashLock.RUnlock()

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(imOffset)
				if hashL == l && hashHH == hh {
					seqNum, dataType, timestamp, slot, version, size, pre, next, err = v.kvHolder.getMeta(imOffset)
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

func (v *innerVectorTable) Has(h, l uint64) (seqNum uint64, ok bool) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	v.rehashLock.RLock()
	defer v.rehashLock.RUnlock()

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(imOffset)
				if hashL == l && hashHH == hh {
					seqNum = v.kvHolder.getSeqNum(imOffset)
					v.rwLock[rwLockPos].RUnlock()
					ok = true
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

func (v *innerVectorTable) Delete(h, l, seqNum uint64) (ok bool) {
	// TODO Special treatment for expired and deleted keys
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
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(imOffset)
				if hashL == l && hashHH == hh {
					if !v.kvHolder.checkSeq(imOffset, seqNum) {
						if simd.MetaMatchEmpty(&v.ctrl[g]) != 0 {
							v.ctrl[g][s] = simd.Empty
							v.kvHolder.GetHeader().resident--
						} else {
							v.ctrl[g][s] = simd.Tombstone
							v.kvHolder.GetHeader().dead++
						}
						v.kvHolder.GetHeader().delKeys++
						v.kvHolder.del(imOffset)
					}

					v.rwLock[rwLockPos].Unlock()
					ok = true
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

func (v *innerVectorTable) DeleteTest(c chan int, h, l, seqNum uint64) (ok bool) {
	// TODO Special treatment for expired and deleted keys
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
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(imOffset)
				if hashL == l && hashHH == hh {
					if !v.kvHolder.checkSeq(imOffset, seqNum) {
						hdr := v.kvHolder.GetHeader()
						if simd.MetaMatchEmpty(&v.ctrl[g]) != 0 {
							v.ctrl[g][s] = simd.Empty
							hdr.resident--
						} else {
							v.ctrl[g][s] = simd.Tombstone
							hdr.dead++
						}
						hdr.delKeys++
						v.kvHolder.delDebug(imOffset, c)
					}

					v.rwLock[rwLockPos].Unlock()
					ok = true
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

func (v *innerVectorTable) SetTimestamp(h, l uint64, seqNum, ts uint64, datatype uint8) error {
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
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH, _, dt := v.kvHolder.getHashAndVersionDT(imOffset)
				if hashL == l && hashHH == hh && dt == datatype {
					if !v.kvHolder.checkSeq(imOffset, seqNum) {
						v.kvHolder.setTTL(imOffset, seqNum, t)
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

func (v *innerVectorTable) GetTimestamp(h, l uint64) (seqNum, ttl uint64, dt uint8, err error) {
	hi, lo := simd.SplitHash64(l)
	hl := uint32(h)
	hh := uint32(h >> 32)

	v.rehashLock.RLock()
	defer v.rehashLock.RUnlock()

	g := simd.ProbeStart64(hi, len(v.ctrl))
	var rwLockPos uint32
	for {
		rwLockPos = v.getRwLockPos(g)
		v.rwLock[rwLockPos].RLock()

		matches := simd.MetaMatchH2(&v.ctrl[g], lo)
		for matches != 0 {
			s := simd.NextMatch(&matches)
			hashHL, offset := v.unmarshalItemMeta(v.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(imOffset)
				if hashL == l && hashHH == hh {
					seqNum, ttl, dt = v.kvHolder.getTTL(imOffset)
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

func (v *innerVectorTable) SetSize(h, l uint64, seqNum uint64, size uint32) error {
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
			imOffset := uint64(offset) << alignDisplacement
			if hashHL == hl {
				hashL, hashHH := v.kvHolder.getHashInfo(imOffset)
				if hashL == l && hashHH == hh {
					if !v.kvHolder.checkSeq(imOffset, seqNum) {
						v.kvHolder.setSize(imOffset, seqNum, size)
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

func (v *innerVectorTable) Checkpoint(fs vfs.FS, destDir string) error {
	v.putLock.Lock()
	defer v.putLock.Unlock()

	v.kvHolder.GetHeader().Marshal(v.kvHolder.GetIndex().data)

	if err := v.kvHolder.sync(); err != nil {
		return err
	}

	vf := v.kvHolder.GetVDataM()
	for _, f := range vf.files {
		if st, err := f.Stat(); err != nil {
			return errors.Wrapf(err, "vt(%d) checkpoint stat err file:%s", v.opts.Index, v.opts.Filename)
		} else {
			filename := st.Name()
			src := buildFilename(v.opts.Dirname, filename)
			dest := buildFilename(destDir, filename)
			if err = vfs.LinkOrCopy(fs, src, dest); err != nil {
				return errors.Wrapf(err, "vt(%d) checkpoint link err src:%s dest:%s", v.opts.Index, src, dest)
			}
		}
	}

	if v.opts.StoreKey {
		kf := v.kvHolder.GetKDataM()
		for _, f := range kf.files {
			if st, err := f.Stat(); err != nil {
				return errors.Wrapf(err, "vt(%d) checkpoint stat err file:%s", v.opts.Index, v.opts.Filename)
			} else {
				filename := st.Name()
				src := buildFilename(v.opts.Dirname, filename)
				dest := buildFilename(destDir, filename)
				if err = vfs.LinkOrCopy(fs, src, dest); err != nil {
					return errors.Wrapf(err, "vt(%d) checkpoint link err src:%s dest:%s", v.opts.Index, src, dest)
				}
			}
		}
	}

	if st, err := v.kvHolder.GetMeta().file.Stat(); err != nil {
		return errors.Wrapf(err, "vt(%d) checkpoint stat err file:%s", v.opts.Index, v.opts.Filename)
	} else {
		filename := st.Name()
		src := buildFilename(v.opts.Dirname, filename)
		dest := buildFilename(destDir, filename)
		if err = vfs.LinkOrCopy(fs, src, dest); err != nil {
			return errors.Wrapf(err, "vt(%d) checkpoint link err src:%s dest:%s", v.opts.Index, src, dest)
		}
	}

	if st, err := v.kvHolder.GetIndex().file.Stat(); err != nil {
		return errors.Wrapf(err, "vt(%d) checkpoint stat err file:%s", v.opts.Index, v.opts.Filename)
	} else {
		filename := st.Name()
		src := buildFilename(v.opts.Dirname, filename)
		dest := buildFilename(destDir, filename)
		if err = vfs.LinkOrCopy(fs, src, dest); err != nil {
			return errors.Wrapf(err, "vt(%d) checkpoint link err src:%s dest:%s", v.opts.Index, src, dest)
		}
	}

	return nil
}

//go:inline
func (v *innerVectorTable) nextSize() (n uint32) {
	lg := len(v.groups)
	// group num * simd.MaxAvgGroupLoad
	if lg <= 1<<16 {
		n = uint32(math.Ceil(float64(lg) * 2))
	} else if lg <= 1<<18 {
		n = uint32(math.Ceil(float64(lg) * 1.8))
	} else if lg <= 1<<20 {
		n = uint32(math.Ceil(float64(lg) * 1.7))
	} else if lg <= 1<<22 {
		n = uint32(math.Ceil(float64(lg) * 1.6))
	} else if lg <= 1<<24 {
		n = uint32(math.Ceil(float64(lg) * 1.5))
	} else {
		n = uint32(math.Ceil(float64(lg) * 1.35))
	}

	if v.kvHolder.GetHeader().dead >= (v.kvHolder.GetHeader().resident / 2) {
		n = uint32(len(v.groups))
	}
	return
}

func (v *innerVectorTable) needRehash(force bool) error {
	v.putLock.Lock()
	defer v.putLock.Unlock()

	if v.kvHolder.GetStat() != StatNormal {
		return base.ErrVTBusy
	}

	if force || float64(v.kvHolder.GetHeader().resident) >= float64(v.limit)*rehashThresholdRatio {
		v.kvHolder.SetStat(StatRehashing)
		oldSize := v.kvHolder.GetHeader().groupNum
		startTime := time.Now()
		if errR := v.rehash(); errR != nil {
			v.opts.Logger.Errorf("[VTREHASH %d] vt:%s need rehash fail err:%s", v.opts.Index, v.opts.Filename, errR)
		} else {
			v.opts.Logger.Infof("[VTREHASH %d] vt:%s need rehash done force:%v oldSize:%d newSize:%d cost:%.3fs",
				v.opts.Index, v.opts.Filename, force, oldSize,
				v.kvHolder.GetHeader().groupNum,
				time.Since(startTime).Seconds())
		}

		v.kvHolder.SetStat(StatNormal)
	}
	return nil
}

func (v *innerVectorTable) rehash() error {
	n := v.nextSize()
	tmpIdx, err := NewTmpIndex(v.opts.Dirname, v.opts.Filename, v.kvHolder.GetHeader(), n)
	if err != nil {
		return err
	}

	groups := uint32(len(tmpIdx.ctrl))

	var resident uint32
	for g := range v.ctrl {
		for s := range v.ctrl[g] {
			c := v.ctrl[g][s]
			if c == simd.Empty || c == simd.Tombstone {
				continue
			}
			_, offset := v.unmarshalItemMeta(v.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			hashL := v.kvHolder.getHashL(imOffset)

			hi, lo := simd.SplitHash64(hashL)
			gN := simd.ProbeStart64(hi, int(groups))
			for {
				matches := simd.MetaMatchEmpty(&tmpIdx.ctrl[gN])
				if matches != 0 {
					sN := simd.NextMatch(&matches)
					copy(tmpIdx.groups[gN][sN][:], v.groups[g][s][:])
					tmpIdx.ctrl[gN][sN] = int8(lo)
					resident++
					break
				}
				gN++
				if gN >= groups {
					gN = 0
				}
			}
		}
	}

	h := v.kvHolder.GetHeader()
	if h.resident-h.dead != resident {
		v.opts.Logger.Errorf("panic rehash keys not equal oldResident:%d dead:%d newResident:%d", h.resident, h.dead, resident)
	}

	v.rehashLock.Lock()
	v.ctrl = tmpIdx.ctrl
	v.groups = tmpIdx.groups
	v.kvHolder.SwapHeader(tmpIdx.header)
	idx := v.kvHolder.GetIndex()
	v.kvHolder.SwapIndex(tmpIdx.index)
	tmpIdx.index = idx
	v.limit = n * simd.MaxAvgGroupLoad
	v.kvHolder.GetHeader().resident, v.kvHolder.GetHeader().dead = resident, 0
	v.kvHolder.GetHeader().groupNum = n
	v.rehashLock.Unlock()
	if err = tmpIdx.index.release(func(fns []string) {
		for _, fn := range fns {
			if e := os.Remove(fn); e != nil {
				v.opts.Logger.Errorf("panic remove rehash index file:%s error:%s", fn, e)
			}
		}
	}); err != nil {
		return err
	}

	return nil
}

func (v *innerVectorTable) rehashTest(ch chan struct{}) error {
	n := v.nextSize()
	tmpIdx, err := NewTmpIndex(v.opts.Dirname, v.opts.Filename, v.kvHolder.GetHeader(), n)
	if err != nil {
		return err
	}

	groups := uint32(len(tmpIdx.ctrl))

	var resident uint32
	for g := range v.ctrl {
		for s := range v.ctrl[g] {
			select {
			case <-ch:
				return ErrPanic
			default:
			}

			c := v.ctrl[g][s]
			if c == simd.Empty || c == simd.Tombstone {
				continue
			}
			_, offset := v.unmarshalItemMeta(v.groups[g][s])
			imOffset := uint64(offset) << alignDisplacement
			hashL := v.kvHolder.getHashL(imOffset)

			hi, lo := simd.SplitHash64(hashL)
			gN := simd.ProbeStart64(hi, int(groups))
			for {
				select {
				case <-ch:
					return ErrPanic
				default:
				}
				matches := simd.MetaMatchEmpty(&tmpIdx.ctrl[gN])
				if matches != 0 {
					select {
					case <-ch:
						return ErrPanic
					default:
					}
					sN := simd.NextMatch(&matches)
					copy(tmpIdx.groups[gN][sN][:], v.groups[g][s][:])
					select {
					case <-ch:
						return ErrPanic
					default:
					}
					tmpIdx.ctrl[gN][sN] = int8(lo)
					select {
					case <-ch:
						return ErrPanic
					default:
					}
					resident++
					select {
					case <-ch:
						return ErrPanic
					default:
					}
					break
				}
				gN++
				select {
				case <-ch:
					return ErrPanic
				default:
				}
				if gN >= groups {
					select {
					case <-ch:
						return ErrPanic
					default:
					}
					gN = 0
				}
			}

		}
	}
	v.rehashLock.Lock()
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	v.ctrl = tmpIdx.ctrl
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	v.groups = tmpIdx.groups
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	v.kvHolder.SwapHeader(tmpIdx.header)
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	idx := v.kvHolder.GetIndex()
	v.kvHolder.SwapIndex(tmpIdx.index)
	tmpIdx.index = idx
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	v.limit = n * simd.MaxAvgGroupLoad
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	v.kvHolder.GetHeader().resident, v.kvHolder.GetHeader().dead = resident, 0
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	v.kvHolder.GetHeader().groupNum = n
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	v.rehashLock.Unlock()
	select {
	case <-ch:
		return ErrPanic
	default:
	}
	if err := tmpIdx.index.release(func(fns []string) {
		for _, fn := range fns {
			if err := os.Remove(fn); err != nil {
				v.opts.Logger.Errorf("vectortable: remove rehash index file:%s error:%s", fn, err.Error())
			}
		}
	}); err != nil {
		return err
	}

	return nil
}

//go:inline
func (v *innerVectorTable) needGC(testNow ...uint64) (needGC bool) {
	var now, g, exCap, kDataSize uint64

	v.putLock.RLock()

	hdr := v.kvHolder.GetHeader()
	if v.opts.StoreKey {
		kDataSize = v.kvHolder.GetKDataM().TotalSize()
	}
	vDataSize := v.kvHolder.GetVDataM().TotalSize()
	kvDataSize := vDataSize + kDataSize
	if kvDataSize > 0 && kvDataSize < v.gcFileSize/4 {
		v.putLock.RUnlock()
		v.opts.Logger.Infof("[VTGC %d] vt:%s needGC:%v resident:%d dead:%d kDataSize:%d vDataSize:%d gcThresholdTotalSize:%d",
			v.opts.Index, v.opts.Filename, needGC, hdr.resident, hdr.dead, kDataSize, vDataSize, v.gcFileSize/4)
		return false
	}

	if len(testNow) > 0 {
		now = testNow[0]
	} else {
		now = v.opts.GetNowTimestamp()
	}
	exCap = v.kvHolder.GetExpireCap(now)

	items := uint64(hdr.resident - hdr.dead + hdr.delKeys)
	if items == 0 {
		if hdr.resident > residentMaxSize {
			needGC = true
		}
		v.putLock.RUnlock()
		v.opts.Logger.Infof("[VTGC %d] vt:%s needGC:%v items:0 resident:%d dead:%d",
			v.opts.Index, v.opts.Filename, needGC, hdr.resident, hdr.dead)
		return needGC
	}

	if v.opts.StoreKey {
		g = (kDataSize/items)*uint64(hdr.delKeys) + hdr.delVCap + exCap
	} else {
		g = hdr.delVCap + exCap
	}

	em := v.kvHolder.GetEM()
	minExTime := em.getMinExTime()
	exSlotSize := em.getOverExSize()

	v.putLock.RUnlock()

	vds := float64(kvDataSize)
	gcfs := float64(v.gcFileSize)
	ratio := float64(g) / vds
	if ratio > 0.9 ||
		vds > gcfs && ratio > v.gcGRatio ||
		vds > gcfs*0.9 && ratio > v.gcGRatio*1.1 ||
		vds > gcfs*0.8 && ratio > v.gcGRatio*1.2 ||
		vds > gcfs*0.7 && ratio > v.gcGRatio*1.3 ||
		vds > gcfs*0.6 && ratio > v.gcGRatio*1.4 {
		needGC = true
	} else {
		needGC = false
	}

	v.opts.Logger.Infof("[VTGC %d] vt:%s needGC:%v resident:%d dead:%d useSpace:%d garbageSpace:%d delVCap:%d expireCap:%d minExTime:%d exSlotSize:%d ratio:%.3f",
		v.opts.Index, v.opts.Filename, needGC, hdr.resident, hdr.dead, kvDataSize, g, hdr.delVCap, exCap, minExTime, exSlotSize, ratio)
	return
}

func (v *innerVectorTable) FileSize() (g, dKCap, dVCap, exCap, kDataSize, vDataSize, kvDataSize uint64) {
	now := v.opts.GetNowTimestamp()
	v.putLock.RLock()
	defer v.putLock.RUnlock()

	hdr := v.kvHolder.GetHeader()
	if v.opts.StoreKey {
		kDataSize = v.kvHolder.GetKDataM().TotalSize()
	}
	vDataSize = v.kvHolder.GetVDataM().TotalSize()
	kvDataSize = vDataSize + kDataSize
	exCap = v.kvHolder.GetExpireCap(now)
	items := uint64(hdr.resident - hdr.dead + hdr.delKeys)
	dVCap = hdr.delVCap
	if v.opts.StoreKey {
		if items == 0 {
			dKCap = 0
		} else {
			dKCap = (kDataSize / items) * uint64(hdr.delKeys)
		}
		g = dKCap + dVCap + exCap
	} else {
		g = dVCap + exCap
	}
	return
}

func (v *innerVectorTable) size() uint32 {
	return v.kvHolder.GetHeader().resident - v.kvHolder.GetHeader().dead
}

func (v *innerVectorTable) sync() error {
	v.putLock.Lock()
	defer v.putLock.Unlock()
	return v.kvHolder.sync()
}

func (v *innerVectorTable) syncBuffer() error {
	v.putLock.Lock()
	defer v.putLock.Unlock()
	return v.kvHolder.syncBuffer()
}

func (v *innerVectorTable) Close(isFree bool) error {
	v.putLock.Lock()
	defer v.putLock.Unlock()

	var idxF, metaF, kF, vF string
	if isFree {
		idxF, metaF, kF, vF = buildVTFileName(v.opts.Dirname, v.opts.Filename, v.kvHolder.GetHeader().idxSeq, v.kvHolder.GetHeader().dataSeq)
	}
	fs := []string{idxF, metaF}
	if v.opts.StoreKey {
		for i := uint16(0); i <= v.kvHolder.GetHeader().kFileIdx; i++ {
			fs = append(fs, fmt.Sprintf("%s.%d", kF, i))
		}
	}
	for i := uint16(0); i <= v.kvHolder.GetHeader().vFileIdx; i++ {
		fs = append(fs, fmt.Sprintf("%s.%d", vF, i))
	}

	err := v.kvHolder.Close()
	if err == nil && isFree {
		v.opts.ReleaseFunc(fs)
	}
	v.ctrl = nil
	v.groups = nil
	v.kvHolder = nil
	v.opts = nil
	return err
}

func parseSeq(files []string) ([]*vtSeq, error) {
	var ret []*vtSeq
	var seqMap = make(map[uint64]*vtSeq)
	for _, file := range files {
		fi := file[strings.LastIndex(file, "/")+1:]
		ext := fi[strings.Index(fi, ".")+1:]
		if strings.HasPrefix(ext, fileExtIndex) {
			sli := strings.Split(ext[strings.Index(ext, ".")+1:], ".")
			if len(sli) != 2 {
				return nil, errors.Errorf("vectortable: invalid index file:%s", file)
			}
			dataSeq, err := strconv.ParseUint(sli[0], 10, 32)
			if err != nil {
				return nil, errors.Errorf("vectortable: invalid index file:%s", file)
			}
			idxSeq, err := strconv.ParseUint(sli[1], 10, 32)
			if err != nil {
				return nil, errors.Errorf("vectortable: invalid index file:%s", file)
			}
			if _, ok := seqMap[dataSeq]; ok {
				seqMap[dataSeq].idxSeq = append(seqMap[dataSeq].idxSeq, uint32(idxSeq))
			} else {
				var s = vtSeq{
					dataSeq: uint32(dataSeq),
					idxSeq:  []uint32{uint32(idxSeq)},
				}
				ret = append(ret, &s)
				seqMap[dataSeq] = &s
			}
		}
	}
	for x := range ret {
		sort.Slice(ret[x].idxSeq, func(i, j int) bool { return ret[x].idxSeq[i] > ret[x].idxSeq[j] })
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].dataSeq > ret[j].dataSeq })
	return ret, nil
}

//go:inline
func combine(low56 uint64, high8 uint8) uint64 {
	return low56&maskLow56 | uint64(high8)<<56
}

//go:inline
func split(u uint64) (uint64, uint8) {
	return u & maskLow56, uint8(u >> 56)
}

func splitHigh8(u uint64) uint8 {
	return uint8(u >> 56)
}

func (v *innerVectorTable) marshalItemMeta(buf []byte, hl uint32, itemOffset uint32) {
	binary.LittleEndian.PutUint32(buf[0:], hl)
	binary.LittleEndian.PutUint32(buf[4:], itemOffset)
}

func (v *innerVectorTable) unmarshalItemMeta(buf [8]byte) (hl uint32, itemOffset uint32) {
	hl = binary.LittleEndian.Uint32(buf[0:])
	itemOffset = binary.LittleEndian.Uint32(buf[4:])
	return
}

func (v *innerVectorTable) getRwLockPos(n uint32) uint32 {
	return n & v.rwLockMask
}

func numGroups(n uint32) (groups uint32) {
	groups = (n + simd.MaxAvgGroupLoad - 1) / simd.MaxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}
