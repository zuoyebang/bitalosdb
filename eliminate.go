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
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
)

const (
	eliminateInterval    = 300
	scanTimeOffset       = 0
	scanLowerBoundLength = 8
	scanUpperBoundLength = 16
)

type eliminateTask struct {
	db        *DB
	taskWg    *sync.WaitGroup
	closed    atomic.Bool
	slotId    uint16
	scanTs    uint64
	jobId     uint64
	scanLower [kkv.SubKeyHeaderLength]byte
	scanUpper [kkv.SubKeyUpperBoundLength]byte
}

func calcExpireKeyTime(t uint64) uint64 {
	return (t/eliminateInterval + 1) * eliminateInterval
}

func (d *DB) newEliminateTask() error {
	d.eliTask = &eliminateTask{
		db:     d,
		taskWg: &d.taskWg,
		slotId: consts.EliminateSlotId,
	}

	if err := d.newBituple(int(d.eliTask.slotId), slotBitupleCreated); err != nil {
		return err
	}

	d.eliTask.scanTs = d.meta.getEliminateScanTs()
	if d.eliTask.scanTs == 0 {
		d.eliTask.saveScanTimestamp(calcExpireKeyTime(uint64(time.Now().Unix())))
	}

	kkv.EncodeSubKeyLowerBound(d.eliTask.scanLower[:], kkv.DataTypeExpireKey, 0)
	kkv.EncodeSubKeyUpperBound(d.eliTask.scanUpper[:], kkv.DataTypeExpireKey, 0)
	d.eliTask.run()
	d.opts.Logger.Infof("newEliminateTask success scanTs:%d", d.eliTask.scanTs)
	return nil
}

func (e *eliminateTask) run() {
	e.taskWg.Add(1)
	go func() {
		nowTime := uint64(time.Now().Unix())
		if e.scanTs < nowTime {
			oldScanTs := e.scanTs
			scanCount := 0
			for e.scanTs < nowTime {
				e.scanEliminateData()
				scanCount++
			}
			e.db.opts.Logger.Infof("eliminate task run scanTs(%d) less than nowTime(%d) immediately scan count:%d", oldScanTs, nowTime, scanCount)
		}

		ticker := time.NewTicker(time.Duration(eliminateInterval) * time.Second)
		jobId := 1

		defer func() {
			ticker.Stop()
			e.taskWg.Done()
			e.db.opts.Logger.Info("eliminate task background exit...")
		}()

		for {
			select {
			case <-e.db.taskClosed:
				return
			case <-ticker.C:
				if e.isClosed() {
					return
				}

				e.scanEliminateData()

				e.db.doMemShardIterReadAmp(jobId)
				jobId++
			}
		}
	}()
}

func (e *eliminateTask) saveScanTimestamp(ts uint64) {
	e.db.meta.setEliminateScanTs(ts)
	e.scanTs = ts
}

func (e *eliminateTask) setExpireKey(sid uint16, timestamp, version uint64, kind InternalKeyKind) (err error) {
	var keyBuf [kkv.ExpireKeyLength]byte
	ts := calcExpireKeyTime(timestamp)
	kkv.EncodeExpireKey(keyBuf[:], ts, version, sid)
	switch kind {
	case InternalKeyKindSet:
		return e.db.kkvSet(keyBuf[:], e.slotId, nil)
	case InternalKeyKindDelete:
		return e.db.kkvDeleteKey(keyBuf[:], e.slotId)
	default:
		return err
	}
}

func (e *eliminateTask) updateExpireKey(sid uint16, version, oldMilli, newMilli uint64) {
	if oldMilli == newMilli {
		return
	}
	if oldMilli > 0 {
		oldSec := utils.FmtUnixMilliToSec(oldMilli)
		_ = e.setExpireKey(sid, oldSec, version, InternalKeyKindDelete)
	}
	if newMilli > 0 {
		newSec := utils.FmtUnixMilliToSec(newMilli)
		_ = e.setExpireKey(sid, newSec, version, InternalKeyKindSet)
	}
}

func (e *eliminateTask) existExpireKey(sid uint16, version, milli uint64) bool {
	var keyBuf [kkv.ExpireKeyLength]byte
	sec := utils.FmtUnixMilliToSec(milli)
	ts := calcExpireKeyTime(sec)
	kkv.EncodeExpireKey(keyBuf[:], ts, version, sid)
	exist := e.db.kkvExist(keyBuf[:], e.slotId)
	return exist
}

func (e *eliminateTask) newIter() *KKVIterator {
	binary.LittleEndian.PutUint64(e.scanLower[scanTimeOffset:scanLowerBoundLength], e.scanTs)
	binary.LittleEndian.PutUint64(e.scanUpper[scanTimeOffset:scanLowerBoundLength], e.scanTs)
	opts := &options.IterOptions{
		LowerBound: e.scanLower[:],
		UpperBound: e.scanUpper[:],
		SlotId:     uint32(e.slotId),
		DataType:   kkv.DataTypeExpireKey,
	}
	return e.db.NewKKVIterator(opts)
}

func (e *eliminateTask) scanEliminateData() int {
	var err error
	start := time.Now()
	delNum := 0
	e.jobId++
	logTag := fmt.Sprintf("[ELIMINATETASK %d] scan", e.jobId)
	e.db.opts.Logger.Infof("%s start scanTs:%d", logTag, e.scanTs)
	defer func() {
		if r := recover(); r != any(nil) {
			e.db.opts.Logger.Fatalf("%s error:%v stack:%s", logTag, r, string(debug.Stack()))
		}

		scanTs := e.scanTs + eliminateInterval
		e.saveScanTimestamp(scanTs)
		cost := time.Now().Sub(start).Seconds()
		e.db.opts.Logger.Infof("%s end nextScanTs:%d delKeys:%d cost:%.3fs", logTag, scanTs, delNum, cost)
	}()

	e.db.dbState.RLockTask()
	defer e.db.dbState.RUnlockTask()

	iter := e.newIter()
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if e.isClosed() || e.db.IsCheckpointHighPriority() {
			break
		}

		_, version, _, slotId := kkv.DecodeExpireKey(iter.key)
		if slotId >= consts.SlotNum {
			e.db.opts.Logger.Errorf("%s decodeExpireKey fail slotId:%d", logTag, slotId)
			continue
		}
		err = e.db.kkvSetPrefixDelete(slotId, version)
		if err == nil {
			err = e.db.kkvDeleteKey(iter.Key(), e.slotId)
		}
		if err != nil {
			e.db.opts.Logger.Errorf("%s set pdKey fail version:%d err:%s", logTag, version, err)
		} else {
			delNum++
		}
	}

	return delNum
}

func (e *eliminateTask) isClosed() bool {
	return e.closed.Load()
}

func (e *eliminateTask) close() {
	e.closed.Store(true)
}
