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
	"runtime/debug"
	"time"

	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
)

func (d *DB) runBithashGCTask() {
	d.taskWg.Add(1)
	go func() {
		defer func() {
			d.taskWg.Done()
			d.opts.Logger.Info("bithash compact task background exit...")
		}()

		startHour := d.opts.CompactInfo.StartHour
		endHour := d.opts.CompactInfo.EndHour
		interval := time.Duration(consts.BithashCompactIntervalDefault)
		deletePercent := d.opts.CompactInfo.BithashDeletePercent
		jobId := 1

		d.opts.Logger.Infof("bithash compact task background running timeRange:%d-%d internal:%ds", startHour, endHour, interval)

		doFunc := func() {
			defer func() {
				if r := recover(); r != nil {
					d.opts.Logger.Errorf("auto compact task run panic err:%v stack:%s", r, string(debug.Stack()))
				}
			}()

			if d.opts.AutoCompact {
				currentHour := time.Now().Hour()
				if currentHour < startHour || currentHour > endHour {
					d.opts.Logger.Info("bithash compact skip by time forbid")
					return
				}

				d.doBithashCompact(jobId, deletePercent)
				jobId++
			}
		}

		ticker := time.NewTicker(interval * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.taskClosed:
				return
			case <-ticker.C:
				if d.IsClosed() {
					return
				}

				doFunc()
			}
		}
	}()
}

func (d *DB) doBithashCompact(jobId int, deletePercent float64) {
	defer func() {
		if r := recover(); r != nil {
			d.opts.Logger.Fatalf("bithash compact run err:%v stack:%s", r, string(debug.Stack()))
		}
	}()

	d.opts.Logger.Infof("[JOB %d] bithash compact check start", jobId)

	d.dbState.RLockTask()
	defer d.dbState.RUnlockTask()

	for i := 0; i < metaSlotNum; i++ {
		if d.IsClosed() || d.IsCheckpointHighPriority() {
			return
		}

		bt := d.getBitupleSafe(i)
		if bt != nil {
			bt.kkv.CompactBithash(deletePercent)
		}
	}

	d.opts.Logger.Infof("[JOB %d] bithash compact check done", jobId)
	jobId++
}

func (d *DB) doMemShardIterReadAmp(jobId int) {
	for i := range d.memShard {
		slowCount := d.memShard[i].iterSlowCount.Load()
		if slowCount == 0 {
			continue
		}

		flushed := false
		if slowCount > consts.IterReadAmplificationThreshold {
			d.memShard[i].iterSlowCount.Store(0)
			d.memShard[i].asyncFlush(false)
			flushed = true
		}

		if flushed || jobId%10 == 0 {
			d.opts.Logger.Infof("[JOBREADAMP %d] iterator check stats memShard:%d slowCount:%d flushed:%t",
				jobId, i, slowCount, flushed)
		}
	}
}
