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

package bitalosdb

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/zuoyebang/bitalosdb/bitforest"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

type ForestInfo = bitforest.Stats

func (d *DB) ForestInfo() ForestInfo {
	return d.bf.Stats()
}

func (d *DB) CacheInfo() string {
	if d.cache == nil {
		return ""
	}

	return d.cache.MetricsInfo()
}

func (d *DB) DebugInfo() string {
	return d.bf.DebugInfo(d.opts.DataType)
}

func (d *DB) RunCompactTask() {
	d.opts.Logger.Info("run bitforest compact task start")
	d.bgTasks.Add(1)
	go func() {
		defer func() {
			d.bgTasks.Done()
			if r := recover(); r != nil {
				d.opts.Logger.Errorf("bitalosdb: run bitforest compact task panic err:%v stack:%s", r, string(debug.Stack()))
				d.RunCompactTask()
			}
		}()

		jobId := 1
		partStep := 10
		interval := time.Duration(d.opts.CompactInfo.Interval / partStep)

		ticker := time.NewTicker(interval * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.bgTasksCh:
				return
			case <-ticker.C:
				if d.IsClosed() {
					return
				}

				d.CheckIterReadAmplification(jobId)
				if jobId%partStep == 0 {
					d.CheckAndCompact(jobId)
				}
				jobId++
			}
		}
	}()
}

func (d *DB) CheckAndCompact(jobId int) {
	logTag := fmt.Sprintf("[COMPACT %d] compact check", jobId)
	btreeMaxSize := d.opts.CompactInfo.BitreeMaxSize
	startHour := d.opts.CompactInfo.StartHour
	endHour := d.opts.CompactInfo.EndHour
	currentHour := time.Now().Hour()
	if currentHour < startHour || currentHour > endHour {
		d.opts.Logger.Infof("%s skip by time forbid", logTag)
		return
	}

	if d.bf.CheckCompactToBitable(btreeMaxSize) {
		d.bf.CompactBitree(jobId)
	}

	if d.opts.IOWriteLoadThresholdFunc() {
		d.bf.CompactBithash(jobId, d.opts.CompactInfo.DeletePercent)
	} else {
		d.opts.Logger.Infof("%s skip bithash by gt maxQPS", logTag)
	}
}

func (d *DB) CompactBitree(jobId int) {
	if d.bf.CheckCompactToBitable(consts.UseBitableForceCompactMaxSize) {
		d.bf.CompactBitree(jobId)
	}
}
