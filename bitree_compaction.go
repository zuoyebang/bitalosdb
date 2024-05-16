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
	"runtime/debug"
	"sync"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/consts"
)

func (d *DB) runCompactTask() {
	d.taskWg.Add(1)
	go func() {
		defer func() {
			d.taskWg.Done()
			d.opts.Logger.Infof("auto compact task background exit...")
		}()

		jobId := 0
		partStep := 10

		doFunc := func() {
			defer func() {
				if r := recover(); r != nil {
					d.opts.Logger.Errorf("auto compact task run panic err:%v stack:%s", r, string(debug.Stack()))
				}
			}()

			jobId++
			d.compactIterReadAmplification(jobId)

			if d.opts.AutoCompact && jobId%partStep == 0 {
				d.CheckAndCompact(jobId)
			}
		}

		interval := time.Duration(d.opts.CompactInfo.Interval / partStep)
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

	d.opts.Logger.Infof("auto compact task background running...")
}

func (d *DB) compactIterReadAmplification(jobId int) {
	for i := range d.bitowers {
		slowCount := d.bitowers[i].iterSlowCount.Load()
		flushed := false
		if slowCount > consts.IterReadAmplificationThreshold {
			d.bitowers[i].iterSlowCount.Store(0)
			d.bitowers[i].AsyncFlush()
			flushed = true
		}

		if slowCount > 0 && jobId%50 == 0 {
			d.opts.Logger.Infof("[JOBREADAMP %d] iterator check stats bitower:%d slowCount:%d flushed:%t",
				jobId, i, slowCount, flushed)
		}
	}
}

func (d *DB) SetAutoCompact(newVal bool) {
	d.opts.AutoCompact = newVal
}

func (d *DB) GetAutoCompact() bool {
	return d.opts.AutoCompact
}

func (d *DB) CheckAndCompact(jobId int) {
	btreeMaxSize := d.opts.CompactInfo.BitreeMaxSize
	startHour := d.opts.CompactInfo.StartHour
	endHour := d.opts.CompactInfo.EndHour
	currentHour := time.Now().Hour()
	if currentHour < startHour || currentHour > endHour {
		d.opts.Logger.Infof("[JOB %d] compact skip by time forbid", jobId)
		return
	}

	if d.checkCompactToBitable(btreeMaxSize) {
		d.compactToBitable(jobId)
	}

	if d.opts.IOWriteLoadThresholdFunc() {
		d.opts.Logger.Infof("[JOB %d] compact bithash start", jobId)
		d.compactBithash(d.opts.CompactInfo.DeletePercent)
		d.opts.Logger.Infof("[JOB %d] compact bithash done", jobId)
	} else {
		d.opts.Logger.Infof("[JOB %d] compact bithash skip by gt maxQPS", jobId)
	}
}

func (d *DB) compactBithash(deletePercent float64) {
	if !d.opts.UseBithash {
		return
	}

	for i := range d.bitowers {
		if d.IsClosed() {
			break
		}
		d.bitowers[i].btree.CompactBithash(deletePercent)
	}
}

func (d *DB) CompactBitree(jobId int) {
	if d.checkCompactToBitable(consts.UseBitableForceCompactMaxSize) {
		d.compactToBitable(jobId)
	}
}

func (d *DB) checkCompactToBitable(maxSize int64) bool {
	if !d.opts.UseBitable {
		return false
	}

	diskSize := d.statsDiskSize()
	d.opts.Logger.Infof("compact to bitable check diskSize:%d, maxSize:%d", diskSize, maxSize)

	return diskSize >= maxSize
}

func (d *DB) compactToBitable(jobId int) {
	if !d.opts.UseBitable {
		return
	}

	d.setFlushedBitable()
	startTime := time.Now()
	d.opts.Logger.Infof("[JOB %d] compact bitree to bitable start", jobId)

	var wg sync.WaitGroup
	for i, bitower := range d.bitowers {
		if d.IsClosed() {
			break
		}

		bitower.btree.CompactBitreeToBitable()

		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			if err := d.bitowers[index].btree.ManualCompact(); err != nil {
				d.opts.Logger.Infof("[JOB %d] manual compact bitable fail index:%d err:%s", jobId, index, err)
			}
		}(i)
	}
	wg.Wait()

	cost := time.Since(startTime).Seconds()
	d.opts.Logger.Infof("[JOB %d] compact bitree to bitable finish cost:%.2fs", jobId, cost)
}
