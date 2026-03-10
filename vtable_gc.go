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

const (
	vtNeedRehashCycle = 4
)

func (d *DB) VtableGC(slotId uint16) {
	if slotId >= metaSlotNum {
		return
	}

	t := d.getBitupleSafe(int(slotId))
	if t != nil {
		t.RunVtGC(true)
	}
}

func (d *DB) VtableRehash(slotId uint16) {
	if slotId >= metaSlotNum {
		return
	}

	t := d.getBitupleSafe(int(slotId))
	if t != nil {
		t.RunVtRehash(true)
	}
}

func (d *DB) runVtGCTask() {
	d.taskWg.Add(1)
	go func() {
		defer func() {
			d.taskWg.Done()
			d.opts.Logger.Infof("vtgc task background exit...")
		}()

		startHour := d.opts.CompactInfo.StartHour
		endHour := d.opts.CompactInfo.EndHour
		interval := time.Duration(consts.VectorTableGCInternalDefault)
		rehashCycle := 0

		d.opts.Logger.Infof("vtgc task background running timeRange:%d-%d internal:%ds", startHour, endHour, interval)

		doGC := func() {
			defer func() {
				if r := recover(); r != nil {
					d.opts.Logger.Fatalf("[VTGC] task run err:%v stack:%s", r, string(debug.Stack()))
				}
			}()

			if d.opts.AutoCompact {
				currentHour := time.Now().Hour()
				if currentHour < startHour || currentHour > endHour {
					d.opts.Logger.Info("[VTGC] do task skip by time forbid")
					return
				}

				d.opts.Logger.Info("[VTGC] do task start")

				d.dbState.RLockTask()
				defer d.dbState.RUnlockTask()

				d.doVtGC(false)

				rehashCycle++
				if rehashCycle == vtNeedRehashCycle {
					rehashCycle = 0
					d.doVtRehash(false)
				}
			}
		}

		ticker := time.NewTicker(interval * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.taskClosed:
				return
			case <-ticker.C:
				doGC()
			}
		}
	}()
}

func (d *DB) doVtGC(force bool) {
	for i := 0; i < metaSlotNum; i++ {
		if d.IsClosed() || d.IsCheckpointSyncing() {
			return
		}

		bt := d.getBitupleSafe(i)
		if bt != nil {
			bt.RunVtGC(force)
		}
	}
}

func (d *DB) doVtRehash(force bool) {
	for i := 0; i < metaSlotNum; i++ {
		if d.IsClosed() || d.IsCheckpointSyncing() {
			return
		}

		bt := d.getBitupleSafe(i)
		if bt != nil {
			bt.RunVtRehash(force)
		}
	}
}
