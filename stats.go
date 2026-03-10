// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitalosdb

import (
	"sync"
	"sync/atomic"
	"time"
)

type tableFlushStats struct {
	lastCost     atomic.Int64
	avgCost      atomic.Int64
	mu           sync.Mutex
	day          int
	dayCostCount int64
	dayCostTotal int64
}

func (fs *tableFlushStats) AddFlushTime(t int64) {
	fs.lastCost.Store(t)
	day := getNowDateInt()

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.day != day {
		fs.day = day
		fs.dayCostCount = 0
		fs.dayCostTotal = 0
	}
	fs.dayCostCount++
	fs.dayCostTotal += t
	avgCost := fs.dayCostTotal / fs.dayCostCount
	fs.avgCost.Store(avgCost)
}

type dbFlushStats struct {
	vmTable  *tableFlushStats
	memTable *tableFlushStats
}

func NewFlushStats() *dbFlushStats {
	stat := &dbFlushStats{
		vmTable: &tableFlushStats{
			dayCostCount: 0,
			dayCostTotal: 0,
			day:          getNowDateInt(),
		},
		memTable: &tableFlushStats{
			dayCostCount: 0,
			dayCostTotal: 0,
			day:          getNowDateInt(),
		},
	}
	return stat
}

func (s *dbFlushStats) setFlushVmTableTime(t int64) {
	s.vmTable.AddFlushTime(t)
}

func (s *dbFlushStats) setFlushMemTableTime(t int64) {
	s.memTable.AddFlushTime(t)
}

type DbStats struct {
	VmTableFlushLastCost  int64
	VmTableFlushAvgCost   int64
	MemTableFlushLastCost int64
	MemTableFlushAvgCost  int64
}

func (d *DB) GetStats() DbStats {
	s := DbStats{
		VmTableFlushLastCost:  d.flushStats.vmTable.lastCost.Load(),
		VmTableFlushAvgCost:   d.flushStats.vmTable.avgCost.Load(),
		MemTableFlushLastCost: d.flushStats.memTable.lastCost.Load(),
		MemTableFlushAvgCost:  d.flushStats.memTable.avgCost.Load(),
	}
	return s
}

func getNowDateInt() int {
	y, m, d := time.Now().Date()
	return y*10000 + int(m)*100 + d
}
