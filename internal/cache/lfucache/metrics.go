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

package lfucache

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

func (lfc *LfuCache) MetricsInfo() string {
	return lfc.Metrics().String()
}

func (lfc *LfuCache) Metrics() Metrics {
	var m Metrics
	m.ShardsMetrics = make([]ShardMetrics, lfc.shardNum)
	for i := range lfc.shards {
		s := lfc.shards[i]
		s.mu.Lock()
		atSize := s.arrayTableInuseSize()
		memLen := len(s.mu.memQueue)
		memSize := s.memTableInuseSize()
		size := atSize + memSize
		s.mu.Unlock()
		m.ShardsMetrics[i] = ShardMetrics{
			Size:         size,
			InusePercent: int(size * 100 / s.maxSize),
			MemLen:       memLen,
			MemCount:     s.memTableCount(),
			MemSize:      memSize,
			AtCount:      s.arrayTableCount(),
			AtSize:       atSize,
		}
		m.Size += size
		m.Hits += atomic.LoadInt64(&s.atomic.hits)
		m.Misses += atomic.LoadInt64(&s.atomic.misses)
	}
	return m
}

type ShardMetrics struct {
	Size         uint64
	InusePercent int
	MemLen       int
	MemCount     int
	MemSize      uint64
	AtCount      int
	AtSize       uint64
}

type Metrics struct {
	Size          uint64
	Hits          int64
	Misses        int64
	ShardsMetrics []ShardMetrics
}

func (m Metrics) String() string {
	var shards bytes.Buffer
	for i := range m.ShardsMetrics {
		shards.WriteString(fmt.Sprintf("[%d:%d:%d:%d:%d:%d:%d:%d]",
			i,
			m.ShardsMetrics[i].Size,
			m.ShardsMetrics[i].InusePercent,
			m.ShardsMetrics[i].MemLen,
			m.ShardsMetrics[i].MemCount,
			m.ShardsMetrics[i].MemSize,
			m.ShardsMetrics[i].AtCount,
			m.ShardsMetrics[i].AtSize))
	}
	return fmt.Sprintf("size:%d hit:%d mis:%d shards:%s", m.Size, m.Hits, m.Misses, shards.String())
}
