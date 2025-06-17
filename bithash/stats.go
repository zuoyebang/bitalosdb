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

package bithash

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/internal/utils"
)

type Stats struct {
	FileTotal   atomic.Uint32
	KeyTotal    atomic.Uint64
	DelKeyTotal atomic.Uint64
}

func (s *Stats) String() string {
	return fmt.Sprintf("fileTotal:%d keyTotal:%d delKeyTotal:%d",
		s.FileTotal.Load(), s.KeyTotal.Load(), s.DelKeyTotal.Load())
}

func (b *Bithash) StatsAddKeyTotal(n int) {
	b.stats.KeyTotal.Add(uint64(n))
}

func (b *Bithash) StatsSubKeyTotal(n int) {
	b.stats.KeyTotal.Add(^uint64(n - 1))
}

func (b *Bithash) StatsGetDelKeyTotal() uint64 {
	return b.stats.DelKeyTotal.Load()
}

func (b *Bithash) StatsSubDelKeyTotal(n int) {
	b.stats.DelKeyTotal.Add(^uint64(n - 1))
}

func (b *Bithash) StatsSetDelKeyTotal(n int) {
	b.stats.DelKeyTotal.Store(uint64(n))
}

func (b *Bithash) Stats() *Stats {
	return b.stats
}

func (b *Bithash) StatsToString() string {
	return b.stats.String()
}

func (b *Bithash) DebugInfo(dataType string) string {
	b.meta.mu.RLock()
	defer b.meta.mu.RUnlock()

	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "%s-bithash%d:dirname=%s dirSize=%s files=%d\n",
		dataType, b.index, b.dirname,
		utils.FmtSize(utils.GetDirSize(b.dirname)),
		len(b.meta.mu.filesMeta))

	for fn, fileMeta := range b.meta.mu.filesMeta {
		if fileMeta.state == fileMetaStateImmutable {
			var delPercent float64
			if fileMeta.keyNum == 0 {
				delPercent = 0
			} else {
				delPercent = float64(fileMeta.delKeyNum) / float64(fileMeta.keyNum)
			}
			fmt.Fprintf(buf, "%s-bithash%d-%d.bht:%s delPercent=%.2f\n", dataType, b.index, fn, fileMeta.String(), delPercent)
		} else {
			fmt.Fprintf(buf, "%s-bithash%d-%d.bht:%s\n", dataType, b.index, fn, fileMeta.String())
		}
	}

	return buf.String()
}
