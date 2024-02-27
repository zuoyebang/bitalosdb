// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitforest

import (
	"sync"
)

func (bf *Bitforest) CompactBithash(jobId int, deletePercent float64) {
	if !bf.opts.UseBithash {
		return
	}

	bf.opts.Logger.Infof("[COMPACTBITHASH %d] compact bithash start", jobId)
	for i := 0; i < len(bf.trees); i++ {
		if bf.IsClosed() {
			break
		}

		bf.getBitree(i).CompactBithash(jobId, deletePercent)
	}
	bf.opts.Logger.Infof("[COMPACTBITHASH %d] compact bithash end", jobId)
}

func (bf *Bitforest) CheckCompactToBitable(maxSize int64) bool {
	if !bf.opts.UseBitable {
		return false
	}

	diskSize := bf.StatsDiskSize()
	bf.opts.Logger.Infof("compact to bitable check diskSize:%d, maxSize:%d", diskSize, maxSize)

	return diskSize >= maxSize
}

func (bf *Bitforest) CompactBitree(jobId int) {
	if !bf.opts.UseBitable {
		return
	}

	bf.SetFlushedBitable()

	var wg sync.WaitGroup
	for i := 0; i < len(bf.trees); i++ {
		if bf.IsClosed() {
			break
		}

		bf.opts.Logger.Infof("[COMPACTBITABLE %d] [tree:%d] bitree compact to bitable start", jobId, i)
		bf.getBitree(i).CompactBitreeToBitable(jobId)
		bf.opts.Logger.Infof("[COMPACTBITABLE %d] [tree:%d] bitree compact to bitable end", jobId, i)

		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			bf.opts.Logger.Infof("[COMPACTBITABLE %d] [tree:%d] manual compact bitable start", jobId, index)
			err := bf.getBitree(index).ManualCompact()
			bf.opts.Logger.Infof("[COMPACTBITABLE %d] [tree:%d] manual compact bitable end", jobId, index)
			if err != nil {
				bf.opts.Logger.Infof("[COMPACTBITABLE %d] [tree:%d] manual compact bitable fail err:%s", jobId, index, err)
			}
		}(i)
	}
	wg.Wait()
}
