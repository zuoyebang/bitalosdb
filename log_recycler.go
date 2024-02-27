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
	"sync"

	"github.com/cockroachdb/errors"
)

type logRecycler struct {
	limit            int
	minRecycleLogNum FileNum

	mu struct {
		sync.Mutex
		logs      []fileInfo
		maxLogNum FileNum
	}
}

func (r *logRecycler) add(logInfo fileInfo) bool {
	if logInfo.fileNum < r.minRecycleLogNum {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if logInfo.fileNum <= r.mu.maxLogNum {
		return true
	}
	r.mu.maxLogNum = logInfo.fileNum
	if len(r.mu.logs) >= r.limit {
		return false
	}
	r.mu.logs = append(r.mu.logs, logInfo)
	return true
}

func (r *logRecycler) peek() (fileInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.logs) == 0 {
		return fileInfo{}, false
	}
	return r.mu.logs[0], true
}

func (r *logRecycler) stats() (count int, size uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count = len(r.mu.logs)
	for i := 0; i < count; i++ {
		size += r.mu.logs[i].fileSize
	}
	return count, size
}

func (r *logRecycler) pop(logNum FileNum) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.logs) == 0 {
		return errors.New("bitalosdb: log recycler empty")
	}
	if r.mu.logs[0].fileNum != logNum {
		return errors.Errorf("bitalosdb: log recycler invalid %d vs %d", errors.Safe(logNum), errors.Safe(fileInfoNums(r.mu.logs)))
	}
	r.mu.logs = r.mu.logs[1:]
	return nil
}

func fileInfoNums(finfos []fileInfo) []FileNum {
	if len(finfos) == 0 {
		return nil
	}
	nums := make([]FileNum, len(finfos))
	for i := range finfos {
		nums[i] = finfos[i].fileNum
	}
	return nums
}
