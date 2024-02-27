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

package bdb

import (
	"fmt"
	"testing"
)

func TestTx_allocatePageStats(t *testing.T) {
	f := newTestFreelist()
	ids := []pgid{2, 3}
	f.readIDs(ids)

	tx := &Tx{
		db: &DB{
			freelist: f,
			pageSize: defaultPageSize,
		},
		meta:  &meta{},
		pages: make(map[pgid]*page),
	}

	prePageTotal := tx.Stats().PageCount
	allocateCnt := f.free_count()
	fmt.Println(allocateCnt)

	if _, _, err := tx.allocate(allocateCnt); err != nil {
		t.Fatal(err)
	}

	if tx.Stats().PageCount != prePageTotal+allocateCnt {
		t.Errorf("Allocated %d but got %d page in stats", allocateCnt, tx.Stats().PageCount)
	}
}
