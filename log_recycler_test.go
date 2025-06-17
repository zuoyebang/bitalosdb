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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func (r *logRecycler) logNums() []FileNum {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fileInfoNums(r.mu.logs)
}

func (r *logRecycler) maxLogNum() FileNum {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.maxLogNum
}

func TestLogRecycler(t *testing.T) {
	r := logRecycler{limit: 3, minRecycleLogNum: 4}

	require.False(t, r.add(fileInfo{1, 0}))
	require.False(t, r.add(fileInfo{2, 0}))
	require.False(t, r.add(fileInfo{3, 0}))

	require.True(t, r.add(fileInfo{4, 0}))
	require.EqualValues(t, []FileNum{4}, r.logNums())
	require.EqualValues(t, 4, r.maxLogNum())
	fi, ok := r.peek()
	require.True(t, ok)
	require.EqualValues(t, 4, fi.fileNum)
	require.True(t, r.add(fileInfo{5, 0}))
	require.EqualValues(t, []FileNum{4, 5}, r.logNums())
	require.EqualValues(t, 5, r.maxLogNum())
	require.True(t, r.add(fileInfo{6, 0}))
	require.EqualValues(t, []FileNum{4, 5, 6}, r.logNums())
	require.EqualValues(t, 6, r.maxLogNum())

	require.False(t, r.add(fileInfo{7, 0}))
	require.EqualValues(t, []FileNum{4, 5, 6}, r.logNums())
	require.EqualValues(t, 7, r.maxLogNum())

	require.True(t, r.add(fileInfo{4, 0}))
	require.EqualValues(t, []FileNum{4, 5, 6}, r.logNums())
	require.EqualValues(t, 7, r.maxLogNum())

	require.Regexp(t, `invalid 5 vs \[000004 000005 000006\]`, r.pop(5))

	require.NoError(t, r.pop(4))
	require.EqualValues(t, []FileNum{5, 6}, r.logNums())

	require.True(t, r.add(fileInfo{7, 0}))
	require.EqualValues(t, []FileNum{5, 6}, r.logNums())

	require.True(t, r.add(fileInfo{8, 0}))
	require.EqualValues(t, []FileNum{5, 6, 8}, r.logNums())
	require.EqualValues(t, 8, r.maxLogNum())

	require.NoError(t, r.pop(5))
	require.EqualValues(t, []FileNum{6, 8}, r.logNums())
	require.NoError(t, r.pop(6))
	require.EqualValues(t, []FileNum{8}, r.logNums())
	require.NoError(t, r.pop(8))
	require.EqualValues(t, []FileNum(nil), r.logNums())

	require.Regexp(t, `empty`, r.pop(9))
}

func TestRecycleLogs(t *testing.T) {
	defer os.RemoveAll(testDirname)
	os.RemoveAll(testDirname)

	db := openTestDB(testDirname, nil)
	bitower := db.bitowers[testSlotId]

	logNum := func() FileNum {
		bitower.mu.Lock()
		defer bitower.mu.Unlock()
		return bitower.mu.log.queue[len(bitower.mu.log.queue)-1].fileNum
	}

	require.EqualValues(t, []FileNum(nil), bitower.logRecycler.logNums())
	curLog := logNum()
	require.NoError(t, db.Set(makeTestSlotKey([]byte("a")), nil, nil))
	require.NoError(t, bitower.Flush())
	require.EqualValues(t, []FileNum{curLog}, bitower.logRecycler.logNums())
	curLog = logNum()
	require.NoError(t, db.Set(makeTestSlotKey([]byte("b")), nil, nil))
	require.NoError(t, bitower.Flush())
	require.EqualValues(t, []FileNum{curLog}, bitower.logRecycler.logNums())
	require.NoError(t, db.Close())

	db = openTestDB(testDirname, nil)
	bitower = db.bitowers[testSlotId]
	if recycled := bitower.logRecycler.logNums(); len(recycled) != 0 {
		t.Fatalf("expected no recycled WAL files after recovery, but found %d", recycled)
	}
	require.NoError(t, db.Close())
}
