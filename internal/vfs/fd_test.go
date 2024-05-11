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

package vfs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileWrappersHaveFd(t *testing.T) {
	// Use the real filesystem so that we can test vfs.Default, which returns
	// files with Fd().
	tmpf, err := os.CreateTemp("", "bitalosdb-db-fd-file")
	require.NoError(t, err)
	filename := tmpf.Name()
	defer os.Remove(filename)

	// File wrapper case 1: Check if diskHealthCheckingFile has Fd().
	fs2 := WithDiskHealthChecks(Default, 10*time.Second, func(s string, duration time.Duration) {})
	f2, err := fs2.Open(filename)
	require.NoError(t, err)
	if _, ok := f2.(fdGetter); !ok {
		t.Fatal("expected diskHealthCheckingFile to export Fd() method")
	}
	// File wrapper case 2: Check if syncingFile has Fd().
	f3 := NewSyncingFile(f2, SyncingFileOptions{BytesPerSync: 8 << 10 /* 8 KB */})
	if _, ok := f3.(fdGetter); !ok {
		t.Fatal("expected syncingFile to export Fd() method")
	}
	require.NoError(t, f2.Close())
}
