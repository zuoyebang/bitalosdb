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
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/stretchr/testify/require"
)

func TestCheckpointFlush(t *testing.T) {
	defer func() {
		os.RemoveAll("./data")
		os.RemoveAll("./checkpoints")
	}()
	fs := vfs.Default
	d, err := Open("./data", &Options{FS: fs})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer cancel()
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			if err := d.Set([]byte(fmt.Sprintf("key%d", i)), nil, nil); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer cancel()
		defer wg.Done()
		for ctx.Err() == nil {
			if err := d.Flush(); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	loopNum := 20
	check := make([]string, loopNum)
	go func() {
		defer ctx.Done()
		defer cancel()
		defer wg.Done()
		for i := 0; ctx.Err() == nil && i < loopNum; i++ {
			dir := fmt.Sprintf("checkpoints/checkpoint%d", i)
			if err := d.Checkpoint(dir); err != nil {
				t.Error(err)
				return
			}
			check[i] = dir
		}
	}()
	<-ctx.Done()
	wg.Wait()
	require.NoError(t, d.Close())

	opts := &Options{FS: fs}
	for _, dir := range check {
		d2, err := Open(dir, opts)
		if err != nil {
			t.Error(err)
			return
		}
		if err := d2.Close(); err != nil {
			t.Error(err)
		}
	}
}

func TestCheckpointFlushWAL(t *testing.T) {
	const checkpointPath = "./checkpoints/checkpoint"

	defer func() {
		os.RemoveAll("./data")
		os.RemoveAll("./checkpoints")
	}()

	fs := vfs.Default
	opts := &Options{FS: fs}
	key, value := []byte("key"), []byte("value")

	{
		d, err := Open("./data", opts)
		require.NoError(t, err)
		{
			wb := d.NewBatch()
			err = wb.Set(key, value, nil)
			require.NoError(t, err)
			err = d.Apply(wb, NoSync)
			require.NoError(t, err)
		}
		err = d.Checkpoint(checkpointPath, WithFlushedWAL())
		require.NoError(t, err)
		require.NoError(t, d.Close())
	}

	{
		files, err := fs.List(checkpointPath)
		require.NoError(t, err)
		hasLogFile := false
		for _, f := range files {
			info, err := fs.Stat(fs.PathJoin(checkpointPath, f))
			require.NoError(t, err)
			if strings.HasSuffix(f, ".log") {
				hasLogFile = true
				require.NotZero(t, info.Size())
			}
		}
		require.True(t, hasLogFile)
	}

	{
		d, err := Open(checkpointPath, opts)
		require.NoError(t, err)
		iter := d.NewIter(nil)
		require.True(t, iter.First())
		require.Equal(t, key, iter.Key())
		require.Equal(t, value, iter.Value())
		require.False(t, iter.Next())
		require.NoError(t, iter.Close())
		require.NoError(t, d.Close())
	}
}
