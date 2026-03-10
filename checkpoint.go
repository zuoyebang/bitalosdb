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

	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

const (
	ckpStatusNone = iota
	ckpStatusSnapshoStart
	ckpStatusSnapshotEnd
)

func (d *DB) checkpointSyncedSwitchVmVt() {
	for _, shard := range d.vmShard {
		shard.switchVmVtable()
	}
}

func (d *DB) Checkpoint(dstDir string) (func(), error) {
	d.opts.Logger.Infof("checkpoint start to run dstDir:%s", dstDir)
	defer d.opts.Logger.Cost("checkpoint done")()

	if os2.IsExist(dstDir) {
		return nil, os.ErrExist
	}

	fs := vfs.WithSyncingFS(d.opts.FS, vfs.SyncingFileOptions{
		BytesPerSync: d.opts.BytesPerSync,
	})

	var dir vfs.File
	var err error
	defer func() {
		if dir != nil {
			_ = dir.Close()
		}
		if err != nil {
			paths, _ := fs.List(dstDir)
			for i := range paths {
				_ = fs.Remove(paths[i])
			}
			_ = fs.Remove(dstDir)
		}
	}()
	dir, err = vfs.MkdirAllAndSyncParents(fs, dstDir)
	if err != nil {
		return nil, err
	}

	if err = d.Flush(false); err != nil {
		return nil, err
	}

	d.ckpStatus.Store(ckpStatusSnapshoStart)
	d.dbState.LockTask()
	d.dbState.LockDbKKVWrite()
	d.opts.Logger.Info("bitalosdb checkpoint LockTask")

	closer := func() {
		d.dbState.UnlockTask()
		d.ckpStatus.Store(ckpStatusNone)
		d.opts.Logger.Info("bitalosdb checkpoint UnlockTask")
		d.checkpointSyncedSwitchVmVt()
	}

	defer func() {
		d.dbState.UnlockDbKKVWrite()
		d.ckpStatus.Store(ckpStatusSnapshotEnd)
		if err != nil {
			d.dbState.UnlockTask()
			d.ckpStatus.Store(ckpStatusNone)
			closer = nil
		}
	}()

	srcMetaPath := d.meta.path
	if err = vfs.Copy(fs, srcMetaPath, fs.PathJoin(dstDir, fs.PathBase(srcMetaPath))); err != nil {
		return nil, err
	}

	for i := range d.bituples {
		bt := d.getBitupleSafe(i)
		if bt != nil {
			if err = bt.checkpoint(fs, dstDir); err != nil {
				return nil, err
			}
		}
	}

	if err = dir.Sync(); err != nil {
		return nil, err
	}
	if err = dir.Close(); err != nil {
		return nil, err
	}
	return closer, nil
}

func (d *DB) IsCheckpointHighPriority() bool {
	return d.ckpStatus.Load() == ckpStatusSnapshoStart
}

func (d *DB) IsCheckpointSyncing() bool {
	return d.ckpStatus.Load() != ckpStatusNone
}
