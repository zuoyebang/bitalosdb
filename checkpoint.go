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
	"os"

	"github.com/cockroachdb/errors/oserror"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type checkpointOptions struct {
	flushWAL bool
}

type CheckpointOption func(*checkpointOptions)

func WithFlushedWAL() CheckpointOption {
	return func(opt *checkpointOptions) {
		opt.flushWAL = true
	}
}

func mkdirAllAndSyncParents(fs vfs.FS, destDir string) (vfs.File, error) {
	var parentPaths []string
	if err := fs.MkdirAll(destDir, 0755); err != nil {
		return nil, err
	}

	foundExistingAncestor := false
	for parentPath := fs.PathDir(destDir); parentPath != "."; parentPath = fs.PathDir(parentPath) {
		parentPaths = append(parentPaths, parentPath)
		_, err := fs.Stat(parentPath)
		if err == nil {
			foundExistingAncestor = true
			break
		}
		if !oserror.IsNotExist(err) {
			return nil, err
		}
	}

	if !foundExistingAncestor {
		parentPaths = append(parentPaths, "")
	}

	for _, parentPath := range parentPaths {
		parentDir, err := fs.OpenDir(parentPath)
		if err != nil {
			return nil, err
		}
		err = parentDir.Sync()
		if err != nil {
			_ = parentDir.Close()
			return nil, err
		}
		err = parentDir.Close()
		if err != nil {
			return nil, err
		}
	}
	return fs.OpenDir(destDir)
}

func (d *DB) Checkpoint(destDir string, opts ...CheckpointOption) (ckErr error) {
	d.opts.Logger.Info("checkpoint start to run")
	defer d.opts.Logger.Cost("checkpoint done")()

	opt := &checkpointOptions{}
	for _, fn := range opts {
		fn(opt)
	}

	if _, err := d.opts.FS.Stat(destDir); !oserror.IsNotExist(err) {
		if err == nil {
			return &os.PathError{
				Op:   "checkpoint",
				Path: destDir,
				Err:  oserror.ErrExist,
			}
		}
		return err
	}

	if opt.flushWAL && !d.opts.DisableWAL {
		if err := d.LogData(nil /* data */, Sync); err != nil {
			return err
		}
	}

	d.mu.Lock()
	d.disableFileDeletions()
	defer func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.enableFileDeletions()
	}()

	memQueue := d.mu.mem.queue
	d.mu.Unlock()

	fs := vfs.WithSyncingFS(d.opts.FS, vfs.SyncingFileOptions{
		BytesPerSync: d.opts.BytesPerSync,
	})

	var dir vfs.File
	defer func() {
		if dir != nil {
			_ = dir.Close()
		}
		if ckErr != nil {
			paths, _ := fs.List(destDir)
			for _, path := range paths {
				_ = fs.Remove(path)
			}
			_ = fs.Remove(destDir)
		}
	}()
	dir, ckErr = mkdirAllAndSyncParents(fs, destDir)
	if ckErr != nil {
		return ckErr
	}

	{
		srcPath := base.MakeFilepath(fs, d.dirname, fileTypeMeta, 0)
		destFile := fs.PathJoin(destDir, fs.PathBase(srcPath))
		err := vfs.Copy(fs, srcPath, destFile)
		if err != nil {
			return err
		}
	}

	for i := range memQueue {
		logNum := memQueue[i].logNum
		if logNum == 0 {
			continue
		}
		srcPath := base.MakeFilepath(fs, d.walDirname, fileTypeLog, logNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		ckErr = vfs.Copy(fs, srcPath, destPath)
		if ckErr != nil {
			return ckErr
		}
	}

	{
		ckErr = d.bf.Checkpoint(destDir)
		if ckErr != nil {
			return ckErr
		}
	}

	ckErr = dir.Sync()
	if ckErr != nil {
		return ckErr
	}
	ckErr = dir.Close()
	dir = nil
	return ckErr
}

func (d *DB) SetCheckpointLock(lock bool) {
	if lock {
		d.LockTask()
		d.dbState.LockDbWrite()
	} else {
		d.dbState.UnlockDbWrite()
		d.UnlockTask()
	}
}

func (d *DB) SetCheckpointHighPriority(v bool) {
	d.dbState.SetHighPriority(v)
}
