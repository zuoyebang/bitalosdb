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

//go:build linux && !arm

package vfs

import "syscall"

type syncFileRange func(fd int, off int64, n int64, flags int) (err error)

// sync_file_range depends on both the filesystem, and the broader kernel
// support. In particular, Windows Subsystem for Linux does not support
// sync_file_range, even when used with ext{2,3,4}. syncRangeSmokeTest performs
// a test of of sync_file_range, returning false on ENOSYS, and true otherwise.
func syncRangeSmokeTest(fd uintptr, fn syncFileRange) bool {
	err := fn(int(fd), 0 /* offset */, 0 /* nbytes */, 0 /* flags */)
	return err != syscall.ENOSYS
}

func isSyncRangeSupported(fd uintptr) bool {
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(int(fd), &stat); err != nil {
		return false
	}

	// Allowlist which filesystems we allow using sync_file_range with as some
	// filesystems treat that syscall as a noop (notably ZFS). A allowlist is
	// used instead of a denylist in order to have a more graceful failure mode
	// in case a filesystem we haven't tested is encountered. Currently only
	// ext2/3/4 are known to work properly.
	const extMagic = 0xef53
	switch stat.Type {
	case extMagic:
		return syncRangeSmokeTest(fd, syscall.SyncFileRange)
	}
	return false
}

func (f *syncingFile) init() {
	if f.fd == 0 {
		return
	}
	f.timeDiskOp(func() {
		f.useSyncRange = isSyncRangeSupported(f.fd)
	})
	if f.useSyncRange {
		f.syncTo = f.syncToRange
	} else {
		f.syncTo = f.syncToFdatasync
	}
	f.syncData = f.syncFdatasync
}

func (f *syncingFile) syncFdatasync() error {
	if f.fd == 0 {
		return f.File.Sync()
	}
	var err error
	f.timeDiskOp(func() {
		err = syscall.Fdatasync(int(f.fd))
	})
	return err
}

func (f *syncingFile) syncToFdatasync(_ int64) error {
	return f.Sync()
}

func (f *syncingFile) syncToRange(offset int64) error {
	const (
		waitBefore = 0x1
		write      = 0x2
		// waitAfter = 0x4
	)

	// Note that syncToRange is only called with an offset that is guaranteed to
	// be less than atomic.offset (i.e. the write offset). This implies the
	// syncingFile.Close will Sync the rest of the data, as well as the file's
	// metadata.
	f.ratchetSyncOffset(offset)

	// By specifying write|waitBefore for the flags, we're instructing
	// SyncFileRange to a) wait for any outstanding data being written to finish,
	// and b) to queue any other dirty data blocks in the range [0,offset] for
	// writing. The actual writing of this data will occur asynchronously. The
	// use of `waitBefore` is to limit how much dirty data is allowed to
	// accumulate. Linux sometimes behaves poorly when a large amount of dirty
	// data accumulates, impacting other I/O operations.
	var err error
	f.timeDiskOp(func() {
		err = syscall.SyncFileRange(int(f.fd), 0, offset, write|waitBefore)
	})
	return err
}
