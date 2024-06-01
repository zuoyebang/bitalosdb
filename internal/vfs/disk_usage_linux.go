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

//go:build linux

package vfs

import "golang.org/x/sys/unix"

func (defaultFS) GetDiskUsage(path string) (DiskUsage, error) {
	stat := unix.Statfs_t{}
	if err := unix.Statfs(path, &stat); err != nil {
		return DiskUsage{}, err
	}

	// We use stat.Frsize here rather than stat.Bsize because on
	// Linux Bavail and Bfree are in Frsize units.
	//
	// On most filesystems Frsize and Bsize will be set to the
	// same value, but on some filesystems bsize returns the
	// "optimal transfer block size"[1] which may be different
	// (typically larger) than the actual block size.
	//
	// This confusion is cleared up in the statvfs[2] libc function,
	// but the statfs system call used above varies across
	// platforms.
	//
	// Frsize is used by GNU coreutils and other libraries, so
	// this also helps ensure that we get the same results as one
	// would get if they ran `df` on the given path.
	//
	// [1] https://man7.org/linux/man-pages/man2/statfs.2.html
	// [2] https://man7.org/linux/man-pages/man3/statvfs.3.html
	freeBytes := uint64(stat.Frsize) * uint64(stat.Bfree)
	availBytes := uint64(stat.Frsize) * uint64(stat.Bavail)
	totalBytes := uint64(stat.Bsize) * uint64(stat.Blocks)
	return DiskUsage{
		AvailBytes: availBytes,
		TotalBytes: totalBytes,
		UsedBytes:  totalBytes - freeBytes,
	}, nil
}
