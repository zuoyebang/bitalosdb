//go:build darwin || openbsd || dragonfly || freebsd
// +build darwin openbsd dragonfly freebsd

package vfs

import "golang.org/x/sys/unix"

func (defaultFS) GetDiskUsage(path string) (DiskUsage, error) {
	stat := unix.Statfs_t{}
	if err := unix.Statfs(path, &stat); err != nil {
		return DiskUsage{}, err
	}

	freeBytes := uint64(stat.Bsize) * uint64(stat.Bfree)
	availBytes := uint64(stat.Bsize) * uint64(stat.Bavail)
	totalBytes := uint64(stat.Bsize) * uint64(stat.Blocks)
	return DiskUsage{
		AvailBytes: availBytes,
		TotalBytes: totalBytes,
		UsedBytes:  totalBytes - freeBytes,
	}, nil
}
