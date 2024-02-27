//go:build !linux
// +build !linux

package vfs

func preallocExtend(fd uintptr, offset, length int64) error {
	// It is ok for correctness to no-op file preallocation. WAL recycling is the
	// more important mechanism for WAL sync performance and it doesn't rely on
	// fallocate or posix_fallocate in order to be effective.
	return nil
}
