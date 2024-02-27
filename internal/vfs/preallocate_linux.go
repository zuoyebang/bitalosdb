//go:build linux
// +build linux

package vfs

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func preallocExtend(fd uintptr, offset, length int64) error {
	return syscall.Fallocate(int(fd), unix.FALLOC_FL_KEEP_SIZE, offset, length)
}
