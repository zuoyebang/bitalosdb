//go:build windows
// +build windows

package vfs

import "golang.org/x/sys/windows"

func (defaultFS) GetDiskUsage(path string) (DiskUsage, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return DiskUsage{}, err
	}
	var freeBytes uint64
	du := DiskUsage{}
	err = windows.GetDiskFreeSpaceEx(p, &du.AvailBytes, &du.TotalBytes, &freeBytes)
	du.UsedBytes = du.TotalBytes - freeBytes
	return du, err
}
