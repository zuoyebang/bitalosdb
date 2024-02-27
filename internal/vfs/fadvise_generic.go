//go:build !linux
// +build !linux

package vfs

func fadviseRandom(f uintptr) error {
	return nil
}

func fadviseSequential(f uintptr) error {
	return nil
}
