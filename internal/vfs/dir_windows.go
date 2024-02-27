//go:build windows
// +build windows

package vfs

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

type windowsDir struct {
	File
}

func (windowsDir) Sync() error {
	return nil
}

func (defaultFS) OpenDir(name string) (File, error) {
	f, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return windowsDir{f}, nil
}
