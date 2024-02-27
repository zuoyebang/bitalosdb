package vfs

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

func (defaultFS) OpenDir(name string) (File, error) {
	f, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	return f, errors.WithStack(err)
}
