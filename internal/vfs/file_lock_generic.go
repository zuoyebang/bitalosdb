//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows
// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!windows

package vfs

import (
	"io"
	"runtime"

	"github.com/cockroachdb/errors"
)

func (defFS) Lock(name string) (io.Closer, error) {
	return nil, errors.Errorf("bitalosdb: file locking is not implemented on %s/%s",
		errors.Safe(runtime.GOOS), errors.Safe(runtime.GOARCH))
}
