//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package vfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
)

var lockedFiles struct {
	mu struct {
		sync.Mutex
		files map[string]bool
	}
}

// lockCloser hides all of an os.File's methods, except for Close.
type lockCloser struct {
	name string
	f    *os.File
}

func (l lockCloser) Close() error {
	lockedFiles.mu.Lock()
	defer lockedFiles.mu.Unlock()
	if !lockedFiles.mu.files[l.name] {
		fmt.Printf("panic: lock file %q is not locked\n", l.name)
	}
	delete(lockedFiles.mu.files, l.name)

	return l.f.Close()
}

func (defaultFS) Lock(name string) (io.Closer, error) {
	lockedFiles.mu.Lock()
	defer lockedFiles.mu.Unlock()
	if lockedFiles.mu.files == nil {
		lockedFiles.mu.files = map[string]bool{}
	}
	if lockedFiles.mu.files[name] {
		return nil, errors.New("lock held by current process")
	}

	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	spec := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0, // 0 means to lock the entire file.
		Pid:    int32(os.Getpid()),
	}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &spec); err != nil {
		f.Close()
		return nil, err
	}
	lockedFiles.mu.files[name] = true
	return lockCloser{name, f}, nil
}
