// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

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
