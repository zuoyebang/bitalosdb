// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
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

package vfs

import (
	"sync/atomic"
	"time"
)

const (
	// defaultTickInterval is the default interval between two ticks of each
	// diskHealthCheckingFile loop iteration.
	defaultTickInterval = 2 * time.Second
)

// diskHealthCheckingFile is a File wrapper to detect slow disk operations, and
// call onSlowDisk if a disk operation is seen to exceed diskSlowThreshold.
//
// This struct creates a goroutine (in startTicker()) that, at every tick
// interval, sees if there's a disk operation taking longer than the specified
// duration. This setup is preferable to creating a new timer at every disk
// operation, as it reduces overhead per disk operation.
type diskHealthCheckingFile struct {
	File

	onSlowDisk        func(time.Duration)
	diskSlowThreshold time.Duration
	tickInterval      time.Duration

	stopper        chan struct{}
	lastWriteNanos int64
}

// newDiskHealthCheckingFile instantiates a new diskHealthCheckingFile, with the
// specified time threshold and event listener.
func newDiskHealthCheckingFile(
	file File, diskSlowThreshold time.Duration, onSlowDisk func(time.Duration),
) *diskHealthCheckingFile {
	return &diskHealthCheckingFile{
		File:              file,
		onSlowDisk:        onSlowDisk,
		diskSlowThreshold: diskSlowThreshold,
		tickInterval:      defaultTickInterval,

		stopper: make(chan struct{}),
	}
}

// startTicker starts a new goroutine with a ticker to monitor disk operations.
// Can only be called if the ticker goroutine isn't running already.
func (d *diskHealthCheckingFile) startTicker() {
	if d.diskSlowThreshold == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(d.tickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-d.stopper:
				return

			case <-ticker.C:
				lastWriteNanos := atomic.LoadInt64(&d.lastWriteNanos)
				if lastWriteNanos == 0 {
					continue
				}
				lastWrite := time.Unix(0, lastWriteNanos)
				now := time.Now()
				if lastWrite.Add(d.diskSlowThreshold).Before(now) {
					// diskSlowThreshold was exceeded. Call the passed-in
					// listener.
					d.onSlowDisk(now.Sub(lastWrite))
				}
			}
		}
	}()
}

// stopTicker stops the goroutine started in startTicker.
func (d *diskHealthCheckingFile) stopTicker() {
	close(d.stopper)
}

// Write implements the io.Writer interface.
func (d *diskHealthCheckingFile) Write(p []byte) (n int, err error) {
	d.timeDiskOp(func() {
		n, err = d.File.Write(p)
	})
	return n, err
}

// Seek implements the io.Seeker interface.
func (d *diskHealthCheckingFile) Seek(offset int64, whence int) (n int64, err error) {
	d.timeDiskOp(func() {
		n, err = d.File.Seek(offset, whence)
	})
	return n, err
}

// Close implements the io.Closer interface.
func (d *diskHealthCheckingFile) Close() error {
	d.stopTicker()
	return d.File.Close()
}

// Sync implements the io.Syncer interface.
func (d *diskHealthCheckingFile) Sync() (err error) {
	d.timeDiskOp(func() {
		err = d.File.Sync()
	})
	return err
}

// timeDiskOp runs the specified closure and makes its timing visible to the
// monitoring goroutine, in case it exceeds one of the slow disk durations.
func (d *diskHealthCheckingFile) timeDiskOp(op func()) {
	if d == nil {
		op()
		return
	}

	atomic.StoreInt64(&d.lastWriteNanos, time.Now().UnixNano())
	defer func() {
		atomic.StoreInt64(&d.lastWriteNanos, 0)
	}()
	op()
}

type diskHealthCheckingFS struct {
	FS

	diskSlowThreshold time.Duration
	onSlowDisk        func(string, time.Duration)
}

// WithDiskHealthChecks wraps an FS and ensures that all
// write-oriented created with that FS are wrapped with disk health detection
// checks. Disk operations that are observed to take longer than
// diskSlowThreshold trigger an onSlowDisk call.
func WithDiskHealthChecks(
	fs FS, diskSlowThreshold time.Duration, onSlowDisk func(string, time.Duration),
) FS {
	return diskHealthCheckingFS{
		FS:                fs,
		diskSlowThreshold: diskSlowThreshold,
		onSlowDisk:        onSlowDisk,
	}
}

// Create implements the vfs.FS interface.
func (d diskHealthCheckingFS) Create(name string) (File, error) {
	f, err := d.FS.Create(name)
	if err != nil {
		return f, err
	}
	if d.diskSlowThreshold == 0 {
		return f, nil
	}
	checkingFile := newDiskHealthCheckingFile(f, d.diskSlowThreshold, func(duration time.Duration) {
		d.onSlowDisk(name, duration)
	})
	checkingFile.startTicker()
	return WithFd(f, checkingFile), nil
}

// ReuseForWrite implements the vfs.FS interface.
func (d diskHealthCheckingFS) ReuseForWrite(oldname, newname string) (File, error) {
	f, err := d.FS.ReuseForWrite(oldname, newname)
	if err != nil {
		return f, err
	}
	if d.diskSlowThreshold == 0 {
		return f, nil
	}
	checkingFile := newDiskHealthCheckingFile(f, d.diskSlowThreshold, func(duration time.Duration) {
		d.onSlowDisk(newname, duration)
	})
	checkingFile.startTicker()
	return WithFd(f, checkingFile), nil
}

// OpenForWrite implements the vfs.FS interface.
func (d diskHealthCheckingFS) OpenForWrite(name string) (File, error) {
	f, err := d.FS.OpenForWrite(name)
	if err != nil {
		return f, err
	}
	if d.diskSlowThreshold == 0 {
		return f, nil
	}
	checkingFile := newDiskHealthCheckingFile(f, d.diskSlowThreshold, func(duration time.Duration) {
		d.onSlowDisk(name, duration)
	})
	checkingFile.startTicker()
	return WithFd(f, checkingFile), nil
}
