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
)

// SyncingFileOptions holds the options for a syncingFile.
type SyncingFileOptions struct {
	BytesPerSync    int
	PreallocateSize int
}

type syncingFile struct {
	File
	fd              uintptr
	useSyncRange    bool
	bytesPerSync    int64
	preallocateSize int64
	atomic          struct {
		// The offset at which dirty data has been written.
		offset int64
		// The offset at which data has been synced. Note that if SyncFileRange is
		// being used, the periodic syncing of data during writing will only ever
		// sync up to offset-1MB. This is done to avoid rewriting the tail of the
		// file multiple times, but has the side effect of ensuring that Close will
		// sync the file's metadata.
		syncOffset int64
	}
	preallocatedBlocks int64
	syncData           func() error
	syncTo             func(offset int64) error
	timeDiskOp         func(op func())
}

// NewSyncingFile wraps a writable file and ensures that data is synced
// periodically as it is written. The syncing does not provide persistency
// guarantees for these periodic syncs, but is used to avoid latency spikes if
// the OS automatically decides to write out a large chunk of dirty filesystem
// buffers. The underlying file is fully synced upon close.
func NewSyncingFile(f File, opts SyncingFileOptions) File {
	s := &syncingFile{
		File:            f,
		bytesPerSync:    int64(opts.BytesPerSync),
		preallocateSize: int64(opts.PreallocateSize),
	}
	// Ensure a file that is opened and then closed will be synced, even if no
	// data has been written to it.
	s.atomic.syncOffset = -1

	type fd interface {
		Fd() uintptr
	}
	if d, ok := f.(fd); ok {
		s.fd = d.Fd()
	}
	type dhChecker interface {
		timeDiskOp(op func())
	}
	if d, ok := f.(dhChecker); ok {
		s.timeDiskOp = d.timeDiskOp
	} else {
		s.timeDiskOp = func(op func()) {
			op()
		}
	}

	s.init()

	if s.syncData == nil {
		s.syncData = s.File.Sync
	}
	return WithFd(f, s)
}

// NB: syncingFile.Write is unsafe for concurrent use!
func (f *syncingFile) Write(p []byte) (n int, err error) {
	_ = f.preallocate(atomic.LoadInt64(&f.atomic.offset))

	n, err = f.File.Write(p)
	if err != nil {
		return n, err
	}
	// The offset is updated atomically so that it can be accessed safely from
	// Sync.
	atomic.AddInt64(&f.atomic.offset, int64(n))
	if err := f.maybeSync(); err != nil {
		return 0, err
	}
	return n, nil
}

func (f *syncingFile) preallocate(offset int64) error {
	if f.fd == 0 || f.preallocateSize == 0 {
		return nil
	}

	newPreallocatedBlocks := (offset + f.preallocateSize - 1) / f.preallocateSize
	if newPreallocatedBlocks <= f.preallocatedBlocks {
		return nil
	}

	length := f.preallocateSize * (newPreallocatedBlocks - f.preallocatedBlocks)
	offset = f.preallocateSize * f.preallocatedBlocks
	f.preallocatedBlocks = newPreallocatedBlocks
	return preallocExtend(f.fd, offset, length)
}

func (f *syncingFile) ratchetSyncOffset(offset int64) {
	for {
		syncOffset := atomic.LoadInt64(&f.atomic.syncOffset)
		if syncOffset >= offset {
			return
		}
		if atomic.CompareAndSwapInt64(&f.atomic.syncOffset, syncOffset, offset) {
			return
		}
	}
}

func (f *syncingFile) Sync() error {
	// We update syncOffset (atomically) in order to avoid spurious syncs in
	// maybeSync. Note that even if syncOffset is larger than the current file
	// offset, we still need to call the underlying file's sync for persistence
	// guarantees (which are not provided by sync_file_range).
	f.ratchetSyncOffset(atomic.LoadInt64(&f.atomic.offset))
	return f.syncData()
}

func (f *syncingFile) maybeSync() error {
	if f.bytesPerSync <= 0 {
		return nil
	}

	const syncRangeBuffer = 1 << 20 // 1 MB
	offset := atomic.LoadInt64(&f.atomic.offset)
	if offset <= syncRangeBuffer {
		return nil
	}

	const syncRangeAlignment = 4 << 10 // 4 KB
	syncToOffset := offset - syncRangeBuffer
	syncToOffset -= syncToOffset % syncRangeAlignment
	syncOffset := atomic.LoadInt64(&f.atomic.syncOffset)
	if syncToOffset < 0 || (syncToOffset-syncOffset) < f.bytesPerSync {
		return nil
	}

	if f.fd == 0 {
		return f.Sync()
	}

	// Note that syncTo will always be called with an offset < atomic.offset. The
	// syncTo implementation may choose to sync the entire file (i.e. on OSes
	// which do not support syncing a portion of the file). The syncTo
	// implementation must call ratchetSyncOffset with as much of the file as it
	// has synced.
	return f.syncTo(syncToOffset)
}

func (f *syncingFile) Close() error {
	// Sync any data that has been written but not yet synced. Note that if
	// SyncFileRange was used, atomic.syncOffset will be less than
	// atomic.offset. See syncingFile.syncToRange.
	if atomic.LoadInt64(&f.atomic.offset) > atomic.LoadInt64(&f.atomic.syncOffset) {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return f.File.Close()
}
