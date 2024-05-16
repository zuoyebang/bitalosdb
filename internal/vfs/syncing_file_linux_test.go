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

//go:build linux && !arm

package vfs

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"unsafe"
)

func TestSyncRangeSmokeTest(t *testing.T) {
	testCases := []struct {
		err      error
		expected bool
	}{
		{nil, true},
		{syscall.EINVAL, true},
		{syscall.ENOSYS, false},
	}
	for i, c := range testCases {
		t.Run("", func(t *testing.T) {
			ok := syncRangeSmokeTest(uintptr(i),
				func(fd int, off int64, n int64, flags int) (err error) {
					if i != fd {
						t.Fatalf("expected fd %d, but got %d", i, fd)
					}
					return c.err
				})
			if c.expected != ok {
				t.Fatalf("expected %t, but got %t: %v", c.expected, ok, c.err)
			}
		})
	}
}

func BenchmarkDirectIOWrite(b *testing.B) {
	const targetSize = 16 << 20
	const alignment = 4096

	var wsizes []int
	if testing.Verbose() {
		wsizes = []int{4 << 10, 8 << 10, 16 << 10, 32 << 10}
	} else {
		wsizes = []int{4096}
	}

	for _, wsize := range wsizes {
		b.Run(fmt.Sprintf("wsize=%d", wsize), func(b *testing.B) {
			tmpf, err := os.CreateTemp("", "bitalosdb-db-syncing-file-")
			if err != nil {
				b.Fatal(err)
			}
			filename := tmpf.Name()
			_ = tmpf.Close()
			defer os.Remove(filename)

			var f *os.File
			var size int
			buf := make([]byte, wsize+alignment)
			if a := uintptr(unsafe.Pointer(&buf[0])) & uintptr(alignment-1); a != 0 {
				buf = buf[alignment-a:]
			}
			buf = buf[:wsize]
			init := true

			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if f == nil {
					b.StopTimer()
					f, err = os.OpenFile(filename, syscall.O_DIRECT|os.O_RDWR, 0666)
					if err != nil {
						b.Fatal(err)
					}
					if init {
						for size = 0; size < targetSize; size += len(buf) {
							if _, err := f.WriteAt(buf, int64(size)); err != nil {
								b.Fatal(err)
							}
						}
					}
					if err := f.Sync(); err != nil {
						b.Fatal(err)
					}
					size = 0
					b.StartTimer()
				}
				if _, err := f.WriteAt(buf, int64(size)); err != nil {
					b.Fatal(err)
				}
				size += len(buf)
				if size >= targetSize {
					_ = f.Close()
					f = nil
				}
			}
			b.StopTimer()
		})
	}
}
