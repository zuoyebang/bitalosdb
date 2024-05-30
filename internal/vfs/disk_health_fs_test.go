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
	"io"
	"os"
	"testing"
	"time"
)

type mockFile struct {
	syncDuration time.Duration
}

func (m mockFile) Close() error {
	return nil
}

func (m mockFile) Read(p []byte) (n int, err error) {
	panic("unimplemented")
}

func (m mockFile) ReadAt(p []byte, off int64) (n int, err error) {
	panic("unimplemented")
}

func (m mockFile) Write(p []byte) (n int, err error) {
	time.Sleep(m.syncDuration)
	return len(p), nil
}

func (m mockFile) Seek(offset int64, whence int) (int64, error) {
	panic("implement me")
}

func (m mockFile) Stat() (os.FileInfo, error) {
	panic("unimplemented")
}

func (m mockFile) Sync() error {
	time.Sleep(m.syncDuration)
	return nil
}

var _ File = &mockFile{}

type mockFS struct {
	syncDuration time.Duration
}

func (m mockFS) Create(name string) (File, error) {
	return mockFile(m), nil
}

func (m mockFS) Link(oldname, newname string) error {
	panic("unimplemented")
}

func (m mockFS) Open(name string, opts ...OpenOption) (File, error) {
	panic("unimplemented")
}

func (m mockFS) OpenDir(name string) (File, error) {
	panic("unimplemented")
}

func (m mockFS) Remove(name string) error {
	panic("unimplemented")
}

func (m mockFS) RemoveAll(name string) error {
	panic("unimplemented")
}

func (m mockFS) Rename(oldname, newname string) error {
	panic("unimplemented")
}

func (m mockFS) ReuseForWrite(oldname, newname string) (File, error) {
	return mockFile(m), nil
}

func (m mockFS) MkdirAll(dir string, perm os.FileMode) error {
	panic("unimplemented")
}

func (m mockFS) Lock(name string) (io.Closer, error) {
	panic("unimplemented")
}

func (m mockFS) List(dir string) ([]string, error) {
	panic("unimplemented")
}

func (m mockFS) Stat(name string) (os.FileInfo, error) {
	panic("unimplemented")
}

func (m mockFS) PathBase(path string) string {
	panic("unimplemented")
}

func (m mockFS) PathJoin(elem ...string) string {
	panic("unimplemented")
}

func (m mockFS) PathDir(path string) string {
	panic("unimplemented")
}

func (m mockFS) GetDiskUsage(path string) (DiskUsage, error) {
	panic("unimplemented")
}

func (m mockFS) OpenForWrite(path string) (File, error) {
	panic("unimplemented")
}

func (m mockFS) OpenWR(path string) (File, error) {
	panic("unimplemented")
}

var _ FS = &mockFS{}

func TestDiskHealthChecking(t *testing.T) {
	diskSlow := make(chan time.Duration, 100)
	slowThreshold := 1 * time.Second
	mockFS := &mockFS{syncDuration: 3 * time.Second}
	fs := WithDiskHealthChecks(mockFS, slowThreshold, func(s string, duration time.Duration) {
		diskSlow <- duration
	})
	dhFile, _ := fs.Create("test")
	defer dhFile.Close()

	dhFile.Sync()

	select {
	case d := <-diskSlow:
		if d.Seconds() < slowThreshold.Seconds() {
			t.Fatalf("expected %0.1f to be greater than threshold %0.1f", d.Seconds(), slowThreshold.Seconds())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disk stall detector did not detect slow disk operation")
	}
}
