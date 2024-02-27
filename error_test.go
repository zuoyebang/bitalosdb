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

package bitalosdb

import (
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/errorfs"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type panicLogger struct{}

func (l panicLogger) Cost(arg ...interface{}) func() {
	return func() {}
}
func (l panicLogger) Info(args ...interface{}) {
}
func (l panicLogger) Warn(args ...interface{}) {
}
func (l panicLogger) Error(args ...interface{}) {
}
func (l panicLogger) Infof(format string, args ...interface{}) {
}
func (l panicLogger) Warnf(format string, args ...interface{}) {
}
func (l panicLogger) Errorf(format string, args ...interface{}) {
}
func (l panicLogger) Fatalf(format string, args ...interface{}) {
	panic(errors.Errorf("fatal: "+format, args...))
}

type corruptFS struct {
	vfs.FS
	index     int32
	bytesRead int32
}

func (fs corruptFS) maybeCorrupt(n int32, p []byte) {
	newBytesRead := atomic.AddInt32(&fs.bytesRead, n)
	pIdx := newBytesRead - 1 - fs.index
	if pIdx >= 0 && pIdx < n {
		p[pIdx]++
	}
}

func (fs *corruptFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name)
	if err != nil {
		return nil, err
	}
	cf := corruptFile{f, fs}
	for _, opt := range opts {
		opt.Apply(cf)
	}
	return cf, nil
}

type corruptFile struct {
	vfs.File
	fs *corruptFS
}

func (f corruptFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.fs.maybeCorrupt(int32(n), p)
	return n, err
}

func (f corruptFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.fs.maybeCorrupt(int32(n), p)
	return n, err
}

func TestErrors(t *testing.T) {
	run := func(fs *errorfs.FS) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					err = e
				} else {
					t.Fatal(r)
				}
			}
		}()

		d, err := Open("", &Options{
			FS:     fs,
			Logger: panicLogger{},
		})
		if err != nil {
			return err
		}

		key := []byte("a")
		value := []byte("b")
		if err := d.Set(key, value, nil); err != nil {
			return err
		}
		if err := d.Flush(); err != nil {
			return err
		}

		iter := d.NewIter(nil)
		for valid := iter.First(); valid; valid = iter.Next() {
		}
		if err := iter.Close(); err != nil {
			return err
		}
		return d.Close()
	}

	errorCounts := make(map[string]int)
	for i := int32(0); ; i++ {
		fs := errorfs.Wrap(vfs.NewMem(), errorfs.OnIndex(i))
		err := run(fs)
		if err == nil {
			t.Logf("success %d\n", i)
			break
		}
		errorCounts[err.Error()]++
	}

	expectedErrors := []string{
		"fatal: MANIFEST flush failed: injected error",
		"fatal: MANIFEST sync failed: injected error",
		"fatal: MANIFEST set current failed: injected error",
		"fatal: MANIFEST dirsync failed: injected error",
	}
	for _, expected := range expectedErrors {
		if errorCounts[expected] == 0 {
			t.Errorf("expected error %q did not occur", expected)
		}
	}
}

func TestDBWALRotationCrash(t *testing.T) {
	memfs := vfs.NewStrictMem()

	var index int32
	inj := errorfs.InjectorFunc(func(op errorfs.Op, _ string) error {
		if op.OpKind() == errorfs.OpKindWrite && atomic.AddInt32(&index, -1) == -1 {
			memfs.SetIgnoreSyncs(true)
		}
		return nil
	})
	triggered := func() bool { return atomic.LoadInt32(&index) < 0 }

	run := func(fs *errorfs.FS, k int32) (err error) {
		opts := &Options{
			FS:           fs,
			Logger:       panicLogger{},
			MemTableSize: 1024,
		}
		d, err := Open("", opts)
		if err != nil || triggered() {
			return err
		}

		atomic.StoreInt32(&index, k)
		key := []byte("test")
		for i := 0; i < 10; i++ {
			v := []byte(strings.Repeat("b", i))
			err = d.Set(key, v, nil)
			if err != nil || triggered() {
				break
			}
		}
		err = utils.FirstError(err, d.Close())
		return err
	}

	fs := errorfs.Wrap(memfs, inj)
	for k := int32(0); ; k++ {
		atomic.StoreInt32(&index, math.MaxInt32)
		err := run(fs, k)
		if !triggered() {
			t.Logf("No crash at write operation %d\n", k)
			if err != nil {
				t.Fatalf("Filesystem did not 'crash', but error returned: %s", err)
			}
			break
		}
		t.Logf("Crashed at write operation % 2d, error: %v\n", k, err)

		memfs.ResetToSyncedState()
		memfs.SetIgnoreSyncs(false)
		atomic.StoreInt32(&index, math.MaxInt32)
		require.NoError(t, run(fs, k))
	}
}
