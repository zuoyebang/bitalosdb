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
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type syncedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func (b *syncedBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedBuffer) Cost(args ...interface{}) func() {
	begin := time.Now()
	return func() {
		s := fmt.Sprint(args...) + base.FmtDuration(time.Since(begin))
		b.mu.Lock()
		defer b.mu.Unlock()
		b.buf.Write([]byte(s))
		if n := len(s); n == 0 || s[n-1] != '\n' {
			b.buf.Write([]byte("\n"))
		}
	}
}

func (b *syncedBuffer) Info(args ...interface{}) {
	s := fmt.Sprint(args...)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Write([]byte(s))
	if n := len(s); n == 0 || s[n-1] != '\n' {
		b.buf.Write([]byte("\n"))
	}
}

func (b *syncedBuffer) Warn(args ...interface{}) {
	s := fmt.Sprint(args...)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Write([]byte(s))
	if n := len(s); n == 0 || s[n-1] != '\n' {
		b.buf.Write([]byte("\n"))
	}
}

func (b *syncedBuffer) Error(args ...interface{}) {
	s := fmt.Sprint(args...)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Write([]byte(s))
	if n := len(s); n == 0 || s[n-1] != '\n' {
		b.buf.Write([]byte("\n"))
	}
}

func (b *syncedBuffer) Infof(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Write([]byte(s))
	if n := len(s); n == 0 || s[n-1] != '\n' {
		b.buf.Write([]byte("\n"))
	}
}

func (b *syncedBuffer) Warnf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Write([]byte(s))
	if n := len(s); n == 0 || s[n-1] != '\n' {
		b.buf.Write([]byte("\n"))
	}
}

func (b *syncedBuffer) Errorf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Write([]byte(s))
	if n := len(s); n == 0 || s[n-1] != '\n' {
		b.buf.Write([]byte("\n"))
	}
}

func (b *syncedBuffer) Fatalf(format string, args ...interface{}) {
	b.Infof(format, args...)
	runtime.Goexit()
}

func (b *syncedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type loggingFS struct {
	vfs.FS
	w io.Writer
}

func (fs loggingFS) Create(name string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "create: %s\n", name)
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) Link(oldname, newname string) error {
	fmt.Fprintf(fs.w, "link: %s -> %s\n", oldname, newname)
	return fs.FS.Link(oldname, newname)
}

func (fs loggingFS) OpenDir(name string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "open-dir: %s\n", name)
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) Rename(oldname, newname string) error {
	fmt.Fprintf(fs.w, "rename: %s -> %s\n", oldname, newname)
	return fs.FS.Rename(oldname, newname)
}

func (fs loggingFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "reuseForWrite: %s -> %s\n", oldname, newname)
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err == nil {
		f = loggingFile{f, newname, fs.w}
	}
	return f, err
}

func (fs loggingFS) MkdirAll(dir string, perm os.FileMode) error {
	fmt.Fprintf(fs.w, "mkdir-all: %s %#o\n", dir, perm)
	return fs.FS.MkdirAll(dir, perm)
}

func (fs loggingFS) Lock(name string) (io.Closer, error) {
	fmt.Fprintf(fs.w, "lock: %s\n", name)
	return fs.FS.Lock(name)
}

type loggingFile struct {
	vfs.File
	name string
	w    io.Writer
}

func (f loggingFile) Close() error {
	fmt.Fprintf(f.w, "close: %s\n", f.name)
	return f.File.Close()
}

func (f loggingFile) Sync() error {
	fmt.Fprintf(f.w, "sync: %s\n", f.name)
	return f.File.Sync()
}

func TestWriteStallEvents(t *testing.T) {
	const flushCount = 10
	const writeStallEnd = "write stall ending"

	testCases := []struct {
		delayFlush bool
		expected   string
	}{
		{true, "memtable count limit reached"},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			stallEnded := make(chan struct{}, 1)
			createReleased := make(chan struct{}, flushCount)
			var buf syncedBuffer
			listener := EventListener{
				WriteStallBegin: func(info WriteStallBeginInfo) {
					fmt.Fprintln(&buf, info.String())
					createReleased <- struct{}{}
				},
				WriteStallEnd: func() {
					fmt.Fprintln(&buf, writeStallEnd)
					select {
					case stallEnded <- struct{}{}:
					default:
					}
				},
			}
			dir := testDirname
			defer os.RemoveAll(dir)
			os.RemoveAll(dir)
			d, err := Open(dir, &Options{
				EventListener:               listener,
				FS:                          vfs.Default,
				MemTableSize:                256 << 10,
				MemTableStopWritesThreshold: 2,
			})
			require.NoError(t, err)
			defer d.Close()

			for i := 0; i < flushCount; i++ {
				require.NoError(t, d.Set([]byte("a"), nil, NoSync))

				ch, err := d.AsyncFlush()
				require.NoError(t, err)

				if !c.delayFlush {
					<-ch
				}
				if strings.Contains(buf.String(), c.expected) {
					break
				}
			}
			<-stallEnded

			events := buf.String()
			require.Contains(t, events, c.expected)
			require.Contains(t, events, writeStallEnd)
			if testing.Verbose() {
				t.Logf("\n%s", events)
			}
		})
	}
}

func TestEventListenerEnsureDefaultsBackgroundError(t *testing.T) {
	e := EventListener{}
	e.EnsureDefaults(nil)
	e.BackgroundError(errors.New("an example error"))
}

func TestEventListenerEnsureDefaultsSetsAllCallbacks(t *testing.T) {
	e := EventListener{}
	e.EnsureDefaults(nil)
	testAllCallbacksSetInEventListener(t, e)
}

func TestMakeLoggingEventListenerSetsAllCallbacks(t *testing.T) {
	e := MakeLoggingEventListener(nil)
	testAllCallbacksSetInEventListener(t, e)
}

func TestTeeEventListenerSetsAllCallbacks(t *testing.T) {
	e := TeeEventListener(EventListener{}, EventListener{})
	testAllCallbacksSetInEventListener(t, e)
}

func testAllCallbacksSetInEventListener(t *testing.T, e EventListener) {
	t.Helper()
	v := reflect.ValueOf(e)
	for i := 0; i < v.NumField(); i++ {
		fType := v.Type().Field(i)
		fVal := v.Field(i)
		require.Equal(t, reflect.Func, fType.Type.Kind(), "unexpected non-func field: %s", fType.Name)
		require.False(t, fVal.IsNil(), "unexpected nil field: %s", fType.Name)
	}
}
