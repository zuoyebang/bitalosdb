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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/errors/oserror"
	"github.com/stretchr/testify/require"
)

func normalizeError(err error) error {
	// It is OS-specific which errors match IsExist, IsNotExist, and
	// IsPermission, with OS-specific error messages. We normalize to the
	// oserror.Err* errors which have standard error messages across
	// platforms.
	switch {
	case oserror.IsExist(err):
		return oserror.ErrExist
	case oserror.IsNotExist(err):
		return oserror.ErrNotExist
	case oserror.IsPermission(err):
		return oserror.ErrPermission
	}
	return err
}

type loggingFS struct {
	FS
	base    string
	w       io.Writer
	linkErr error
}

func (fs loggingFS) stripBase(path string) string {
	if strings.HasPrefix(path, fs.base+"/") {
		return path[len(fs.base)+1:]
	}
	return path
}

func (fs loggingFS) Create(name string) (File, error) {
	f, err := fs.FS.Create(name)
	fmt.Fprintf(fs.w, "create: %s [%v]\n", fs.stripBase(name), normalizeError(err))
	return loggingFile{f, fs.PathBase(name), fs.w}, err
}

func (fs loggingFS) Link(oldname, newname string) error {
	err := fs.linkErr
	if err == nil {
		err = fs.FS.Link(oldname, newname)
	}
	fmt.Fprintf(fs.w, "link: %s -> %s [%v]\n",
		fs.stripBase(oldname), fs.stripBase(newname), normalizeError(err))
	return err
}

func (fs loggingFS) ReuseForWrite(oldname, newname string) (File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err == nil {
		f = loggingFile{f, fs.PathBase(newname), fs.w}
	}
	fmt.Fprintf(fs.w, "reuseForWrite: %s -> %s [%v]\n",
		fs.stripBase(oldname), fs.stripBase(newname), normalizeError(err))
	return f, err
}

func (fs loggingFS) MkdirAll(dir string, perm os.FileMode) error {
	err := fs.FS.MkdirAll(dir, perm)
	fmt.Fprintf(fs.w, "mkdir: %s [%v]\n", fs.stripBase(dir), normalizeError(err))
	return err
}

func (fs loggingFS) Open(name string, opts ...OpenOption) (File, error) {
	f, err := fs.FS.Open(name, opts...)
	fmt.Fprintf(fs.w, "open: %s [%v]\n", fs.stripBase(name), normalizeError(err))
	return loggingFile{f, fs.stripBase(name), fs.w}, err
}

func (fs loggingFS) Remove(name string) error {
	err := fs.FS.Remove(name)
	fmt.Fprintf(fs.w, "remove: %s [%v]\n", fs.stripBase(name), normalizeError(err))
	return err
}

func (fs loggingFS) RemoveAll(name string) error {
	err := fs.FS.RemoveAll(name)
	fmt.Fprintf(fs.w, "remove-all: %s [%v]\n", fs.stripBase(name), normalizeError(err))
	return err
}

type loggingFile struct {
	File
	name string
	w    io.Writer
}

func (f loggingFile) Close() error {
	err := f.File.Close()
	fmt.Fprintf(f.w, "close: %s [%v]\n", f.name, err)
	return err
}

func (f loggingFile) Sync() error {
	err := f.File.Sync()
	fmt.Fprintf(f.w, "sync: %s [%v]\n", f.name, err)
	return err
}

func TestVFSGetDiskUsage(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-free-space")
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	_, err = Default.GetDiskUsage(dir)
	require.Nil(t, err)
}

func TestVFSCreateLinkSemantics(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-create-link")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	for _, fs := range []FS{Default, NewMem()} {
		t.Run(fmt.Sprintf("%T", fs), func(t *testing.T) {
			writeFile := func(path, contents string) {
				path = fs.PathJoin(dir, path)
				f, err := fs.Create(path)
				require.NoError(t, err)
				_, err = f.Write([]byte(contents))
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
			readFile := func(path string) string {
				path = fs.PathJoin(dir, path)
				f, err := fs.Open(path)
				require.NoError(t, err)
				b, err := ioutil.ReadAll(f)
				require.NoError(t, err)
				require.NoError(t, f.Close())
				return string(b)
			}
			require.NoError(t, fs.MkdirAll(dir, 0755))

			// Write a file 'foo' and create a hardlink at 'bar'.
			writeFile("foo", "foo")
			require.NoError(t, fs.Link(fs.PathJoin(dir, "foo"), fs.PathJoin(dir, "bar")))

			// Both files should contain equal contents, because they're backed by
			// the same inode.
			require.Equal(t, "foo", readFile("foo"))
			require.Equal(t, "foo", readFile("bar"))

			// Calling Create on 'bar' must NOT truncate 'foo'. It should create a
			// new file at path 'bar' with a new inode.
			writeFile("bar", "bar")

			require.Equal(t, "foo", readFile("foo"))
			require.Equal(t, "bar", readFile("bar"))
		})
	}
}
