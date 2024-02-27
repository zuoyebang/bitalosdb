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

package errorfs

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/internal/vfs"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

var ErrInjected = errors.New("injected error")

type Op int

const (
	OpCreate Op = iota
	OpLink
	OpOpen
	OpOpenDir
	OpRemove
	OpRemoveAll
	OpRename
	OpReuseForRewrite
	OpOpenForWrite
	OpMkdirAll
	OpLock
	OpList
	OpStat
	OpGetDiskUsage
	OpFileClose
	OpFileRead
	OpFileReadAt
	OpFileWrite
	OpFileSeek
	OpFileStat
	OpFileSync
	OpFileFlush
)

func (o Op) OpKind() OpKind {
	switch o {
	case OpOpen, OpOpenDir, OpList, OpStat, OpGetDiskUsage, OpFileRead, OpFileReadAt, OpFileStat, OpFileSeek:
		return OpKindRead
	case OpCreate, OpLink, OpRemove, OpRemoveAll, OpRename, OpReuseForRewrite, OpMkdirAll, OpLock, OpFileClose, OpFileWrite, OpFileSync, OpFileFlush:
		return OpKindWrite
	default:
		panic(fmt.Sprintf("unrecognized op %v\n", o))
	}
}

type OpKind int

const (
	OpKindRead OpKind = iota
	OpKindWrite
)

func OnIndex(index int32) *InjectIndex {
	return &InjectIndex{index: index}
}

type InjectIndex struct {
	index int32
}

func (ii *InjectIndex) Index() int32 { return atomic.LoadInt32(&ii.index) }

func (ii *InjectIndex) SetIndex(v int32) { atomic.StoreInt32(&ii.index, v) }

func (ii *InjectIndex) MaybeError(_ Op, _ string) error {
	if atomic.AddInt32(&ii.index, -1) == -1 {
		return errors.WithStack(ErrInjected)
	}
	return nil
}

func WithProbability(op OpKind, p float64) Injector {
	mu := new(sync.Mutex)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return InjectorFunc(func(currOp Op, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		if currOp.OpKind() == op && rnd.Float64() < p {
			return errors.WithStack(ErrInjected)
		}
		return nil
	})
}

type InjectorFunc func(Op, string) error

func (f InjectorFunc) MaybeError(op Op, path string) error { return f(op, path) }

type Injector interface {
	MaybeError(op Op, path string) error
}

type FS struct {
	fs  vfs.FS
	inj Injector
}

func Wrap(fs vfs.FS, inj Injector) *FS {
	return &FS{
		fs:  fs,
		inj: inj,
	}
}

func WrapFile(f vfs.File, inj Injector) vfs.File {
	return &errorFile{file: f, inj: inj}
}

func (fs *FS) Unwrap() vfs.FS {
	return fs.fs
}

func (fs *FS) Create(name string) (vfs.File, error) {
	if err := fs.inj.MaybeError(OpCreate, name); err != nil {
		return nil, err
	}
	f, err := fs.fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &errorFile{name, f, fs.inj}, nil
}

func (fs *FS) Link(oldname, newname string) error {
	if err := fs.inj.MaybeError(OpLink, oldname); err != nil {
		return err
	}
	return fs.fs.Link(oldname, newname)
}

func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if err := fs.inj.MaybeError(OpOpen, name); err != nil {
		return nil, err
	}
	f, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	ef := &errorFile{name, f, fs.inj}
	for _, opt := range opts {
		opt.Apply(ef)
	}
	return ef, nil
}

func (fs *FS) OpenDir(name string) (vfs.File, error) {
	if err := fs.inj.MaybeError(OpOpenDir, name); err != nil {
		return nil, err
	}
	f, err := fs.fs.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &errorFile{name, f, fs.inj}, nil
}

func (fs *FS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	if err := fs.inj.MaybeError(OpGetDiskUsage, path); err != nil {
		return vfs.DiskUsage{}, err
	}
	return fs.fs.GetDiskUsage(path)
}

func (fs *FS) PathBase(p string) string {
	return fs.fs.PathBase(p)
}

func (fs *FS) PathDir(p string) string {
	return fs.fs.PathDir(p)
}

func (fs *FS) PathJoin(elem ...string) string {
	return fs.fs.PathJoin(elem...)
}

func (fs *FS) Remove(name string) error {
	if _, err := fs.fs.Stat(name); oserror.IsNotExist(err) {
		return nil
	}

	if err := fs.inj.MaybeError(OpRemove, name); err != nil {
		return err
	}
	return fs.fs.Remove(name)
}

func (fs *FS) RemoveAll(fullname string) error {
	if err := fs.inj.MaybeError(OpRemoveAll, fullname); err != nil {
		return err
	}
	return fs.fs.RemoveAll(fullname)
}

func (fs *FS) Rename(oldname, newname string) error {
	if err := fs.inj.MaybeError(OpRename, oldname); err != nil {
		return err
	}
	return fs.fs.Rename(oldname, newname)
}

func (fs *FS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	if err := fs.inj.MaybeError(OpReuseForRewrite, oldname); err != nil {
		return nil, err
	}
	return fs.fs.ReuseForWrite(oldname, newname)
}

func (fs *FS) OpenForWrite(name string) (vfs.File, error) {
	if err := fs.inj.MaybeError(OpOpenForWrite, name); err != nil {
		return nil, err
	}
	return fs.fs.OpenForWrite(name)
}

func (fs *FS) OpenWR(name string) (vfs.File, error) {
	if err := fs.inj.MaybeError(OpOpenForWrite, name); err != nil {
		return nil, err
	}
	return fs.fs.OpenWR(name)
}

func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	if err := fs.inj.MaybeError(OpMkdirAll, dir); err != nil {
		return err
	}
	return fs.fs.MkdirAll(dir, perm)
}

func (fs *FS) Lock(name string) (io.Closer, error) {
	if err := fs.inj.MaybeError(OpLock, name); err != nil {
		return nil, err
	}
	return fs.fs.Lock(name)
}

func (fs *FS) List(dir string) ([]string, error) {
	if err := fs.inj.MaybeError(OpList, dir); err != nil {
		return nil, err
	}
	return fs.fs.List(dir)
}

func (fs *FS) Stat(name string) (os.FileInfo, error) {
	if err := fs.inj.MaybeError(OpStat, name); err != nil {
		return nil, err
	}
	return fs.fs.Stat(name)
}

type errorFile struct {
	path string
	file vfs.File
	inj  Injector
}

func (f *errorFile) Close() error {
	return f.file.Close()
}

func (f *errorFile) Read(p []byte) (int, error) {
	if err := f.inj.MaybeError(OpFileRead, f.path); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}

func (f *errorFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.inj.MaybeError(OpFileReadAt, f.path); err != nil {
		return 0, err
	}
	return f.file.ReadAt(p, off)
}

func (f *errorFile) Write(p []byte) (int, error) {
	if err := f.inj.MaybeError(OpFileWrite, f.path); err != nil {
		return 0, err
	}
	return f.file.Write(p)
}

func (f *errorFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.inj.MaybeError(OpFileSeek, f.path); err != nil {
		return 0, err
	}
	return f.file.Seek(offset, whence)
}

func (f *errorFile) Stat() (os.FileInfo, error) {
	if err := f.inj.MaybeError(OpFileStat, f.path); err != nil {
		return nil, err
	}
	return f.file.Stat()
}

func (f *errorFile) Sync() error {
	if err := f.inj.MaybeError(OpFileSync, f.path); err != nil {
		return err
	}
	return f.file.Sync()
}
