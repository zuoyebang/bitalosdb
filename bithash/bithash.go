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

package bithash

import (
	"sync"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/compress"
	"github.com/zuoyebang/bitalosdb/internal/list2"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/utils"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type FS vfs.FS
type File vfs.File

type Bithash struct {
	meta            *BithashMetadata
	fs              FS
	index           int
	logger          base.Logger
	compressor      compress.Compressor
	dirname         string
	tableMaxSize    int
	bhtReaders      sync.Map
	rwwWriters      sync.Map
	tLock           sync.Mutex
	stats           *Stats
	cLogWriter      *compactLogWriter
	cLogUpdate      bool
	closeTableWg    sync.WaitGroup
	deleteFilePacer *base.DeletionFileLimiter
	bytesPerSync    int

	mufn struct {
		sync.RWMutex
		fnMap map[FileNum]FileNum
	}

	mutw struct {
		sync.Mutex
		mutableWriters *list2.Stack
	}
}

func Open(dirname string, opts *options.BithashOptions) (b *Bithash, err error) {
	b = &Bithash{
		dirname:         dirname,
		fs:              opts.FS,
		tableMaxSize:    opts.TableMaxSize,
		logger:          opts.Logger,
		compressor:      opts.Compressor,
		index:           opts.Index,
		deleteFilePacer: opts.DeleteFilePacer,
		bytesPerSync:    opts.BytesPerSync,
		bhtReaders:      sync.Map{},
		rwwWriters:      sync.Map{},
		stats:           &Stats{},
	}

	if err = b.fs.MkdirAll(dirname, 0755); err != nil {
		return nil, err
	}

	b.mufn.fnMap = make(map[FileNum]FileNum, 1<<10)
	b.mutw.mutableWriters = list2.NewStack()

	if err = initManifest(b); err != nil {
		return nil, err
	}

	if err = b.initTables(); err != nil {
		return nil, err
	}

	if err = initFileNumMap(b); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Bithash) TableMaxSize() int64 {
	return int64(b.tableMaxSize)
}

func (b *Bithash) Get(key []byte, khash uint32, fn FileNum) (value []byte, putPool func(), err error) {
	if rwwWriter, ok := b.rwwWriters.Load(fn); ok {
		value, putPool, err = rwwWriter.(*Writer).Get(key, khash)
		if err == nil && value != nil {
			return
		}
	}

	fileNum := b.GetFileNumMap(fn)
	if fileNum == FileNum(0) {
		return nil, nil, ErrBhFileNumZero
	}

	if bhtReader, ok := b.bhtReaders.Load(fileNum); ok {
		return bhtReader.(*Reader).Get(key, khash)
	}

	return nil, nil, ErrBhNotFound
}

func (b *Bithash) FlushStart() (*BithashWriter, error) {
	return b.NewBithashWriter(false)
}

func (b *Bithash) FlushFinish(wr *BithashWriter) error {
	if wr == nil {
		return nil
	}
	return wr.Finish()
}

func (b *Bithash) Delete(fn FileNum) error {
	fileNum := b.GetFileNumMap(fn)
	if fileNum == FileNum(0) {
		fileNum = fn
	}

	b.stats.DelKeyTotal.Add(1)
	b.meta.updateFileDelKeyNum(fileNum, 1)
	return nil
}

func (b *Bithash) Close() (err error) {
	if b.meta != nil {
		if err = b.meta.close(); err != nil {
			b.logger.Errorf("bithash[%s] close meta fail err:%s", b.dirname, err)
		}
	}

	if b.cLogWriter != nil {
		if err = b.cLogWriter.close(); err != nil {
			b.logger.Errorf("bithash[%s] close compactLog fail err:%s", b.dirname, err)
		}
	}

	b.rwwWriters.Range(func(fn, w interface{}) bool {
		err = w.(*Writer).Close()
		return true
	})

	b.bhtReaders.Range(func(fn, r interface{}) bool {
		err = r.(*Reader).Close()
		return true
	})

	b.closeTableWg.Wait()

	return
}

func (b *Bithash) RemoveTableFiles(fileNums []FileNum) {
	var deleteFiles []string
	for _, fileNum := range fileNums {
		b.DeleteReaders(fileNum)
		b.meta.freeFileMetadata(fileNum)
		filename := MakeFilepath(b.fs, b.dirname, fileTypeTable, fileNum)
		if utils.IsFileNotExist(filename) {
			b.logger.Errorf("bithash RemoveTableFiles not exist file:%s", filename)
			continue
		}

		deleteFiles = append(deleteFiles, filename)
	}

	if len(deleteFiles) > 0 {
		b.deleteFilePacer.AddFiles(deleteFiles)
		b.stats.FileTotal.Add(^uint32(len(deleteFiles) - 1))
	}
}

func (b *Bithash) closeMutableWriters() (err error) {
	b.mutw.Lock()
	defer b.mutw.Unlock()

	for !b.mutw.mutableWriters.Empty() {
		w := b.mutw.mutableWriters.Pop()
		if w == nil {
			continue
		}
		if err = b.closeTable(w.(*Writer), true); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bithash) popMutableWriters() *Writer {
	b.mutw.Lock()
	bhWriter := b.mutw.mutableWriters.Pop()
	b.mutw.Unlock()

	if bhWriter == nil {
		return nil
	}

	return bhWriter.(*Writer)
}

func (b *Bithash) pushMutableWriters(wr *Writer) {
	b.mutw.Lock()
	b.mutw.mutableWriters.Push(wr)
	b.mutw.Unlock()
	return
}

func (b *Bithash) emptyMutableWriters() bool {
	b.mutw.Lock()
	isEmpty := b.mutw.mutableWriters.Empty()
	b.mutw.Unlock()
	return isEmpty
}

func (b *Bithash) addReaders(r *Reader) {
	b.bhtReaders.Store(r.fileNum, r)
}

func (b *Bithash) DeleteReaders(fn FileNum) {
	b.bhtReaders.Delete(fn)
}

func (b *Bithash) addRwwWriters(w *Writer) {
	b.rwwWriters.Store(w.fileNum, w)
}

func (b *Bithash) deleteRwwWriters(fn FileNum) {
	b.rwwWriters.Delete(fn)
}

func (b *Bithash) SetFileNumMap(dst FileNum, src FileNum) {
	b.mufn.Lock()
	b.mufn.fnMap[src] = dst
	_ = b.cLogWriter.writeRecord(compactLogKindSet, src, dst)
	b.mufn.Unlock()
}

func (b *Bithash) DeleteFileNumMap(fn FileNum) {
	b.mufn.Lock()
	delete(b.mufn.fnMap, fn)
	_ = b.cLogWriter.writeRecord(compactLogKindDelete, fn, fn)
	b.mufn.Unlock()
}

func (b *Bithash) GetFileNumMap(src FileNum) (dst FileNum) {
	b.mufn.RLock()
	if val, ok := b.mufn.fnMap[src]; ok {
		dst = val
	} else {
		dst = FileNum(0)
	}
	b.mufn.RUnlock()
	return
}

func (b *Bithash) NewBithashWriter(compact bool) (*BithashWriter, error) {
	bhWriter := &BithashWriter{
		b:       b,
		compact: compact,
	}

	if !compact && !b.emptyMutableWriters() {
		mutableWriter := b.popMutableWriters()
		if mutableWriter != nil {
			bhWriter.wr = mutableWriter
			return bhWriter, nil
		}
	}

	writer, err := NewTableWriter(b, compact)
	if err != nil {
		return nil, err
	}

	bhWriter.wr = writer
	return bhWriter, nil
}
