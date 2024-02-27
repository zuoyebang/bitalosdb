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

package bithash

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"runtime/debug"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalosdb/internal/bindex"
	"github.com/zuoyebang/bitalosdb/internal/bytepools"
	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

type ReadableFile interface {
	io.ReaderAt
	io.Closer
	Stat() (os.FileInfo, error)
}

type Reader struct {
	b           *Bithash
	file        ReadableFile
	fs          vfs.FS
	filename    string
	fileNum     FileNum
	err         error
	footer      footer
	indexHashBH BlockHandle
	indexHash   *bindex.HashIndex
	conflictBH  BlockHandle
	conflictBuf []byte
	dataBH      BlockHandle
	dataBlock   block2Reader
	readOnly    bool
}

type ReaderOption interface {
	readerApply(*Reader)
}

type FileReopenOpt struct {
	fs       vfs.FS
	filename string
	fileNum  FileNum
	readOnly bool
}

func (f FileReopenOpt) readerApply(r *Reader) {
	if r.fs == nil {
		r.fs = f.fs
		r.filename = f.filename
		r.fileNum = f.fileNum
		r.readOnly = f.readOnly
	}
}

func NewReader(b *Bithash, f ReadableFile, extraOpts ...ReaderOption) (r *Reader, err error) {
	r = &Reader{
		b:         b,
		file:      f,
		indexHash: bindex.NewHashIndex(false),
	}

	defer func() {
		if c := recover(); c != nil {
			r.err = errors.Errorf("bithash: NewReader panic file:%s err:%v stack:%s", r.filename, c, string(debug.Stack()))
			err = r.Close()
			r = nil
		}
	}()

	if f == nil {
		r.err = ErrBhNewReaderNoFile
		return nil, r.Close()
	}

	for _, opt := range extraOpts {
		opt.readerApply(r)
	}

	r.footer, err = readTableFooter(f)
	if err != nil {
		r.err = err
		return nil, r.Close()
	}

	err = r.readMeta()
	if err != nil {
		r.err = err
		return nil, r.Close()
	}

	if err = r.readIndexHash(); err != nil {
		r.err = err
		return nil, r.Close()
	}

	return r, nil
}

func (r *Reader) readMeta() (err error) {
	buf := make([]byte, r.footer.metaBH.Length)
	if _, err = r.file.ReadAt(buf, int64(r.footer.metaBH.Offset)); err != nil {
		return err
	}

	iter, err := newBlockIter(bytes.Compare, buf)
	if err != nil {
		return err
	}
	defer iter.Close()

	var bhCounter int
	for k, v := iter.First(); iter.Valid(); k, v = iter.Next() {
		if bytes.Equal(k.UserKey, []byte(MetaIndexHashBH)) {
			r.indexHashBH = decodeBlockHandle(v)
			bhCounter++
			continue
		}
		if bytes.Equal(k.UserKey, []byte(MetaConflictBH)) {
			r.conflictBH = decodeBlockHandle(v)
			bhCounter++
			continue
		}
		if bytes.Equal(k.UserKey, []byte(MetaDataBH)) {
			r.dataBH = decodeBlockHandle(v)
			bhCounter++
			continue
		}
	}

	if bhCounter != blockHandleSum {
		return errors.Errorf("bithash: read meta blockHandleSum mismatch file:%s", r.filename)
	}

	if r.conflictBH.Length > 0 {
		r.conflictBuf = make([]byte, r.conflictBH.Length)
		if _, err = r.file.ReadAt(r.conflictBuf, int64(r.conflictBH.Offset)); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reader) readIndexHash() (err error) {
	indexHashBH := make([]byte, r.indexHashBH.Length)
	if _, err = r.file.ReadAt(indexHashBH, int64(r.indexHashBH.Offset)); err != nil {
		return err
	}

	iter, err := newBlockIter(bytes.Compare, indexHashBH)
	if err != nil {
		return err
	}
	defer iter.Close()

	for k, v := iter.First(); iter.Valid(); k, v = iter.Next() {
		if bytes.Equal(k.UserKey, []byte(IndexHashData)) {
			if !r.indexHash.SetReader(v) {
				return ErrBhHashIndexReadFail
			}
		}
	}

	return nil
}

func (r *Reader) Close() error {
	if r.err != nil {
		if r.file != nil {
			r.file.Close()
			r.file = nil
		}
		return r.err
	}

	if r.file != nil {
		r.err = r.file.Close()
		r.file = nil
		if r.err != nil {
			return r.err
		}
	}

	r.indexHash.Finish()

	r.err = ErrBhReaderClosed

	return nil
}

func (r *Reader) Get(key []byte, khash uint32) ([]byte, func(), error) {
	vint, ok := r.indexHash.Get64(khash)
	if !ok {
		return nil, nil, ErrBhNotFound
	}

	var buf [blockHandleLen]byte
	binary.LittleEndian.PutUint64(buf[:], vint)
	bh := decodeBlockHandle(buf[:])

	if r.conflictBH.Length != 0 && bh.Offset >= r.conflictBH.Offset && bh.Length <= r.conflictBH.Length {
		conflictBH, err := r.readConflict(key)
		if err != nil {
			return nil, nil, err
		}
		if conflictBH.Length == 0 && conflictBH.Offset == 0 {
			return nil, nil, ErrBhIllegalBlockLength
		}
		bh = conflictBH
	}

	return r.readData(bh)
}

func (r *Reader) readData(bh BlockHandle) ([]byte, func(), error) {
	if bh.Length <= 0 {
		return nil, nil, ErrBhIllegalBlockLength
	}

	var err error
	var v []byte
	var n int

	buf, closer := bytepools.DefaultBytePools.GetBytePool(int(bh.Length))
	defer func() {
		if err != nil {
			closer()
		}
	}()

	length := int(bh.Length)
	buf = buf[:length]
	n, err = r.file.ReadAt(buf, int64(bh.Offset))
	if err != nil {
		return nil, nil, err
	}
	if n != length {
		err = ErrBhReadAtIncomplete
		return nil, nil, err
	}

	_, val, _ := r.dataBlock.readRecord(buf)
	if val == nil {
		err = ErrBhReadRecordNil
		return nil, nil, err
	}

	v, err = r.b.compressor.Decode(nil, val)
	if err != nil {
		return nil, nil, err
	}

	return v, closer, nil
}

func (r *Reader) readConflict(key []byte) (BlockHandle, error) {
	bh := BlockHandle{}

	iter, err := newBlockIter(bytes.Compare, r.conflictBuf)
	if err != nil {
		return bh, err
	}
	defer iter.Close()

	ik, iv := iter.SeekGE(key)
	if iter.Valid() && bytes.Equal(ik.UserKey, key) {
		bh = decodeBlockHandle(iv)
	}

	return bh, nil
}
