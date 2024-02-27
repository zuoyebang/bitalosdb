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
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"os"

	"github.com/cockroachdb/errors/oserror"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

const (
	fileNumMapMagicLen    = 8
	fileNumMapFooterLen   = 4 + fileNumMapMagicLen
	fileNumMapMagic       = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	fileNumMapMagicOffset = fileNumMapFooterLen - fileNumMapMagicLen
	fileNumMapVersion     = 1
	fileNumMapRecordLen   = 8
)

const (
	compactLogRecordLen   = 10
	compactLogHeaderLen   = 16
	compactLogWriteOffset = 0
	compactLogReadOffset  = 8
	compactLogDataOffset  = compactLogHeaderLen
)

const (
	compactLogKindSet uint16 = 1 + iota
	compactLogKindDelete
)

const (
	compactMaxFileNum  = 32
	compactMaxMiniSize = 64 << 20
)

type CompactFiles struct {
	FileNum    FileNum
	DelPercent float64
	Size       int64
}

func (b *Bithash) CheckFilesDelPercent(jobId int, cfgPercent float64) []CompactFiles {
	var compactFiles []CompactFiles
	var findNum int

	b.meta.mu.RLock()
	defer b.meta.mu.RUnlock()

	for fn, fileMeta := range b.meta.mu.filesMeta {
		if fileMeta.state != fileMetaStateImmutable || fileMeta.keyNum == 0 || (cfgPercent > 0.0 && fileMeta.delKeyNum == 0) {
			continue
		}

		delPercent := float64(fileMeta.delKeyNum) / float64(fileMeta.keyNum)
		if delPercent >= cfgPercent {
			b.logger.Infof("[COMPACTBITHASH %d] [tree:%d] CheckFilesDelPercent %s delPercent:%.4f cfgPercent:%.2f", jobId, b.index, fileMeta, delPercent, cfgPercent)
			compactFiles = append(compactFiles, CompactFiles{
				FileNum:    fn,
				DelPercent: delPercent,
			})
			findNum++
			if findNum >= compactMaxFileNum {
				break
			}
		}
	}

	return compactFiles
}

func (b *Bithash) CheckFilesMiniSize(jobId int) []CompactFiles {
	var compactFiles []CompactFiles

	b.meta.mu.RLock()
	defer b.meta.mu.RUnlock()

	for fn, fileMeta := range b.meta.mu.filesMeta {
		if fileMeta.state != fileMetaStateImmutable {
			continue
		}

		fileSize := b.fileSize(fn)
		if fileSize <= compactMaxMiniSize {
			b.logger.Infof("[COMPACTBITHASH %d] [tree:%d] CheckFilesMiniSize %s fileSize:%s",
				jobId, b.index, fileMeta,
				utils.FmtSize(uint64(fileSize)))
			compactFiles = append(compactFiles, CompactFiles{
				FileNum: fn,
				Size:    fileSize,
			})
		}
	}

	return compactFiles
}

type compactLogWriter struct {
	b           *Bithash
	file        *os.File
	filename    string
	writeOffset uint64
	readOffset  uint64
	recordBuf   [compactLogRecordLen]byte
	headerBuf   [compactLogHeaderLen]byte
}

func initCompactLog(b *Bithash) (err error) {
	var file *os.File
	var isNewFile bool
	filename := MakeFilepath(b.fs, b.dirname, fileTypeCompactLog, 0)
	_, err = b.fs.Stat(filename)
	if errors.Is(err, fs.ErrNotExist) {
		isNewFile = true
	}
	file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = file.Close()
		}
	}()

	w := &compactLogWriter{
		b:           b,
		file:        file,
		filename:    filename,
		writeOffset: compactLogDataOffset,
		readOffset:  compactLogDataOffset,
	}

	if isNewFile {
		err = w.setHeader()
	} else {
		err = w.readHeader()
	}
	if err != nil {
		return err
	}

	b.logger.Infof("open compactLog file filename:%s writeOffset:%d readOffset:%d", filename, w.writeOffset, w.readOffset)

	b.cLogWriter = w

	// 重放未读的log日志
	if err = b.cLogWriter.replayLog(); err != nil {
		return err
	}

	return nil
}

func (w *compactLogWriter) readHeader() error {
	var buf [16]byte
	if _, err := w.file.ReadAt(buf[:], compactLogWriteOffset); err != nil {
		return err
	}
	w.writeOffset = binary.LittleEndian.Uint64(buf[0:8])
	w.readOffset = binary.LittleEndian.Uint64(buf[8:16])
	return nil
}

func (w *compactLogWriter) setHeader() error {
	binary.LittleEndian.PutUint64(w.headerBuf[0:8], w.writeOffset)
	binary.LittleEndian.PutUint64(w.headerBuf[8:16], w.readOffset)
	_, err := w.file.WriteAt(w.headerBuf[:], compactLogWriteOffset)
	return err
}

func (w *compactLogWriter) reset() error {
	w.writeOffset = compactLogDataOffset
	w.readOffset = compactLogDataOffset
	return w.setHeader()
}

func (w *compactLogWriter) writeRecord(kind uint16, srcFn, dstFn FileNum) error {
	binary.LittleEndian.PutUint16(w.recordBuf[0:2], kind)
	binary.LittleEndian.PutUint32(w.recordBuf[2:6], uint32(srcFn))
	binary.LittleEndian.PutUint32(w.recordBuf[6:10], uint32(dstFn))
	_, err := w.file.WriteAt(w.recordBuf[:], int64(w.writeOffset))
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(w.headerBuf[:8], w.writeOffset+compactLogRecordLen)
	if _, err = w.file.WriteAt(w.headerBuf[:8], compactLogWriteOffset); err != nil {
		return err
	}

	w.writeOffset += compactLogRecordLen
	return nil
}

func (w *compactLogWriter) replayLog() (err error) {
	if w.readOffset == w.writeOffset {
		return nil
	}

	w.b.mufn.Lock()
	defer w.b.mufn.Unlock()

	var buf [compactLogRecordLen]byte
	var srcFn, dstFn FileNum
	var kind uint16
	num := 0
	for w.readOffset < w.writeOffset {
		if _, err = w.file.ReadAt(buf[:], int64(w.readOffset)); err != nil {
			break
		}

		kind = binary.LittleEndian.Uint16(buf[0:2])
		srcFn = FileNum(binary.LittleEndian.Uint32(buf[2:6]))
		dstFn = FileNum(binary.LittleEndian.Uint32(buf[6:10]))
		if kind == compactLogKindSet {
			w.b.mufn.fnMap[srcFn] = dstFn
		} else if kind == compactLogKindDelete {
			delete(w.b.mufn.fnMap, srcFn)
		}

		w.readOffset += compactLogRecordLen
		num++
	}

	if err != nil && err != io.EOF {
		return err
	}

	if err = w.setHeader(); err != nil {
		return err
	}

	w.b.cLogUpdate = true
	w.b.logger.Infof("bithash replay end logNum:%d", num)
	return nil
}

func (w *compactLogWriter) close() (err error) {
	if err = w.file.Sync(); err != nil {
		return
	}
	if err = w.file.Close(); err != nil {
		return
	}
	return
}

func initFileNumMap(b *Bithash) error {
	filename := MakeFilepath(b.fs, b.dirname, fileTypeFileNumMap, 0)
	_, err := b.fs.Stat(filename)
	if oserror.IsNotExist(err) {
		if err = createFileNumMapFile(b, filename); err != nil {
			return err
		}
	}

	if err = readFileNumMapFile(b, filename); err != nil {
		return err
	}

	if err = initCompactLog(b); err != nil {
		return err
	}

	if err = writeFileNumMapFile(b); err != nil {
		return err
	}

	if err = b.cLogWriter.reset(); err != nil {
		return err
	}

	b.logger.Infof("bithash initFileNumMap success compactLog readOffset:%d writeOffset:%d",
		b.cLogWriter.readOffset, b.cLogWriter.writeOffset)

	return nil
}

func encodeFileNumMapFooter() []byte {
	buf := make([]byte, fileNumMapFooterLen)
	binary.LittleEndian.PutUint32(buf[0:4], fileNumMapVersion)
	copy(buf[4:fileNumMapFooterLen], fileNumMapMagic)
	buf = buf[:fileNumMapFooterLen]
	return buf
}

func createFileNumMapFile(b *Bithash, filename string) (err error) {
	var file File
	file, err = b.fs.Create(filename)
	if err != nil {
		return err
	}

	defer func() {
		if file != nil {
			err = file.Close()
		}
		if err != nil {
			err = b.fs.Remove(filename)
		}
	}()

	if _, err = file.Write(encodeFileNumMapFooter()); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	return nil
}

func readFileNumMapFile(b *Bithash, filename string) (err error) {
	var dataSize int64
	var file File
	file, err = b.fs.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	checkFooter := func(f ReadableFile) bool {
		stat, err := f.Stat()
		if err != nil {
			return false
		}
		dataSize = stat.Size() - fileNumMapFooterLen
		if dataSize < 0 {
			return false
		}
		buf := [fileNumMapMagicLen]byte{}
		n, err := f.ReadAt(buf[:], dataSize+fileNumMapMagicOffset)
		if err != nil && err != io.EOF {
			return false
		}
		return bytes.Equal(buf[:n], []byte(fileNumMapMagic))
	}
	if !checkFooter(file) {
		return ErrBhFileNumMapCheckFail
	}

	if dataSize == 0 {
		return nil
	}

	var readBuf [fileNumMapRecordLen]byte
	var srcFn, dstFn FileNum
	var offset int64
	r := bufio.NewReaderSize(file, int(dataSize))

	b.mufn.Lock()
	defer b.mufn.Unlock()

	for offset < dataSize {
		n, err := r.Read(readBuf[:])
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		if n != fileNumMapRecordLen {
			return errors.New("read FILENUMMAP incomplete data")
		}

		offset += fileNumMapRecordLen
		srcFn = FileNum(binary.LittleEndian.Uint32(readBuf[0:4]))
		dstFn = FileNum(binary.LittleEndian.Uint32(readBuf[4:8]))
		b.mufn.fnMap[srcFn] = dstFn
	}

	return nil
}

func writeFileNumMapFile(b *Bithash) (err error) {
	if b.cLogUpdate == false {
		return nil
	}

	var file File
	fileNumMapTmp := MakeFilepath(b.fs, b.dirname, fileTypeFileNumMapTmp, 0)
	fileNumMap := MakeFilepath(b.fs, b.dirname, fileTypeFileNumMap, 0)

	b.logger.Infof("bithash write FileNumMap file start filename:%s", fileNumMap)

	file, err = b.fs.Create(fileNumMapTmp)
	if err != nil {
		return err
	}

	defer func() {
		if _, e := b.fs.Stat(fileNumMapTmp); e == nil {
			err = b.fs.Remove(fileNumMapTmp)
		}
	}()

	b.mufn.RLock()
	defer b.mufn.RUnlock()

	var buf [fileNumMapRecordLen]byte
	fnNum := len(b.mufn.fnMap)
	size := fnNum*fileNumMapRecordLen + fileNumMapFooterLen
	w := bufio.NewWriterSize(file, size)

	for srcFn, dstFn := range b.mufn.fnMap {
		binary.LittleEndian.PutUint32(buf[0:4], uint32(srcFn))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(dstFn))
		if _, err = w.Write(buf[:]); err != nil {
			return err
		}
	}

	if _, err = w.Write(encodeFileNumMapFooter()); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}

	if err = b.fs.Rename(fileNumMapTmp, fileNumMap); err != nil {
		return err
	}

	b.logger.Infof("bithash write FileNumMap file end filename:%s writeFnNum:%d", fileNumMap, fnNum)

	return nil
}
