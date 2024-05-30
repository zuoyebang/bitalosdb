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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"github.com/zuoyebang/bitalosdb/internal/list2"
	"github.com/zuoyebang/bitalosdb/internal/mmap"
)

const (
	versionV1 uint16 = iota + 1
)

const (
	versionCurrent = versionV1

	fileMetadataNum = 10000
	fileMetadataLen = 18
	fileMetaMapLen  = fileMetadataNum * fileMetadataLen

	manifestHeaderLen = 8
	manifestMagicLen  = 8
	manifestFooterLen = manifestMagicLen
	manifestMagic     = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	manifestLen       = manifestHeaderLen + fileMetaMapLen + manifestFooterLen

	versionOffset          = 0
	nextFileNumOffset      = 4
	nextFileMetadataOffset = 8
	footerOffset           = manifestLen - manifestFooterLen
)

const (
	fileMetaStateNone uint16 = iota
	fileMetaStateCompact
	fileMetaStateWrite
	fileMetaStateClosed
	fileMetaStateImmutable
)

var fileMetaStateNames = []string{
	fileMetaStateNone:      "NONE",
	fileMetaStateCompact:   "COMPACT",
	fileMetaStateWrite:     "WRITING",
	fileMetaStateClosed:    "CLOSEDNOTCHECK",
	fileMetaStateImmutable: "IMMUTABLE",
}

func getFileMetaStateName(state uint16) string {
	if int(state) < len(fileMetaStateNames) {
		return fileMetaStateNames[state]
	}
	return fmt.Sprintf("UNKNOWN:%d", state)
}

type BithashMetadata struct {
	b       *Bithash
	version uint16
	mu      struct {
		sync.RWMutex
		manifestMmap   *mmap.MMap
		curFileNum     FileNum
		freesPos       *list2.IntQueue
		filesPos       map[FileNum]int
		filesMeta      map[FileNum]*fileMetadata
		filesMetaArray [fileMetadataNum]fileMetadata
	}
}

type fileMetadata struct {
	fileNum        FileNum
	state          uint16
	keyNum         uint32
	delKeyNum      uint32
	conflictKeyNum uint32
}

func (f *fileMetadata) String() string {
	return fmt.Sprintf("fileNum=%d state=%s keyNum=%d conflictKeyNum=%d delKeyNum=%d",
		f.fileNum, getFileMetaStateName(f.state), f.keyNum, f.conflictKeyNum, f.delKeyNum)
}

func initManifest(b *Bithash) error {
	b.meta = &BithashMetadata{b: b}
	b.meta.mu.filesPos = make(map[FileNum]int, 1<<10)
	b.meta.mu.filesMeta = make(map[FileNum]*fileMetadata, 1<<10)
	b.meta.mu.freesPos = list2.NewIntQueue(fileMetadataNum)

	filename := MakeFilepath(b.fs, b.dirname, fileTypeManifest, 0)
	if _, err := b.fs.Stat(filename); errors.Is(err, fs.ErrNotExist) {
		if err = b.meta.createManifest(filename); err != nil {
			return err
		}
	}

	if err := b.meta.loadManifest(filename); err != nil {
		return err
	}

	b.logger.Infof("[BITHASH %d] openManifest success version:%d len:%d uses:%d frees:%d",
		b.index,
		b.meta.version,
		b.meta.mu.manifestMmap.Len(),
		len(b.meta.mu.filesPos),
		b.meta.mu.freesPos.Len())

	return nil
}

func (m *BithashMetadata) createManifest(filename string) (err error) {
	var (
		manifestFile File
		manifest     *bufio.Writer
	)

	manifestFile, err = m.b.fs.Create(filename)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = m.b.fs.Remove(filename)
		}
		if manifestFile != nil {
			err = manifestFile.Close()
		}
	}()

	manifest = bufio.NewWriterSize(manifestFile, manifestLen)
	buf := make([]byte, manifestLen)
	binary.LittleEndian.PutUint16(buf[0:2], versionCurrent)
	binary.LittleEndian.PutUint32(buf[4:8], 1)
	copy(buf[footerOffset:footerOffset+manifestFooterLen], manifestMagic)

	if _, err = manifest.Write(buf); err != nil {
		return err
	}
	if err = manifest.Flush(); err != nil {
		return err
	}
	if err = manifestFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (m *BithashMetadata) loadManifest(filename string) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mu.manifestMmap, err = mmap.Open(filename, 0)
	if err != nil {
		return err
	}

	m.version = m.mu.manifestMmap.ReadUInt16At(versionOffset)
	m.mu.curFileNum = FileNum(m.mu.manifestMmap.ReadUInt32At(nextFileNumOffset))

	pos := nextFileMetadataOffset
	for arrIdx := 0; arrIdx < fileMetadataNum; arrIdx++ {
		fileMeta := m.readFileMetadata(pos)
		if fileMeta.fileNum > 0 {
			m.mu.filesPos[fileMeta.fileNum] = pos
			m.mu.filesMetaArray[arrIdx] = fileMeta
			m.mu.filesMeta[fileMeta.fileNum] = &(m.mu.filesMetaArray[arrIdx])
			m.b.stats.KeyTotal.Add(uint64(fileMeta.keyNum))
			m.b.stats.DelKeyTotal.Add(uint64(fileMeta.delKeyNum))
			m.b.stats.FileTotal.Add(1)
		} else {
			m.mu.freesPos.Push(int32(pos))
		}

		pos += fileMetadataLen
	}

	return nil
}

func (m *BithashMetadata) close() error {
	if m.mu.manifestMmap != nil {
		return m.mu.manifestMmap.Close()
	}
	return nil
}

func (m *BithashMetadata) getCurrentFileNum() FileNum {
	return m.mu.curFileNum
}

func (m *BithashMetadata) getNextFileNum() FileNum {
	m.mu.Lock()
	defer m.mu.Unlock()

	curFileNum := m.mu.curFileNum
	m.mu.curFileNum++
	m.mu.manifestMmap.WriteUInt32At(uint32(m.mu.curFileNum), nextFileNumOffset)
	return curFileNum
}

func (m *BithashMetadata) getNextFreePos() int {
	if m.mu.freesPos.Empty() {
		panic("bithash has no freemeta")
	}

	value, _ := m.mu.freesPos.Pop()
	return int(value)
}

func (m *BithashMetadata) getPos(fileNum FileNum) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getFilePos(fileNum)
}

func (m *BithashMetadata) getFilePos(fileNum FileNum) int {
	pos, ok := m.mu.filesPos[fileNum]
	if !ok {
		return 0
	}

	return pos
}

func (m *BithashMetadata) newFileMetadata(fileNum FileNum, compact bool) int {
	var state uint16
	if compact {
		state = fileMetaStateCompact
	} else {
		state = fileMetaStateWrite
	}

	m.mu.Lock()
	pos := m.getNextFreePos()
	fileMeta := fileMetadata{
		fileNum:        fileNum,
		state:          state,
		keyNum:         0,
		conflictKeyNum: 0,
		delKeyNum:      0,
	}
	m.writeFileMetadata(pos, &fileMeta)
	arrIdx := (pos - nextFileMetadataOffset) / fileMetadataLen
	m.mu.filesMetaArray[arrIdx] = fileMeta
	m.mu.filesPos[fileNum] = pos
	m.mu.filesMeta[fileNum] = &(m.mu.filesMetaArray[arrIdx])
	m.mu.Unlock()
	return pos
}

func (m *BithashMetadata) freeFileMetadata(fileNum FileNum) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pos := m.getFilePos(fileNum)
	if pos == 0 {
		return
	}

	delete(m.mu.filesPos, fileNum)
	delete(m.mu.filesMeta, fileNum)

	fileMeta := fileMetadata{
		fileNum:        FileNum(0),
		state:          fileMetaStateNone,
		keyNum:         0,
		conflictKeyNum: 0,
		delKeyNum:      0,
	}
	arrIdx := (pos - nextFileMetadataOffset) / fileMetadataLen
	m.mu.filesMetaArray[arrIdx] = fileMeta

	m.writeFileMetadata(pos, &fileMeta)

	m.mu.freesPos.Push(int32(pos))
}

func (m *BithashMetadata) readFileMetadata(pos int) fileMetadata {
	return fileMetadata{
		fileNum:        FileNum(m.mu.manifestMmap.ReadUInt32At(pos)),
		state:          m.mu.manifestMmap.ReadUInt16At(pos + 4),
		keyNum:         m.mu.manifestMmap.ReadUInt32At(pos + 6),
		conflictKeyNum: m.mu.manifestMmap.ReadUInt32At(pos + 10),
		delKeyNum:      m.mu.manifestMmap.ReadUInt32At(pos + 14),
	}
}

func (m *BithashMetadata) getFileMetadata(fileNum FileNum) (fileMeta *fileMetadata) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ok bool
	fileMeta, ok = m.mu.filesMeta[fileNum]
	if !ok {
		pos := m.getFilePos(fileNum)
		if pos > 0 {
			arrIdx := (pos - nextFileMetadataOffset) / fileMetadataLen
			m.mu.filesMetaArray[arrIdx] = m.readFileMetadata(pos)
			fileMeta = &(m.mu.filesMetaArray[arrIdx])
			m.mu.filesMeta[fileNum] = fileMeta
		}
	}
	return
}

func (m *BithashMetadata) updateFileByClosed(fileNum FileNum, wm WriterMetadata) {
	m.mu.Lock()
	pos := m.getFilePos(fileNum)
	if pos > 0 {
		m.mu.manifestMmap.WriteUInt32At(uint32(fileNum), pos)
		m.mu.manifestMmap.WriteUInt16At(fileMetaStateClosed, pos+4)
		m.mu.manifestMmap.WriteUInt32At(wm.keyNum, pos+6)
		m.mu.manifestMmap.WriteUInt32At(wm.conflictKeyNum, pos+10)

		m.mu.filesMeta[fileNum].state = fileMetaStateClosed
		m.mu.filesMeta[fileNum].keyNum = wm.keyNum
		m.mu.filesMeta[fileNum].conflictKeyNum = wm.conflictKeyNum
	}
	m.mu.Unlock()
}

func (m *BithashMetadata) updateFileState(fileNum FileNum, state uint16) {
	m.mu.Lock()
	pos := m.getFilePos(fileNum)
	if pos > 0 {
		m.mu.manifestMmap.WriteUInt16At(state, pos+4)
		m.mu.filesMeta[fileNum].state = state
	}
	m.mu.Unlock()
}

func (m *BithashMetadata) updateFileDelKeyNum(fileNum FileNum, delta uint32) {
	m.mu.Lock()
	pos := m.getFilePos(fileNum)
	if pos > 0 {
		pos += 14
		delKeyNum := m.mu.manifestMmap.ReadUInt32At(pos) + delta
		m.mu.manifestMmap.WriteUInt32At(delKeyNum, pos)
		m.mu.filesMeta[fileNum].delKeyNum = delKeyNum
	}
	m.mu.Unlock()
}

func (m *BithashMetadata) writeFileMetadata(pos int, fileMeta *fileMetadata) {
	m.mu.manifestMmap.WriteUInt32At(uint32(fileMeta.fileNum), pos)
	m.mu.manifestMmap.WriteUInt16At(fileMeta.state, pos+4)
	m.mu.manifestMmap.WriteUInt32At(fileMeta.keyNum, pos+6)
	m.mu.manifestMmap.WriteUInt32At(fileMeta.conflictKeyNum, pos+10)
	m.mu.manifestMmap.WriteUInt32At(fileMeta.delKeyNum, pos+14)

}

func (m *BithashMetadata) isFileWriting(fileNum FileNum) bool {
	fileMeta := m.getFileMetadata(fileNum)
	if fileMeta == nil {
		return false
	}
	if fileMeta.state == fileMetaStateWrite || fileMeta.state == fileMetaStateCompact {
		return true
	}
	return false
}

func (m *BithashMetadata) isFileImmutable(fileNum FileNum) bool {
	fileMeta := m.getFileMetadata(fileNum)
	if fileMeta == nil {
		return false
	}
	return fileMeta.state == fileMetaStateImmutable
}
