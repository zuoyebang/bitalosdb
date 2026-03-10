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

package bitalosdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/mmap"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

const (
	metaVersion1 uint16 = 1 + iota
	metaVersion2
)

const (
	metaSlotNum     = consts.SlotNum
	metaSlotNumMask = metaSlotNum - 1

	metaMaxTupleShards     = consts.VectorTableMaxShards
	metaMaxTupleShardsMask = metaMaxTupleShards - 1

	metaHeaderOffset               = 0
	metaHeaderLen                  = 4
	metaFieldOffset                = metaHeaderLen
	metaFieldEliminateScanTsOffset = metaFieldOffset
	metaFieldSeqNumOffset          = metaFieldEliminateScanTsOffset + 8
	metaFieldKeyVersionOffset      = metaFieldSeqNumOffset + 8
	metaFieldTupleCountOffset      = metaFieldKeyVersionOffset + 8
	metaFieldBitupleStatusOffset   = metaFieldTupleCountOffset + 2
	metaFieldTupleMaxIdOffset      = metaFieldBitupleStatusOffset + metaSlotNum
	metaFieldTupleShardOffset      = metaFieldTupleMaxIdOffset + 4
	metaFieldLen                   = metaFieldTupleShardOffset + metaMaxTupleShards*4
	metaMagicLen                   = 8
	metaFooterLen                  = metaMagicLen
	metaMagic                      = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	metaLen                        = metaFieldLen + metaMagicLen
	metaFooterOffset               = metaLen - metaFooterLen

	metaFieldNumberGap     = 256 << 10
	metaFieldNumberGapMask = 256<<10 - 1
)

type metaHeader struct {
	version uint16
}

type metaShardRange struct {
	start, end uint16
}

type metadata struct {
	path             string
	logger           base.Logger
	header           *metaHeader
	file             *mmap.MMap
	fs               vfs.FS
	mu               sync.RWMutex
	seqNum           atomic.Uint64
	keyVersion       atomic.Uint64
	slotsStatus      [metaSlotNum]uint8
	tupleCount       uint16
	tupleNextId      uint32
	tupleShardRanges []metaShardRange
	tupleList        [metaMaxTupleShards]uint32
}

func openMetadata(dirname string, opts *Options) (m *metadata, err error) {
	m = &metadata{
		fs:          opts.FS,
		logger:      opts.Logger,
		tupleCount:  0,
		tupleNextId: 0,
	}
	m.path = base.MakeFilepath(m.fs, dirname, fileTypeMeta, 0)
	if os2.IsNotExist(m.path) {
		m.tupleCount = opts.VectorTableCount
		if err = m.create(); err != nil {
			return nil, err
		}
	}
	if err = m.load(); err != nil {
		return nil, err
	}

	if err = m.upgradeVersion1ToVersion2(); err != nil {
		return nil, err
	}

	if m.tupleNextId == 0 {
		tsr := m.splitTupleShardRanges(m.tupleCount)
		m.writeTupleShardRanges(tsr)
	} else {
		m.readTupleShardRanges()
	}

	return m, nil
}

func (m *metadata) create() (err error) {
	var metaFile vfs.File
	metaFile, err = m.fs.Create(m.path)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = m.fs.Remove(m.path)
		}
	}()

	var buf [metaLen]byte
	for i := range buf {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[metaHeaderOffset:], metaVersion2)
	binary.LittleEndian.PutUint16(buf[metaFieldTupleCountOffset:], m.tupleCount)
	copy(buf[metaFooterOffset:metaFooterOffset+metaFooterLen], metaMagic)
	if _, err = metaFile.Write(buf[:]); err != nil {
		return err
	}
	if err = metaFile.Close(); err != nil {
		return err
	}
	return nil
}

func (m *metadata) close() error {
	err := m.file.Close()
	return err
}

func (m *metadata) load() (err error) {
	m.file, err = mmap.Open(m.path, 0)
	if err != nil {
		return err
	}

	m.readHeader()
	m.readFields()

	return nil
}

func (m *metadata) readHeader() {
	m.header = &metaHeader{}
	m.header.version = m.file.ReadUInt16At(metaHeaderOffset)
}

func (m *metadata) readFields() {
	m.initSeqNumAndVersion()

	pos := metaFieldBitupleStatusOffset
	for i := range m.slotsStatus {
		m.slotsStatus[i] = m.file.ReadUInt8At(pos)
		pos += 1
	}

	m.tupleCount = m.getTupleCount()
	m.tupleNextId = m.getTupleNextId()
}

func (m *metadata) initSeqNumAndVersion() {
	seqNum := m.getSeqNum()
	m.seqNum.Store(seqNum + metaFieldNumberGap)
	m.writeUint64(seqNum, metaFieldSeqNumOffset)

	keyVersion := m.getKeyVersion()
	m.keyVersion.Store(keyVersion + metaFieldNumberGap)
	m.writeUint64(keyVersion, metaFieldKeyVersionOffset)
}

func (m *metadata) getTupleCount() uint16 {
	return m.file.ReadUInt16At(metaFieldTupleCountOffset)
}

func (m *metadata) getTupleNextId() uint32 {
	return m.file.ReadUInt32At(metaFieldTupleMaxIdOffset)
}

func (m *metadata) getSeqNum() uint64 {
	return m.file.ReadUInt64At(metaFieldSeqNumOffset)
}

func (m *metadata) getCurrentSeqNum() uint64 {
	return m.seqNum.Load()
}

func (m *metadata) getNextSeqNum() uint64 {
	newSeqNum := m.seqNum.Add(1)
	if newSeqNum&metaFieldNumberGapMask == 0 {
		m.writeUint64(newSeqNum, metaFieldSeqNumOffset)
	}
	return newSeqNum
}

func (m *metadata) getKeyVersion() uint64 {
	return m.file.ReadUInt64At(metaFieldKeyVersionOffset)
}

func (m *metadata) getCurrentKeyVersion() uint64 {
	return m.keyVersion.Load()
}

func (m *metadata) getNextKeyVersion() uint64 {
	newKeyVersion := m.keyVersion.Add(1)
	if newKeyVersion&metaFieldNumberGapMask == 0 {
		m.writeUint64(newKeyVersion, metaFieldKeyVersionOffset)
	}
	return newKeyVersion
}

func (m *metadata) writeSlotBituple(i int, n uint8) {
	m.slotsStatus[i] = n
	m.file.WriteUInt8At(n, metaFieldBitupleStatusOffset+i)
}

func (m *metadata) setEliminateScanTs(v uint64) {
	m.file.WriteUInt64At(v, metaFieldEliminateScanTsOffset)
}

func (m *metadata) getEliminateScanTs() uint64 {
	return m.file.ReadUInt64At(metaFieldEliminateScanTsOffset)
}

func (m *metadata) writeUint64(n uint64, pos int) {
	m.mu.Lock()
	m.file.WriteUInt64At(n+metaFieldNumberGap, pos)
	m.mu.Unlock()
}

func (m *metadata) readTupleShardRanges() {
	pos := metaFieldTupleShardOffset
	for i := range m.tupleList {
		m.tupleList[i] = m.file.ReadUInt32At(pos)
		pos += 4
	}
}

func (m *metadata) writeTupleShardRanges(tsr []metaShardRange) {
	pos := metaFieldTupleShardOffset
	for _, mr := range tsr {
		for j := mr.start; j <= mr.end; j++ {
			m.tupleList[j] = m.tupleNextId
			m.file.WriteUInt32At(m.tupleNextId, pos)
			pos += 4
		}
		m.tupleNextId++
	}
	newTupleCount := uint16(len(tsr))
	if m.tupleCount != newTupleCount {
		m.tupleCount = newTupleCount
		m.file.WriteUInt16At(m.tupleCount, metaFieldTupleCountOffset)
	}
	m.file.WriteUInt32At(m.tupleNextId, metaFieldTupleMaxIdOffset)
	m.file.Flush()
}

func (m *metadata) splitTupleShardRanges(count uint16) []metaShardRange {
	var pos, i uint16
	tsr := make([]metaShardRange, count)
	total := uint16(metaMaxTupleShards)
	shareSize := total / count
	remainder := total % count
	for i = 0; i < count; i++ {
		tsr[i].start = pos
		tsr[i].end = tsr[i].start + shareSize - 1
		if remainder > 0 {
			tsr[i].end += 1
			remainder--
		}
		pos = tsr[i].end + 1
	}
	return tsr
}

func (m *metadata) upgradeVersion1ToVersion2() error {
	if m.header.version == metaVersion1 {
		oldSlotsStatus := m.slotsStatus
		oldKeyVersion := m.getKeyVersion()
		oldSeqNum := m.getSeqNum()
		if err := m.close(); err != nil {
			return err
		}
		if err := os.Remove(m.path); err != nil {
			return err
		}
		if err := m.create(); err != nil {
			return err
		}
		if err := m.load(); err != nil {
			return err
		}
		m.file.WriteUInt64At(oldKeyVersion, metaFieldKeyVersionOffset)
		m.file.WriteUInt64At(oldSeqNum, metaFieldSeqNumOffset)
		m.keyVersion.Store(oldKeyVersion)
		m.seqNum.Store(oldSeqNum)
		for i := range oldSlotsStatus {
			m.writeSlotBituple(i, oldSlotsStatus[i])
		}
		m.file.Flush()
		m.logger.Infof("meta upgrade success version from 1 to %d keyVersion:%d seqNum:%d",
			m.header.version, oldKeyVersion, oldSeqNum)
	}
	return nil
}

func (m *metadata) writeVersion1File() error {
	if err := m.close(); err != nil {
		return err
	}
	if err := os.Remove(m.path); err != nil {
		return err
	}

	metaFile, err := m.fs.Create(m.path)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = m.fs.Remove(m.path)
		}
	}()

	var buf [1062]byte
	for i := range buf {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[metaHeaderOffset:], metaVersion1)
	binary.LittleEndian.PutUint16(buf[metaFieldTupleCountOffset:], m.tupleCount)
	copy(buf[1054:1062], metaMagic)
	if _, err = metaFile.Write(buf[:]); err != nil {
		return err
	}
	if err = metaFile.Close(); err != nil {
		return err
	}

	return m.load()
}

func (m *metadata) info() string {
	info := fmt.Sprintf("bitalosdb-meta: version:%d curSeqNum:%d curKeyVersion:%d eliminateScanTs:%d tupleCount:%d tupleNextId:%d tupleList:%v\n",
		m.header.version,
		m.getCurrentSeqNum(),
		m.getCurrentKeyVersion(),
		m.getEliminateScanTs(),
		m.tupleCount,
		m.tupleNextId,
		m.tupleList)
	return info
}
