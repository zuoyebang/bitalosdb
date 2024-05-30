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

package bindex

import (
	"arena"
	"encoding/binary"
	"math"
	"unsafe"
)

type kv64 struct {
	key   uint32
	value uint64
}

type kv32 struct {
	key   uint32
	value uint32
}

type metadata [groupSize]int8
type group32 [groupSize]kv32
type group64 [groupSize]kv64

const (
	VectorVersion        = 2
	h1Mask        uint32 = 0xffff_ff80
	h2Mask        uint32 = 0x0000_007f
	empty         int8   = -128 // 0b1000_0000
)

type h1 uint32

type h2 int8

func splitHash(h uint32) (h1, h2) {
	return h1((h & h1Mask) >> 7), h2(h & h2Mask)
}

func probeStart(hi h1, groups int) uint32 {
	return uint32(hi) % uint32(groups)
}

type VectorValType uint8

const (
	VectorValTypeUint32 VectorValType = 4
	VectorValTypeUint64 VectorValType = 8
)

type VectorHeader struct {
	version uint16
	vtype   VectorValType
	shards  uint32
}

type VectorIndex struct {
	header     VectorHeader
	ctrl       []metadata
	groups32   []group32
	groups64   []group64
	resident   uint32
	limit      uint32
	count      uint32
	groupBytes uint32
	saveGroupN uint32
	data       []byte
	arena      *arena.Arena
}

func NewVectorIndex() (m *VectorIndex) {
	m = &VectorIndex{}
	return
}

func (m *VectorIndex) InitWriter(sz uint32, vtype VectorValType) {
	groups := numGroups(sz)
	m.header = VectorHeader{
		version: VectorVersion,
		vtype:   vtype,
		shards:  groups,
	}
	m.ctrl = make([]metadata, groups)
	m.limit = groups * maxAvgGroupLoad
	m.groupBytes = groupSize * (5 + uint32(vtype))
	m.arena = arena.NewArena()

	switch vtype {
	case VectorValTypeUint32:
		m.groups32 = make([]group32, groups)
	case VectorValTypeUint64:
		m.groups64 = make([]group64, groups)
	}
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata()
	}
}

func (m *VectorIndex) SetReader(d []byte) bool {
	if d == nil {
		return false
	}

	m.data = d
	m.header = readHeader(m.data)
	if m.header.vtype != VectorValTypeUint32 && m.header.vtype != VectorValTypeUint64 {
		return false
	}
	m.groupBytes = groupSize * (5 + uint32(m.header.vtype))
	return m.readMetadata()
}

func (m *VectorIndex) Get(key uint32) (any, bool) {
	if m.header.vtype == VectorValTypeUint32 {
		return m.Get32(key)
	} else {
		return m.Get64(key)
	}
}

func (m *VectorIndex) innerMemGet32(key uint32) (value uint32, ok bool) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups32))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups32[g][s].key {
				value, ok = m.groups32[g][s].value, true
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1
		if g >= uint32(len(m.groups32)) {
			g = 0
		}
	}
}

func (m *VectorIndex) innerMemGet64(key uint32) (value uint64, ok bool) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups64))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups64[g][s].key {
				value, ok = m.groups64[g][s].value, true
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1
		if g >= uint32(len(m.groups64)) {
			g = 0
		}
	}
}

func (m *VectorIndex) rehash32(n uint32) {
	groups, ctrl := m.groups32, m.ctrl
	m.groups32 = make([]group32, n)
	m.ctrl = make([]metadata, n)
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata()
	}
	m.limit = n * maxAvgGroupLoad
	m.resident = 0
	for g := range ctrl {
		for s := range ctrl[g] {
			c := ctrl[g][s]
			if c == empty {
				continue
			}
			m.add32rehash(groups[g][s].key, groups[g][s].value)
		}
	}
}

func (m *VectorIndex) rehash64(n uint32) {
	groups, ctrl := m.groups64, m.ctrl
	m.groups64 = make([]group64, n)
	m.ctrl = make([]metadata, n)
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata()
	}
	m.limit = n * maxAvgGroupLoad
	m.resident = 0
	for g := range ctrl {
		for s := range ctrl[g] {
			c := ctrl[g][s]
			if c == empty {
				continue
			}
			m.add64rehash(groups[g][s].key, groups[g][s].value)
		}
	}
}

func (m *VectorIndex) add32rehash(key uint32, value uint32) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups32))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups32[g][s].key {
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			s := nextMatch(&matches)
			m.groups32[g][s].key = key
			m.groups32[g][s].value = value
			m.ctrl[g][s] = int8(lo)
			m.resident++
			return
		}
		g += 1
		if g >= uint32(len(m.groups32)) {
			g = 0
		}
	}
}

func (m *VectorIndex) add64rehash(key uint32, value uint64) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups64))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups64[g][s].key {
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			s := nextMatch(&matches)
			m.groups64[g][s].key = key
			m.groups64[g][s].value = value
			m.ctrl[g][s] = int8(lo)
			m.resident++
			return
		}
		g += 1
		if g >= uint32(len(m.groups64)) {
			g = 0
		}
	}
}

func (m *VectorIndex) Add32(key uint32, value uint32) {
	if m.header.vtype != VectorValTypeUint32 {
		return
	}
	if m.resident >= m.limit {
		m.rehash32(uint32(math.Ceil(float64(len(m.groups32)) * 1.5)))
	}
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups32))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups32[g][s].key {
				m.groups32[g][s].value = value
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			s := nextMatch(&matches)
			m.groups32[g][s].key = key
			m.groups32[g][s].value = value
			m.ctrl[g][s] = int8(lo)
			m.resident++
			return
		}
		g += 1
		if g >= uint32(len(m.groups32)) {
			g = 0
		}
	}
}

func (m *VectorIndex) Add64(key uint32, value uint64) {
	if m.header.vtype != VectorValTypeUint64 {
		return
	}
	if m.resident >= m.limit {
		m.rehash64(uint32(math.Ceil(float64(len(m.groups64)) * 1.2)))
	}
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.groups64))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups64[g][s].key {
				m.groups64[g][s].value = value
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			s := nextMatch(&matches)
			m.groups64[g][s].key = key
			m.groups64[g][s].value = value
			m.ctrl[g][s] = int8(lo)
			m.resident++
			return
		}
		g += 1
		if g >= uint32(len(m.groups64)) {
			g = 0
		}
	}
}

func (m *VectorIndex) Length() uint32 {
	return m.resident
}

func (m *VectorIndex) Size() uint32 {
	n := m.saveGroups()
	// header+count+ctrl+groups(k+v)
	return 12 + n*m.groupBytes
}

//go:inline
func (m *VectorIndex) calGroupHead(g uint32) uint32 {
	return 12 + g*m.groupBytes
}

//go:inline
func (m *VectorIndex) calGroups(size uint32) uint32 {
	// header+count+ctrl+groups(k+v)
	return (size - 12) / m.groupBytes
}

func (m *VectorIndex) GetData() []byte {
	return m.data
}

func (m *VectorIndex) Capacity() uint32 {
	return m.limit - m.resident
}

//go:inline
func (m *VectorIndex) saveGroups() uint32 {
	n := uint32(math.Ceil(float64(m.resident) / float64(maxAvgGroupLoad)))
	cn := uint32(len(m.groups32))
	sub := cn - n
	if sub > 100 || float32(sub)/float32(cn) > 0.25 {
		return n
	}
	return cn
}

func (m *VectorIndex) Serialize() bool {
	switch m.header.vtype {
	case VectorValTypeUint32:
		return m.Serialize32()
	case VectorValTypeUint64:
		return m.Serialize64()
	default:
		return false
	}
}

func (m *VectorIndex) Serialize32() bool {
	if m.resident <= 0 {
		return false
	}
	m.saveGroupN = m.saveGroups()
	if m.saveGroupN != uint32(len(m.ctrl)) {
		m.rehash32(m.saveGroupN)
	}

	size := int(m.Size())

	if m.data == nil {
		m.data = arena.MakeSlice[byte](m.arena, size, size)
	}
	writeHeader(m.data, m.header)
	writeCount(m.data[8:], m.resident)
	tail := 12
	for g := range m.ctrl {
		copy(m.data[tail:], (*[groupSize]byte)(unsafe.Pointer(&m.ctrl[g]))[:])
		tail += groupSize
		for s := range m.groups32[g] {
			if m.ctrl[g][s] != empty {
				writeKV32(m.data[tail:], m.groups32[g][s])
			}
			tail += 8
		}
	}
	return true
}

func (m *VectorIndex) Serialize64() bool {
	if m.resident <= 0 {
		return false
	}
	m.saveGroupN = m.saveGroups()
	if m.saveGroupN != uint32(len(m.ctrl)) {
		m.rehash64(m.saveGroupN)
	}

	size := int(m.Size())

	if m.data == nil {
		m.data = arena.MakeSlice[byte](m.arena, size, size)
	}
	writeHeader(m.data, m.header)
	writeCount(m.data[8:], m.resident)
	tail := 12
	for g := range m.ctrl {
		copy(m.data[tail:], (*[groupSize]byte)(unsafe.Pointer(&m.ctrl[g]))[:])
		tail += groupSize
		for s := range m.groups64[g] {
			if m.ctrl[g][s] != empty {
				writeKV64(m.data[tail:], m.groups64[g][s])
			}
			tail += 12
		}
	}
	return true
}

func (m *VectorIndex) SetWriter(d []byte) bool {
	if d == nil || len(d) < int(m.Size()) {
		return false
	}

	m.data = d

	return true
}

func (m *VectorIndex) readMetadata() bool {
	m.count = readUint32(m.data[8:])
	if m.count == 0 {
		return false
	}
	m.resident = m.count
	m.limit = m.count
	gs := m.calGroups(uint32(len(m.data)))
	m.ctrl = make([]metadata, gs)
	for i, _ := range m.ctrl {
		m.ctrl[i] = *(*metadata)(unsafe.Pointer(&m.data[12+uint32(i)*m.groupBytes]))
	}
	return true
}

func (m *VectorIndex) Get32(key uint32) (value uint32, ok bool) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.ctrl))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			kIdx := m.calGroupHead(g) + groupSize + s*(4+uint32(m.header.vtype))
			k := readUint32(m.data[kIdx:])
			if key == k {
				value, ok = readKV32Value(m.data[kIdx:]), true
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1
		if g >= uint32(len(m.ctrl)) {
			g = 0
		}
	}
}

func (m *VectorIndex) Get64(key uint32) (value uint64, ok bool) {
	hi, lo := splitHash(key)
	g := probeStart(hi, len(m.ctrl))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			kIdx := m.calGroupHead(g) + groupSize + s*(4+uint32(m.header.vtype))
			k := readUint32(m.data[kIdx:])
			if key == k {
				value, ok = readKV64Value(m.data[kIdx:]), true
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1
		if g >= uint32(len(m.ctrl)) {
			g = 0
		}
	}
}

func (m *VectorIndex) Finish() {
	m.groups64 = nil
	m.groups32 = nil
	if m.arena != nil {
		m.arena.Free()
		m.arena = nil
	}
}

func numGroups(n uint32) (groups uint32) {
	groups = (n + maxAvgGroupLoad - 1) / maxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}

func newEmptyMetadata() (meta metadata) {
	for i := range meta {
		meta[i] = empty
	}
	return
}

func writeHeader(buf []byte, header VectorHeader) {
	binary.BigEndian.PutUint16(buf[0:], header.version)
	binary.BigEndian.PutUint16(buf[2:], uint16(header.vtype))
	binary.BigEndian.PutUint32(buf[4:], header.shards)
}

func writeCount(buf []byte, count uint32) {
	binary.BigEndian.PutUint32(buf[0:], count)
}

func writeKV32(buf []byte, item32 kv32) {
	binary.BigEndian.PutUint32(buf[0:], item32.key)
	binary.BigEndian.PutUint32(buf[4:], item32.value)
}

func writeKV64(buf []byte, item64 kv64) {
	binary.BigEndian.PutUint32(buf[0:], item64.key)
	binary.BigEndian.PutUint64(buf[4:], item64.value)
}

func readHeader(buf []byte) VectorHeader {
	header := VectorHeader{
		version: binary.BigEndian.Uint16(buf[0:]),
		vtype:   VectorValType(binary.BigEndian.Uint16(buf[2:])),
		shards:  binary.BigEndian.Uint32(buf[4:]),
	}

	return header
}

func readUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf[0:])
}

func readKV32Value(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf[4:])
}

func readKV64Value(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf[4:])
}
