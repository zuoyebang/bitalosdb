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

package base

type VectorMemTable interface {
	Close(isFree bool) error
	Delete(h, l, seqNum uint64) bool
	FreeData()
	GetMeta(h, l uint64) (seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64, size uint32, pre uint64, next uint64, err error)
	GetTimestamp(h, l uint64) (seqNum uint64, timestamp uint64, dt uint8, err error)
	GetValue(h, l uint64) (value []byte, seqNum uint64, dataType uint8, timestamp uint64, slot uint16, closer func(), err error)
	Has(h, l uint64) (seqNum uint64, ok bool)
	NewData(buf []byte)
	NewIterator() VectorTableIterator
	Reset()
	Set(key []byte, h, l, seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, value []byte) error
	SetTimestamp(h, l uint64, seqNum, timestamp uint64, dt uint8) error
	GetKeySize() uint32
	MSync() error
}
