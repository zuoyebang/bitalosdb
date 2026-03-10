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

package vectormap

type DataHolder interface {
	close() error

	set(hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (metaOffset uint32, err error)
	setKKVMeta(hi, lo uint64, k []byte, dataType uint8, timestamp,
		seqNum uint64, slot uint16, version uint64, size uint32) (metaOffset uint32, err error)
	setKKVListMeta(hi, lo uint64, k []byte, dataType uint8, timestamp,
		seqNum uint64, slot uint16, version uint64, size uint32, pre, next uint64) (metaOffset uint32, err error)
	getValue(metaOffset uint32) (bs []byte, seqNum uint64, dt uint8, ts uint64, slot uint16, err error)
	getMeta(metaOffset uint32) (
		seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64,
		size uint32, pre, next uint64, err error,
	)
	getKV(metaOffset uint32) (
		key, value []byte, seqNum uint64, dataType uint8,
		timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, err error,
	)
	update(metaOffset uint32, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (err error)
	updateKKVMeta(metaOffset uint32, dataType uint8, timestamp, seqNum uint64, slot uint16, version uint64, size uint32)
	updateKKVListMeta(metaOffset uint32, dataType uint8, timestamp, seqNum uint64, slot uint16, version uint64, size uint32, pre, next uint64)
	getHashL(metaOffset uint32) (hashL uint64)
	getHashInfo(metaOffset uint32) (hashL uint64, hashHH uint32)
	getHashAndVersionDT(metaOffset uint32) (hashL uint64, hashHH uint32, ver uint64, dt uint8)
	checkSeq(metaOffset uint32, seqNum uint64) bool
	getSeqNum(metaOffset uint32) (seqNum uint64)
	getTTL(metaOffset uint32) (seqNum, ts uint64, dt uint8)
	setTTL(metaOffset uint32, seqNum, ttl uint64)
}
