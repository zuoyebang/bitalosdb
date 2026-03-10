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

package vectortable

type DataHolder interface {
	GetHeader() *header
	SyncHeader()
	GetIndex() *tableDesignated
	GetMeta() *tableExtend
	GetKDataM() *tableMulti
	GetVDataM() *tableMulti
	GetEM() *expireManager
	GetStat() uint8
	SetStat(stat uint8)
	GetExpireCap(now uint64) uint64
	MergeExpired(now uint64)
	IncrExpire(expire, size uint64)
	ClearExpired(now uint64)
	SwapHeader(h *header)
	SwapIndex(i *tableDesignated)
	Close() error

	set(hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16) (metaOffset uint64, err error)
	setDebug(hi, lo uint64, k, inV []byte, dataType uint8, timestamp, seqNum uint64, slot uint16, c chan int) (metaOffset uint64, err error)
	setKKVMeta(hi, lo uint64, k []byte, dataType uint8, timestamp,
		seqNum uint64, slot uint16, version uint64, size uint32) (metaOffset uint64, err error)
	setKKVListMeta(hi, lo uint64, k []byte, dataType uint8, timestamp,
		seqNum uint64, slot uint16, version uint64, size uint32, pre, next uint64) (metaOffset uint64, err error)
	getValue(metaOffset uint64) (bs []byte, seqNum uint64, dt uint8, ts uint64, slot uint16, c func(), err error)
	getMeta(metaOffset uint64) (
		seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64,
		size uint32, pre, next uint64, err error,
	)
	getKV(metaOffset uint64) (
		hashL uint64, hashHH uint32,
		key []byte, value []byte, seqNum uint64, dataType uint8, timestamp uint64, slot uint16, version uint64, size uint32,
		pre, next uint64, kCloser func(), vCloser func(), err error,
	)
	getKVCopy(metaOffset uint64) (
		key, value []byte, seqNum uint64, dataType uint8,
		timestamp uint64, slot uint16, version uint64, size uint32, pre, next uint64, err error,
	)
	update(mOffset uint64, inV []byte, dataType uint8, timestamp, seqNum uint64) (err error)
	updateKKVMeta(mOffset uint64, dataType uint8, timestamp, seqNum, version uint64, size uint32)
	updateKKVListMeta(mOffset uint64, dataType uint8, timestamp, seqNum, version uint64, size uint32, pre, next uint64)
	getHashL(metaOffset uint64) (hashL uint64)
	getHashInfo(metaOffset uint64) (hashL uint64, hashHH uint32)
	getHashAndVersionDT(metaOffset uint64) (hashL uint64, hashHH uint32, ver uint64, dt uint8)
	checkSeq(metaOffset uint64, seqNum uint64) bool
	del(itemOffset uint64)
	delDebug(itemOffset uint64, c chan int)
	getSeqNum(metaOffset uint64) (seqNum uint64)
	getTTL(metaOffset uint64) (seqNum, ts uint64, dt uint8)
	setTTL(metaOffset uint64, seqNum, ttl uint64)
	setSize(metaOffset uint64, seqNum uint64, size uint32)
	sync() error
	syncBuffer() error
}

type MarshalMeta func(dh *dataHolder, kIdx, vIdx uint16, kOffset, vOffset uint32)
type SetKVOffset func(dh *dataHolder, kIdx, vIdx uint16, kOffset, vOffset uint32)
