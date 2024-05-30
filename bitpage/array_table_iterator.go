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

package bitpage

import (
	"bytes"
	"sync"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bytepools"
)

var _ base.InternalIterator = (*arrayTableIterator)(nil)

type sharedInfo struct {
	idx int
	key []byte
}

type arrayTableIterator struct {
	at           *arrayTable
	intIndexPos  int
	blockIter    *pageBlockIterator
	blockCloser  func()
	iterKey      internalKey
	iterValue    []byte
	keyBuf       []byte
	sharedCache  *sharedInfo
	disableCache bool
}

var atIterPool = sync.Pool{
	New: func() interface{} {
		return &arrayTableIterator{}
	},
}

func (ai *arrayTableIterator) findItem() (*internalKey, []byte) {
	sharedKey1, sharedKey2, value := ai.at.getSharedKV(ai.intIndexPos, ai.sharedCache)
	if sharedKey1 == nil {
		return nil, nil
	}

	var key []byte
	if sharedKey2 == nil {
		key = sharedKey1
	} else {
		ai.keyBuf = append(ai.keyBuf[:0], sharedKey1...)
		key = append(ai.keyBuf, sharedKey2...)
	}

	ai.iterKey = base.MakeInternalSetKey(key)
	ai.iterValue = value
	return &ai.iterKey, ai.iterValue
}

func (ai *arrayTableIterator) firstInternal() (*internalKey, []byte) {
	ai.intIndexPos = 0
	if ai.at.isNonBlock() {
		return ai.findItem()
	}

	if ai.setBlockIter() {
		return ai.blockIter.First()
	}

	return nil, nil
}

func (ai *arrayTableIterator) First() (*internalKey, []byte) {
	return ai.firstInternal()
}

func (ai *arrayTableIterator) nextInternal() (*internalKey, []byte) {
	if ai.at.isNonBlock() {
		ai.intIndexPos++
		return ai.findItem()
	}

	if ai.blockIter == nil {
		return nil, nil
	}

	ikey, value := ai.blockIter.Next()
	if ikey != nil {
		return ikey, value
	}

	ai.intIndexPos++
	if ai.setBlockIter() {
		return ai.blockIter.First()
	}

	return nil, nil
}

func (ai *arrayTableIterator) Next() (*internalKey, []byte) {
	return ai.nextInternal()
}

func (ai *arrayTableIterator) Prev() (*internalKey, []byte) {
	if ai.at.isNonBlock() {
		ai.intIndexPos--
		return ai.findItem()
	}

	if ai.blockIter == nil {
		return nil, nil
	}

	ikey, value := ai.blockIter.Prev()
	if ikey != nil {
		return ikey, value
	}

	ai.intIndexPos--
	if ai.setBlockIter() {
		return ai.blockIter.Last()
	}

	return nil, nil
}

func (ai *arrayTableIterator) Last() (*internalKey, []byte) {
	ai.intIndexPos = ai.at.num - 1
	if ai.at.isNonBlock() {
		return ai.findItem()
	}

	if ai.setBlockIter() {
		return ai.blockIter.Last()
	}

	return nil, nil
}

func (ai *arrayTableIterator) SeekGE(key []byte) (*internalKey, []byte) {
	ai.intIndexPos = ai.at.findKeyByIntIndex(key)
	if ai.at.isNonBlock() {
		return ai.findItem()
	}

	if ai.setBlockIter() {
		return ai.blockIter.SeekGE(key)
	}

	return nil, nil
}

func (ai *arrayTableIterator) SeekLT(key []byte) (*internalKey, []byte) {
	ai.intIndexPos = ai.at.findKeyByIntIndex(key)
	if ai.at.isNonBlock() {
		poskey, _ := ai.findItem()
		if poskey != nil {
			return ai.Prev()
		}

		lastKey, lastValue := ai.Last()
		if lastKey != nil && bytes.Compare(lastKey.UserKey, key) < 0 {
			return lastKey, lastValue
		}
		return nil, nil
	}

	if ai.intIndexPos >= ai.at.num {
		ai.intIndexPos = ai.at.num - 1
	}

	for i := 0; ai.setBlockIter() && i < 2; i++ {
		ikey, value := ai.blockIter.SeekLT(key)
		if ikey != nil {
			return ikey, value
		}

		ai.intIndexPos--
	}

	return nil, nil
}

func (ai *arrayTableIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (ikey *internalKey, value []byte) {
	return ai.SeekGE(key)
}

func (ai *arrayTableIterator) SetBounds(lower, upper []byte) {
}

func (ai *arrayTableIterator) Error() error {
	return nil
}

func (ai *arrayTableIterator) setBlockIter() bool {
	if !ai.at.checkPositon(ai.intIndexPos) {
		ai.closeBlockIter()
		return false
	}

	blockBuf, closer, blockExist := ai.at.blockCache.GetBlock(ai.at.cacheID, uint64(ai.intIndexPos))
	if !blockExist {
		_, blockBuf = ai.at.getKV(ai.intIndexPos)
		if len(blockBuf) > 0 {
			var compressedBuf []byte
			compressedBuf, closer = bytepools.ReaderBytePools.GetMaxBytePool()
			blockBuf, _ = compressDecode(compressedBuf, blockBuf)
			if !ai.disableCache {
				ai.at.blockCache.SetBlock(ai.at.cacheID, uint64(ai.intIndexPos), blockBuf)
			}
		} else {
			ai.closeBlockIter()
			return false
		}
	}

	ai.closeBlockIter()

	if closer != nil {
		ai.blockCloser = closer
	}

	pb := pageBlock{}
	openPageBlock(&pb, blockBuf)

	ai.blockIter = pb.newIter(nil)

	return true
}

func (ai *arrayTableIterator) closeBlockIter() {
	if ai.blockIter != nil {
		if ai.blockCloser != nil {
			ai.blockCloser()
		}

		ai.blockIter.Close()
	}
	ai.blockIter = nil
	ai.blockCloser = nil
}

func (ai *arrayTableIterator) Close() error {
	ai.closeBlockIter()
	ai.at = nil
	ai.intIndexPos = 0
	ai.iterValue = nil
	ai.disableCache = false
	if ai.keyBuf != nil {
		ai.keyBuf = ai.keyBuf[:0]
	}
	if ai.sharedCache != nil {
		ai.sharedCache.idx = -1
		ai.sharedCache.key = nil
	}
	atIterPool.Put(ai)

	return nil
}

func (ai *arrayTableIterator) String() string {
	return "arrayTableIterator"
}
