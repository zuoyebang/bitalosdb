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
	"github.com/zuoyebang/bitalosdb/v2/internal/kkv"
)

func (a *vectorArrayTable) newIter(opts *iterOptions) InternalKKVIterator {
	if opts == nil {
		opts = &iterOptions{}
	}

	var it InternalKKVIterator
	switch opts.DataType {
	case kkv.DataTypeZsetIndex:
		num := a.zsetIndex.getOffsetSize()
		it = &vatZsetIndexIterator{
			at:  a,
			num: num,
			sp:  0,
			ep:  num - 1,
		}
	case kkv.DataTypeZset:
		it = &vatZsetDataIterator{at: a}
	case kkv.DataTypeList, kkv.DataTypeBitmap:
		num := a.listOffsetIndex.getSize()
		it = &vatListIterator{
			at:       a,
			dataType: opts.DataType,
			num:      num,
			sp:       0,
			ep:       num - 1,
		}
	case kkv.DataTypeHash, kkv.DataTypeSet:
		it = &vatHashIterator{
			at:       a,
			dataType: opts.DataType,
		}
	case kkv.DataTypeExpireKey:
		it = &vatExpireIterator{at: a}
	default:
		it = &vatFlushIterator{at: a}
	}

	if opts.IsTest {
		it.SetBounds(opts.LowerBound, opts.UpperBound)
	}

	return it
}

var _ InternalKKVIterator = (*vatHashIterator)(nil)
var _ InternalKKVIterator = (*vatExpireIterator)(nil)
var _ InternalKKVIterator = (*vatZsetIndexIterator)(nil)
var _ InternalKKVIterator = (*vatListIterator)(nil)
var _ InternalKKVIterator = (*vatFlushIterator)(nil)

type vatHashIterator struct {
	at         *vectorArrayTable
	dataType   uint8
	keyVersion []byte
	nextOffset uint32
	lower      []byte
	upper      []byte
}

func (i *vatHashIterator) findItem(offset, hl, keySize, valueSize uint32) (*InternalKKVKey, []byte) {
	key, value := i.at.getKeyValue(offset+hl, keySize, valueSize)
	ikey := kkv.MakeInternalSetKeyOptimal(i.keyVersion, key, i.dataType)
	i.nextOffset = offset + hl + keySize + valueSize
	return &ikey, value
}

func (i *vatHashIterator) Next() (*InternalKKVKey, []byte) {
	offset := i.nextOffset
	dt, keySize, valueSize, hl, isFirst := i.at.decodeItemHeader(offset)
	if dt != i.dataType || isFirst {
		return nil, nil
	}

	key, value := i.findItem(offset, hl, keySize, valueSize)
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}

	return key, value
}

func (i *vatHashIterator) SeekGE(seek []byte) (*InternalKKVKey, []byte) {
	i.keyVersion = kkv.GetKeyVersion(seek)
	offset, _, exist := i.at.getHashSetListVersionIndex(i.keyVersion)
	if !exist {
		return nil, nil
	}

	dt, keySize, valueSize, hl, isFirst := i.at.decodeItemHeaderOptimal(offset)
	if dt != i.dataType || !isFirst {
		return nil, nil
	}

	key, value := i.findItem(offset, hl, keySize, valueSize)
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}

	return key, value
}

func (i *vatHashIterator) First() (*InternalKKVKey, []byte) { panic("not support") }
func (i *vatHashIterator) SeekLT([]byte) (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatHashIterator) Prev() (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatHashIterator) Last() (*InternalKKVKey, []byte) {
	panic("not support")
}

func (i *vatHashIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *vatHashIterator) Error() error {
	return nil
}

func (i *vatHashIterator) Close() error {
	i.at = nil
	return nil
}

func (i *vatHashIterator) String() string {
	return "vatHashIterator"
}

type vatExpireIterator struct {
	at         *vectorArrayTable
	curKeyNum  uint32
	curVersion []byte
	curOffset  uint32
	iterNum    uint32
	iterSize   uint32
	lower      []byte
	upper      []byte
}

func (i *vatExpireIterator) findItem(offset, hl, keySize, valueSize uint32) (*InternalKKVKey, []byte) {
	key, value := i.at.getKeyValue(offset+hl, keySize, valueSize)
	ikey := kkv.MakeInternalSetKeyOptimal(i.curVersion, key, kkv.DataTypeExpireKey)
	i.curOffset = offset + hl + keySize + valueSize
	return &ikey, value
}

func (i *vatExpireIterator) newReadVersion(offset, keyNum uint32) {
	i.curVersion = i.at.dataFile.GetBytes(offset, vatKeyVersionSize)
	i.curKeyNum = keyNum
	i.curOffset = offset + vatKeyVersionSize
	i.iterSize = vatKeyVersionSize
	i.iterNum = 0
}

func (i *vatExpireIterator) Next() (*InternalKKVKey, []byte) {
	if i.iterNum >= i.curKeyNum {
		return nil, nil
	}

	subKey := i.at.dataFile.GetBytes(i.curOffset, vatExpireKeySize)
	key := kkv.MakeInternalSetKeyOptimal(i.curVersion, subKey, kkv.DataTypeExpireKey)
	if kkv.IsUserKeyGEUpperBound(&key, i.upper) {
		return nil, nil
	}

	i.iterSize += vatExpireKeySize
	i.curOffset += vatExpireKeySize
	i.iterNum++
	return &key, nil
}

func (i *vatExpireIterator) SeekGE(seek []byte) (*InternalKKVKey, []byte) {
	i.curVersion = kkv.GetKeyVersion(seek)
	offset, err := i.at.expireVersionIndex.Get(i.curVersion)
	if err != nil {
		return nil, nil
	}

	dt, keySize, _, hl, isFirst := i.at.decodeItemHeader(offset)
	if dt != kkv.DataTypeExpireKey || !isFirst {
		return nil, nil
	}

	i.newReadVersion(offset+hl, keySize)
	return i.Next()
}

func (i *vatExpireIterator) First() (*InternalKKVKey, []byte) { panic("not support") }
func (i *vatExpireIterator) SeekLT([]byte) (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatExpireIterator) Prev() (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatExpireIterator) Last() (*InternalKKVKey, []byte) {
	panic("not support")
}

func (i *vatExpireIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *vatExpireIterator) Error() error {
	return nil
}

func (i *vatExpireIterator) Close() error {
	i.at = nil
	return nil
}

func (i *vatExpireIterator) String() string {
	return "vatExpireIterator"
}

type vatListIterator struct {
	at         *vectorArrayTable
	dataType   uint8
	pos        int
	sp         int
	ep         int
	num        int
	keyVersion []byte
	lower      []byte
	upper      []byte
}

func (i *vatListIterator) findItem() (*InternalKKVKey, []byte) {
	if i.pos < i.sp || i.pos > i.ep {
		return nil, nil
	}

	offset := i.at.listOffsetIndex.getOffset(i.pos)
	dt, keySize, valueSize, hl, isFirst := i.at.decodeItemHeader(offset)
	if i.dataType != dt {
		return nil, nil
	}

	offset += hl
	if isFirst {
		i.keyVersion = i.at.dataFile.GetBytes(offset, vatKeyVersionSize)
		offset += vatKeyVersionSize
	}

	key, value := i.at.getKeyValue(offset, keySize, valueSize)
	ikey := kkv.MakeInternalSetKeyOptimal(i.keyVersion, key, i.dataType)
	offset += keySize + valueSize
	return &ikey, value
}

func (i *vatListIterator) findKeyPos(k []byte) (int, bool) {
	startPos, endPos, pos, found := i.at.findZindexKeyRange(k)
	if !found {
		return 0, false
	}
	i.sp = startPos
	i.ep = endPos
	if pos == i.sp-i.ep+1 {
		return 0, false
	}
	return startPos + pos, true
}

func (i *vatListIterator) First() (*InternalKKVKey, []byte) {
	i.pos = 0
	key, value := i.findItem()
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}

	return key, value
}

func (i *vatListIterator) Next() (*InternalKKVKey, []byte) {
	i.pos++
	key, value := i.findItem()
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}

	return key, value
}

func (i *vatListIterator) Prev() (*InternalKKVKey, []byte) {
	i.pos--
	key, value := i.findItem()
	if kkv.IsUserKeyLTLowerBound(key, i.lower) {
		return nil, nil
	}

	return key, value
}

func (i *vatListIterator) Last() (*InternalKKVKey, []byte) {
	i.pos = i.ep
	key, value := i.findItem()
	if kkv.IsUserKeyLTLowerBound(key, i.lower) {
		return nil, nil
	}

	return key, value
}

func (i *vatListIterator) SeekGE(seek []byte) (*InternalKKVKey, []byte) {
	startPos, endPos, pos, found := i.at.findListKeyRange(seek)
	if !found || pos == endPos-startPos+1 {
		return nil, nil
	}

	i.sp = startPos
	i.ep = endPos
	i.pos = startPos + pos
	i.keyVersion = kkv.GetKeyVersion(seek)
	return i.findItem()
}

func (i *vatListIterator) SeekLT(seek []byte) (*InternalKKVKey, []byte) {
	startPos, endPos, pos, found := i.at.findListKeyRange(seek)
	if !found {
		return nil, nil
	}

	i.sp = startPos
	i.ep = endPos
	i.keyVersion = kkv.GetKeyVersion(seek)
	if pos < endPos-startPos+1 {
		i.pos = startPos + pos
		return i.Prev()
	}

	return i.Last()
}

func (i *vatListIterator) Close() error {
	i.at = nil
	return nil
}

func (i *vatListIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *vatListIterator) Error() error {
	return nil
}
func (i *vatListIterator) String() string {
	return "vatListIterator"
}

type vatZsetIndexIterator struct {
	at         *vectorArrayTable
	pos        int
	sp         int
	ep         int
	num        int
	keyVersion []byte
	lower      []byte
	upper      []byte
}

func (i *vatZsetIndexIterator) findItem() (*InternalKKVKey, []byte) {
	if i.pos < i.sp || i.pos > i.ep {
		return nil, nil
	}

	offset := i.at.zsetIndex.getOffset(i.pos)
	dt, keySize, valueSize, hl, isFirst := i.at.decodeItemHeader(offset)
	if dt != kkv.DataTypeZsetIndex {
		return nil, nil
	}

	offset += hl
	if isFirst {
		i.keyVersion = i.at.dataFile.GetBytes(offset, vatKeyVersionSize)
		offset += vatKeyVersionSize
	}

	key, value := i.at.getKeyValue(offset, keySize, valueSize)
	ikey := kkv.MakeInternalSetKeyOptimal(i.keyVersion, key, kkv.DataTypeZsetIndex)
	offset += keySize + valueSize
	return &ikey, value
}

func (i *vatZsetIndexIterator) findKeyPos(k []byte) (int, bool) {
	startPos, endPos, pos, found := i.at.findZindexKeyRange(k)
	if !found {
		return 0, false
	}
	i.sp = startPos
	i.ep = endPos
	if pos == i.sp-i.ep+1 {
		return 0, false
	}
	return startPos + pos, true
}

func (i *vatZsetIndexIterator) First() (*InternalKKVKey, []byte) {
	i.pos = i.sp
	key, value := i.findItem()
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}

	return key, value
}

func (i *vatZsetIndexIterator) Next() (*InternalKKVKey, []byte) {
	i.pos++
	key, value := i.findItem()
	if kkv.IsUserKeyGEUpperBound(key, i.upper) {
		return nil, nil
	}

	return key, value
}

func (i *vatZsetIndexIterator) Prev() (*InternalKKVKey, []byte) {
	i.pos--
	key, value := i.findItem()
	if kkv.IsUserKeyLTLowerBound(key, i.lower) {
		return nil, nil
	}
	return key, value
}

func (i *vatZsetIndexIterator) Last() (*InternalKKVKey, []byte) {
	i.pos = i.ep
	key, value := i.findItem()
	if kkv.IsUserKeyLTLowerBound(key, i.lower) {
		return nil, nil
	}
	return key, value
}

func (i *vatZsetIndexIterator) SeekGE(seek []byte) (*InternalKKVKey, []byte) {
	startPos, endPos, pos, found := i.at.findZindexKeyRange(seek)
	if !found || pos == endPos-startPos+1 {
		return nil, nil
	}

	i.sp = startPos
	i.ep = endPos
	i.pos = startPos + pos
	i.keyVersion = kkv.GetKeyVersion(seek)
	return i.findItem()
}

func (i *vatZsetIndexIterator) SeekLT(seek []byte) (*InternalKKVKey, []byte) {
	startPos, endPos, pos, found := i.at.findZindexKeyRange(seek)
	if !found {
		return nil, nil
	}

	i.sp = startPos
	i.ep = endPos
	i.keyVersion = kkv.GetKeyVersion(seek)
	if pos < endPos-startPos+1 {
		i.pos = startPos + pos
		return i.Prev()
	}

	return i.Last()
}

func (i *vatZsetIndexIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *vatZsetIndexIterator) Error() error {
	return nil
}

func (i *vatZsetIndexIterator) Close() error {
	i.at = nil
	return nil
}

func (i *vatZsetIndexIterator) String() string {
	return "vatZsetIndexIterator"
}

type vatZsetDataIterator struct {
	at         *vectorArrayTable
	curKeyNum  uint32
	curVersion []byte
	curOffset  uint32
	iterNum    uint32
	iterSize   uint32
	lower      []byte
	upper      []byte
}

func (i *vatZsetDataIterator) newReadVersion(offset, keyNum uint32) {
	i.curVersion = i.at.dataFile.GetBytes(offset, vatKeyVersionSize)
	i.curKeyNum = keyNum
	i.curOffset = offset + vatKeyVersionSize
	i.iterSize = vatKeyVersionSize
	i.iterNum = 0
}

func (i *vatZsetDataIterator) Next() (*InternalKKVKey, []byte) {
	if i.iterNum >= i.curKeyNum {
		return nil, nil
	}

	member := i.at.dataFile.GetBytes(i.curOffset, vatZdataMemberSize)
	key := kkv.MakeInternalSetKeyOptimal(i.curVersion, member, kkv.DataTypeZset)
	value := i.at.dataFile.GetBytes(i.curOffset+vatZdataMemberSize, vatZdataValueSize)
	i.iterSize += vatZdataItemLength
	i.curOffset += vatZdataItemLength
	i.iterNum++
	return &key, value
}

func (i *vatZsetDataIterator) SeekGE(seek []byte) (*InternalKKVKey, []byte) {
	i.curVersion = kkv.GetKeyVersion(seek)
	offset, exist := i.at.zsetIndex.getZdataVersionIndex(i.curVersion)
	if !exist {
		return nil, nil
	}

	dt, keySize, _, hl, isFirst := i.at.decodeItemHeader(offset)
	if dt != kkv.DataTypeZset || !isFirst {
		return nil, nil
	}

	i.newReadVersion(offset+hl, keySize)
	return i.Next()
}

func (i *vatZsetDataIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

func (i *vatZsetDataIterator) First() (*InternalKKVKey, []byte) { panic("not support") }
func (i *vatZsetDataIterator) Prev() (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatZsetDataIterator) Last() (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatZsetDataIterator) SeekLT([]byte) (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatZsetDataIterator) Error() error   { return nil }
func (i *vatZsetDataIterator) Close() error   { return nil }
func (i *vatZsetDataIterator) String() string { return "vatZsetDataIterator" }

type vatFlushIterator struct {
	at           *vectorArrayTable
	nextOffset   uint32
	curVersion   []byte
	listKeyPos   int
	isIterZdata  bool
	zdataIter    *vatZsetDataIterator
	isIterExpire bool
	expireIter   *vatExpireIterator
}

func (i *vatFlushIterator) findItem() (*InternalKKVKey, []byte) {
	if i.isIterZdata {
		ikey, value := i.getNextZdataKey()
		if ikey != nil {
			return ikey, value
		}
	} else if i.isIterExpire {
		ikey, value := i.getNextExpireKey()
		if ikey != nil {
			return ikey, value
		}
	}

	dt, keySize, valueSize, hl, isFirst := i.at.decodeItemHeader(i.nextOffset)
	if dt == 0 {
		return nil, nil
	}

	i.nextOffset += hl

	switch dt {
	case kkv.DataTypeZset:
		if i.zdataIter == nil {
			i.zdataIter = &vatZsetDataIterator{at: i.at}
		}
		i.zdataIter.newReadVersion(i.nextOffset, keySize)
		i.isIterZdata = true
		return i.getNextZdataKey()
	case kkv.DataTypeExpireKey:
		if i.expireIter == nil {
			i.expireIter = &vatExpireIterator{at: i.at}
		}
		i.expireIter.newReadVersion(i.nextOffset, keySize)
		i.isIterExpire = true
		return i.getNextExpireKey()
	default:
		if isFirst {
			i.curVersion = i.at.dataFile.GetBytes(i.nextOffset, vatKeyVersionSize)
			i.nextOffset += vatKeyVersionSize
		}

		key, value := i.at.getKeyValue(i.nextOffset, keySize, valueSize)
		ikey := kkv.MakeInternalSetKeyOptimal(i.curVersion, key, dt)
		i.nextOffset += keySize + valueSize
		return &ikey, value
	}
}

func (i *vatFlushIterator) getNextZdataKey() (*InternalKKVKey, []byte) {
	key, value := i.zdataIter.Next()
	if key != nil {
		return key, value
	}

	i.nextOffset += i.zdataIter.iterSize
	i.isIterZdata = false
	return nil, nil
}

func (i *vatFlushIterator) getNextExpireKey() (*InternalKKVKey, []byte) {
	key, value := i.expireIter.Next()
	if key != nil {
		return key, value
	}

	i.nextOffset += i.expireIter.iterSize
	i.isIterExpire = false
	return nil, nil
}

func (i *vatFlushIterator) First() (*InternalKKVKey, []byte) {
	i.nextOffset = i.at.getDataOffset()
	return i.findItem()
}

func (i *vatFlushIterator) Next() (*InternalKKVKey, []byte) {
	return i.findItem()
}

func (i *vatFlushIterator) SetBounds(_, _ []byte)                     {}
func (i *vatFlushIterator) SeekGE(_ []byte) (*InternalKKVKey, []byte) { panic("not support") }
func (i *vatFlushIterator) Prev() (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatFlushIterator) Last() (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatFlushIterator) SeekLT([]byte) (*InternalKKVKey, []byte) {
	panic("not support")
}
func (i *vatFlushIterator) Error() error   { return nil }
func (i *vatFlushIterator) Close() error   { return nil }
func (i *vatFlushIterator) String() string { return "vatFlushIterator" }
