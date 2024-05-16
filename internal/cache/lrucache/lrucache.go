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

package lrucache

import (
	"bytes"
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/internal/cache"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

var _ cache.ICache = (*LruCache)(nil)

func New(opts *options.CacheOptions) cache.ICache {
	return newShards(opts)
}

func NewLrucache(opts *options.CacheOptions) *LruCache {
	return newShards(opts)
}

func (lrc *LruCache) decodeValue(key, eval []byte) []byte {
	cLen := len(eval)
	if eval == nil || cLen <= 4 {
		return nil
	}

	kLen := int(binary.BigEndian.Uint32(eval[0:4]))
	vPos := 4 + kLen
	if cLen <= vPos || !bytes.Equal(key, eval[4:vPos]) {
		return nil
	}

	return eval[vPos:]
}

func (lrc *LruCache) GetKeyHash(_ []byte) uint32 {
	return 0
}

func (lrc *LruCache) ExistAndDelete(key []byte, _ uint32) error {
	return lrc.Delete(key, 0)
}

func (lrc *LruCache) Get(key []byte, _ uint32) ([]byte, func(), bool) {
	keyID, keyHash := hash.MD5Uint64(key)
	hd := lrc.getShardByHashId(keyHash).Get(keyID, keyHash)
	value := hd.Get()
	if value == nil {
		hd.Release()
		return nil, nil, false
	}

	return value, func() { hd.Release() }, true
}

func (lrc *LruCache) Set(key, value []byte, _ uint32) error {
	if len(key) == 0 || len(value) == 0 {
		return nil
	}

	keyID, keyHash := hash.MD5Uint64(key)

	vLen := len(value)
	v := lrc.Alloc(vLen)
	buf := v.Buf()
	copy(buf[0:vLen], value)

	lrc.getShardByHashId(keyHash).Set(keyID, keyHash, v).Release()
	return nil
}

func (lrc *LruCache) Delete(key []byte, _ uint32) error {
	if key == nil {
		return nil
	}

	keyID, keyHash := hash.MD5Uint64(key)

	lrc.getShardByHashId(keyHash).Delete(keyID, keyHash)
	return nil
}

func (lrc *LruCache) GetBlock(keyID uint64, keyHash uint64) ([]byte, func(), bool) {
	hd := lrc.getShardByHashId(keyHash).Get(keyID, keyHash)
	value := hd.Get()
	if value == nil {
		hd.Release()
		return nil, nil, false
	}

	return value, func() { hd.Release() }, true
}

func (lrc *LruCache) SetBlock(keyID uint64, keyHash uint64, value []byte) error {
	vLen := len(value)
	v := lrc.Alloc(vLen)
	buf := v.Buf()
	copy(buf[0:vLen], value)

	lrc.getShardByHashId(keyHash).Set(keyID, keyHash, v).Release()
	return nil
}

func (lrc *LruCache) DeleteBlock(keyID uint64, keyHash uint64) error {
	lrc.getShardByHashId(keyHash).Delete(keyID, keyHash)
	return nil
}

func (lrc *LruCache) Close() {
	lrc.Unref()
}
