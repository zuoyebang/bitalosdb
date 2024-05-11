package lrucache

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"

	"github.com/zuoyebang/bitalosdb/internal/cache"
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

func (lrc *LruCache) encodeKeyHash(key []byte) (uint64, uint64) {
	kmd5 := md5.Sum(key)
	keyID := binary.LittleEndian.Uint64(kmd5[0:8])
	keyHash := binary.LittleEndian.Uint64(kmd5[8:16])

	return keyID, keyHash
}

func (lrc *LruCache) GetKeyHash(_ []byte) uint32 {
	return 0
}

func (lrc *LruCache) ExistAndDelete(key []byte, _ uint32) error {
	return lrc.Delete(key, 0)
}

func (lrc *LruCache) Get(key []byte, _ uint32) ([]byte, func(), bool) {
	keyID, keyHash := lrc.encodeKeyHash(key)

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

	keyID, keyHash := lrc.encodeKeyHash(key)

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

	keyID, keyHash := lrc.encodeKeyHash(key)

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
