// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"log"

	"main/internal/bytealloc"

	"github.com/zuoyebang/bitalosdb"
)

// DB specifies the minimal interfaces that need to be implemented
type DB interface {
	NewIter(*bitalosdb.IterOptions) iterator
	NewBatch() batch
	Scan(iter iterator, key []byte, count int64, reverse bool) error
	MetricsInfo() bitalosdb.MetricsInfo
	Flush() error
	Close() error
	Get([]byte) ([]byte, func(), error)
}

type iterator interface {
	SeekLT(key []byte) bool
	SeekGE(key []byte) bool
	Valid() bool
	Key() []byte
	Value() []byte
	First() bool
	Next() bool
	Last() bool
	Prev() bool
	Close() error
}

type batch interface {
	Close() error
	Commit(opts *bitalosdb.WriteOptions) error
	Set(key, value []byte, opts *bitalosdb.WriteOptions) error
	Delete(key []byte, opts *bitalosdb.WriteOptions) error
}

// Adapters for Pebble. Since the interfaces above are based on Pebble's
// interfaces, it can simply forward calls for everything.
type testBitalosDB struct {
	d       *bitalosdb.DB
	ballast []byte
}

func newBitalosDB(dir string) DB {
	opts := &bitalosdb.Options{
		// CacheSize: cacheSize,
		Comparer:                    nil, //&cockroachkvs.Comparer,
		DisableWAL:                  disableWAL,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
	}

	opts.EnsureDefaults()

	p, err := bitalosdb.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}

	return testBitalosDB{
		d:       p,
		ballast: make([]byte, 1<<30),
	}
}

func (p testBitalosDB) Get(key []byte) ([]byte, func(), error) {
	return p.d.Get(key)
}

func (p testBitalosDB) Flush() error {
	return p.d.Flush()
}

func (p testBitalosDB) Close() error {
	return p.d.Close()
}

func (p testBitalosDB) NewIter(opts *bitalosdb.IterOptions) iterator {
	iter := p.d.NewIter(opts)
	return iter
}

func (p testBitalosDB) NewBatch() batch {
	return p.d.NewBatch()
}

func (p testBitalosDB) Scan(iter iterator, key []byte, count int64, reverse bool) error {
	var data bytealloc.A
	if reverse {
		for i, valid := 0, iter.SeekLT(key); valid; valid = iter.Prev() {
			data, _ = data.Copy(iter.Key())
			data, _ = data.Copy(iter.Value())
			i++
			if i >= int(count) {
				break
			}
		}
	} else {
		for i, valid := 0, iter.SeekGE(key); valid; valid = iter.Next() {
			data, _ = data.Copy(iter.Key())
			data, _ = data.Copy(iter.Value())
			i++
			if i >= int(count) {
				break
			}
		}
	}
	return nil
}

func (p testBitalosDB) MetricsInfo() bitalosdb.MetricsInfo {
	return p.d.MetricsInfo()
}
