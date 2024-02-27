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

package bitable

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zuoyebang/bitalosdb/bithash"
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/hash"
	"github.com/zuoyebang/bitalosdb/internal/humanize"
	"github.com/zuoyebang/bitalostable"
	"github.com/zuoyebang/bitalostable/bloom"
)

type Bitable struct {
	db    *bitalostable.DB
	wo    *bitalostable.WriteOptions
	index int
	bhash *bithash.Bithash
	opts  *base.BitableOptions
}

func Open(bhash *bithash.Bithash, dirname string, opts *base.BitableOptions) (b *Bitable, err error) {
	b = &Bitable{
		index: opts.Index,
		wo:    bitalostable.NoSync,
		bhash: bhash,
		opts:  opts,
	}

	l0Size := opts.L0FileSize
	lopts := make([]bitalostable.LevelOptions, 7)
	for l := 0; l < 7; l++ {
		lopts[l] = bitalostable.LevelOptions{
			Compression:    bitalostable.SnappyCompression,
			BlockSize:      32 * 1024,
			TargetFileSize: l0Size,
			FilterPolicy:   bloom.FilterPolicy(10),
		}
		l0Size = l0Size * 2
	}

	btOpts := &bitalostable.Options{
		MemTableSize:                opts.MemTableSize,
		MemTableStopWritesThreshold: opts.MemTableStopWritesThreshold,
		L0CompactionFileThreshold:   opts.L0CompactionFileThreshold,
		L0CompactionThreshold:       opts.L0CompactionThreshold,
		L0StopWritesThreshold:       opts.L0StopWritesThreshold,
		LBaseMaxBytes:               opts.LBaseMaxBytes,
		MaxOpenFiles:                opts.MaxOpenFiles,
		Levels:                      lopts,
		Logger:                      opts.Logger,
		Verbose:                     true,
		LogTag:                      fmt.Sprintf("[bitable/%d]", opts.Index),
		KvCheckExpireFunc:           opts.CheckExpireCB,
	}

	btCache := bitalostable.NewCache(opts.CacheSize)
	defer btCache.Unref()
	btOpts.Cache = btCache

	b.db, err = bitalostable.Open(dirname, btOpts)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Bitable) Close() error {
	return b.db.Close()
}

func (b *Bitable) bithashGet(key []byte, fn uint32) ([]byte, func(), error) {
	khash := hash.Crc32(key)
	return b.bhash.Get(key, khash, bithash.FileNum(fn))
}

func (b *Bitable) Get(key []byte) ([]byte, io.Closer, error) {
	v, closer, err := b.db.Get(key)
	if err == bitalostable.ErrNotFound {
		return nil, nil, base.ErrNotFound
	}
	return v, closer, err
}

func (b *Bitable) Delete(key []byte) error {
	return b.db.Delete(key, b.wo)
}

func (b *Bitable) Checkpoint(destDir string) error {
	return b.db.Checkpoint(destDir)
}

func (b *Bitable) Compact(start, end []byte, parallelize bool) error {
	return b.db.Compact(start, end, parallelize)
}

func (b *Bitable) Metrics() string {
	return b.db.Metrics().String()
}

func (b *Bitable) NewIter(o *base.IterOptions) *BitableIterator {
	btIterOpts := &bitalostable.IterOptions{
		LowerBound: o.GetLowerBound(),
		UpperBound: o.GetUpperBound(),
	}

	iter := &BitableIterator{
		btable: b,
		iter:   b.db.NewIter(btIterOpts),
	}
	return iter
}

func (b *Bitable) OpenAutomaticCompactions() {
	b.db.SetOptsDisableAutomaticCompactions(false)
}

func (b *Bitable) CloseAutomaticCompactions() {
	b.db.SetOptsDisableAutomaticCompactions(true)
}

func hitRate(hits, misses int64) float64 {
	sum := hits + misses
	if sum == 0 {
		return 0
	}
	return 100 * float64(hits) / float64(sum)
}

func (b *Bitable) DebugInfo(dataType string) string {
	metrics := b.db.Metrics()
	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "%s-bitable%d: bcache(count=%s size=%s hitRate=%.1f%%) levels(files=%s %s %s %s %s %s %s)\n",
		dataType, b.index,
		humanize.SI.Int64(metrics.BlockCache.Count),
		humanize.IEC.Int64(metrics.BlockCache.Size),
		hitRate(metrics.BlockCache.Hits, metrics.BlockCache.Misses),
		humanize.SI.Int64(metrics.Levels[0].NumFiles),
		humanize.SI.Int64(metrics.Levels[1].NumFiles),
		humanize.SI.Int64(metrics.Levels[2].NumFiles),
		humanize.SI.Int64(metrics.Levels[3].NumFiles),
		humanize.SI.Int64(metrics.Levels[4].NumFiles),
		humanize.SI.Int64(metrics.Levels[5].NumFiles),
		humanize.SI.Int64(metrics.Levels[6].NumFiles),
	)

	return buf.String()
}
