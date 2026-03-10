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

package bitalosdb

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sort"

	"github.com/zuoyebang/bitalosdb/v2/bitree"
	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/options"
	"github.com/zuoyebang/bitalosdb/v2/internal/os2"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/zuoyebang/bitalosdb/v2/internal/vectortable"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

type Bituple struct {
	db          *DB
	index       int
	opts        *options.BitupleOptions
	dbPath      string
	dirname     string
	tupleCount  uint16
	tupleShards [metaMaxTupleShards]*Tuple
	tupleList   []*Tuple
	kkv         *bitree.Bitree
}

func (b *Bituple) newTuples() error {
	files, err := b.opts.FS.List(b.dirname)
	if err != nil {
		return err
	}

	tvfs := make(map[uint32][]string, b.tupleCount)
	if len(files) > 0 {
		sort.Strings(files)
		for i := range files {
			ft, tn, ok := parseFilename(files[i])
			if ok && ft == fileTypeVectorTable {
				tvfs[tn] = append(tvfs[tn], files[i])
			}
		}
	}

	tupleMap := make(map[uint32]*Tuple, b.tupleCount)
	for shard, tn := range b.db.meta.tupleList {
		tp, exist := tupleMap[tn]
		if !exist {
			if _, ok := tvfs[tn]; !ok {
				tp, err = newTuple(b, tn, nil)
			} else {
				tp, err = newTuple(b, tn, tvfs[tn])
			}
			if err != nil {
				return err
			}

			tupleMap[tn] = tp
			b.tupleList = append(b.tupleList, tp)
		}

		b.tupleShards[shard] = tupleMap[tn]
	}

	return nil
}

func (b *Bituple) isEliminate() bool {
	return b.index == consts.EliminateSlotId
}

func (b *Bituple) RunVtGC(force bool) {
	for i := range b.tupleList {
		if err := b.tupleList[i].runVtGC(force); err != nil {
			b.opts.Logger.Errorf("[BITUPLE %d] tuple(%d) runVtGC error %s", b.index, i, err)
		}
	}
}

func (b *Bituple) RunVtRehash(force bool) {
	for i := range b.tupleList {
		if err := b.tupleList[i].runVtRehash(force); err != nil {
			b.opts.Logger.Errorf("[BITUPLE %d] tuple(%d) runVtRehash error %s", b.index, i, err)
		}
	}
}

func (b *Bituple) Close() (err error) {
	for i := range b.tupleList {
		if err = b.tupleList[i].close(false); err != nil {
			b.opts.Logger.Errorf("[BITUPLE %d] tuple(%d) close error %s", b.index, i, err)
		}
	}

	if err = b.kkv.Close(); err != nil {
		b.opts.Logger.Errorf("[BITUPLE %d] kkv close error %s", b.index, err)
	}

	return err
}

func (b *Bituple) GetDebugInfo() string {
	return b.kkv.BitreeDebugInfo()
}

func (b *Bituple) GetDiskInfo() (bitupleDisk, bitpageDisk, bithashDisk uint64) {
	bitupleDisk = uint64(os2.GetDirSize(b.dirname))
	bitpageDisk = b.kkv.BitreeDiskInfo()
	bithashDisk = b.kkv.BithashDiskInfo()
	return
}

func (b *Bituple) GetDirDiskInfo() string {
	buf := new(bytes.Buffer)

	b.kkv.Index()
	b.kkv.BitreeDirDiskInfo(buf)

	fmt.Fprintf(buf, "dir-bituple%d: dirSize=%s\n", b.index, utils.FmtSize(os2.GetDirSize(b.dirname)))

	return buf.String()
}

func (b *Bituple) GetInfo() string {
	buf := new(bytes.Buffer)

	for i := range b.tupleList {
		fmt.Fprintf(buf, "bituple%d-%d-header: %s\n",
			b.index, b.tupleList[i].tn, b.tupleList[i].vt.GetHeaderInfo())
	}

	return buf.String()
}

func (b *Bituple) RemoveDir() error {
	if err := b.kkv.RemoveDir(); err != nil {
		return err
	}

	if err := os2.RemoveDir(b.dirname); err != nil {
		return err
	}

	return nil
}

func (b *Bituple) deleteObsoleteFiles(fns []string) {
	for _, fn := range fns {
		if os2.IsExist(fn) {
			b.opts.DeleteFilePacer.AddFile(fn)
		}
	}
}

func (b *Bituple) checkpoint(fs vfs.FS, dstDir string) error {
	if err := b.kkv.Checkpoint(fs, dstDir); err != nil {
		return err
	}

	if b.isEliminate() {
		return nil
	}

	dstBtDir := path.Join(dstDir, fs.PathBase(b.dirname))
	if err := os.Mkdir(dstBtDir, 0755); err != nil {
		return err
	}

	dir, err := fs.OpenDir(dstBtDir)
	if err != nil {
		return err
	}

	for i := range b.tupleList {
		if err = b.tupleList[i].checkpoint(fs, dstBtDir); err != nil {
			return err
		}
	}

	if err = dir.Sync(); err != nil {
		return err
	}
	return dir.Close()
}

func (b *Bituple) getHashTuple(h uint64) *Tuple {
	return b.tupleShards[h&metaMaxTupleShardsMask]
}

func (b *Bituple) getMeta(hi, lo uint64) (
	dataType uint8, timestamp, version uint64,
	lindex, rindex uint64, size uint32, err error,
) {
	tuple := b.getHashTuple(lo)
	_, dataType, timestamp, _, version, size, lindex, rindex, err = tuple.vt.GetMeta(hi, lo)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}
	return dataType, timestamp, version, lindex, rindex, size, nil
}

func (b *Bituple) get(hi, lo uint64) (value []byte, dataType uint8, timestamp uint64, closer func(), err error) {
	tuple := b.getHashTuple(lo)
	value, _, dataType, timestamp, _, closer, err = tuple.vt.GetValue(hi, lo)
	return value, dataType, timestamp, closer, err
}

func (b *Bituple) exist(hi, lo uint64) (bool, error) {
	tuple := b.getHashTuple(lo)
	return tuple.exist(hi, lo)
}

func (b *Bituple) getTimestamp(hi, lo uint64) (dataType uint8, timestamp uint64, err error) {
	tuple := b.getHashTuple(lo)
	return tuple.getTimestamp(hi, lo)
}

type Tuple struct {
	index int
	tn    uint32
	vt    *vectortable.VectorTable
}

func newTuple(b *Bituple, tn uint32, files []string) (*Tuple, error) {
	opts := &options.VectorTableOptions{
		Options:         b.opts.Options,
		Index:           b.index,
		Dirname:         b.dirname,
		Filename:        makeFilename(fileTypeVectorTable, tn),
		HashSize:        b.opts.VectorTableHashSize,
		MmapBlockSize:   b.opts.TableMmapBlockSize,
		StoreKey:        !b.opts.DisableStoreKey,
		GCThreshold:     b.opts.VectorTableGCThreshold,
		OpenFiles:       files,
		ReleaseFunc:     b.deleteObsoleteFiles,
		CheckExpireFunc: b.opts.CheckExpireFunc,
		WithSlot:        false,
		WriteBuffer:     true,
	}
	vt, err := vectortable.NewVectorTable(opts)
	if err != nil {
		return nil, err
	}

	t := &Tuple{
		index: b.index,
		tn:    tn,
		vt:    vt,
	}
	return t, nil
}

func (t *Tuple) close(isFree bool) (err error) {
	return t.vt.Close(isFree)
}

func (t *Tuple) checkpoint(fs vfs.FS, dstDir string) error {
	return t.vt.Checkpoint(fs, dstDir)
}

func (t *Tuple) runVtGC(force bool) error {
	return t.vt.GC(force)
}

func (t *Tuple) runVtRehash(force bool) error {
	return t.vt.Rehash(force)
}

func (t *Tuple) getMeta(hi, lo uint64) (uint8, uint64, uint64, uint64, uint64, uint32, error) {
	_, dataType, timestamp, _, version, size, lindex, rindex, err := t.vt.GetMeta(hi, lo)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, base.ErrNotFound
	}
	return dataType, timestamp, version, lindex, rindex, size, nil
}

func (t *Tuple) exist(hi, lo uint64) (bool, error) {
	_, exist := t.vt.Has(hi, lo)
	if !exist {
		return false, base.ErrNotFound
	}
	return true, nil
}

func (t *Tuple) getTimestamp(hi, lo uint64) (uint8, uint64, error) {
	_, timestamp, dataType, err := t.vt.GetTimestamp(hi, lo)
	if err != nil {
		return 0, 0, base.ErrNotFound
	}
	return dataType, timestamp, nil
}
