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

package bdb

import (
	"io"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/errors"
)

type txid uint64

type ReadTx struct {
	ref atomic.Int32
	tx  *Tx
	bkt *Bucket
	bdb *DB
}

func (rt *ReadTx) Init(tx *Tx, bkt *Bucket, bdb *DB) {
	rt.ref.Store(1)
	rt.tx = tx
	rt.bkt = bkt
	rt.bdb = bdb
}

func (rt *ReadTx) Bucket() *Bucket {
	return rt.bkt
}

func (rt *ReadTx) Ref() {
	rt.ref.Add(1)
}

func (rt *ReadTx) Unref(update bool) (err error) {
	if rt.ref.Add(-1) == 0 {
		err = rt.tx.Rollback()
		if update {
			err = rt.bdb.Update(func(tx *Tx) error { return nil })
		}
	}
	return err
}

type Tx struct {
	writable       bool
	managed        bool
	db             *DB
	meta           *meta
	root           Bucket
	pages          map[pgid]*page
	stats          TxStats
	commitHandlers []func()
	WriteFlag      int
}

func (tx *Tx) init(db *DB) {
	tx.db = db
	tx.pages = nil

	tx.meta = &meta{}
	db.meta().copy(tx.meta)

	tx.root = newBucket(tx)
	tx.root.bucket = &bucket{}
	*tx.root.bucket = tx.meta.root

	if tx.writable {
		tx.pages = make(map[pgid]*page, 1<<4)
		tx.meta.txid += txid(1)
	}
}

func (tx *Tx) ID() int {
	return int(tx.meta.txid)
}

func (tx *Tx) DB() *DB {
	return tx.db
}

func (tx *Tx) Size() int64 {
	return int64(tx.meta.pgid) * int64(tx.db.pageSize)
}

func (tx *Tx) Writable() bool {
	return tx.writable
}

func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

func (tx *Tx) Stats() TxStats {
	return tx.stats
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	return tx.root.Bucket(name)
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	return tx.root.CreateBucket(name)
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return tx.root.CreateBucketIfNotExists(name)
}

func (tx *Tx) DeleteBucket(name []byte) error {
	return tx.root.DeleteBucket(name)
}

func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.root.ForEach(func(k, v []byte) error {
		return fn(k, tx.root.Bucket(k))
	})
}

func (tx *Tx) OnCommit(fn func()) {
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

func (tx *Tx) Commit() error {
	_assert(!tx.managed, "managed tx commit not allowed")
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}

	var startTime = time.Now()
	tx.root.rebalance()
	if tx.stats.Rebalance > 0 {
		tx.stats.RebalanceTime += time.Since(startTime)
	}

	startTime = time.Now()
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)

	tx.meta.root.root = tx.root.root

	if tx.meta.freelist != pgidNoFreelist {
		tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist))
	}

	if !tx.db.NoFreelistSync {
		if err := tx.commitFreelist(); err != nil {
			return err
		}
	} else {
		tx.meta.freelist = pgidNoFreelist
	}

	startTime = time.Now()
	if err := tx.write(); err != nil {
		tx.rollback()
		return err
	}

	if tx.db.StrictMode {
		ch := tx.Check()
		var errs []string
		for {
			err, ok := <-ch
			if !ok {
				break
			}
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			panic("check fail: " + strings.Join(errs, "\n"))
		}
	}

	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime)

	tx.close()

	for _, fn := range tx.commitHandlers {
		fn()
	}

	return nil
}

func (tx *Tx) commitFreelist() error {
	opgid := tx.meta.pgid
	rb, freeCount := tx.db.freelist.convertBitmap()
	rbLen := rb.GetSerializedSizeInBytes()
	count := ((int(rbLen) + freelistBitmapHeaderSize) / tx.db.pageSize) + 2
	p, isFreePage, err := tx.allocate(count)
	if err != nil {
		tx.rollback()
		return err
	}

	if isFreePage {
		for pid := p.id; pid < p.id+pgid(count); pid++ {
			if rb.Contains(uint64(pid)) {
				rb.Remove(uint64(pid))
				freeCount--
			}
		}
		rbLen = rb.GetSerializedSizeInBytes()
	}

	if freeCount < 0 {
		freeCount = 0
	}

	if err := tx.db.freelist.writeBitmap(p, rb, rbLen, freeCount); err != nil {
		tx.rollback()
		return err
	}

	tx.meta.freelist = p.id
	if tx.meta.pgid > opgid {
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	return nil
}

func (tx *Tx) Rollback() error {
	_assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.nonPhysicalRollback()
	return nil
}

func (tx *Tx) nonPhysicalRollback() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.freelist.rollback(tx.meta.txid)
	}
	tx.close()
}

func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.freelist.rollback(tx.meta.txid)
		if !tx.db.hasSyncedFreelist() {
			tx.db.freelist.noSyncReload(tx.db.freepages())
		} else {
			tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist), tx.db.meta().version)
		}
	}
	tx.close()
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		var freelistFreeN = tx.db.freelist.free_count()
		var freelistPendingN = tx.db.freelist.pending_count()
		var freelistAlloc = tx.db.freelist.size()

		tx.db.rwtx = nil
		tx.db.rwlock.Unlock()

		tx.db.statlock.Lock()
		tx.db.stats.FreePageN = freelistFreeN
		tx.db.stats.PendingPageN = freelistPendingN
		tx.db.stats.FreeAlloc = (freelistFreeN + freelistPendingN) * tx.db.pageSize
		tx.db.stats.FreelistInuse = freelistAlloc
		tx.db.stats.TxStats.add(&tx.stats)
		tx.db.statlock.Unlock()
	} else {
		tx.db.removeTx(tx)
	}

	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}

func (tx *Tx) Copy(w io.Writer) error {
	_, err := tx.WriteTo(w)
	return err
}

func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	f, err := tx.db.openFile(tx.db.path, os.O_RDONLY|tx.WriteFlag, 0)
	if err != nil {
		return 0, err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()

	buf := make([]byte, tx.db.pageSize)
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = metaPageFlag
	*page.meta() = *tx.meta

	page.id = 0
	page.meta().checksum = page.meta().sum64()
	nn, err := w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, errors.Wrapf(err, "meta 0 copy err")
	}

	page.id = 1
	page.meta().txid -= 1
	page.meta().checksum = page.meta().sum64()
	nn, err = w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, errors.Wrapf(err, "meta 1 copy err")
	}

	if _, err := f.Seek(int64(tx.db.pageSize*2), io.SeekStart); err != nil {
		return n, errors.Wrapf(err, "seek err")
	}

	wn, err := io.CopyN(w, f, tx.Size()-int64(tx.db.pageSize*2))
	n += wn
	if err != nil {
		return n, err
	}

	return n, nil
}

func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	f, err := tx.db.openFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	_, err = tx.WriteTo(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (tx *Tx) Check() <-chan error {
	ch := make(chan error)
	go tx.check(ch)
	return ch
}

func (tx *Tx) check(ch chan error) {
	tx.db.loadFreelist()

	freed := make(map[pgid]bool, 1<<4)
	all := make([]pgid, tx.db.freelist.count())
	tx.db.freelist.copyall(all)
	for _, id := range all {
		if freed[id] {
			ch <- errors.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	reachable := make(map[pgid]*page, 1<<4)
	reachable[0] = tx.page(0)
	reachable[1] = tx.page(1)
	if tx.meta.freelist != pgidNoFreelist {
		for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
			reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
		}
	}

	tx.checkBucket(&tx.root, reachable, freed, ch)

	for i := pgid(0); i < tx.meta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {
			ch <- errors.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	close(ch)
}

func (tx *Tx) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	if b.root == 0 {
		return
	}

	b.tx.forEachPage(b.root, 0, func(p *page, _ int) {
		if p.id > tx.meta.pgid {
			ch <- errors.Errorf("page %d: out of bounds: %d", int(p.id), int(b.tx.meta.pgid))
		}

		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- errors.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		if freed[p.id] {
			ch <- errors.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- errors.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

func (tx *Tx) allocate(count int) (*page, bool, error) {
	p, isFreePage, err := tx.db.allocate(tx.meta.txid, count)
	if err != nil {
		return nil, isFreePage, err
	}

	tx.pages[p.id] = p

	tx.stats.PageCount += count
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, isFreePage, nil
}

func (tx *Tx) write() error {
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}

	tx.pages = make(map[pgid]*page, 1<<4)
	sort.Sort(pages)

	for _, p := range pages {
		rem := (uint64(p.overflow) + 1) * uint64(tx.db.pageSize)
		offset := int64(p.id) * int64(tx.db.pageSize)
		var written uintptr

		for {
			sz := rem
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}
			buf := unsafeByteSlice(unsafe.Pointer(p), written, 0, int(sz))

			if _, err := tx.db.ops.writeAt(buf, offset); err != nil {
				return err
			}

			tx.stats.Write++

			rem -= sz
			if rem == 0 {
				break
			}

			offset += int64(sz)
			written += uintptr(sz)
		}
	}

	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	for _, p := range pages {
		if int(p.overflow) != 0 {
			continue
		}

		buf := unsafeByteSlice(unsafe.Pointer(p), 0, 0, tx.db.pageSize)

		for i := range buf {
			buf[i] = 0
		}
		tx.db.pagePool.Put(buf)
	}

	return nil
}

func (tx *Tx) writeMeta() (err error) {
	buf := make([]byte, tx.db.pageSize)
	p := tx.db.pageInBuffer(buf, 0)

	metaOldVersion := tx.meta.version
	if tx.db.version == versionFreelistBitmap && metaOldVersion != versionFreelistBitmap {
		tx.meta.version = versionFreelistBitmap
	}

	tx.meta.write(p)

	defer func() {
		if err != nil && tx.meta.version != metaOldVersion {
			tx.meta.version = metaOldVersion
		}
	}()

	if _, err = tx.db.ops.writeAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {

		return err
	}
	if !tx.db.NoSync || IgnoreNoSync {
		if err = fdatasync(tx.db); err != nil {
			tx.meta.version = metaOldVersion
			return err
		}
	}

	tx.stats.Write++

	return nil
}

func (tx *Tx) page(id pgid) *page {
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}

	return tx.db.page(id)
}

func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := tx.page(pgid)

	fn(p, depth)

	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			tx.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

func (tx *Tx) Page(id int) (*PageInfo, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if pgid(id) >= tx.meta.pgid {
		return nil, nil
	}

	p := tx.db.page(pgid(id))
	info := &PageInfo{
		ID:            id,
		Count:         int(p.count),
		OverflowCount: int(p.overflow),
	}

	if tx.db.freelist.freed(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

type TxStats struct {
	PageCount     int
	PageAlloc     int
	CursorCount   int
	NodeCount     int
	NodeDeref     int
	Rebalance     int
	RebalanceTime time.Duration
	Split         int
	Spill         int
	SpillTime     time.Duration
	Write         int
	WriteTime     time.Duration
}

func (s *TxStats) add(other *TxStats) {
	s.PageCount += other.PageCount
	s.PageAlloc += other.PageAlloc
	s.CursorCount += other.CursorCount
	s.NodeCount += other.NodeCount
	s.NodeDeref += other.NodeDeref
	s.Rebalance += other.Rebalance
	s.RebalanceTime += other.RebalanceTime
	s.Split += other.Split
	s.Spill += other.Spill
	s.SpillTime += other.SpillTime
	s.Write += other.Write
	s.WriteTime += other.WriteTime
}

func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.PageCount - other.PageCount
	diff.PageAlloc = s.PageAlloc - other.PageAlloc
	diff.CursorCount = s.CursorCount - other.CursorCount
	diff.NodeCount = s.NodeCount - other.NodeCount
	diff.NodeDeref = s.NodeDeref - other.NodeDeref
	diff.Rebalance = s.Rebalance - other.Rebalance
	diff.RebalanceTime = s.RebalanceTime - other.RebalanceTime
	diff.Split = s.Split - other.Split
	diff.Spill = s.Spill - other.Spill
	diff.SpillTime = s.SpillTime - other.SpillTime
	diff.Write = s.Write - other.Write
	diff.WriteTime = s.WriteTime - other.WriteTime
	return diff
}
