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
	"bytes"
	"fmt"
	"hash/fnv"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/bitask"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/errors"
	"github.com/zuoyebang/bitalosdb/internal/options"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

const maxMmapStep = 1 << 30

const (
	versionFreelistUint uint32 = 2 + iota
	versionFreelistBitmap
)
const version uint32 = 3

const magic uint32 = 0xED0CDAED

const pgidNoFreelist pgid = 0xffffffffffffffff

const IgnoreNoSync = runtime.GOOS == "openbsd"

const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
)

var defaultPageSize = os.Getpagesize()

const flockRetryTimeout = 50 * time.Millisecond

type DB struct {
	StrictMode     bool
	NoSync         bool
	NoFreelistSync bool
	FreelistType   string
	NoGrowSync     bool
	MmapFlags      int
	MaxBatchSize   int
	MaxBatchDelay  time.Duration
	AllocSize      int
	Mlock          bool

	ops struct {
		writeAt           func(b []byte, off int64) (n int, err error)
		pushTaskCB        func(task *bitask.BitpageTaskData)
		checkPageSplitted func(pn uint32) bool
	}

	path            string
	openFile        func(string, int, os.FileMode) (*os.File, error)
	file            *os.File
	dataref         []byte
	data            *[maxMapSize]byte
	datasz          int
	filesz          int
	meta0           *meta
	meta1           *meta
	pageSize        int
	opened          bool
	rwtx            *Tx
	txs             []*Tx
	stats           Stats
	pagePool        sync.Pool
	batchMu         sync.Mutex
	batch           *batch
	rwlock          sync.Mutex
	metalock        sync.Mutex
	mmaplock        sync.RWMutex
	statlock        sync.RWMutex
	readOnly        bool
	freelist        *freelist
	freelistLoad    sync.Once
	upgradeFreelist bool
	version         uint32
	logger          base.Logger
	cmp             base.Compare
	index           int
}

func (db *DB) Path() string {
	return db.path
}

func (db *DB) GoString() string {
	return fmt.Sprintf("bdb.DB{path:%q}", db.path)
}

func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

func Open(path string, opts *options.BdbOptions) (*DB, error) {
	db := &DB{
		opened: true,
	}

	if opts == nil {
		opts = options.DefaultBdbOptions
	}

	db.NoSync = opts.NoSync
	db.NoGrowSync = opts.NoGrowSync
	db.MmapFlags = opts.MmapFlags
	db.NoFreelistSync = opts.NoFreelistSync
	db.FreelistType = opts.FreelistType
	db.Mlock = opts.Mlock
	db.logger = opts.Logger
	db.cmp = opts.Cmp
	db.index = opts.Index

	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = consts.BdbAllocSize

	flag := os.O_RDWR
	if opts.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	db.openFile = opts.OpenFile
	if db.openFile == nil {
		db.openFile = os.OpenFile
	}

	var err error
	if db.file, err = db.openFile(path, flag|os.O_CREATE, consts.FileMode); err != nil {
		_ = db.close()
		return nil, err
	}
	db.path = db.file.Name()

	if err = flock(db, !db.readOnly, opts.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	db.ops.writeAt = db.file.WriteAt
	db.ops.pushTaskCB = opts.BitpageTaskPushFunc
	db.ops.checkPageSplitted = opts.CheckPageSplitted

	if db.pageSize = opts.PageSize; db.pageSize == 0 {
		db.pageSize = defaultPageSize
	}

	if info, err := db.file.Stat(); err != nil {
		_ = db.close()
		return nil, err
	} else if info.Size() == 0 {
		if err := db.init(); err != nil {
			_ = db.close()
			return nil, err
		}
	} else {
		var buf [0x1000]byte
		if bw, err := db.file.ReadAt(buf[:], 0); err == nil && bw == len(buf) {
			if m := db.pageInBuffer(buf[:], 0).meta(); m.validate() == nil {
				db.pageSize = int(m.pageSize)
			}
		} else {
			_ = db.close()
			return nil, ErrInvalid
		}
	}

	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	if err := db.mmap(opts.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	if db.readOnly {
		return db, nil
	}

	db.version = version
	db.loadFreelist()
	if err := db.upgradeFreelistVersion(); err != nil {
		_ = db.close()
		return nil, err
	}

	if !db.NoFreelistSync && !db.hasSyncedFreelist() {
		tx, err := db.Begin(true)
		if tx != nil {
			err = tx.Commit()
		}
		if err != nil {
			_ = db.close()
			return nil, err
		}
	}

	return db, nil
}

func (db *DB) loadFreelist() {
	db.freelistLoad.Do(func() {
		db.freelist = newFreelist(db.FreelistType)
		if !db.hasSyncedFreelist() {
			db.freelist.readIDs(db.freepages())
		} else {
			dbMeta := db.meta()
			db.logger.Infof("[BDB %d] load freelist [version:%d] [txid:%d]", db.index, dbMeta.version, dbMeta.txid)
			if dbMeta.version == versionFreelistBitmap {
				db.freelist.readFromBitmap(db.page(dbMeta.freelist))
			} else {
				db.freelist.read(db.page(dbMeta.freelist))
				db.upgradeFreelist = true
			}
		}
		db.stats.FreePageN = db.freelist.free_count()
	})
}

func (db *DB) upgradeFreelistVersion() error {
	if !db.upgradeFreelist {
		return nil
	}

	tx, err := db.Begin(true)
	if tx != nil {
		err = tx.Commit()
	}
	if err != nil {
		_ = db.close()
		return err
	}

	db.upgradeFreelist = false
	db.logger.Infof("bdb upgradeFreelistVersion success [bdb:%s] [version:%d] [txid:%d]", db.path, db.meta().version, db.meta().txid)
	return nil
}

func (db *DB) hasSyncedFreelist() bool {
	return db.meta().freelist != pgidNoFreelist
}

func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return errors.Wrapf(err, "mmap stat err")
	} else if int(info.Size()) < db.pageSize*2 {
		return errors.New("file size too small")
	}

	fileSize := int(info.Size())
	var size = fileSize
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	if db.Mlock {
		if err := db.munlock(fileSize); err != nil {
			return err
		}
	}

	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	if err := db.munmap(); err != nil {
		return err
	}

	if err := mmap(db, size); err != nil {
		return err
	}

	if db.Mlock {
		if err := db.mlock(fileSize); err != nil {
			return err
		}
	}

	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return errors.Wrapf(err, "unmap err")
	}
	return nil
}

func (db *DB) mmapSize(size int) (int, error) {
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	if size > maxMapSize {
		return 0, errors.New("mmap too large")
	}

	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

func (db *DB) munlock(fileSize int) error {
	if err := munlock(db, fileSize); err != nil {
		return errors.Wrapf(err, "munlock err")
	}
	return nil
}

func (db *DB) mlock(fileSize int) error {
	if err := mlock(db, fileSize); err != nil {
		return errors.Wrapf(err, "mlock err")
	}
	return nil
}

func (db *DB) mrelock(fileSizeFrom, fileSizeTo int) error {
	if err := db.munlock(fileSizeFrom); err != nil {
		return err
	}
	if err := db.mlock(fileSizeTo); err != nil {
		return err
	}
	return nil
}

func (db *DB) init() error {
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf, pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	p := db.pageInBuffer(buf, pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	p = db.pageInBuffer(buf, pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}
	db.filesz = len(buf)

	return nil
}

func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	db.freelist = nil

	db.ops.writeAt = nil

	if err := db.munmap(); err != nil {
		return err
	}

	if db.file != nil {
		if !db.readOnly {
			if err := funlock(db); err != nil {
				return errors.Wrapf(err, "funlock err")
			}
		}

		if err := db.file.Close(); err != nil {
			return errors.Wrapf(err, "file close err")
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) beginTx() (*Tx, error) {
	db.metalock.Lock()
	db.mmaplock.RLock()

	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	t := &Tx{}
	t.init(db)

	db.txs = append(db.txs, t)
	n := len(db.txs)

	db.metalock.Unlock()

	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	db.rwlock.Lock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t
	db.freePages()
	return t, nil
}

func (db *DB) freePages() {
	var m, m1, m2, m3 pgids
	sort.Sort(txsById(db.txs))
	minid := txid(0xFFFFFFFFFFFFFFFF)
	if len(db.txs) > 0 {
		minid = db.txs[0].meta.txid
	}
	if minid > 0 {
		m1 = db.freelist.release(minid - 1)
	}
	for _, t := range db.txs {
		m2 = db.freelist.releaseRange(minid, t.meta.txid-1)
		minid = t.meta.txid + 1
	}
	m3 = db.freelist.releaseRange(minid, txid(0xFFFFFFFFFFFFFFFF))

	dstCache := make(map[pgid]struct{}, 0)
	mergePgids := func(pids pgids) {
		if len(pids) == 0 {
			return
		}
		for i := range pids {
			if _, ok := dstCache[pids[i]]; !ok {
				dstCache[pids[i]] = struct{}{}
				m = append(m, pids[i])
			}
		}
	}
	mergePgids(m1)
	mergePgids(m2)
	mergePgids(m3)
	db.freePagesForTask(m)
}

func (db *DB) freePagesForTask(pids pgids) {
	pidLen := len(pids)
	if pidLen == 0 {
		return
	}

	sort.Sort(pids)

	var skipPid pgid
	pns := make([]uint32, 0, 16)
	pnsDup := make(map[uint32]struct{})

	parseValue := func(v []byte) {
		if len(v) != 4 {
			return
		}

		pn := utils.BytesToUint32(v)
		if db.ops.checkPageSplitted(pn) {
			if _, exist := pnsDup[pn]; !exist {
				pns = append(pns, pn)
				pnsDup[pn] = struct{}{}
			}
		}
	}

	for index := 0; index < pidLen; index++ {
		pid := pids[index]
		if pid <= skipPid {
			continue
		}
		p := db.page(pid)
		if p.overflow > 0 {
			skipPid = pid + pgid(p.overflow)
		}
		if (p.flags & leafPageFlag) == 0 {
			continue
		}

		for i := 0; i < int(p.count); i++ {
			elem := p.leafPageElement(uint16(i))
			if elem.flags == bucketLeafFlag {
				if !bytes.Equal(consts.BdbBucketName, elem.key()) {
					continue
				}
				bktPage := openBucketPage(elem.value())
				if bktPage == nil || (bktPage.flags&leafPageFlag) == 0 {
					continue
				}

				for j := 0; j < int(bktPage.count); j++ {
					e := bktPage.leafPageElement(uint16(j))
					parseValue(e.value())
				}
			} else {
				parseValue(elem.value())
			}
		}
	}

	if len(pns) == 0 {
		return
	}

	db.ops.pushTaskCB(&bitask.BitpageTaskData{
		Index: db.index,
		Event: bitask.BitpageEventFreePage,
		Pns:   pns,
	})
}

type txsById []*Tx

func (t txsById) Len() int           { return len(t) }
func (t txsById) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t txsById) Less(i, j int) bool { return t[i].meta.txid < t[j].meta.txid }

func (db *DB) removeTx(tx *Tx) {
	db.mmaplock.RUnlock()
	db.metalock.Lock()

	for i, t := range db.txs {
		if t == tx {
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	db.metalock.Unlock()

	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()
}

func (db *DB) NewIter(rtx *ReadTx) *BdbIterator {
	return &BdbIterator{
		iter: rtx.Bucket().Cursor(),
		rTx:  rtx,
	}
}

func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true

	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true

	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Rollback()
}

func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

func (b *batch) trigger() {
	b.start.Do(b.run)
}

func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			c.err <- trySolo
			continue retry
		}

		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

func (db *DB) Sync() error { return fdatasync(db) }

func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

func (db *DB) DiskSize() int64 {
	if info, err := db.file.Stat(); err != nil {
		return 0
	} else {
		return info.Size()
	}
}

func (db *DB) TotalPage() int {
	return int(db.DiskSize() / int64(db.pageSize))
}

func (db *DB) FreePage() int {
	stat := db.Stats()
	return stat.FreePageN
}

func (db *DB) GetPageSize() int {
	return db.pageSize
}

func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

func (db *DB) meta() *meta {
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	panic("bdb.DB.meta(): invalid meta pages")
}

func (db *DB) allocate(txid txid, count int) (*page, bool, error) {
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	if p.id = db.freelist.allocate(txid, count); p.id != 0 {
		return p, true, nil
	}

	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, false, errors.Wrapf(err, "mmap allocate err")
		}
	}

	db.rwtx.meta.pgid += pgid(count)

	return p, false, nil
}

func (db *DB) grow(sz int) error {
	if sz <= db.filesz {
		return nil
	}

	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return errors.Wrapf(err, "file resize err")
			}
		}
		if err := db.file.Sync(); err != nil {
			return errors.Wrapf(err, "file sync err")
		}
		if db.Mlock {
			if err := db.mrelock(db.filesz, sz); err != nil {
				return errors.Wrapf(err, "mlock/munlock err")
			}
		}
	}

	db.filesz = sz
	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

func (db *DB) freepages() []pgid {
	tx, err := db.beginTx()
	defer func() {
		err = tx.Rollback()
		if err != nil {
			panic("freepages: failed to rollback tx")
		}
	}()
	if err != nil {
		panic("freepages: failed to open read only tx")
	}

	reachable := make(map[pgid]*page)
	nofreed := make(map[pgid]bool)
	ech := make(chan error)
	go func() {
		for e := range ech {
			panic(fmt.Sprintf("freepages: failed to get all reachable pages (%v)", e))
		}
	}()
	tx.checkBucket(&tx.root, reachable, nofreed, ech)
	close(ech)

	var fids []pgid
	for i := pgid(2); i < db.meta().pgid; i++ {
		if _, ok := reachable[i]; !ok {
			fids = append(fids, i)
		}
	}
	return fids
}

type Stats struct {
	FreePageN     int
	PendingPageN  int
	FreeAlloc     int
	FreelistInuse int
	TxN           int
	OpenTxN       int
	TxStats       TxStats
}

func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

type Info struct {
	Data     uintptr
	PageSize int
}

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     bucket
	freelist pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != versionFreelistUint && m.version != versionFreelistBitmap {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

func (m *meta) copy(dest *meta) {
	*dest = *m
}

func (m *meta) setVersion(ver uint32) {
	m.version = ver
}

func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid && m.freelist != pgidNoFreelist {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	m.checksum = m.sum64()

	m.copy(p.meta())
}

func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
