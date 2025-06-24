package main

import (
	"os"

	"github.com/zuoyebang/bitalosdb"
)

type BitalosDB struct {
	db   *bitalosdb.DB
	ro   *bitalosdb.IterOptions
	wo   *bitalosdb.WriteOptions
	opts *bitalosdb.Options
}

func openBitalosDB(dir string, walDir string) (*BitalosDB, error) {
	compactInfo := bitalosdb.CompactEnv{
		DeletePercent: 0.5,
		Interval:      300,
		StartHour:     0,
		EndHour:       23,
	}
	opts := &bitalosdb.Options{
		MemTableSize:                512 * 1024 * 1024,
		MemTableStopWritesThreshold: maxWriteBufferNumber,
		AutoCompact:                 true,
		DisableWAL:                  true,
		DataType:                    "test",
		Verbose:                     true,
		UseBithash:                  false,
		UseBitable:                  false,
		UseMapIndex:                 true,
		BytesPerSync:                1 << 20,
		CompactInfo:                 compactInfo,
		IOWriteLoadThresholdFunc:    func() bool { return true },
		DeleteFileInternal:          1,
	}
	wo := &bitalosdb.WriteOptions{Sync: false}
	if len(walDir) > 0 {
		_, err := os.Stat(walDir)
		if nil != err && !os.IsExist(err) {
			err = os.MkdirAll(walDir, 0775)
			if nil != err {
				return nil, err
			}
			opts.WALDir = walDir
		}
	}
	_, err := os.Stat(dir)
	if nil != err && !os.IsExist(err) {
		err = os.MkdirAll(dir, 0775)
		if nil != err {
			return nil, err
		}
		opts.WALDir = walDir
	}
	pdb, err := bitalosdb.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	ro := &bitalosdb.IterOptions{}
	return &BitalosDB{
		db:   pdb,
		ro:   ro,
		wo:   wo,
		opts: opts,
	}, nil
}

func (db *BitalosDB) Checkpoint(dir string) {
	db.db.Checkpoint(dir)
}

func (db *BitalosDB) Compact() {
	db.db.CheckAndCompact(0)
}
