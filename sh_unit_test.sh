cd bithash
go test -v -timeout 2000s
cd -

cd bitpage
go test -v -timeout 2000s
cd -

cd bitree
go test -v -timeout 2000s
cd -

cd bitforest
go test -v -timeout 2000s
cd -

# db_test.go
go test -v -run TestTry$
go test -v -run TestBasicReads$
go test -v -run TestBasicWrites$
go test -v -run TestRandomWrites$
go test -v -run TestLargeBatch$
go test -v -run TestGetNoCache$
go test -v -run TestLogData$
go test -v -run TestDeleteGet$
go test -v -run TestDeleteFlush$
go test -v -run TestUnremovableDelete$
go test -v -run TestMemTableReservation$
go test -v -run TestMemTableReservationLeak$
go test -v -run TestFlushEmpty$
go test -v -run TestDBConcurrentCommitCompactFlush$
go test -v -run TestDBApplyBatchNilDB$
go test -v -run TestDBApplyBatchMismatch$

# open_test.go
go test -v -run TestOpenCloseOpenClose$
go test -v -run TestOpenOptionsCheck$
go test -v -run TestOpenReadOnly$
go test -v -run TestOpenWALReplay$
go test -v -run TestOpenWALReplay2$
go test -v -run TestOpenWALReplayReadOnlySeqNums$
go test -v -run TestOpenWALReplayMemtableGrowth$

# bitalos_test.go
go test -v -run TestBitalosdbWrite
go test -v -run TestBitalosdbRead
go test -v -run TestBitalosdbWriteRead
go test -v -run TestBitalosdbOpen
go test -v -run TestBitalosdbDeleteGet
go test -v -run TestBitalosdbDeleteIterator
go test -v -run TestBitalosdbSeqNum
go test -v -run TestBitalosdbWriteKvSeparate
go test -v -run TestBitalosdbBatchSetMulti
go test -v -run TestBitalosdbWriteByKeyHashPos
go test -v -run TestBitalosdbFlushByDelPercent
go test -v -run TestBitalosdbMemGet
go test -v -run TestBitalosdbMemIterator

# bitalos_iterator_test.go
go test -v -run TestBitalosdbIter
go test -v -run TestBitalosdbIterSeek
go test -v -run TestBitalosdbIterPrefix
go test -v -run TestBitalosdbIterSeekGE
go test -v -run TestBitalosdbIterSeekLT

# bitalos_cache_test.go
go test -v -run TestBitalosdbCacheGetSet
go test -v -run TestBitalosdbCacheSet
go test -v -run TestBitalosdbCacheGetWhenFlushing

# bitalos_expire_test.go
go test -v -run TestBitalosdb_MemFlush_CheckExpire
go test -v -run TestBitalosdb_Bitable_CheckExpire

# bitalos_bitable_test.go
go test -v -run TestBitalosdbBitableDelete
go test -v -run TestBitalosdbBitableIter
go test -v -run TestBitalosdbBitableIterSeek
go test -v -run TestBitalosdbBitableSameDelete