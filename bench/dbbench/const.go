package main

import "errors"

const (
	DB_BITALOS           = 1
	bitalosdbDir         = "./data/bitalosdb"
	randValueCount       = 1024
	globalKeyIndex       = 0
	maxWriteBufferNumber = 8

	pprofPort = ":8709"
)

const (
	COMMAND_START  = 0
	COMMAND_FINISH = 1
)

var (
	ErrNotFound = errors.New("not found")
)
