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

package bdb

import "errors"

var (
	ErrDatabaseNotOpen = errors.New("database not open")
	ErrDatabaseOpen    = errors.New("database already open")
	ErrInvalid         = errors.New("invalid database")
	ErrVersionMismatch = errors.New("version mismatch")
	ErrChecksum        = errors.New("checksum error")
	ErrTimeout         = errors.New("timeout")
)

var (
	ErrTxNotWritable    = errors.New("tx not writable")
	ErrTxClosed         = errors.New("tx closed")
	ErrDatabaseReadOnly = errors.New("database is in read-only mode")
)

var (
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBucketExists       = errors.New("bucket already exists")
	ErrBucketNameRequired = errors.New("bucket name required")
	ErrKeyRequired        = errors.New("key required")
	ErrKeyTooLarge        = errors.New("key too large")
	ErrValueTooLarge      = errors.New("value too large")
	ErrIncompatibleValue  = errors.New("incompatible value")
)
