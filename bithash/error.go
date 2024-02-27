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

package bithash

import "errors"

var (
	ErrBhNewReaderNoFile     = errors.New("bithash new reader nil file")
	ErrBhNewReaderFail       = errors.New("bithash: new reader fail")
	ErrBhReaderClosed        = errors.New("bithash: reader is closed")
	ErrBhWriterClosed        = errors.New("bithash: writer is closed")
	ErrBhIllegalBlockLength  = errors.New("bithash: illegal block handle length")
	ErrBhCreateTableFile     = errors.New("bithash: create table file fail")
	ErrBhOpenTableFile       = errors.New("bithash: open table file fail")
	ErrBhFileNumError        = errors.New("bithash: fileNum error")
	ErrBhFileNotImmutable    = errors.New("bithash: table is not immutable")
	ErrBhFileNumZero         = errors.New("bithash: fileNum zero")
	ErrBhReadRecordNil       = errors.New("bithash: read record nil")
	ErrBhReadAtIncomplete    = errors.New("bithash: readAt incomplete")
	ErrBhNotFound            = errors.New("bithash: not found")
	ErrBhKeyTooLarge         = errors.New("bithash: key too large")
	ErrBhValueTooLarge       = errors.New("bithash: value too large")
	ErrBhHashIndexWriteFail  = errors.New("bithash: hash_index write fail")
	ErrBhHashIndexReadFail   = errors.New("bithash: hash_index read fail")
	ErrBhFileNumMapCheckFail = errors.New("bithash: check fileNumMap file footer fail")
	ErrBhInvalidTableSize    = errors.New("bithash: invalid table file size is too small")
	ErrBhInvalidTableMeta    = errors.New("bithash:invalid table bad metaBH")
)
