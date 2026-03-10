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

package base

import "errors"

var (
	ErrNotFound                 = errors.New("bitalosdb: not found")
	ErrFileFull                 = errors.New("bitalosdb: panic vt file full")
	ErrWrongType                = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrTableOpenType            = errors.New("tbl open type not support")
	ErrTableFull                = errors.New("allocation failed because table is full")
	ErrIndexOutOfRange          = errors.New("ERR index out of range")
	ErrNoSuchKey                = errors.New("ERR no such key")
	ErrRecordExists             = errors.New("record with this key already exists")
	ErrVTBusy                   = errors.New("bitalosdb: vector table busy")
	ErrVTFull                   = errors.New("bitalosdb: panic rehash failed vector table full")
	ErrMemTableExceedDelPercent = errors.New("memtable exceed delpercent")
	ErrPageNotFound             = errors.New("page not exist")
	ErrPageSplitted             = errors.New("page splitted")
	ErrPageNotFreed             = errors.New("page not freed")
	ErrPageFlushState           = errors.New("page flush state err")
	ErrPageFlushEmpty           = errors.New("page flush empty")
)

func DisableErrNotFound(err error) error {
	if err == ErrNotFound {
		return nil
	} else {
		return err
	}
}
