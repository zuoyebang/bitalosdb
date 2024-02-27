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

import (
	"syscall"
	"unsafe"
)

const (
	msAsync = 1 << iota
	msSync
	msInvalidate
)

func msync(db *DB) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(db.data)), uintptr(db.datasz), msInvalidate)
	if errno != 0 {
		return errno
	}
	return nil
}

func fdatasync(db *DB) error {
	if db.data != nil {
		return msync(db)
	}
	return db.file.Sync()
}
