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

//go:build !windows

package bdb

import "golang.org/x/sys/unix"

func mlock(db *DB, fileSize int) error {
	sizeToLock := fileSize
	if sizeToLock > db.datasz {
		sizeToLock = db.datasz
	}
	if err := unix.Mlock(db.dataref[:sizeToLock]); err != nil {
		return err
	}
	return nil
}

func munlock(db *DB, fileSize int) error {
	if db.dataref == nil {
		return nil
	}

	sizeToUnlock := fileSize
	if sizeToUnlock > db.datasz {
		sizeToUnlock = db.datasz
	}

	if err := unix.Munlock(db.dataref[:sizeToUnlock]); err != nil {
		return err
	}
	return nil
}
