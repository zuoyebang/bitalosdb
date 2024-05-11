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

//go:build !linux

package vfs

// Prefetch signals the OS (on supported platforms) to fetch the next size
// bytes in file (as returned by os.File.Fd()) after offset into cache. Any
// subsequent reads in that range will not issue disk IO.
func Prefetch(file uintptr, offset uint64, size uint64) error {
	// No-op.
	return nil
}
