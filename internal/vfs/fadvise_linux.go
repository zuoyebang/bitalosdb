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

//go:build linux

package vfs

import "golang.org/x/sys/unix"

// Calls Fadvise with FADV_RANDOM to disable readahead on a file descriptor.
func fadviseRandom(f uintptr) error {
	return unix.Fadvise(int(f), 0, 0, unix.FADV_RANDOM)
}

// Calls Fadvise with FADV_SEQUENTIAL to enable readahead on a file descriptor.
func fadviseSequential(f uintptr) error {
	return unix.Fadvise(int(f), 0, 0, unix.FADV_SEQUENTIAL)
}
