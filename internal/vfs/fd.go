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

package vfs

// fdGetter is an interface for a file with an Fd() method. A lot of
// File related optimizations (eg. Prefetch(), WAL recycling) rely on the
// existence of the Fd method to return a raw file descriptor.
type fdGetter interface {
	Fd() uintptr
}

// fdFileWrapper is a File wrapper that also exposes an Fd() method. Used to
// wrap outer (wrapped) Files that could unintentionally hide the Fd() method
// exposed by the inner (unwrapped) File. It effectively lets the Fd() method
// bypass the outer File and go to the inner File.
type fdFileWrapper struct {
	File

	// All methods usually pass through to File above, except for Fd(), which
	// bypasses it and gets called directly on the inner file.
	inner fdGetter
}

func (f *fdFileWrapper) Fd() uintptr {
	return f.inner.Fd()
}

// WithFd takes an inner (unwrapped) and an outer (wrapped) vfs.File,
// and returns an fdFileWrapper if the inner file has an Fd() method. Use this
// method to fix the hiding of the Fd() method and the subsequent unintentional
// disabling of Fd-related file optimizations.
func WithFd(inner, outer File) File {
	if f, ok := inner.(fdGetter); ok {
		return &fdFileWrapper{
			File:  outer,
			inner: f,
		}
	}
	return outer
}
