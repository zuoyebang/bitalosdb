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

package fastrand

import _ "unsafe" // required by go:linkname

// Uint32 returns a lock free uint32 value.
//
//go:linkname Uint32 runtime.fastrand
func Uint32() uint32

// Uint32n returns a lock free uint32 value in the interval [0, n).
//
//go:linkname Uint32n runtime.fastrandn
func Uint32n(n uint32) uint32
