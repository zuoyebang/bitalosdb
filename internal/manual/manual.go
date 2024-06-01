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

package manual

// #include <stdlib.h>
import "C"
import "unsafe"

// The go:linkname directives provides backdoor access to private functions in
// the runtime. Below we're accessing the throw function.

//go:linkname throw runtime.throw
func throw(s string)

func New(n int) []byte {
	if n == 0 {
		return make([]byte, 0)
	}

	ptr := C.calloc(C.size_t(n), 1)
	if ptr == nil {
		throw("out of memory")
	}

	return (*[MaxArrayLen]byte)(unsafe.Pointer(ptr))[:n:n]
}

func Free(b []byte) {
	if cap(b) != 0 {
		if len(b) == 0 {
			b = b[:cap(b)]
		}
		ptr := unsafe.Pointer(&b[0])
		C.free(ptr)
	}
}
