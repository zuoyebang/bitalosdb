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

package bitpage

func binaryFind(n int, cmp func(int) int) (i int, found bool) {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		res := cmp(h)
		if res == 0 {
			return h, true
		} else if res > 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < n && cmp(i) == 0
}

func binarySearch(n int, cmp func(int) int) int {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		res := cmp(h)
		if res == 0 {
			return h
		} else if res < 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func sizeVarint(x uint64) (n int) {
	if x == 0 {
		return 0
	}

	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
