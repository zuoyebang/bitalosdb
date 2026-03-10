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

package sklindex

import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/v2/internal/manual"
)

type Arena struct {
	sync.Mutex
	n    uint64
	bufn uint32
	bufs [][]byte
}

const (
	BufStepSize uint32 = 64 << 10
	BufStepMod  uint32 = BufStepSize - 1
)

func NewArena() *Arena {
	an := &Arena{
		n:    0,
		bufn: 1,
	}

	buf := manual.New(int(BufStepSize))
	an.bufs = append(an.bufs, buf)

	return an
}

func (a *Arena) size() uint32 {
	s := atomic.LoadUint64(&a.n)
	if s > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(s)
}

func (a *Arena) free() {
	if a.bufs != nil {
		for i := range a.bufs {
			manual.Free(a.bufs[i])
		}
		a.bufs = nil
	}
}

func (a *Arena) alloc(size, overflow uint32) (uint32, error) {
	a.Lock()
	defer a.Unlock()

	newBufn := a.bufn + size
	if newBufn+overflow > BufStepSize {
		buf := manual.New(int(BufStepSize))
		a.bufs = append(a.bufs, buf)
		a.bufn = size
		a.n = uint64(uint32(len(a.bufs)-1)*BufStepSize + size)
	} else {
		a.bufn = newBufn
		a.n += uint64(size)
	}

	offset := uint32(a.n) - size

	return offset, nil
}

func (a *Arena) getPointer(offset uint32) unsafe.Pointer {
	pos := offset / BufStepSize
	bufOffset := offset & BufStepMod
	return unsafe.Pointer(&a.bufs[pos][bufOffset])
}
