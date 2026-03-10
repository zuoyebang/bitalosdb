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

package buffer

import (
	"io"
)

type Reader struct {
	buf  []byte
	size int
	rd   io.Reader
	r, w int
	err  error
}

func NewReaderSize(rd io.Reader, size int) *Reader {
	b, ok := rd.(*Reader)
	if ok && len(b.buf) >= size {
		return b
	}
	r := new(Reader)
	r.reset(make([]byte, size), rd)
	return r
}

func NewReader(rd io.Reader) *Reader {
	return NewReaderSize(rd, defaultBufSize)
}

func (b *Reader) reset(buf []byte, r io.Reader) {
	*b = Reader{
		buf:  buf,
		size: len(buf),
		rd:   r,
	}
}

func (b *Reader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

func (b *Reader) Buffered() int {
	return b.w - b.r
}

func (b *Reader) fill() error {
	if b.r == b.w {
		b.r = 0
		b.w = 0
		var n int
		n, b.err = b.rd.Read(b.buf)
		if n < 0 {
			panic("reader returned negative count from Read")
		} else if n == 0 {
			return b.readErr()
		}
		b.w += n
	}
	return nil
}

func (b *Reader) Read(p []byte) (n int, err error) {
	pLen := len(p)
	if pLen == 0 {
		return 0, b.readErr()
	}

	bufLeft := b.w - b.r
	if pLen <= bufLeft {
		n = copy(p, b.buf[b.r:b.w])
		b.r += n
		return n, nil
	}

	if bufLeft > 0 {
		n = copy(p, b.buf[b.r:b.w])
		b.r += n
	}

	for b.r == b.w {
		if err = b.fill(); err != nil {
			return 0, b.readErr()
		}

		cn := copy(p[n:], b.buf[b.r:b.w])
		b.r += cn
		n += cn
		if n == pLen {
			break
		}
	}

	return n, nil
}
