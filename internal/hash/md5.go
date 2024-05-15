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

package hash

import (
	"encoding/binary"
	"errors"
	"hash"
)

// Size The size of an MD5 checksum in bytes.
const Size = 16

// BlockSize The blocksize of MD5 in bytes.
const BlockSize = 64

const (
	init0 = 0x67452301
	init1 = 0xEFCDAB89
	init2 = 0x98BADCFE
	init3 = 0x10325476
)

// digest represents the partial evaluation of a checksum.
type digest struct {
	s   [4]uint32
	x   [BlockSize]byte
	nx  int
	len uint64
}

func (d *digest) Reset() {
	d.s[0] = init0
	d.s[1] = init1
	d.s[2] = init2
	d.s[3] = init3
	d.nx = 0
	d.len = 0
}

const (
	magic         = "md5\x01"
	marshaledSize = len(magic) + 4*4 + BlockSize + 8
)

func (d *digest) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, marshaledSize)
	b = append(b, magic...)
	b = binary.BigEndian.AppendUint32(b, d.s[0])
	b = binary.BigEndian.AppendUint32(b, d.s[1])
	b = binary.BigEndian.AppendUint32(b, d.s[2])
	b = binary.BigEndian.AppendUint32(b, d.s[3])
	b = append(b, d.x[:d.nx]...)
	b = b[:len(b)+len(d.x)-d.nx] // already zero
	b = binary.BigEndian.AppendUint64(b, d.len)
	return b, nil
}

func (d *digest) UnmarshalBinary(b []byte) error {
	if len(b) < len(magic) || string(b[:len(magic)]) != magic {
		return errors.New("crypto/md5: invalid hash state identifier")
	}
	if len(b) != marshaledSize {
		return errors.New("crypto/md5: invalid hash state size")
	}
	b = b[len(magic):]
	b, d.s[0] = consumeUint32(b)
	b, d.s[1] = consumeUint32(b)
	b, d.s[2] = consumeUint32(b)
	b, d.s[3] = consumeUint32(b)
	b = b[copy(d.x[:], b):]
	b, d.len = consumeUint64(b)
	d.nx = int(d.len % BlockSize)
	return nil
}

func consumeUint64(b []byte) ([]byte, uint64) {
	return b[8:], binary.BigEndian.Uint64(b[0:8])
}

func consumeUint32(b []byte) ([]byte, uint32) {
	return b[4:], binary.BigEndian.Uint32(b[0:4])
}

// New returns a new hash.Hash computing the MD5 checksum. The Hash also
// implements [encoding.BinaryMarshaler] and [encoding.BinaryUnmarshaler] to
// marshal and unmarshal the internal state of the hash.
func New() hash.Hash {
	d := new(digest)
	d.Reset()
	return d
}

func (d *digest) Size() int { return Size }

func (d *digest) BlockSize() int { return BlockSize }

func (d *digest) Write(p []byte) (nn int, err error) {
	// Note that we currently call block or blockGeneric
	// directly (guarded using haveAsm) because this allows
	// escape analysis to see that p and d don't escape.
	nn = len(p)
	d.len += uint64(nn)
	if d.nx > 0 {
		n := copy(d.x[d.nx:], p)
		d.nx += n
		if d.nx == BlockSize {
			if haveAsm {
				block(d, d.x[:])
			} else {
				blockGeneric(d, d.x[:])
			}
			d.nx = 0
		}
		p = p[n:]
	}
	if len(p) >= BlockSize {
		n := len(p) &^ (BlockSize - 1)
		if haveAsm {
			block(d, p[:n])
		} else {
			blockGeneric(d, p[:n])
		}
		p = p[n:]
	}
	if len(p) > 0 {
		d.nx = copy(d.x[:], p)
	}
	return
}

func (d *digest) Sum(in []byte) []byte {
	// Make a copy of d so that caller can keep writing and summing.
	d0 := *d
	hash := d0.checkSum()
	return append(in, hash[:]...)
}

func (d *digest) checkSum() [Size]byte {
	// Append 0x80 to the end of the message and then append zeros
	// until the length is a multiple of 56 bytes. Finally append
	// 8 bytes representing the message length in bits.
	//
	// 1 byte end marker :: 0-63 padding bytes :: 8 byte length
	tmp := [1 + 63 + 8]byte{0x80}
	pad := (55 - d.len) % 64                             // calculate number of padding bytes
	binary.LittleEndian.PutUint64(tmp[1+pad:], d.len<<3) // append length in bits
	d.Write(tmp[:1+pad+8])

	// The previous write ensures that a whole number of
	// blocks (i.e. a multiple of 64 bytes) have been hashed.
	if d.nx != 0 {
		panic("d.nx != 0")
	}

	var digest [Size]byte
	binary.LittleEndian.PutUint32(digest[0:], d.s[0])
	binary.LittleEndian.PutUint32(digest[4:], d.s[1])
	binary.LittleEndian.PutUint32(digest[8:], d.s[2])
	binary.LittleEndian.PutUint32(digest[12:], d.s[3])
	return digest
}

// Sum returns the MD5 checksum of the data.
func Sum(data []byte) [Size]byte {
	var d digest
	d.Reset()
	d.Write(data)
	return d.checkSum()
}

func MD5(data []byte) (h []byte) {
	hasher := New()
	hasher.Write(data)
	h = hasher.Sum(nil)
	return h
}

func MD5Hash(data []byte) (h []byte, hi, lo uint64) {
	hasher := New()
	hasher.Write(data)
	h = hasher.Sum(nil)
	hi = binary.BigEndian.Uint64(h[:8])
	lo = binary.BigEndian.Uint64(h[8:16])
	return
}

//go:inline
func MD5Sum(p []byte) (h []byte, hi, lo uint64) {
	var d = digest{s: [4]uint32{init0, init1, init2, init3}}
	// Write
	d.len += uint64(len(p))
	if d.nx > 0 {
		n := copy(d.x[d.nx:], p)
		d.nx += n
		if d.nx == BlockSize {
			if haveAsm {
				block(&d, d.x[:])
			} else {
				blockGeneric(&d, d.x[:])
			}
			d.nx = 0
		}
		p = p[n:]
	}
	if len(p) >= BlockSize {
		n := len(p) &^ (BlockSize - 1)
		if haveAsm {
			block(&d, p[:n])
		} else {
			blockGeneric(&d, p[:n])
		}
		p = p[n:]
	}
	if len(p) > 0 {
		d.nx = copy(d.x[:], p)
	}

	// Checksum
	tmp := [1 + 63 + 8]byte{0x80}
	pad := (55 - d.len) % 64                             // calculate number of padding bytes
	binary.LittleEndian.PutUint64(tmp[1+pad:], d.len<<3) // append length in bits
	// d.Write(tmp[:1+pad+8])
	tp := tmp[:1+pad+8]
	d.len += uint64(len(tp))
	if d.nx > 0 {
		n := copy(d.x[d.nx:], tp)
		d.nx += n
		if d.nx == BlockSize {
			if haveAsm {
				block(&d, d.x[:])
			} else {
				blockGeneric(&d, d.x[:])
			}
			d.nx = 0
		}
		tp = tp[n:]
	}
	if len(tp) >= BlockSize {
		n := len(tp) &^ (BlockSize - 1)
		if haveAsm {
			block(&d, tp[:n])
		} else {
			blockGeneric(&d, tp[:n])
		}
		tp = tp[n:]
	}
	if len(tp) > 0 {
		d.nx = copy(d.x[:], tp)
	}

	if d.nx != 0 {
		panic("d.nx != 0")
	}

	h = make([]byte, Size)
	binary.LittleEndian.PutUint32(h[0:], d.s[0])
	binary.LittleEndian.PutUint32(h[4:], d.s[1])
	binary.LittleEndian.PutUint32(h[8:], d.s[2])
	binary.LittleEndian.PutUint32(h[12:], d.s[3])

	hi = binary.BigEndian.Uint64(h[:8])
	lo = binary.BigEndian.Uint64(h[8:16])
	return
}

//go:inline
func MD5Uint64(p []byte) (hi, lo uint64) {
	var d = digest{s: [4]uint32{init0, init1, init2, init3}}
	// Write
	d.len += uint64(len(p))
	if d.nx > 0 {
		n := copy(d.x[d.nx:], p)
		d.nx += n
		if d.nx == BlockSize {
			if haveAsm {
				block(&d, d.x[:])
			} else {
				blockGeneric(&d, d.x[:])
			}
			d.nx = 0
		}
		p = p[n:]
	}
	if len(p) >= BlockSize {
		n := len(p) &^ (BlockSize - 1)
		if haveAsm {
			block(&d, p[:n])
		} else {
			blockGeneric(&d, p[:n])
		}
		p = p[n:]
	}
	if len(p) > 0 {
		d.nx = copy(d.x[:], p)
	}

	// Checksum
	tmp := [1 + 63 + 8]byte{0x80}
	pad := (55 - d.len) % 64                             // calculate number of padding bytes
	binary.LittleEndian.PutUint64(tmp[1+pad:], d.len<<3) // append length in bits
	// d.Write(tmp[:1+pad+8])
	tp := tmp[:1+pad+8]
	d.len += uint64(len(tp))
	if d.nx > 0 {
		n := copy(d.x[d.nx:], tp)
		d.nx += n
		if d.nx == BlockSize {
			if haveAsm {
				block(&d, d.x[:])
			} else {
				blockGeneric(&d, d.x[:])
			}
			d.nx = 0
		}
		tp = tp[n:]
	}
	if len(tp) >= BlockSize {
		n := len(tp) &^ (BlockSize - 1)
		if haveAsm {
			block(&d, tp[:n])
		} else {
			blockGeneric(&d, tp[:n])
		}
		tp = tp[n:]
	}
	if len(tp) > 0 {
		d.nx = copy(d.x[:], tp)
	}

	if d.nx != 0 {
		panic("d.nx != 0")
	}

	hi = uint64(d.s[0])<<32 | uint64(d.s[1])
	lo = uint64(d.s[2])<<32 | uint64(d.s[3])
	return
}

//go:inline
func MD5HL(h []byte) (hi, lo uint64) {
	hi = binary.BigEndian.Uint64(h[0:8])
	lo = binary.BigEndian.Uint64(h[8:16])
	return hi, lo
}
