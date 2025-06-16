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

package bdb

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/errors"
)

const (
	MaxKeySize   = consts.MaxKeySize + 1<<8
	MaxValueSize = (1 << 31) - 2
)

const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

const DefaultFillPercent = 1.0

type Bucket struct {
	*bucket
	tx          *Tx
	buckets     map[string]*Bucket
	page        *page
	rootNode    *node
	nodes       map[pgid]*node
	FillPercent float64
}

type bucket struct {
	root     pgid
	sequence uint64
}

func newBucket(tx *Tx) Bucket {
	var b = Bucket{tx: tx, FillPercent: DefaultFillPercent}
	if tx.writable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node, 1<<4)
	}
	return b
}

func (b *Bucket) Tx() *Tx {
	return b.tx
}

func (b *Bucket) Root() pgid {
	return b.root
}

func (b *Bucket) Writable() bool {
	return b.tx.writable
}
func (b *Bucket) Cursor() *Cursor {
	b.tx.stats.CursorCount++

	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

func (b *Bucket) Bucket(name []byte) *Bucket {
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	c := b.Cursor()
	k, v, flags := c.seek(name)

	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	var child = b.openBucket(v)
	if b.buckets != nil {
		b.buckets[string(name)] = child
	}

	return child
}

func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.tx)

	const unalignedMask = unsafe.Alignof(struct {
		bucket
		page
	}{}) - 1
	unaligned := uintptr(unsafe.Pointer(&value[0]))&unalignedMask != 0
	if unaligned {
		value = cloneBytes(value)
	}

	if b.tx.writable && !unaligned {
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	if child.root == 0 {
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}

	return &child
}

func openBucketPage(value []byte) *page {
	var pg *page
	bkt := (*bucket)(unsafe.Pointer(&value[0]))
	if bkt.root == 0 {
		pg = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}
	return pg
}

func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	c := b.Cursor()
	k, _, flags := c.seek(key)

	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		}
		return nil, ErrIncompatibleValue
	}

	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}
	var value = bucket.write()

	key = cloneBytes(key)
	c.node().put(key, key, value, 0, bucketLeafFlag)

	b.page = nil

	return b.Bucket(key), nil
}

func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

func (b *Bucket) DeleteBucket(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	c := b.Cursor()
	k, _, flags := c.seek(key)

	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		return ErrIncompatibleValue
	}

	child := b.Bucket(key)
	err := child.ForEach(func(k, v []byte) error {
		if _, _, childFlags := child.Cursor().seek(k); (childFlags & bucketLeafFlag) != 0 {
			if err := child.DeleteBucket(k); err != nil {
				return errors.Wrapf(err, "delete bucket err")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	delete(b.buckets, string(key))

	child.nodes = nil
	child.rootNode = nil
	child.free()

	c.node().del(key)

	return nil
}

func (b *Bucket) Get(key []byte) []byte {
	k, v, flags := b.Cursor().seek(key)

	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

func (b *Bucket) Seek(key []byte) ([]byte, []byte) {
	return b.Cursor().Seek(key)
}

func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	if len(key) > MaxKeySize {
		key = key[:MaxKeySize]
	}

	c := b.Cursor()
	k, _, flags := c.seek(key)

	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	key = cloneBytes(key)
	c.node().put(key, key, value, 0, 0)

	return nil
}

func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	c := b.Cursor()
	k, _, flags := c.seek(key)

	if !bytes.Equal(key, k) {
		return nil
	}

	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	c.node().del(key)

	return nil
}

func (b *Bucket) Sequence() uint64 { return b.bucket.sequence }

func (b *Bucket) SetSequence(v uint64) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	b.bucket.sequence = v
	return nil
}

func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	b.bucket.sequence++
	return b.bucket.sequence, nil
}

func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bucket) Stats() BucketStats {
	var s, subStats BucketStats
	pageSize := b.tx.db.pageSize
	s.BucketN += 1
	if b.root == 0 {
		s.InlineBucketN += 1
	}
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			s.KeyN += int(p.count)

			used := pageHeaderSize

			if p.count != 0 {
				used += leafPageElementSize * uintptr(p.count-1)

				lastElement := p.leafPageElement(p.count - 1)
				used += uintptr(lastElement.pos + lastElement.ksize + lastElement.vsize)
			}

			if b.root == 0 {
				s.InlineBucketInuse += int(used)
			} else {
				s.LeafPageN++
				s.LeafInuse += int(used)
				s.LeafOverflowN += int(p.overflow)

				for i := uint16(0); i < p.count; i++ {
					e := p.leafPageElement(i)
					if (e.flags & bucketLeafFlag) != 0 {
						subStats.Add(b.openBucket(e.value()).Stats())
					}
				}
			}
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)

			used := pageHeaderSize + (branchPageElementSize * uintptr(p.count-1))

			used += uintptr(lastElement.pos + lastElement.ksize)
			s.BranchInuse += int(used)
			s.BranchOverflowN += int(p.overflow)
		}

		if depth+1 > s.Depth {
			s.Depth = (depth + 1)
		}
	})

	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize

	s.Depth += subStats.Depth
	s.Add(subStats)
	return s
}

func (b *Bucket) forEachPage(fn func(*page, int)) {
	if b.page != nil {
		fn(b.page, 0)
		return
	}

	b.tx.forEachPage(b.root, 0, fn)
}

func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	if b.page != nil {
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}

func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	fn(p, n, depth)

	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

func (b *Bucket) spill() error {
	for name, child := range b.buckets {
		var value []byte
		if child.inlineable() {
			child.free()
			value = child.write()
		} else {
			if err := child.spill(); err != nil {
				return err
			}
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket
		}

		if child.rootNode == nil {
			continue
		}

		var c = b.Cursor()
		k, _, flags := c.seek([]byte(name))
		if !bytes.Equal([]byte(name), k) {
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	if b.rootNode == nil {
		return nil
	}

	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	if b.rootNode.pgid >= b.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	}
	b.root = b.rootNode.pgid

	return nil
}

func (b *Bucket) inlineable() bool {
	var n = b.rootNode

	if n == nil || !n.isLeaf {
		return false
	}

	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + uintptr(len(inode.key)) + uintptr(len(inode.value))

		if inode.flags&bucketLeafFlag != 0 {
			return false
		} else if size > b.maxInlineBucketSize() {
			return false
		}
	}

	return true
}

func (b *Bucket) maxInlineBucketSize() uintptr {
	return uintptr(b.tx.db.pageSize / 4)
}

func (b *Bucket) write() []byte {
	var n = b.rootNode
	var value = make([]byte, bucketHeaderSize+n.size())

	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	n.write(p)

	return value
}

func (b *Bucket) rebalance() {
	for _, n := range b.nodes {
		n.rebalance()
	}
	for _, child := range b.buckets {
		child.rebalance()
	}
}

func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")

	if n := b.nodes[pgid]; n != nil {
		return n
	}

	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}

	var p = b.page
	if p == nil {
		p = b.tx.page(pgid)
	}

	n.read(p)
	b.nodes[pgid] = n

	b.tx.stats.NodeCount++

	return n
}

func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			tx.db.freelist.free(tx.meta.txid, p)
		} else {
			n.free()
		}
	})
	b.root = 0
}

func (b *Bucket) dereference() {
	if b.rootNode != nil {
		b.rootNode.root().dereference()
	}

	for _, child := range b.buckets {
		child.dereference()
	}
}

func (b *Bucket) pageNode(id pgid) (*page, *node) {
	if b.root == 0 {
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		return b.page, nil
	}

	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	return b.tx.page(id), nil
}

type BucketStats struct {
	BranchPageN       int
	BranchOverflowN   int
	LeafPageN         int
	LeafOverflowN     int
	KeyN              int
	Depth             int
	BranchAlloc       int
	BranchInuse       int
	LeafAlloc         int
	LeafInuse         int
	BucketN           int
	InlineBucketN     int
	InlineBucketInuse int
}

func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.BranchOverflowN += other.BranchOverflowN
	s.LeafPageN += other.LeafPageN
	s.LeafOverflowN += other.LeafOverflowN
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse

	s.BucketN += other.BucketN
	s.InlineBucketN += other.InlineBucketN
	s.InlineBucketInuse += other.InlineBucketInuse
}

func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
