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

package bdb

import (
	"bytes"
	"fmt"
	"sort"
)

type Cursor struct {
	bucket *Bucket
	stack  []elemRef
}

func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	c.first()

	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) Last() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	ref := elemRef{page: p, node: n}
	ref.index = ref.count() - 1
	c.stack = append(c.stack, ref)
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) Prev() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 {
			elem.index--
			break
		}
		c.stack = c.stack[:i]
	}

	if len(c.stack) == 0 {
		return nil, nil
	}

	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	k, v, flags := c.seek(seek)

	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) Delete() error {
	if c.bucket.tx.db == nil {
		return ErrTxClosed
	} else if !c.bucket.Writable() {
		return ErrTxNotWritable
	}

	key, _, flags := c.keyValue()
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	c.node().del(key)

	return nil
}

func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	c.stack = c.stack[:0]
	c.search(seek, c.bucket.root)

	return c.keyValue()
}

func (c *Cursor) first() {
	for {
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

func (c *Cursor) last() {
	for {
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)

		var nextRef = elemRef{page: p, node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 {
				elem.index++
				break
			}
		}

		if i == -1 {
			return nil, nil, 0
		}

		c.stack = c.stack[:i+1]
		c.first()

		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		return c.keyValue()
	}
}

func (c *Cursor) search(key []byte, pgid pgid) {
	p, n := c.bucket.pageNode(pgid)
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
	}
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	if e.isLeaf() {
		c.nsearch(key)
		return
	}

	if n != nil {
		c.searchNode(key, n)
		return
	}
	c.searchPage(key, p)
}

func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	index := sort.Search(len(n.inodes), func(i int) bool {
		ret := bytes.Compare(n.inodes[i].key, key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	c.search(key, n.inodes[index].pgid)
}

func (c *Cursor) searchPage(key []byte, p *page) {
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	c.search(key, inodes[index].pgid)
}

func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1
		})
		e.index = index
		return
	}

	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = index
}

func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	ref := &c.stack[len(c.stack)-1]

	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	if ref.node != nil {
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}

	elem := ref.page.leafPageElement(uint16(ref.index))
	return elem.key(), elem.value(), elem.flags
}

func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	var n = c.stack[0].node
	if n == nil {
		n = c.bucket.node(c.stack[0].page.id, nil)
	}
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		n = n.childAt(ref.index)
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

type elemRef struct {
	page  *page
	node  *node
	index int
}

func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}
