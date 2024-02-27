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
	"testing"
	"unsafe"
)

func TestNode_put(t *testing.T) {
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{meta: &meta{pgid: 1}}}}
	n.put([]byte("baz"), []byte("baz"), []byte("2"), 0, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("0"), 0, 0)
	n.put([]byte("bar"), []byte("bar"), []byte("1"), 0, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("3"), 0, leafPageFlag)

	if len(n.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "bar" || string(v) != "1" {
		t.Fatalf("exp=<bar,1>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "baz" || string(v) != "2" {
		t.Fatalf("exp=<baz,2>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[2].key, n.inodes[2].value; string(k) != "foo" || string(v) != "3" {
		t.Fatalf("exp=<foo,3>; got=<%s,%s>", k, v)
	}
	if n.inodes[2].flags != uint32(leafPageFlag) {
		t.Fatalf("not a leaf: %d", n.inodes[2].flags)
	}
}

func TestNode_read_LeafPage(t *testing.T) {
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = leafPageFlag
	page.count = 2

	nodes := (*[3]leafPageElement)(unsafe.Pointer(uintptr(unsafe.Pointer(page)) + unsafe.Sizeof(*page)))
	nodes[0] = leafPageElement{flags: 0, pos: 32, ksize: 3, vsize: 4}
	nodes[1] = leafPageElement{flags: 0, pos: 23, ksize: 10, vsize: 3}

	const s = "barfoozhelloworldbye"
	data := unsafeByteSlice(unsafe.Pointer(&nodes[2]), 0, 0, len(s))
	copy(data, s)

	n := &node{}
	n.read(page)

	if !n.isLeaf {
		t.Fatal("expected leaf")
	}
	if len(n.inodes) != 2 {
		t.Fatalf("exp=2; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "bar" || string(v) != "fooz" {
		t.Fatalf("exp=<bar,fooz>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "helloworld" || string(v) != "bye" {
		t.Fatalf("exp=<helloworld,bye>; got=<%s,%s>", k, v)
	}
}

func TestNode_write_LeafPage(t *testing.T) {
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("susy"), []byte("susy"), []byte("que"), 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), []byte("lake"), 0, 0)
	n.put([]byte("john"), []byte("john"), []byte("johnson"), 0, 0)

	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	n.write(p)

	n2 := &node{}
	n2.read(p)

	if len(n2.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n2.inodes))
	}
	if k, v := n2.inodes[0].key, n2.inodes[0].value; string(k) != "john" || string(v) != "johnson" {
		t.Fatalf("exp=<john,johnson>; got=<%s,%s>", k, v)
	}
	if k, v := n2.inodes[1].key, n2.inodes[1].value; string(k) != "ricki" || string(v) != "lake" {
		t.Fatalf("exp=<ricki,lake>; got=<%s,%s>", k, v)
	}
	if k, v := n2.inodes[2].key, n2.inodes[2].value; string(k) != "susy" || string(v) != "que" {
		t.Fatalf("exp=<susy,que>; got=<%s,%s>", k, v)
	}
}

func TestNode_split(t *testing.T) {
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)

	n.split(100)

	var parent = n.parent
	if len(parent.children) != 2 {
		t.Fatalf("exp=2; got=%d", len(parent.children))
	}
	if len(parent.children[0].inodes) != 2 {
		t.Fatalf("exp=2; got=%d", len(parent.children[0].inodes))
	}
	if len(parent.children[1].inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(parent.children[1].inodes))
	}
}

func TestNode_split_MinKeys(t *testing.T) {
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)

	n.split(20)
	if n.parent != nil {
		t.Fatalf("expected nil parent")
	}
}

func TestNode_split_SinglePage(t *testing.T) {
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)

	n.split(4096)
	if n.parent != nil {
		t.Fatalf("expected nil parent")
	}
}
