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

package bithash

import (
	"fmt"
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/vfs"
)

var testMemFs = vfs.NewMem()
var testFs = vfs.Default

func TestFooter(t *testing.T) {
	f0, err := testMemFs.Create("filename")
	if err != nil {
		panic(err)
	}
	var ft = footer{
		metaBH: BlockHandle{5, 4},
	}
	buf := make([]byte, bithashFooterLen)
	data := ft.encode(buf)
	fmt.Println("write data:", data)
	f0.Write([]byte("hello"))
	n, err := f0.Write(data)
	if err != nil {
		panic(err)
	}
	if err := f0.Sync(); err != nil {
		panic(err)
	}
	fmt.Println("n:", n)
	if err := f0.Close(); err != nil {
		panic(err)
	}
	f1, err := testMemFs.Open("filename")
	if err != nil {
		panic(err)
	}
	ftt, err := readTableFooter(f1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("read footer:%+v", ftt)
}

func TestFooterEncodeDecode(t *testing.T) {
	var ft = footer{
		metaBH: BlockHandle{1, 2},
	}
	buf := make([]byte, bithashFooterLen)
	newbuf := ft.encode(buf)
	fmt.Println("encode:", newbuf)
	f, err := decodeTableFooter(newbuf, bithashFooterLen)
	if err != nil {
		panic(err)
	}
	fmt.Printf("decode result:%+v", f)
}
