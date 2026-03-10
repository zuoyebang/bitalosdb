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

package vectortable

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestReloadVT(t *testing.T) {
	path := "/home/homework/dev/bituple.0"
	files, err := filesInDir(path)
	if err != nil {
		log.Println(err)
		return
	}
	vt, err := newTestVectorTableIndexN(path, 0, 1, files, true, false)

	if err != nil {
		log.Println(err)
		return
	}
	vt.GC(false)
}

func TestReloadVTIterator(t *testing.T) {
	path := "/data/smb/homework/bituple.99"
	files, err := filesInDir(path)
	if err != nil {
		log.Println(err)
		return
	}
	vt, err := newTestVectorTableIndexN(path, 0, 1, files, false, false)

	if err != nil {
		log.Println(err)
		return
	}
	it := vt.NewIterator()
	k, _, _, _, dataType, tm, _, _, _, _, _, v, f := it.Next()
	var m = make(map[uint8]int)
	var kl, vl, ke, ve int
	for ; !f; k, _, _, _, dataType, tm, _, _, _, _, _, v, f = it.Next() {
		m[dataType]++
		kl += len(k)
		vl += len(v)
		if tm > 0 && tm < vt.options.GetNowTimestamp() {
			ke += len(k)
			ve += len(v)
		}
	}

	for k, v := range m {
		log.Printf("dataType: %d, count: %d\n", k, v)
	}
	log.Printf("key length: %d, value length: %d, key expire:%d, value expire: %d\n", kl, vl, ke, ve)
	it.Close()
	dh := vt.stable.kvHolder.(*dataHolderNoKey)
	sum := 0
	for i := 0; i < len(dh.em.list); i++ {
		sum += int(dh.em.list[i])
	}
	fmt.Println("sum: ", sum)
	vt.Close(false)
}

func filesInDir(dir string) ([]string, error) {
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	files, err := d.ReadDir(0)
	if err != nil {
		return nil, err
	}
	var fileNames = make([]string, 0, len(files))
	for _, f := range files {
		fileNames = append(fileNames, f.Name())
	}
	return fileNames, nil
}
