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

package utils

import (
	"encoding/binary"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func Float64ToByteSort(float float64, buf []byte) []byte {
	bits := math.Float64bits(float)
	if buf == nil {
		buf = make([]byte, 8, 8)
	}
	_ = buf[7]
	binary.BigEndian.PutUint64(buf, bits)
	if buf[0] < 1<<7 {
		buf[0] ^= 1 << 7
	} else {
		buf[0] ^= 0xff
		buf[1] ^= 0xff
		buf[2] ^= 0xff
		buf[3] ^= 0xff
		buf[4] ^= 0xff
		buf[5] ^= 0xff
		buf[6] ^= 0xff
		buf[7] ^= 0xff
	}
	return buf
}

func CloneBytes(v []byte) []byte {
	if v == nil {
		return nil
	}
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}

func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func Uint32ToBytes(u uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, u)
	return buf
}

func FuncRandBytes(n int) []byte {
	randStr := "1qaz2wsx3edc4rfv5tgb6yhn7ujm8ik9ol0pabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	randStrLen := len(randStr)
	b := make([]byte, n)
	for i := range b {
		b[i] = randStr[rand.Int63()%int64(randStrLen)]
	}
	return b
}

func FirstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

func CalcBitsSize(sz int) int {
	return 1 << bits.Len(uint(sz))
}

func GetDirSize(dir string) int64 {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return 0
	}
	res := strings.Split(Command("du", "-sb", dir), "\t")
	if len(res) < 2 || len(res) > 2 {
		return 0
	}
	size, err := strconv.ParseInt(res[0], 10, 64)
	if err != nil {
		return 0
	}
	return size
}

func Command(key string, arg ...string) string {
	cmd := exec.Command(key, arg...)
	b, _ := cmd.CombinedOutput()
	return strings.TrimSpace(string(b))
}

func IsFileNotExist(name string) bool {
	if len(name) == 0 {
		return true
	}
	_, err := os.Stat(name)
	return err != nil && os.IsNotExist(err)
}

func IsFileExist(name string) bool {
	if len(name) == 0 {
		return false
	}
	_, err := os.Stat(name)
	return err == nil || !os.IsNotExist(err)
}
