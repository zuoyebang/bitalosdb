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

package utils

import (
	"math/bits"
	"math/rand"
)

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
