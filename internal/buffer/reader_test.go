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
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/v2/internal/consts"
	"github.com/zuoyebang/bitalosdb/v2/internal/utils"
	"github.com/zuoyebang/bitalosdb/v2/internal/vfs"
)

var testPath = filepath.Join(os.TempDir(), "testreaderfile")

func TestReaderRead(t *testing.T) {
	filename := testPath
	defer os.Remove(filename)
	os.Remove(filename)

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, consts.FileMode)
	require.NoError(t, err)

	var n int
	data := utils.FuncRandBytes(10 << 20)
	dataLen := len(data)
	n, err = file.Write(data)
	require.NoError(t, err)
	require.Equal(t, dataLen, n)
	require.NoError(t, file.Close())

	f, err1 := vfs.Default.Open(filename)
	require.NoError(t, err1)
	dataOffset := 0
	rbuf := make([]byte, 0, 1000)
	reader := NewReaderSize(f, 10<<10)
	for {
		randLen := int(rand.Int31n(1000))
		if dataOffset+randLen > dataLen {
			randLen = dataLen - dataOffset
		}

		if cap(rbuf) < randLen {
			rbuf = make([]byte, 0, randLen*2)
		}
		rbuf = rbuf[:randLen]
		rn, err2 := reader.Read(rbuf)
		require.NoError(t, err2)
		require.Equal(t, rn, randLen)
		require.Equal(t, data[dataOffset:dataOffset+randLen], rbuf)
		dataOffset += rn
		if dataOffset >= dataLen {
			break
		}
	}

	require.NoError(t, f.Close())

	f, err1 = vfs.Default.Open(filename)
	require.NoError(t, err1)
	dataOffset = 0
	rbuf = make([]byte, 0, 10)
	reader = NewReaderSize(f, 100)
	for {
		randLen := int(rand.Int31n(200))
		if dataOffset+randLen > dataLen {
			randLen = dataLen - dataOffset
		}

		if cap(rbuf) < randLen {
			rbuf = make([]byte, 0, randLen*2)
		}
		rbuf = rbuf[:randLen]
		rn, err2 := reader.Read(rbuf)
		require.NoError(t, err2)
		require.Equal(t, randLen, rn)
		require.Equal(t, data[dataOffset:dataOffset+randLen], rbuf)
		dataOffset += rn
		if dataOffset >= dataLen {
			break
		}
	}

	require.NoError(t, f.Close())

}
