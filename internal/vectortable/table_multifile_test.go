package vectortable

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestMultiFileWR(t *testing.T) {
	testMultiFileWR(t, true)
	testMultiFileWR(t, false)
}

func testMultiFileWR(t *testing.T, buf bool) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(path)
	maxSize := 512 << 10
	var ok OffsetKeeper
	filePath := path + "/testfile"
	tb, err := openTableMulti(filePath, &ok, 0, nil, buf)
	if err != nil {
		t.Fatal(err)
	}
	tb.maxFileSize = uint64(maxSize)
	vLen := 128 << 10

	times := 10
	var m = make(map[int][]byte, times)

	for i := 0; i < times; i++ {
		b := bytes.Repeat([]byte{byte(i)}, vLen)
		m[i] = b
		fIdx, off, n, err := tb.Write(b)
		if err != nil {
			t.Fatal(err)
		}
		exFIdx := (i) * vLen / maxSize
		assert.Equal(t, vLen, n)
		assert.Equal(t, uint16(exFIdx), fIdx)
		assert.Equal(t, uint32((i)*vLen-exFIdx*maxSize), off)
	}
	if err := tb.Flush(); err != nil {
		t.Fatal(err)
		return
	}

	ov := make([]byte, vLen)
	idx := 0
	for i := 0; i <= int(tb.maxIdx); i++ {
		for j := 0; j < maxSize; j += vLen {
			n, err := tb.ReadAt(uint16(i), ov, uint32(j))
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, vLen, n)
			assert.Equal(t, m[idx], ov)
			idx++
			if idx == times {
				break
			}
		}
	}

	if err := tb.close(); err != nil {
		t.Fatal(err)
	}
}
