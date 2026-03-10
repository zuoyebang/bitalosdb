package vectortable

import (
	"bytes"
	"os"
	"testing"

	"github.com/zuoyebang/bitalosdb/v2/internal/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTableExtend(t *testing.T) (tbl *tableExtend) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	require.NoError(t, err)
	defer os.RemoveAll(path)
	var tail OffsetKeeper
	f := path + "/tbl.bin"
	tbl, err = openTableExtend(f, &tail, 1<<20, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func TestTableExtendReuse(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	require.NoError(t, err)
	defer os.RemoveAll(path)
	var tail OffsetKeeper
	f := path + "/tbl.bin"
	tbl, err := openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
	if err != nil {
		t.Fatal(err)
	}
	m := map[int]uint64{}
	sz := uint64(bytesMeta40)
	for i := 0; i < 16; i++ {
		m[i], _ = tbl.ReuseAlloc(sz)
		v := bytes.Repeat([]byte{byte(i + 1)}, int(sz))
		copy(tbl.data[m[i]:], v)
	}

	for i := 0; i < 10; i++ {
		tbl.ReuseFree(m[i], uint32(sz))
		assert.Equal(t, tbl.ReuseCap(uint32(sz)), i+1)
	}

	count := tbl.ReuseCap(uint32(sz))

	defer func() {
		if r := recover(); r != nil {
			c := r.(int)
			tbl, _ = openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
			if c == 1 {
				assert.Equal(t, count-1, tbl.ReuseCap(uint32(sz)))
			}

			if c == 2 {
				assert.Equal(t, count-1, tbl.ReuseCap(uint32(sz)))
			}

			if c == 3 {
				assert.Equal(t, count, tbl.ReuseCap(uint32(sz)))
			}

			if c == 4 {
				assert.Equal(t, count+1, tbl.ReuseCap(uint32(sz)))
			}
		}
	}()

	tbl.ReuseAllocPanic(1, sz) // 1

	//tbl.ReuseFreePanic(4, m[10], uint32(sz)) // 3, 4

}

func TestTableExtendReopen(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	require.NoError(t, err)
	defer os.RemoveAll(path)
	var tail OffsetKeeper
	f := path + "/tbl.bin"
	tbl, _ := openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
	m := map[int]uint64{}
	sz := uint64(bytesMeta40)
	for i := 0; i < 16; i++ {
		m[i], _ = tbl.ReuseAlloc(sz)
		v := bytes.Repeat([]byte{byte(i + 1)}, int(sz))
		copy(tbl.data[m[i]:], v)
	}

	require.Equal(t, 1048576, tbl.filesz)
	require.Equal(t, uint64(1048576), tbl.upperOffset)
	require.Equal(t, uint64(692), tbl.offset.GetOffset())
	require.NoError(t, tbl.close())

	tbl, _ = openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
	require.Equal(t, 1049268, tbl.filesz)
	require.Equal(t, uint64(1049268), tbl.upperOffset)
	require.Equal(t, uint64(692), tbl.offset.GetOffset())
	require.NoError(t, tbl.close())

	tbl, _ = openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
	require.Equal(t, 1049268, tbl.filesz)
	require.Equal(t, uint64(1049268), tbl.upperOffset)
	require.Equal(t, uint64(692), tbl.offset.GetOffset())
	require.NoError(t, tbl.close())
}

func TestTableExtendReuseAndReopen(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	require.NoError(t, err)
	defer os.RemoveAll(path)
	var tail OffsetKeeper
	f := path + "/tbl.bin"
	tbl, _ := openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
	m := map[int]uint64{}
	sz := uint64(bytesMeta64)
	for i := 0; i < 16; i++ {
		m[i], _ = tbl.ReuseAlloc(sz)
		v := bytes.Repeat([]byte{byte(i + 1)}, int(sz))
		copy(tbl.data[m[i]:], v)
	}

	for i := 0; i < 10; i++ {
		tbl.ReuseFree(m[i], uint32(sz))
		assert.Equal(t, tbl.ReuseCap(uint32(sz)), i+1)
	}

	tbl.close()

	tbl, _ = openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
	for i := 0; i < 10; i++ {
		off, err := tbl.ReuseAlloc(sz)
		assert.NoError(t, err)
		assert.Equal(t, m[10-i-1], off)
	}

	tbl.close()
}

func TestTableExtendReuseAndReopen1(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	require.NoError(t, err)
	defer os.RemoveAll(path)
	var tail OffsetKeeper
	f := path + "/tbl.bin"
	tbl, _ := openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
	m := map[int]uint64{}
	sz := uint64(bytesMeta40)
	for i := 0; i < 16; i++ {
		m[i], _ = tbl.ReuseAlloc(sz)
		v := bytes.Repeat([]byte{byte(i + 1)}, int(sz))
		copy(tbl.data[m[i]:], v)
	}

	for i := 0; i < 10; i++ {
		tbl.ReuseFree(m[i], uint32(sz))
		assert.Equal(t, tbl.ReuseCap(uint32(sz)), i+1)
	}

	tbl.close()

	tbl, _ = openTableExtend(f, &tail, 1<<20, 1, base.DefaultLogger)
	for i := 0; i < 10; i++ {
		off, err := tbl.ReuseAlloc(sz)
		assert.NoError(t, err)
		assert.Equal(t, m[10-i-1], off)
	}

	tbl.close()
}
