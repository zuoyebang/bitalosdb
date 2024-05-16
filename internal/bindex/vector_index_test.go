package bindex

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVectorIndex_Perf32(t *testing.T) {
	const count = 10 << 20

	golist := make([]uint32, 0, count)
	gomap := make(map[uint32]uint32, count)
	for i := 0; i < count; i++ {
		tmp := uint32(rand.Int31n(1 << 30))
		gomap[tmp] = tmp
	}

	vindex := NewVectorIndex()
	vindex.InitWriter(count, VectorValTypeUint32)

	hindex := NewHashIndex(true)
	hindex.InitWriter()

	for k, v := range gomap {
		vindex.Add32(k, v)
		hindex.Add(k, v)
		golist = append(golist, k)
	}

	vindex.Serialize()
	hindex.Serialize()

	llen := len(golist)

	errno := 0
	bt := time.Now()
	for i := 0; i < llen; i++ {
		v, ok := gomap[golist[i]]
		if !ok || v != golist[i] {
			errno++
		}
	}
	et := time.Since(bt)
	fmt.Printf("gomap cost=%v; llen=%d; errno=%d\n", et, llen, errno)

	errno = 0
	bt = time.Now()
	for i := 0; i < llen; i++ {
		v, ok := vindex.Get32(golist[i])
		if !ok || v != golist[i] {
			errno++
		}
	}
	et = time.Since(bt)
	fmt.Printf("vector_index cost=%v; llen=%d; errno=%d; size=%dMB\n", et, llen, errno, vindex.Size()/1024/1024)

	errno = 0
	bt = time.Now()
	for i := 0; i < llen; i++ {
		v, ok := hindex.Get32(golist[i])
		if !ok || v != golist[i] {
			errno++
		}
	}
	et = time.Since(bt)
	fmt.Printf("hash_index cost=%v; llen=%d; errno=%d; size=%dMB\n", et, llen, errno, hindex.size/1024/1024)

	vindex.Finish()
	hindex.Finish()
}

func TestVectorIndex_AddUint32(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(5, VectorValTypeUint32)
	for i := 0; i < 256; i++ {
		vindex.Add32(uint32(i), uint32(i))
	}
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()
	for i := 0; i < 256; i++ {
		v, ok := vindex.innerMemGet32(uint32(i))
		assert.Equal(t, true, ok)
		assert.Equal(t, uint32(i), v)
	}
	vindex.Finish()
}

func TestVectorIndex_AddUint64(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(5, VectorValTypeUint64)
	for i := 0; i < 256; i++ {
		vindex.Add64(uint32(i), uint64(i))
	}
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()
	for i := 0; i < 256; i++ {
		v, ok := vindex.innerMemGet64(uint32(i))
		assert.Equal(t, true, ok)
		assert.Equal(t, uint64(i), v)
	}
	vindex.Finish()
}

func TestVectorIndex_AddGetUint32(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(5, VectorValTypeUint32)
	for i := 0; i < 256; i++ {
		vindex.Add32(uint32(i), uint32(i))
	}
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()

	ni := NewVectorIndex()
	ni.SetReader(data)
	for i := 0; i < 256; i++ {
		v, ok := ni.Get32(uint32(i))
		assert.Equal(t, true, ok)
		assert.Equal(t, uint32(i), v)
	}
	vindex.Finish()
}

func TestVectorIndex_AddGetUint64(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(5, VectorValTypeUint64)
	for i := 0; i < 256; i++ {
		vindex.Add64(uint32(i), uint64(i))
	}
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()

	ni := NewVectorIndex()
	ni.SetReader(data)
	for i := 0; i < 256; i++ {
		v, ok := ni.Get64(uint32(i))
		assert.Equal(t, true, ok)
		assert.Equal(t, uint64(i), v)
	}
	vindex.Finish()
}

func TestVectorIndex_AddGetRandomUint32(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(256, VectorValTypeUint32)
	m := make(map[uint32]uint32, 256)
	for i := 0; i < 256; i++ {
		k, v := uint32(rand.Int31n(1<<24)), uint32(i)
		m[k] = v
		vindex.Add32(k, v)
	}
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()

	ni := NewVectorIndex()
	ni.SetReader(data)
	for k, v := range m {
		v1, ok := ni.Get32(k)
		assert.Equal(t, true, ok)
		assert.Equal(t, v, v1)
	}
	vindex.Finish()
}

func TestVectorIndex_AddGetRandomUint64(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(256, VectorValTypeUint64)
	m := make(map[uint32]uint64, 256)
	for i := 0; i < 256; i++ {
		k, v := uint32(rand.Int31n(1<<24)), uint64(i)
		m[k] = v
		vindex.Add64(k, v)
	}
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()
	ni := NewVectorIndex()
	ni.SetReader(data)
	for k, v := range m {
		v1, ok := ni.Get64(k)
		assert.Equal(t, true, ok)
		assert.Equal(t, v, v1)
	}
	vindex.Finish()
}

func TestVectorIndex_AddGetRandomRehashUint32(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(1, VectorValTypeUint32)
	m := make(map[uint32]uint32, 256)
	for i := 0; i < 256; i++ {
		k, v := uint32(rand.Int31n(1<<24)), uint32(i)
		m[k] = v
		vindex.Add32(k, v)
	}
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()

	ni := NewVectorIndex()
	ni.SetReader(data)
	for k, v := range m {
		v1, ok := ni.Get32(k)
		assert.Equal(t, true, ok)
		assert.Equal(t, v, v1)
	}
	vindex.Finish()
}

func TestVectorIndex_AddGetRandomRehashUint64(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(1, VectorValTypeUint64)
	m := make(map[uint32]uint64, 256)
	for i := 0; i < 256; i++ {
		k, v := uint32(rand.Int31n(1<<24)), uint64(i)
		m[k] = v
		vindex.Add64(k, v)
	}
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()

	ni := NewVectorIndex()
	ni.SetReader(data)
	for k, v := range m {
		v1, ok := ni.Get64(k)
		assert.Equal(t, true, ok)
		assert.Equal(t, v, v1)
	}
	vindex.Finish()
}

func TestVector32LenCap(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(1, VectorValTypeUint32)
	var count uint32 = 1024
	m := make(map[uint32]uint32, count)
	for i := 0; i < int(count); i++ {
		k, v := uint32(rand.Int31n(1<<24)), uint32(i)
		m[k] = v
		vindex.Add32(k, v)
	}
	assert.Equal(t, count, vindex.Length())
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()

	ni := NewVectorIndex()
	ni.SetReader(data)
	assert.Equal(t, count, ni.Length())
	assert.Equal(t, uint32(0), ni.Capacity())
}

func TestVector64LenCap(t *testing.T) {
	vindex := NewVectorIndex()
	vindex.InitWriter(1, VectorValTypeUint64)
	var count uint32 = 1024
	for i := 0; i < int(count); i++ {
		k, v := uint32(rand.Int31n(1<<24)), uint64(i)
		vindex.Add64(k, v)
	}
	assert.Equal(t, count, vindex.Length())
	var data = make([]byte, vindex.Size())
	vindex.SetWriter(data)
	vindex.Serialize()

	ni := NewVectorIndex()
	ni.SetReader(data)
	assert.Equal(t, count, ni.Length())
	assert.Equal(t, uint32(0), ni.Capacity())
}
