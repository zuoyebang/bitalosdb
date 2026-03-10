package vectortable

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/zuoyebang/bitalosdb/v2/internal/simd"
)

func TestFileSize(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "vt")
	if err != nil {
		t.Fatal(err)
	}
	var tail OffsetKeeper
	groups := 2500000
	size := 9*int(groups)*simd.GroupSize + headerSize
	filePath := path + "/testfile"
	tbl, err := openTableDesignated(filePath, &tail, &tableOptions{
		openType:     tableWriteMmap,
		initMmapSize: size,
		logger:       nil,
	})
	if err != nil {
		t.Error(err)
	}

	fmt.Println(cap(tbl.data))
	tbl.alloc(uint64(size))
	binary.LittleEndian.PutUint64(tbl.data[size-8:], 0xffffffffffffffff)

	tbl.msync()
	tbl.close()
}

func TestTags(t *testing.T) {
	tg := Tags(rand.Int31n(1<<16 - 1))
	tg.setExpireManage(true)
	if !tg.isExpireManage() {
		t.Errorf("need true")
	}

	tg.setExpireManage(false)
	if tg.isExpireManage() {
		t.Errorf("need true")
	}
}
