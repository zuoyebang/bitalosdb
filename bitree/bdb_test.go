package bitree

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/bitpage"
	"github.com/zuoyebang/bitalosdb/bitree/bdb"
	"github.com/zuoyebang/bitalosdb/internal/consts"
	"github.com/zuoyebang/bitalosdb/internal/sortedkv"
	"github.com/zuoyebang/bitalosdb/internal/utils"
)

func TestBitree_Bdb_Seek(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	seekFunc := func(index int) {
		btree, _ := testOpenBitree()

		pn, sentinel, closer := btree.FindKeyPageNum(consts.BdbMaxKey)
		require.Equal(t, bitpage.PageNum(1), pn)
		require.Equal(t, consts.BdbMaxKey, sentinel)
		closer()

		if index == 0 {
			for i := 0; i < 10; i++ {
				key := makeTestKey(i)
				pn, sentinel, closer = btree.FindKeyPageNum(key)
				require.Equal(t, bitpage.PageNum(1), pn)
				require.Equal(t, consts.BdbMaxKey, sentinel)
				closer()
			}
			err := btree.bdb.Update(func(tx *bdb.Tx) error {
				bkt := tx.Bucket(consts.BdbBucketName)
				if bkt == nil {
					return bdb.ErrBucketNotFound
				}
				for i := 2; i < 10; i++ {
					if i%3 != 0 {
						continue
					}

					key := makeTestKey(i)
					require.NoError(t, bkt.Put(key, bitpage.PageNum(i).ToByte()))
				}
				return nil
			})
			btree.txPool.Update()
			require.NoError(t, err)
		}

		checkValue := func(i, exp int) {
			key := makeTestKey(i)
			pn, sentinel, closer = btree.FindKeyPageNum(key)
			require.Equal(t, bitpage.PageNum(exp), pn)
			closer()
			if exp == 1 {
				require.Equal(t, consts.BdbMaxKey, sentinel)
			} else {
				require.Equal(t, makeTestKey(exp), sentinel)
			}
		}

		checkValue(0, 3)
		checkValue(3, 3)
		checkValue(12, 3)
		checkValue(4, 6)
		checkValue(5, 6)
		checkValue(51, 6)
		checkValue(6, 6)
		checkValue(60, 9)
		checkValue(899, 9)
		checkValue(9, 9)
		checkValue(90, 1)
		checkValue(91, 1)

		require.NoError(t, testBitreeClose(btree))
	}

	for i := 0; i < 5; i++ {
		seekFunc(i)
	}
}

func TestBitree_Bdb_Seek_LargeKey(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)

	btree, _ := testOpenBitree()
	keyPrefix := utils.FuncRandBytes(1100)
	makeLargeKey := func(i int) []byte {
		return []byte(fmt.Sprintf("bdb_%s_%d", keyPrefix, i))
	}
	err := btree.bdb.Update(func(tx *bdb.Tx) error {
		bkt := tx.Bucket(consts.BdbBucketName)
		if bkt == nil {
			return bdb.ErrBucketNotFound
		}
		for i := 2; i < 200; i++ {
			if i%3 != 0 {
				continue
			}
			key := makeLargeKey(i)
			require.NoError(t, bkt.Put(key, bitpage.PageNum(i).ToByte()))
		}
		return nil
	})
	btree.txPool.Update()
	require.NoError(t, err)

	expPns := []int{102, 102, 21, 3, 42, 51, 6, 72, 81, 9, 102, 111, 12, 132, 141, 15, 162, 171, 18, 192, 21, 21, 24, 24,
		24, 27, 27, 27, 3, 3, 30, 33, 33, 33, 36, 36, 36, 39, 39, 39, 42, 42, 42, 45, 45, 45, 48, 48, 48, 51, 51, 51, 54, 54, 54,
		57, 57, 57, 6, 6, 60, 63, 63, 63, 66, 66, 66, 69, 69, 69, 72, 72, 72, 75, 75, 75, 78, 78, 78, 81, 81, 81, 84, 84, 84, 87,
		87, 87, 9, 9, 90, 93, 93, 93, 96, 96, 96, 99, 99, 99, 102, 102, 102, 105, 105, 105, 108, 108, 108, 111, 111, 111, 114,
		114, 114, 117, 117, 117, 12, 12, 120, 123, 123, 123, 126, 126, 126, 129, 129, 129, 132, 132, 132, 135, 135, 135,
		138, 138, 138, 141, 141, 141, 144, 144, 144, 147, 147, 147, 15, 15, 150, 153, 153, 153, 156, 156, 156, 159, 159,
		159, 162, 162, 162, 165, 165, 165, 168, 168, 168, 171, 171, 171, 174, 174, 174, 177, 177, 177, 18, 18, 180, 183,
		183, 183, 186, 186, 186, 189, 189, 189, 192, 192, 192, 195, 195, 195, 198, 198, 198, 21}

	for i := 0; i < 200; i++ {
		key := makeLargeKey(i)
		pn, _, closer := btree.FindKeyPageNum(key)
		require.Equal(t, bitpage.PageNum(expPns[i]), pn)
		closer()
	}

	checkValue := func(i, exp int) {
		key := makeLargeKey(i)
		pn, sentinel, closer := btree.FindKeyPageNum(key)
		require.Equal(t, bitpage.PageNum(exp), pn)
		closer()
		if exp == 1 {
			require.Equal(t, consts.BdbMaxKey, sentinel)
		} else {
			require.Equal(t, makeLargeKey(exp), sentinel)
		}
	}

	checkValue(201, 21)
	checkValue(255, 27)
	checkValue(402, 42)
	checkValue(518, 54)
	checkValue(627, 63)
	checkValue(734, 75)
	checkValue(846, 87)
	checkValue(899, 9)
	checkValue(953, 96)
	checkValue(999, 1)

	require.NoError(t, testBitreeClose(btree))
}

func TestBitree_Bdb_SeekPrefixDeleteKey(t *testing.T) {
	for _, isLarge := range []bool{false, true} {
		t.Run(fmt.Sprintf("isLarge=%t", isLarge), func(t *testing.T) {
			defer os.RemoveAll(testDir)
			os.RemoveAll(testDir)

			btree, _ := testOpenBitree()
			defer testBitreeClose(btree)

			var keyPrefix, largePrefix []byte
			slotId := uint16(1)
			verNum := 5
			verStart := uint64(1024)
			verEnd := uint64(1030)
			if isLarge {
				largePrefix = utils.FuncRandBytes(1100)
			} else {
				largePrefix = []byte("")
			}

			makeKey := func(n int, v uint64) []byte {
				keyPrefix = []byte(fmt.Sprintf("bdb_%s_%d", largePrefix, n))
				return sortedkv.MakeKey2(keyPrefix, slotId, v)
			}

			err := btree.bdb.Update(func(tx *bdb.Tx) error {
				bkt := tx.Bucket(consts.BdbBucketName)
				if bkt == nil {
					return bdb.ErrBucketNotFound
				}

				for ver := verStart; ver <= verEnd; ver++ {
					for i := 1; i <= verNum; i++ {
						key := makeKey(i, ver)
						require.NoError(t, bkt.Put(key, bitpage.PageNum(i).ToByte()))
					}
				}
				return nil
			})
			btree.txPool.Update()
			require.NoError(t, err)

			checkSeek := func(n int, version uint64) {
				rtx := btree.txPool.Load()
				defer rtx.Unref(true)

				bkt := rtx.Bucket()
				if bkt == nil {
					t.Fatal("bkt is nil")
				}

				seek := makeKey(n, version)
				pns, sentinels := btree.findPrefixDeleteKeyPageNums(seek, bkt.Cursor())

				if version > verEnd {
					require.Equal(t, 1, len(pns))
					require.Equal(t, 1, len(sentinels))
					require.Equal(t, consts.BdbMaxKey, sentinels[0])
					return
				}

				expNum := verNum - n + 2
				sentinelsNum := len(sentinels)
				require.Equal(t, expNum, len(pns))
				require.Equal(t, expNum, sentinelsNum)

				seekPrefixDelete := btree.opts.KeyPrefixDeleteFunc(seek)
				for i := range sentinels {
					s := btree.opts.KeyPrefixDeleteFunc(sentinels[i])
					exp := seekPrefixDelete
					if i == sentinelsNum-1 {
						if version == verEnd {
							require.Equal(t, consts.BdbMaxKey, sentinels[i])
							break
						}
						exp = seekPrefixDelete + 1
					}
					require.Equal(t, exp, s)
				}
			}

			for ver := verStart; ver <= verEnd+5; ver++ {
				for i := 1; i <= verNum; i++ {
					checkSeek(i, ver)
				}
			}
		})
	}
}
