package base

import "github.com/zuoyebang/bitalosdb/internal/consts"

func GetBitowerIndex(i int) int {
	return i & consts.DefaultBitowerNumMask
}
