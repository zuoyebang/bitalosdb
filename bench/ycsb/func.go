package main

import (
	"encoding/binary"
	"hash/fnv"
)

func FillKeySlot(key []byte) {
	slotId := uint16(Fnv(key[2:]) % 1024)
	binary.BigEndian.PutUint16(key[0:2], slotId)
}

func Fnv(key []byte) uint32 {
	h := fnv.New32()
	h.Write(key)
	return h.Sum32()
}
