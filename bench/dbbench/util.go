package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
)

type threadStat struct {
	executeNum int
	errorNum   int
}

func randValue(val []byte, length int) []byte {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		b := r.Intn(26) + 65
		val[i] = byte(b)
	}
	return val
}

func FillFullKey(k []byte, klen, idx int) {
	binary.LittleEndian.PutUint64(k[2:], uint64(idx))
	if klen > 10 {
		for i := 10; i < klen; i++ {
			k[i] = '0'
		}
	}
	FillKeySlot(k)
}

func makeQueryKey(keyIndex int) []byte {
	keySlice := make([]byte, 18, 18)
	// if *randKey == 0 {
	keyStr := strconv.Itoa(keyIndex)
	lenKeyStr := len(keyStr)
	if lenKeyStr < 16 {
		pos := copy(keySlice[2:], []byte(keyStr))
		pos += 2
		for i := 0; i < 16-lenKeyStr; i++ {
			keySlice[pos] = 0
			pos++
		}
	} else {
		panic("key len >= 16")
	}
	FillKeySlot(keySlice)
	return keySlice
	// return utils.FuncMakeKey([]byte(fmt.Sprintf("key_%010d", keyIndex)))
	// } else {
	// 	return utils.FuncMakeKey(md5Sum([]byte(fmt.Sprintf("key_%d", keyIndex))))
	// }
}

func makeValue(keyIndex int, gid int) []byte {
	return randValueList[keyIndex%randValueCount]
	// if *valueSize == 1 {
	// 	return smallValueList[keyIndex%randValueCount]
	// } else {
	// 	return largeValueList[keyIndex%randValueCount]
	// }
}

func getIndexFromKey(key []byte) int {
	parts := strings.Split(string(key), "_")
	index, _ := strconv.Atoi(parts[len(parts)-1])
	return index
}

func md5Sum(s []byte) []byte {
	return []byte(fmt.Sprintf("%x", md5.Sum(s)))
}

func md5Sum16(s []byte) []byte {
	return []byte(fmt.Sprintf("%s", md5.Sum(s)))
}

func randomSleep(n int) {
	time.Sleep(time.Duration(rand.Intn(n)+1) * time.Second)
}

func randInt(r *rand.Rand, n int) int {
	return r.Intn(n)
}

// 写入cgroup配置
func writeCgroupCpuConfig(cgroupDir string, cpuNum int) error {
	cgroupPid := os.Getpid()
	cmd := fmt.Sprintf("echo %d > %s", cgroupPid, path.Join(cgroupDir, "cgroup.procs"))
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		return err
	}
	cmd = fmt.Sprintf("echo %d > %s", 50000, path.Join(cgroupDir, "cpu.cfs_period_us"))
	_, err = exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		return err
	}
	cmd = fmt.Sprintf("echo %d > %s", cpuNum*50000, path.Join(cgroupDir, "cpu.cfs_quota_us"))
	_, err = exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		return err
	}
	return nil
}

// 写入cgroup配置
func writeCgroupMemoryConfig(cgroupDir string) error {
	cgroupPid := os.Getpid()
	cmd := fmt.Sprintf("echo %d > %s", cgroupPid, path.Join(cgroupDir, "cgroup.procs"))
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		return err
	}
	return nil
}

func FillKeySlot(key []byte) {
	slotId := uint16(Fnv(key[2:]) % 1024)
	binary.BigEndian.PutUint16(key[0:2], slotId)
}

func Fnv(key []byte) uint32 {
	h := fnv.New32()
	h.Write(key)
	return h.Sum32()
}
