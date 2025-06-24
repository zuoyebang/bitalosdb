//go:build linux
// +build linux

package main

import (
	"bufio"
	"fmt"
	"os"
	"syscall"
	"time"
)

// #include <unistd.h>
import "C"

type Usage struct {
	Utime  time.Duration `json:"utime"`
	Stime  time.Duration `json:"stime"`
	Cutime time.Duration `json:"cutime"`
	Cstime time.Duration `json:"cstime"`
	VmSize int64         `json:"vm_size"`
	VmRss  int64         `json:"vm_rss"`
	VmShr  int64         `json:"vm_share"`
}

func (u *Usage) MemTotal() int64 {
	return u.VmRss
}

func (u *Usage) MemShr() int64 {
	return u.VmShr
}

func (u *Usage) CPUTotal() time.Duration {
	return time.Duration(u.Utime + u.Stime + u.Cutime + u.Cstime)
}

func GetUsage() (*Usage, error) {
	f, err := os.Open("/proc/self/stat")
	if err != nil {
		return nil, err
	}
	fm, err := os.Open("/proc/self/statm")
	if err != nil {
		return nil, err
	}
	defer func() {
		f.Close()
		fm.Close()
	}()

	var ignore struct {
		s string
		d int64
	}

	r := bufio.NewReader(f)
	u := &Usage{}
	if _, err := fmt.Fscanf(r, "%d %s %s %d %d %d",
		&ignore.d, &ignore.s, &ignore.s, &ignore.d, &ignore.d, &ignore.d); err != nil {
		return nil, err
	}
	if _, err := fmt.Fscanf(r, "%d %d %d",
		&ignore.d, &ignore.d, &ignore.d); err != nil {
		return nil, err
	}
	if _, err := fmt.Fscanf(r, "%d %d %d %d",
		&ignore.d, &ignore.d, &ignore.d, &ignore.d); err != nil {
		return nil, err
	}

	var ticks struct {
		u int64
		s int64
	}
	unit := time.Second / time.Duration(C.sysconf(C._SC_CLK_TCK))

	if _, err := fmt.Fscanf(r, "%d %d", &ticks.u, &ticks.s); err != nil {
		return nil, err
	}
	u.Utime = time.Duration(ticks.u) * unit
	u.Stime = time.Duration(ticks.s) * unit

	if _, err := fmt.Fscanf(r, "%d %d", &ticks.u, &ticks.s); err != nil {
		return nil, err
	}
	u.Cutime = time.Duration(ticks.u) * unit
	u.Cstime = time.Duration(ticks.s) * unit

	rm := bufio.NewReader(fm)
	if _, err := fmt.Fscanf(rm, "%d %d %d", &ignore.d, &u.VmRss, &u.VmShr); err != nil {
		return nil, err
	}
	u.VmRss *= int64(syscall.Getpagesize())
	u.VmShr *= int64(syscall.Getpagesize())

	return u, nil
}
