//go:build !linux
// +build !linux

package main

import (
	"syscall"
	"time"
)

// #include <unistd.h>
import "C"

type Usage struct {
	Utime  time.Duration `json:"utime"`   // user CPU time used
	Stime  time.Duration `json:"stime"`   // system CPU time used
	MaxRss int64         `json:"max_rss"` // maximum resident set size
	Ixrss  int64         `json:"ix_rss"`  // integral shared memory size
	Idrss  int64         `json:"id_rss"`  // integral unshared data size
	Isrss  int64         `json:"is_rss"`  // integral unshared stack size
}

func (u *Usage) MemTotal() int64 {
	return u.Ixrss + u.Idrss + u.Isrss
}

func (u *Usage) MemShr() int64 {
	return u.Ixrss
}

func (u *Usage) CPUTotal() time.Duration {
	return u.Utime + u.Stime
}

func GetUsage() (*Usage, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return nil, err
	}
	u := &Usage{}
	u.Utime = time.Duration(usage.Utime.Nano())
	u.Stime = time.Duration(usage.Stime.Nano())

	unit := 1024 * int64(C.sysconf(C._SC_CLK_TCK))

	u.MaxRss = usage.Maxrss
	u.Ixrss = unit * usage.Ixrss
	u.Idrss = unit * usage.Idrss
	u.Isrss = unit * usage.Isrss
	return u, nil
}
