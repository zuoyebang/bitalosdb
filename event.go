// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitalosdb

import (
	"time"

	"github.com/zuoyebang/bitalosdb/internal/humanize"

	"github.com/cockroachdb/redact"
)

type DiskSlowInfo struct {
	Path     string
	Duration time.Duration
}

func (i DiskSlowInfo) String() string {
	return redact.StringWithoutMarkers(i)
}

func (i DiskSlowInfo) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("disk slowness detected: write to file %s has been ongoing for %0.1fs",
		i.Path, redact.Safe(i.Duration.Seconds()))
}

type FlushInfo struct {
	Index               int
	Reason              string
	Input               int
	Iterated            uint64
	Written             int64
	keyWritten          int64
	keyPrefixDeleteKind int64
	prefixDeleteNum     int64
	Duration            time.Duration
	Done                bool
	Err                 error
}

func (i FlushInfo) String() string {
	return redact.StringWithoutMarkers(i)
}

func (i FlushInfo) SafeFormat(w redact.SafePrinter, _ rune) {
	if i.Err != nil {
		w.Printf("[BITOWER %d] flush error: %s", i.Index, i.Err)
		return
	}

	if !i.Done {
		w.Printf("[BITOWER %d] flushing %d memtable to bitforest", i.Index, i.Input)
		return
	}

	w.Printf("[BITOWER %d] flushed %d memtable to bitforest iterated(%s) written(%s) keys(%d) keysPdKind(%d) pdNum(%d), in %.3fs, output rate %s/s",
		i.Index,
		i.Input,
		humanize.Uint64(i.Iterated),
		humanize.Int64(i.Written),
		i.keyWritten,
		i.keyPrefixDeleteKind,
		i.prefixDeleteNum,
		i.Duration.Seconds(),
		humanize.Uint64(uint64(float64(i.Written)/i.Duration.Seconds())))
}

type WALCreateInfo struct {
	Index           int
	Path            string
	FileNum         FileNum
	RecycledFileNum FileNum
	Err             error
}

func (i WALCreateInfo) String() string {
	return redact.StringWithoutMarkers(i)
}

func (i WALCreateInfo) SafeFormat(w redact.SafePrinter, _ rune) {
	if i.Err != nil {
		w.Printf("[BITOWER %d] WAL create error: %s", redact.Safe(i.Index), i.Err)
		return
	}

	if i.RecycledFileNum == 0 {
		w.Printf("[BITOWER %d] WAL created %s", redact.Safe(i.Index), redact.Safe(i.FileNum))
		return
	}

	w.Printf("[BITOWER %d] WAL created %s (recycled %s)",
		redact.Safe(i.Index), redact.Safe(i.FileNum), redact.Safe(i.RecycledFileNum))
}

type WALDeleteInfo struct {
	Index   int
	Path    string
	FileNum FileNum
	Err     error
}

func (i WALDeleteInfo) String() string {
	return redact.StringWithoutMarkers(i)
}

func (i WALDeleteInfo) SafeFormat(w redact.SafePrinter, _ rune) {
	if i.Err != nil {
		w.Printf("[BITOWER %d] WAL delete error: %s", redact.Safe(i.Index), i.Err)
		return
	}
	w.Printf("[BITOWER %d] WAL deleted %s", redact.Safe(i.Index), redact.Safe(i.FileNum))
}

type WriteStallBeginInfo struct {
	Index  int
	Reason string
}

func (i WriteStallBeginInfo) String() string {
	return redact.StringWithoutMarkers(i)
}

func (i WriteStallBeginInfo) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[BITOWER %d] write stall beginning: %s", redact.Safe(i.Index), redact.Safe(i.Reason))
}

type EventListener struct {
	BackgroundError func(error)
	DiskSlow        func(DiskSlowInfo)
	FlushBegin      func(FlushInfo)
	FlushEnd        func(FlushInfo)
	WALCreated      func(WALCreateInfo)
	WALDeleted      func(WALDeleteInfo)
	WriteStallBegin func(WriteStallBeginInfo)
	WriteStallEnd   func()
}

func (l *EventListener) EnsureDefaults(logger Logger) {
	if l.BackgroundError == nil {
		if logger != nil {
			l.BackgroundError = func(err error) {
				logger.Errorf("background error: %s", err)
			}
		} else {
			l.BackgroundError = func(error) {}
		}
	}
	if l.DiskSlow == nil {
		l.DiskSlow = func(info DiskSlowInfo) {}
	}
	if l.FlushBegin == nil {
		l.FlushBegin = func(info FlushInfo) {}
	}
	if l.FlushEnd == nil {
		l.FlushEnd = func(info FlushInfo) {}
	}
	if l.WALCreated == nil {
		l.WALCreated = func(info WALCreateInfo) {}
	}
	if l.WALDeleted == nil {
		l.WALDeleted = func(info WALDeleteInfo) {}
	}
	if l.WriteStallBegin == nil {
		l.WriteStallBegin = func(info WriteStallBeginInfo) {}
	}
	if l.WriteStallEnd == nil {
		l.WriteStallEnd = func() {}
	}
}

func MakeLoggingEventListener(logger Logger) EventListener {
	if logger == nil {
		logger = DefaultLogger
	}

	return EventListener{
		BackgroundError: func(err error) {
			logger.Infof("background error: %s", err)
		},
		DiskSlow: func(info DiskSlowInfo) {
			logger.Infof("%s", info)
		},
		FlushBegin: func(info FlushInfo) {
			logger.Infof("%s", info)
		},
		FlushEnd: func(info FlushInfo) {
			logger.Infof("%s", info)
		},
		WALCreated: func(info WALCreateInfo) {
			logger.Infof("%s", info)
		},
		WALDeleted: func(info WALDeleteInfo) {
			logger.Infof("%s", info)
		},
		WriteStallBegin: func(info WriteStallBeginInfo) {
			logger.Infof("%s", info)
		},
		WriteStallEnd: func() {
			logger.Infof("write stall ending")
		},
	}
}

func TeeEventListener(a, b EventListener) EventListener {
	a.EnsureDefaults(nil)
	b.EnsureDefaults(nil)
	return EventListener{
		BackgroundError: func(err error) {
			a.BackgroundError(err)
			b.BackgroundError(err)
		},
		DiskSlow: func(info DiskSlowInfo) {
			a.DiskSlow(info)
			b.DiskSlow(info)
		},
		FlushBegin: func(info FlushInfo) {
			a.FlushBegin(info)
			b.FlushBegin(info)
		},
		FlushEnd: func(info FlushInfo) {
			a.FlushEnd(info)
			b.FlushEnd(info)
		},
		WALCreated: func(info WALCreateInfo) {
			a.WALCreated(info)
			b.WALCreated(info)
		},
		WALDeleted: func(info WALDeleteInfo) {
			a.WALDeleted(info)
			b.WALDeleted(info)
		},
		WriteStallBegin: func(info WriteStallBeginInfo) {
			a.WriteStallBegin(info)
			b.WriteStallBegin(info)
		},
		WriteStallEnd: func() {
			a.WriteStallEnd()
			b.WriteStallEnd()
		},
	}
}
