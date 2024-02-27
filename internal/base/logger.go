// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
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

package base

import (
	"fmt"
	"log"
	"os"
	"time"
)

const logTagFmt = "%s %s"

type Logger interface {
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Cost(arg ...interface{}) func()
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

func NewLogger(logger Logger, tag string) Logger {
	if logger == nil {
		return defaultLogger{
			tag: tag,
		}
	} else {
		return customLogger{
			clog: logger,
			tag:  tag,
		}
	}
}

type customLogger struct {
	clog Logger
	tag  string
}

func (l customLogger) Info(args ...interface{}) {
	l.clog.Info(l.tag, " ", fmt.Sprint(args...))
}

func (l customLogger) Warn(args ...interface{}) {
	l.clog.Warn(l.tag, " ", fmt.Sprint(args...))
}

func (l customLogger) Error(args ...interface{}) {
	l.clog.Error(l.tag, " ", fmt.Sprint(args...))
}

func (l customLogger) Infof(format string, args ...interface{}) {
	l.clog.Infof(logTagFmt, l.tag, fmt.Sprintf(format, args...))
}

func (l customLogger) Warnf(format string, args ...interface{}) {
	l.clog.Warnf(logTagFmt, l.tag, fmt.Sprintf(format, args...))
}

func (l customLogger) Errorf(format string, args ...interface{}) {
	l.clog.Errorf(logTagFmt, l.tag, fmt.Sprintf(format, args...))
}

func (l customLogger) Fatalf(format string, args ...interface{}) {
	l.clog.Fatalf(logTagFmt, l.tag, fmt.Sprintf(format, args...))
}

func (l customLogger) Cost(args ...interface{}) func() {
	return l.clog.Cost(l.tag, " ", fmt.Sprint(args...))
}

type defaultLogger struct {
	tag string
}

var DefaultLogger = defaultLogger{tag: ""}

func (l defaultLogger) Info(args ...interface{}) {
	_ = log.Output(2, fmt.Sprint(l.tag, " ", fmt.Sprint(args...)))
}

func (l defaultLogger) Warn(args ...interface{}) {
	_ = log.Output(2, fmt.Sprint(l.tag, " ", fmt.Sprint(args...)))
}

func (l defaultLogger) Error(args ...interface{}) {
	_ = log.Output(2, fmt.Sprint(l.tag, " ", fmt.Sprint(args...)))
}

func (l defaultLogger) Infof(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(logTagFmt, l.tag, fmt.Sprintf(format, args...)))
}

func (l defaultLogger) Warnf(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(logTagFmt, l.tag, fmt.Sprintf(format, args...)))
}

func (l defaultLogger) Errorf(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(logTagFmt, l.tag, fmt.Sprintf(format, args...)))
}

func (l defaultLogger) Fatalf(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(logTagFmt+format, l.tag, fmt.Sprint(args...)))
	os.Exit(1)
}

func (l defaultLogger) Cost(args ...interface{}) func() {
	begin := time.Now()
	return func() {
		_ = log.Output(2, fmt.Sprint(l.tag, " ", fmt.Sprint(args...), " ", FmtDuration(time.Since(begin))))
	}
}

func FmtDuration(d time.Duration) string {
	if d > time.Second {
		return fmt.Sprintf("cost:%d.%03ds", d/time.Second, d/time.Millisecond%1000)
	}
	if d > time.Millisecond {
		return fmt.Sprintf("cost:%d.%03dms", d/time.Millisecond, d/time.Microsecond%1000)
	}
	if d > time.Microsecond {
		return fmt.Sprintf("cost:%d.%03dus", d/time.Microsecond, d%1000)
	}
	return fmt.Sprintf("cost:%dns", d)
}
