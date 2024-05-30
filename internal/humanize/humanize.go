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

package humanize

import (
	"fmt"
	"math"

	"github.com/cockroachdb/redact"
)

func logn(n, b float64) float64 {
	return math.Log(n) / math.Log(b)
}

func humanate(s uint64, base float64, suffixes []string) string {
	if s < 10 {
		return fmt.Sprintf("%d%s", s, suffixes[0])
	}
	e := math.Floor(logn(float64(s), base))
	suffix := suffixes[int(e)]
	val := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f%s"
	if val < 10 {
		f = "%.1f%s"
	}

	return fmt.Sprintf(f, val, suffix)
}

type config struct {
	base   float64
	suffix []string
}

var IEC = config{1024, []string{" B", " K", " M", " G", " T", " P", " E"}}

var SI = config{1000, []string{"", " K", " M", " G", " T", " P", " E"}}

func (c *config) Int64(s int64) FormattedString {
	if s < 0 {
		return FormattedString("-" + humanate(uint64(-s), c.base, c.suffix))
	}
	return FormattedString(humanate(uint64(s), c.base, c.suffix))
}

func (c *config) Uint64(s uint64) FormattedString {
	return FormattedString(humanate(s, c.base, c.suffix))
}

func Int64(s int64) FormattedString {
	return IEC.Int64(s)
}

func Uint64(s uint64) FormattedString {
	return IEC.Uint64(s)
}

type FormattedString string

var _ redact.SafeValue = FormattedString("")

func (fs FormattedString) SafeValue() {}

func (fs FormattedString) String() string { return string(fs) }
