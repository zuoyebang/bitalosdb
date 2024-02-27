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

package utils

import (
	"fmt"
	"time"
)

const (
	B  = 1
	KB = 1 << 10
	MB = 1 << 20
	GB = 1 << 30
	TB = 1 << 40
	EB = 1 << 50
)

func FmtSize(size uint64) string {
	if size < KB {
		return fmt.Sprintf("%dB", size)
	} else if size < MB {
		return fmt.Sprintf("%d.%03dKB", size>>10, size%KB)
	} else if size < GB {
		return fmt.Sprintf("%d.%03dMB", size>>20, (size>>10)%KB)
	} else if size < TB {
		return fmt.Sprintf("%d.%03dGB", size>>30, (size>>20)%KB)
	} else if size < EB {
		return fmt.Sprintf("%d.%03dTB", size>>40, (size>>30)%KB)
	} else {
		return fmt.Sprintf("%d.%03dEB", size>>50, (size>>40)%KB)
	}
}

func FmtUnixMillTime(ms int64) string {
	return time.UnixMilli(ms).Format(time.DateTime)
}

func FmtUnixTime(s int64) string {
	return time.Unix(s, 0).Format(time.DateTime)
}
