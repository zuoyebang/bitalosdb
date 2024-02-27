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

package bitforest

import (
	"bytes"
	"encoding/json"
)

type Stats struct {
	FlushMemTime       int64 `json:"-"`
	BithashFileTotal   int   `json:"bithash_file_total"`
	BithashKeyTotal    int   `json:"bithash_key_total"`
	BithashDelKeyTotal int   `json:"bithash_del_key_total"`
}

func (s Stats) String() string {
	str, err := json.Marshal(s)
	if err != nil {
		return ""
	} else {
		return string(str)
	}
}

func (bf *Bitforest) Stats() Stats {
	var stat Stats
	for _, tree := range bf.trees {
		bs := tree.BithashStats()
		if bs != nil {
			fileTotal := int(bs.FileTotal.Load())
			stat.BithashFileTotal += fileTotal
			stat.BithashKeyTotal += int(bs.KeyTotal.Load())
			stat.BithashDelKeyTotal += int(bs.DelKeyTotal.Load())
		}
	}

	stat.FlushMemTime = bf.GetFlushMemTime()
	return stat
}

func (bf *Bitforest) StatsDiskSize() int64 {
	var size int64
	for _, tree := range bf.trees {
		ps := tree.BitpageStats()
		size += ps.Size
	}

	return size
}

func (bf *Bitforest) DebugInfo(dataType string) string {
	if dataType == "" {
		dataType = "base"
	}

	dinfo := new(bytes.Buffer)
	for _, tree := range bf.trees {
		bpInfo := tree.BitreeDebugInfo(dataType)
		if bpInfo != "" {
			dinfo.WriteString(bpInfo)
		}

		btInfo := tree.BitableDebugInfo(dataType)
		if btInfo != "" {
			dinfo.WriteString(btInfo)
		}

		bhInfo := tree.BithashDebugInfo(dataType)
		if bhInfo != "" {
			dinfo.WriteString(bhInfo)
		}
	}

	return dinfo.String()
}
