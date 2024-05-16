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

package bitalosdb

import (
	"bytes"
	"encoding/json"
)

type MetricsInfo struct {
	FlushMemTime       int64 `json:"-"`
	BithashFileTotal   int   `json:"bithash_file_total"`
	BithashKeyTotal    int   `json:"bithash_key_total"`
	BithashDelKeyTotal int   `json:"bithash_del_key_total"`
}

func (s MetricsInfo) String() string {
	str, err := json.Marshal(s)
	if err != nil {
		return ""
	} else {
		return string(str)
	}
}

func (d *DB) statsDiskSize() int64 {
	var size int64
	for _, bitower := range d.bitowers {
		ps := bitower.btree.BitpageStats()
		size += ps.Size
	}
	return size
}

func (d *DB) MetricsInfo() MetricsInfo {
	var stat MetricsInfo
	for i := range d.bitowers {
		bs := d.bitowers[i].btree.BithashStats()
		if bs != nil {
			fileTotal := int(bs.FileTotal.Load())
			stat.BithashFileTotal += fileTotal
			stat.BithashKeyTotal += int(bs.KeyTotal.Load())
			stat.BithashDelKeyTotal += int(bs.DelKeyTotal.Load())
		}
	}

	stat.FlushMemTime = d.flushMemTime.Load()
	return stat
}

func (d *DB) CacheInfo() string {
	if d.cache == nil {
		return ""
	}
	return d.cache.MetricsInfo()
}

func (d *DB) DebugInfo() string {
	dataType := d.opts.DataType
	if dataType == "" {
		dataType = "base"
	}

	dinfo := new(bytes.Buffer)
	for _, bitower := range d.bitowers {
		bpInfo := bitower.btree.BitreeDebugInfo(dataType)
		if bpInfo != "" {
			dinfo.WriteString(bpInfo)
		}

		btInfo := bitower.btree.BitableDebugInfo(dataType)
		if btInfo != "" {
			dinfo.WriteString(btInfo)
		}

		bhInfo := bitower.btree.BithashDebugInfo(dataType)
		if bhInfo != "" {
			dinfo.WriteString(bhInfo)
		}
	}

	return dinfo.String()
}
