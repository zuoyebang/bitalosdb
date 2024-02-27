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
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/cache"
	"github.com/zuoyebang/bitalosdb/internal/cache/lfucache"
	"github.com/zuoyebang/bitalosdb/internal/cache/lrucache"
	"github.com/zuoyebang/bitalosdb/internal/consts"
)

func NewCache(kind int, opts *base.CacheOptions) (c cache.ICache) {
	if kind == consts.CacheTypeLfu {
		c = lfucache.New(opts)
	} else {
		c = lrucache.New(opts)
	}
	return c
}

func (d *DB) GetCache(key []byte) ([]byte, func(), bool) {
	if d.cache == nil {
		return nil, nil, false
	}

	khash := d.cache.GetKeyHash(key)
	return d.cache.Get(key, khash)
}
