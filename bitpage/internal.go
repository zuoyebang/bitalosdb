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

package bitpage

import (
	"github.com/zuoyebang/bitalosdb/internal/base"
	"github.com/zuoyebang/bitalosdb/internal/options"
)

const (
	internalKeyKindDelete       = base.InternalKeyKindDelete
	internalKeyKindSet          = base.InternalKeyKindSet
	internalKeyKindSetBithash   = base.InternalKeyKindSetBithash
	internalKeyKindPrefixDelete = base.InternalKeyKindPrefixDelete
	internalKeyKindInvalid      = base.InternalKeyKindInvalid
)

type internalKeyKind = base.InternalKeyKind
type internalKey = base.InternalKey
type internalIterator = base.InternalIterator
type iterOptions = options.IterOptions

var iterCompactOpts = iterOptions{DisableCache: true}
