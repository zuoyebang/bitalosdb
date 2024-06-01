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

package bithash

import "github.com/zuoyebang/bitalosdb/internal/base"

type InternalKey = base.InternalKey

const (
	InternalKeyKindDelete  = base.InternalKeyKindDelete
	InternalKeyKindSet     = base.InternalKeyKindSet
	InternalKeyKindInvalid = base.InternalKeyKindInvalid
)

type Compare = base.Compare
type Equal = base.Equal
type Separator = base.Separator
type Successor = base.Successor
type Split = base.Split
type Comparer = base.Comparer
