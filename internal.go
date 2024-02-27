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

import "github.com/zuoyebang/bitalosdb/internal/base"

const (
	fileTypeLog  = base.FileTypeLog
	fileTypeLock = base.FileTypeLock
	fileTypeMeta = base.FileTypeMeta
)

const (
	InternalKeyKindDelete  = base.InternalKeyKindDelete
	InternalKeyKindSet     = base.InternalKeyKindSet
	InternalKeyKindLogData = base.InternalKeyKindLogData
	InternalKeyKindMax     = base.InternalKeyKindMax
	InternalKeySeqNumMax   = base.InternalKeySeqNumMax
)

type InternalKeyKind = base.InternalKeyKind

type InternalKey = base.InternalKey

type internalIterator = base.InternalIterator

type FileNum = base.FileNum

type Compare = base.Compare

type Equal = base.Equal

type Split = base.Split

type Comparer = base.Comparer

var DefaultComparer = base.DefaultComparer
