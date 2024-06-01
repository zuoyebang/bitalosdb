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

import "github.com/zuoyebang/bitalosdb/internal/base"

const (
	fileTypeLog      = base.FileTypeLog
	fileTypeLock     = base.FileTypeLock
	fileTypeManifest = base.FileTypeManifest
	fileTypeMeta     = base.FileTypeMeta
	fileTypeCurrent  = base.FileTypeCurrent
)

const (
	InternalKeyKindDelete       = base.InternalKeyKindDelete
	InternalKeyKindSet          = base.InternalKeyKindSet
	InternalKeyKindLogData      = base.InternalKeyKindLogData
	InternalKeyKindPrefixDelete = base.InternalKeyKindPrefixDelete
	InternalKeyKindMax          = base.InternalKeyKindMax
	InternalKeySeqNumMax        = base.InternalKeySeqNumMax
)

type InternalKeyKind = base.InternalKeyKind

type InternalKey = base.InternalKey

type internalIterator = base.InternalIterator

type FileNum = base.FileNum

type Compare = base.Compare

type Equal = base.Equal

type Split = base.Split

type Comparer = base.Comparer

type Logger = base.Logger

var DefaultComparer = base.DefaultComparer

var DefaultLogger = base.DefaultLogger
