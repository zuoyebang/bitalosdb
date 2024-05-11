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

package options

import "github.com/zuoyebang/bitalosdb/internal/base"

type IterOptions struct {
	LowerBound   []byte
	UpperBound   []byte
	Logger       base.Logger
	SlotId       uint32
	IsAll        bool
	DisableCache bool
}

func (o *IterOptions) GetLowerBound() []byte {
	if o == nil {
		return nil
	}
	return o.LowerBound
}

func (o *IterOptions) GetUpperBound() []byte {
	if o == nil {
		return nil
	}
	return o.UpperBound
}

func (o *IterOptions) GetLogger() base.Logger {
	if o == nil || o.Logger == nil {
		return base.DefaultLogger
	}
	return o.Logger
}

func (o *IterOptions) GetSlotId() uint32 {
	if o == nil {
		return 0
	}
	return o.SlotId
}

func (o *IterOptions) IsGetAll() bool {
	return o == nil || o.IsAll
}
