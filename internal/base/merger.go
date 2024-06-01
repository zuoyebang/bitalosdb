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

package base

import "io"

type Merge func(key, value []byte) (ValueMerger, error)

type ValueMerger interface {
	MergeNewer(value []byte) error
	MergeOlder(value []byte) error
	Finish(includesBase bool) ([]byte, io.Closer, error)
}

type Merger struct {
	Merge Merge
	Name  string
}

type AppendValueMerger struct {
	buf []byte
}

func (a *AppendValueMerger) MergeNewer(value []byte) error {
	a.buf = append(a.buf, value...)
	return nil
}

func (a *AppendValueMerger) MergeOlder(value []byte) error {
	buf := make([]byte, len(a.buf)+len(value))
	copy(buf, value)
	copy(buf[len(value):], a.buf)
	a.buf = buf
	return nil
}

func (a *AppendValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return a.buf, nil, nil
}

var DefaultMerger = &Merger{
	Merge: func(key, value []byte) (ValueMerger, error) {
		res := &AppendValueMerger{}
		res.buf = append(res.buf, value...)
		return res, nil
	},

	Name: "bitalosdb.concatenate",
}
