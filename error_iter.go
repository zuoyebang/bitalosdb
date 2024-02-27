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

type errorIter struct {
	err error
}

var _ base.InternalIterator = (*errorIter)(nil)

func newErrorIter(err error) *errorIter {
	return &errorIter{err: err}
}

func (c *errorIter) SeekGE(key []byte) (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) SeekLT(key []byte) (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) First() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Last() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Next() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Prev() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Key() *InternalKey {
	return nil
}

func (c *errorIter) Value() []byte {
	return nil
}

func (c *errorIter) Valid() bool {
	return false
}

func (c *errorIter) Error() error {
	return c.err
}

func (c *errorIter) Close() error {
	return c.err
}

func (c *errorIter) String() string {
	return "error"
}

func (c *errorIter) SetBounds(lower, upper []byte) {}
