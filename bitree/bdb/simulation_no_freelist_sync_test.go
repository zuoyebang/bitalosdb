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

package bdb_test

import (
	"testing"

	"github.com/zuoyebang/bitalosdb/internal/options"
)

func TestSimulateNoFreeListSync_1op_1p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 1, 1)
}
func TestSimulateNoFreeListSync_10op_1p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 10, 1)
}
func TestSimulateNoFreeListSync_100op_1p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 100, 1)
}
func TestSimulateNoFreeListSync_1000op_1p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 1000, 1)
}
func TestSimulateNoFreeListSync_10000op_1p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 10000, 1)
}
func TestSimulateNoFreeListSync_10op_10p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 10, 10)
}
func TestSimulateNoFreeListSync_100op_10p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 100, 10)
}
func TestSimulateNoFreeListSync_1000op_10p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 1000, 10)
}
func TestSimulateNoFreeListSync_10000op_10p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 10000, 10)
}
func TestSimulateNoFreeListSync_100op_100p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 100, 100)
}
func TestSimulateNoFreeListSync_1000op_100p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 1000, 100)
}
func TestSimulateNoFreeListSync_10000op_100p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 10000, 100)
}
func TestSimulateNoFreeListSync_10000op_1000p(t *testing.T) {
	testSimulate(t, &options.BdbOptions{NoFreelistSync: true}, 8, 10000, 1000)
}
