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

//go:build amd64 || arm64 || arm64be || ppc64 || ppc64le || mips64 || mips64le || s390x || sparc64
// +build amd64 arm64 arm64be ppc64 ppc64le mips64 mips64le s390x sparc64

package manual

const (
	MaxArrayLen = 1<<50 - 1
)
