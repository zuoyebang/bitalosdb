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

//go:build amd64

#include "textflag.h"

TEXT ·MatchMetadata(SB), NOSPLIT, $0-18
	MOVQ     metadata+0(FP), AX
	MOVBLSX  hash+8(FP), CX
	MOVD     CX, X0
	PXOR     X1, X1
	PSHUFB   X1, X0
	MOVOU    (AX), X1
	PCMPEQB  X1, X0
	PMOVMSKB X0, AX
	MOVW     AX, ret+16(FP)
	RET
