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

//go:build !noasm && amd64

TEXT ·InitMem128(SB), $0-32
	MOVQ a+0(FP), AX
	MOVQ b+8(FP), BX
	MOVQ n+16(FP), DX
	MOVQ $0, CX
	// MOVUPS (BX), X0
	MOVUPS (BX), X0
LOOP:
	CMPQ CX, DX      // 比较 n 和 0
    JE DONE         // 如果 n <= 0，跳转到DONE
	MOVUPS X0, (AX) // 128bit
	INCQ CX      // i++
	ADDQ $16, AX
	JMP LOOP        // 跳转到LOOP
DONE:       
    RET
