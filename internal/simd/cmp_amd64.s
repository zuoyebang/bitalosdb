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

#include "textflag.h"

// func equal(a, b [16]byte) bool
TEXT ·Equal128(SB),NOSPLIT,$0
    MOVQ a+0(FP), DI    // 加载a的地址到DI
    MOVQ b+8(FP), SI   // 加载b的地址到SI
    
    MOVOU (DI), X0      // 加载a的16字节到XMM0
    MOVOU (SI), X1      // 加载b的16字节到XMM1
    
    PCMPEQB X1, X0      // 按字节比较，相等的位置置为FFh
    PMOVMSKB X0, AX     // 将符号位打包到AX寄存器
    
    CMPL AX, $0xffff    // 检查所有16个比较位是否全1
    SETEQ AX            // 相等时设置AL=1，否则0
    MOVB AX, ret+16(FP) // 返回布尔值
    RET
