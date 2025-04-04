/******************************************************************************
    Copyright (C) Martin Karsten 2015-2023

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
#include "runtime/StackFrame.h"

.text

#if defined(__x86_64__)

.p2align 4
.globl stackDirect
.type stackDirect, @function
stackDirect:                // stack pointer, func, arg1, arg2, arg3
	movq %rdi, %rsp           // set stack pointer
	movq %rsi, %rax           // save 'func' in %rax
	movq %rdx, %rdi           // put arg1 in place
	movq %rcx, %rsi           // put arg2 in place
	movq %r8,  %rdx           // put arg3 in place
//movq %r9,  %rcx           // put arg4 in place // not needed
	pushq $0                  // previous %rip = 0 (fake stack frame)
	pushq $0                  // previous %rbp = 0
	movq %rsp, %rbp           // set base pointer
	jmp *%rax                 // invoke 'func'
.size stackDirect, .-stackDirect

.p2align 4
.globl stackInit
.type stackInit, @function
stackInit:                  // stack, func, arg1, arg2, arg3 -> new stack
	movq %rsi, -64(%rdi)      // store 'func' for stub function (via %rbx)
	movq %rdx, -56(%rdi)      // store 'arg1' for stub function (via %r12)
	movq %rcx, -48(%rdi)      // store 'arg2' for stub function (via %r13)
	movq %r8,  -40(%rdi)      // store 'arg3' for stub function (via %r14)
	movq $0,   -32(%rdi)      // indirectly set %r15 to 0
	movq $0,   -24(%rdi)      // indirectly set %rbp to 0
//movq $stubInit, -16(%rdi) // push stubInit function as return address
	leaq stubInit(%rip), %rax // alternative: use RIP-relative addressing
	movq %rax, -16(%rdi)      // to push stubInit function as return address
	leaq -64(%rdi), %rax      // return stack address, size 64: cf. STACK_PUSH
	retq
.size stackInit, .-stackInit

.p2align 4
.type stubInit, @function
stubInit:                   // stub needed to pass arguments to 'invokeFred'
	movq %rbx, %rdi           // 'func'
	movq %r12, %rsi           // 'arg1'
	movq %r13, %rdx           // 'arg2'
	movq %r14, %rcx           // 'arg3'
//movq %r15, %r8            // 'arg4' not needed
	pushq %rbp                // previous %rip = 0 (fake stack frame)
	pushq %rbp                // previous %rbp = 0
	movq %rsp, %rbp           // set base pointer
	jmp invokeFred@PLT
.size stubInit, .-stubInit

.p2align 4
.globl stackSwitch
.type stackSwitch, @function
stackSwitch:                // (currStack, postFunc, &currSP, nextSP)
	STACK_PUSH                // save register context
	movq %rsp, (%rdx)         // save current stack pointer
	movq %rcx, %rsp           // load next stack pointer
	STACK_POP                 // restore register context
	jmp *%rsi                 // %rdi set -> return directly from postFunc
.size stackSwitch, .-stackSwitch

#elif defined(__aarch64__)

.align  2
.p2align 3,,7
.globl stackDirect
.type stackDirect, @function
stackDirect:                // stack pointer, func, arg1, arg2, arg3
  mov sp, x0                // set stack pointer
  mov x9, x1                // save 'func' in x9
  mov x0, x2                // put arg1 in place
  mov x1, x3                // put arg2 in place
  mov x2, x4                // put arg3 in place
//mov x3, x5                // put arg4 in place // not needed
  stp xzr, xzr, [sp, -16]!  // previous x29 (fp) = 0; previous x30 (lr) = 0 (fake stack frame)
  mov x29, sp               // set frame pointer
  br x9                     // invoke 'func'
.size stackDirect, .-stackDirect

.align  2
.p2align 3,,7
.globl stackInit
.type stackInit, @function
stackInit:                  // stack, func, arg1, arg2, arg3 -> new stack
  ldr x9, =stubInit         // load stubInit function address into x9
  stp xzr, x9,  [x0, -160]! // indirectly set x29 (fp) to 0; push stubInit function as return address - x30 (lr)
  stp xzr, xzr, [x0, 16]    // indirectly set x27, x28 to 0
  stp xzr, xzr, [x0, 32]    // indirectly set x25, x26 to 0
  stp xzr, xzr, [x0, 48]    // indirectly set x23, x24 to 0
  stp x3,  x4,  [x0, 64]    // store 'arg2' for stub function (via x21); store 'arg3' for stub function (via x22)
  stp x1,  x2,  [x0, 80]    // store 'func' for stub function (via x19); store 'arg1' for stub function (via x20)
  stp xzr, xzr, [x0, 96]    // indirectly set d8, d9 to 0
  stp xzr, xzr, [x0, 112]   // indirectly set d10, d11 to 0
  stp xzr, xzr, [x0, 128]   // indirectly set d12, d13 to 0
  stp xzr, xzr, [x0, 144]   // indirectly set d14, d15 to 0
  ret                       // return stack address (x0), size 96: cf. STACK_PUSH
.size stackInit, .-stackInit

.align  2
.p2align 3,,7
.type stubInit, @function
stubInit:                   // stub needed to pass arguments to 'invokeFred'
  mov x0, x19               // 'func'
  mov x1, x20               // 'arg1'
  mov x2, x21               // 'arg2'
  mov x3, x22               // 'arg3'
//mov x4, x23               // 'arg4' not needed
  stp xzr, xzr, [sp, -16]!  // previous x29 (fp) = 0; previous x30 (lr) = 0 (fake stack frame)
  mov x29, sp               // set frame pointer
  b invokeFred
.size stubInit, .-stubInit

.align  2
.p2align 3,,7
.globl stackSwitch
.type stackSwitch, @function
stackSwitch:                // (currStack, postFunc, &currSP, nextSP)
  STACK_PUSH                // save register context
  mov x9, sp                // copy current stack pointer into an integer register
  str x9, [x2]              // save current stack pointer
  mov sp, x3                // load next stack pointer
  STACK_POP                 // restore register context
  br x1                     // x0 set -> return directly from postFunc
.size stackSwitch, .-stackSwitch

#else
#error unsupported architecture: only __x86_64__ or __aarch64__ supported at this time
#endif
