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
#ifndef _StackFrame_h_
#define _StackFrame_h_ 1

// ISR_PUSH/POP: save callee-owned registers during asynchronous interrupt
// caller-owned regs automatically saved by compiler code during routine calls

// STACK_PUSH/POP: save caller-owned registers during synchronous stack switch
// callee-owned regs automatically saved by compiler code before routine calls

#if defined(__x86_64__)

// GCC: arguments in %rdi, %rsi, %rdx, %rcx , %r8, %r9, see 'System V AMD64 ABI' in
// see http://en.wikipedia.org/wiki/X86_calling_conventions

// caller- vs. callee-owned registers
// see http://x86-64.org/documentation/abi.pdf, Sec 3.2.1

.set ISRFRAME, 72

.macro ISR_PUSH                       /* ISRFRAME bytes pushed */
	pushq %rax
	pushq %rcx
	pushq %rdx
	pushq %rdi
	pushq %rsi
	pushq %r8
	pushq %r9
	pushq %r10
	pushq %r11
.endm

.macro ISR_POP                        /* ISRFRAME bytes popped */
	popq %r11
	popq %r10
	popq %r9
	popq %r8
	popq %rsi
	popq %rdi
	popq %rdx
	popq %rcx
	popq %rax
.endm

.macro STACK_PUSH
  pushq %rbp
  movq  %rsp,%rbp                    /* produce clean stack for debugging */
  pushq %r15
  pushq %r14
  pushq %r13
  pushq %r12
  pushq %rbx
.endm

.macro STACK_POP
  popq %rbx
  popq %r12
  popq %r13
  popq %r14
  popq %r15
  popq %rbp
.endm

#elif defined(__aarch64__)

.set ISRFRAME, 160                    /* stack alignment 16 */

.macro ISR_PUSH                       /* ISRFRAME bytes pushed */
  stp x0, x1,   [sp, -160]!           /* pre-index: SP -= 160 */
  stp x2, x3,   [sp, 16]
  stp x4, x5,   [sp, 32]
  stp x6, x7,   [sp, 48]
  stp x8, x9,   [sp, 64]
  stp x10, x11, [sp, 80]
  stp x12, x13, [sp, 96]
  stp x14, x15, [sp, 112]
  stp x16, x17, [sp, 128]
  str x18,      [sp, 144]
.endm

.macro ISR_POP                        /* ISRFRAME bytes popped */
  ldr x18,      [sp, 144]
  ldp x16, x17, [sp, 128]
  ldp x14, x15, [sp, 112]
  ldp x12, x13, [sp, 96]
  ldp x10, x11, [sp, 80]
  ldp x8, x9,   [sp, 64]
  ldp x6, x7,   [sp, 48]
  ldp x4, x5,   [sp, 32]
  ldp x2, x3,   [sp, 16]
  ldp x0, x1,   [sp], 160             /* post-index: SP += 160 */
.endm

.macro STACK_PUSH
  stp x29, x30, [sp, -160]!           /* pre-index: SP -= 160 */
  mov x29, sp                         /* produce clean stack for debugging */
  stp x27, x28, [sp, 16]
  stp x25, x26, [sp, 32]
  stp x23, x24, [sp, 48]
  stp x21, x22, [sp, 64]
  stp x19, x20, [sp, 80]
  stp d8,  d9,  [sp, 96]              /* low 64-bit of FP regs  v7-v15 */
  stp d10, d11, [sp, 112]
  stp d12, d13, [sp, 128]
  stp d14, d15, [sp, 144]

.endm

.macro STACK_POP
  ldp d14, d15, [sp, 144]             /* low 64-bit of FP regs  v7-v15 */
  ldp d12, d13, [sp, 128]
  ldp d10, d11, [sp, 112]
  ldp d8,  d9,  [sp, 96]
  ldp x19, x20, [sp, 80]
  ldp x21, x22, [sp, 64]
  ldp x23, x24, [sp, 48]
  ldp x25, x26, [sp, 32]
  ldp x27, x28, [sp, 16]
  ldp x29, x30, [sp], 160             /* post-index: SP += 160 */
.endm

#else
#error unsupported architecture: only __x86_64__ or __aarch64__ supported at this time
#endif

#endif /* _StackFrame_h_ */
