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
#ifndef _stack_h_
#define _stack_h_ 1

#include "runtime/Basics.h"

class Fred;
typedef void PostFunc(Fred*);

// initialize stack and switch directly to 'func(arg1,arg2,arg3,arg4)'
extern "C" void stackDirect(vaddr stack, ptr_t func, ptr_t arg1, ptr_t arg2, ptr_t arg3) __noreturn;
// initialize stack for indirect invocation of 'invokeFred(func,arg1,arg2,arg3)'
extern "C" vaddr stackInit(vaddr stack, ptr_t func, ptr_t arg1, ptr_t arg2, ptr_t arg3);
// save stack to 'currSP', switch to stack in 'nextSP', then call 'postFunc(currFred)'
extern "C" void stackSwitch(Fred* currFred, PostFunc postFunc, vaddr* currSP, vaddr nextSP);

#endif /* _stack_h_ */
