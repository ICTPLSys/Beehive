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
#ifndef _syscall_macro_h_
#define _syscall_macro_h_

#include <stdio.h>
#include <sys/errno.h>

#ifndef fastpath
#define fastpath(x)   (__builtin_expect((bool(x)),true))
#endif

#ifndef slowpath
#define slowpath(x)   (__builtin_expect((bool(x)),false))
#endif

#if !defined(SYSCALL_HAVE_ERRNONAME)
static inline char const * errnoname(int) { return ""; }
#endif

#if !defined(SYSCALL_HAVE_SYSERRNO)
static inline int _SysErrno() { return errno; }
static inline int& _SysErrnoSet() { return errno; }
#endif

#if !defined(SYSCALL_HAVE_ABORT)
static inline void _SYSCALLabort() { abort(); }
static inline void _SYSCALLabortLock() {}
static inline void _SYSCALLabortUnlock() {}
#endif

#if TESTING_ENABLE_ASSERTIONS

#ifndef SYSCALL_CMP
#define SYSCALL_CMP(call,cmp,expected,errcode) ({\
  int ret ## __COUNTER__ = call;\
  if slowpath(!(ret ## __COUNTER__ cmp expected || ret ## __COUNTER__ == errcode || (errcode != 0 && _SysErrno() == errcode))) {\
    _SYSCALLabortLock();\
    printf("FAILED SYSCALL at %s:%d\n%s\nEXPECTED %s %lli ACCEPTED: %d RETURN: %d errno: %d %s\n", __FILE__, __LINE__, #call, #cmp, (long long)expected, errcode, ret ## __COUNTER__, _SysErrno(), errnoname(_SysErrno()));\
    _SYSCALLabortUnlock();\
    _SYSCALLabort();\
  }\
  ret ## __COUNTER__; })
#endif

#ifndef SYSCALL_CMP2
#define SYSCALL_CMP2(call,cmp,expected,errcode1,errcode2) ({\
  int ret ## __COUNTER__ = call;\
  if slowpath(!(ret ## __COUNTER__ cmp expected || ret ## __COUNTER__ == errcode1 || ret ## __COUNTER__ == errcode2 || (errcode1 != 0 && _SysErrno() == errcode1) || (errcode2 != 0 && _SysErrno() == errcode2))) {\
    _SYSCALLabortLock();\
    printf("FAILED SYSCALL at %s:%d\n%s\nEXPECTED %s %lli ACCEPTED: %d,%d RETURN: %d errno: %d %s\n", __FILE__, __LINE__, #call, #cmp, (long long)expected, errcode1, errcode2, ret ## __COUNTER__, _SysErrno(), errnoname(_SysErrno()));\
    _SYSCALLabortUnlock();\
    _SYSCALLabort();\
  }\
  ret ## __COUNTER__; })
#endif

#else // TESTING_ENABLE_ASSERTIONS

#ifndef SYSCALL_CMP
#define SYSCALL_CMP(call,cmp,expected,errcode) ({\
  int ret ## __COUNTER__ = call;\
  ret ## __COUNTER__;})
#endif
#ifndef SYSCALL_CMP2
#define SYSCALL_CMP2(call,cmp,expected,errcode1,errcode2) ({\
  int ret ## __COUNTER__ = call;\
  ret ## __COUNTER__;})
#endif

#endif // TESTING_ENABLE_ASSERTIONS

#define SYSCALL(call)                 SYSCALL_CMP(call,==,0,0)
#define SYSCALLIO(call)               SYSCALL_CMP(call,>=,0,0)
#define SYSCALL_EQ(call,val)          SYSCALL_CMP(call,==,val,0)
#define SYSCALL_GE(call,val)          SYSCALL_CMP(call,>=,val,0)
#define TRY_SYSCALL(call,code)        SYSCALL_CMP(call,==,0,code)
#define TRY_SYSCALLIO(call,code)      SYSCALL_CMP(call,>=,0,code)
#define TRY_SYSCALL_EQ(call,val,code) SYSCALL_CMP(call,==,val,code)
#define TRY_SYSCALL_GE(call,val,code) SYSCALL_CMP(call,>=,val,code)

#define TRY_SYSCALL2(call,code1,code2)        SYSCALL_CMP2(call,==,0,code1,code2)
#define TRY_SYSCALLIO2(call,code1,code2)      SYSCALL_CMP2(call,>=,0,code1,code2)
#define TRY_SYSCALL_EQ2(call,val,code1,code2) SYSCALL_CMP2(call,==,val,code1,code2)
#define TRY_SYSCALL_GE2(call,val,code1,code2) SYSCALL_CMP2(call,>=,val,code1,code2)

#endif /* _syscall_macro_h_ */
