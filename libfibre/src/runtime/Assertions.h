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
#ifndef _Assertions_h_
#define _Assertions_h_ 1

#include "runtime/Platform.h"

#include <iostream>
#include <iomanip>

struct FmtHex {
  uintptr_t val;
  int digits;
  FmtHex(uintptr_t p, int d = 0) : val(uintptr_t(p)), digits(d) {}
  FmtHex(   cptr_t p, int d = 0) : val(uintptr_t(p)), digits(d) {}
};

inline std::ostream& operator<<(std::ostream &os, const FmtHex& h) {
  std::ios_base::fmtflags f = os.flags();
  os << "0x" << std::hex << std::uppercase << std::setw(h.digits) << std::setfill('0') << h.val;
  os.flags(f);
  return os;
}

namespace Runtime {
  namespace Assert {
    void lock();
    void unlock();
    void abort() __noreturn;
    void print1(sword);
    void print1(const char*);
    void print1(const FmtHex&);
    void printl();
    static inline void print() {}
    template<typename T, typename... Args>
    static inline void print(const T& v, const Args&... a) {
      print1(v);
      if (sizeof...(a)) print1(" ");
      print(a...);
    }
  }
}

#define RABORT0()                                     { Runtime::Assert::lock(); Runtime::Assert::print(  "ABORT"         " in " __FILE__ ":", __LINE__, __func__);                                      Runtime::Assert::printl(); Runtime::Assert::unlock(); Runtime::Assert::abort(); }
#define RABORT(args...)                               { Runtime::Assert::lock(); Runtime::Assert::print(  "ABORT"         " in " __FILE__ ":", __LINE__, __func__); Runtime::Assert::print(" - ", args); Runtime::Assert::printl(); Runtime::Assert::unlock(); Runtime::Assert::abort(); }
#if TESTING_ENABLE_ASSERTIONS
#define RASSERT0(expr)         { if slowpath(!(expr)) { Runtime::Assert::lock(); Runtime::Assert::print( "ASSERT: " #expr " in " __FILE__ ":", __LINE__, __func__);                                      Runtime::Assert::printl(); Runtime::Assert::unlock(); Runtime::Assert::abort(); } }
#define RASSERT(expr, args...) { if slowpath(!(expr)) { Runtime::Assert::lock(); Runtime::Assert::print( "ASSERT: " #expr " in " __FILE__ ":", __LINE__, __func__); Runtime::Assert::print(" - ", args); Runtime::Assert::printl(); Runtime::Assert::unlock(); Runtime::Assert::abort(); } }
#define RCHECK0(expr)          { if slowpath(!(expr)) { Runtime::Assert::lock(); Runtime::Assert::print( " CHECK: " #expr " in " __FILE__ ":", __LINE__, __func__);                                      Runtime::Assert::printl(); Runtime::Assert::unlock(); } }
#define RCHECK(expr, args...)  { if slowpath(!(expr)) { Runtime::Assert::lock(); Runtime::Assert::print( " CHECK: " #expr " in " __FILE__ ":", __LINE__, __func__); Runtime::Assert::print(" - ", args); Runtime::Assert::printl(); Runtime::Assert::unlock(); } }
#else
#define RASSERT0(expr)         if (expr) {}
#define RASSERT(expr, args...) if (expr) {}
#define RCHECK0(expr)          if (expr) {}
#define RCHECK(expr, args...)  if (expr) {}
#endif

#endif /* _Assertions_h_ */
