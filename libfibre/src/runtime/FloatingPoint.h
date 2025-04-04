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
#ifndef _FloatingPoint_h_
#define _FloatingPoint_h_ 1

#include <stdint.h>

#if defined(__x86_64__)

class FloatingPointFlags { // FP (x87/SSE) control/status words (ABI Section 3.2.3, Fig 3.4)
  uint32_t csr;
  uint32_t cw;
public:
  FloatingPointFlags(uint32_t csr = 0x1FC0, uint32_t cw = 0x037F) : csr(csr), cw(cw) {}
  FloatingPointFlags(bool s) { if (s) save(); }
  void save() {
    asm volatile("stmxcsr %0" : "=m"(csr) :: "memory");
    asm volatile("fnstcw  %0" : "=m"(cw) :: "memory");
  }
  void restore() {
    asm volatile("ldmxcsr %0" :: "m"(csr) : "memory");
    asm volatile("fldcw   %0" :: "m"(cw) : "memory");
  }
};

class FloatingPointContext {
  char fpu[512] __attribute__((__aligned__(16)));     // alignment required for fxsave/fxrstor
  enum State { Init = 0, Clean = 1, Dirty = 2 } state;
public:
  FloatingPointContext() : state(Init) {}
  void setClean() { state = Clean; }
  bool  isClean() const { return state == Clean; }
  static void initCPU() {
    asm volatile("finit" ::: "memory");
  }
  void save() {    // TODO: later use XSAVEOPTS for complete SSE/AVX/etc state!
    asm volatile("fxsave %0" : "=m"(fpu) :: "memory");
    state = Dirty;
  }
  void restore() { // TODO: later use XRSTOR for complete SSE/AVX/etc state!
    if (state == Dirty) asm volatile("fxrstor %0" :: "m"(fpu) : "memory");
    else if (state == Init) initCPU();
    state = Clean;
  }
};

#elif defined(__aarch64__)

class FloatingPointFlags {
public:
  FloatingPointFlags() {}
  FloatingPointFlags(bool) {}
  void save() {}
  void restore() {}
};

class FloatingPointContext; // not available for __aarch64__ yet

#else
#error unsupported architecture: only __x86_64__ or __aarch64__ supported at this time
#endif

#endif /* _FloatingPoint_h_ */
