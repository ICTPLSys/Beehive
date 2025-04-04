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
#ifndef _Platform_h_
#define _Platform_h_ 1

#include "runtime/testoptions.h"

#include <stddef.h>
#include <stdint.h>

// expressions
#define fastpath(x) (__builtin_expect((bool(x)),true))
#define slowpath(x) (__builtin_expect((bool(x)),false))
// functions
#define __yes_inline __attribute__((__always_inline__))
#define __no_inline  __attribute__((__noinline__))
#define __noreturn   __attribute__((__noreturn__))
// data structures
#define __packed     __attribute__((__packed__))
#define __section(x) __attribute__((__section__(x)))
#define __aligned(x) __attribute__((__aligned__(x)))
#ifdef KERNEL
#define __caligned   __attribute__((__aligned__(128)))
#else
#define __caligned
#endif

static inline void unreachable() __noreturn;
static inline void unreachable() {
  __builtin_unreachable();
  __builtin_trap();
}

template <typename T>
static inline constexpr T limit() {
  return ~T(0);
}

template <typename T>
static inline constexpr T slimit() {
  return ~T(0) >> 1;
}

template <typename T>
static inline constexpr T pow2( unsigned int x ) {
  return T(1) << x;
}

template <typename T>
static inline constexpr bool ispow2( T x ) {
  return (x & (x - 1)) == 0;
}

template <typename T>
static inline constexpr T align_up( T x, T a ) {
  return (x + a - 1) & (~(a - 1));
}

template <typename T>
static inline constexpr T align_down( T x, T a ) {
//  return x - (x % a);
  return x & (~(a - 1));
}

template <typename T>
static inline constexpr bool aligned( T x, T a ) {
//  return (x % a) == 0;
  return (x & (a - 1)) == 0;
}

template <typename T>
static inline constexpr T divup( T n, T d ) {
  return ((n - 1) / d) + 1;
}

template <typename T>
static inline constexpr size_t bitsize(); // see below for platform-dependent implementation

template <typename T>
static inline constexpr T bitmask(unsigned int Width) {
  return Width == bitsize<T>() ? limit<T>() : pow2<T>(Width) - 1;
}

template <typename T>
static inline constexpr T bitmask(unsigned int Pos, unsigned int Width) {
  return bitmask<T>(Width) << Pos;
}

#define __var_builtin(x, func) \
  ( sizeof(x) <= sizeof(int)       ? __builtin_ ## func    (x) : \
    sizeof(x) <= sizeof(long)      ? __builtin_ ## func ## l(x) : \
    sizeof(x) <= sizeof(long long) ? __builtin_ ## func ## ll(x) : \
    sizeof(x) * bitsize<char>() )

template <typename T>
static inline constexpr int lsb(T x, T alt = bitsize<T>()) { // result: 0-63 (alt if x = 0)
  return x == 0 ? alt : __var_builtin(x, ctz);
}

template <typename T>
static inline constexpr int msb(T x, T alt = bitsize<T>()) { // result: 0-63 (alt if x = 0)
  return x == 0 ? alt : (bitsize<T>() - 1) - __var_builtin(x, clz);
}

template <typename T>
static inline constexpr int popcount(T x) {
  return __var_builtin(x, popcount);
}

template <typename T>
static inline constexpr int floorlog2( T x ) {
  return msb(x);
}

template <typename T>
static inline constexpr int ceilinglog2( T x ) {
  return msb(x - 1, limit<T>()) + 1; // x = 1 -> msb = -1 (alt) -> result is 0
}

template <typename T>
static inline constexpr int alignment( T x ) {
  return lsb(x);
}

#if defined(__x86_64__) || defined(__aarch64__)

typedef   int64_t sword;
typedef  uint64_t mword;
typedef uintptr_t vaddr;
typedef uintptr_t paddr;

typedef       void*    ptr_t;
typedef const void*   cptr_t;

typedef       char     buf_t;
typedef       char* bufptr_t;

static const vaddr stackAlignment  = 16;

template <typename T> static inline constexpr size_t bitsize() { return sizeof(T) * 8; }

#else
#error unsupported architecture: only __x86_64__ or __aarch64__ supported at this time
#endif

#if defined(__x86_64__)

static inline void Pause()       { asm volatile("pause"); }
static inline void MemoryFence() { asm volatile("mfence" ::: "memory"); }

#elif defined(__aarch64__)

static inline void Pause()       { asm volatile("yield"); }
static inline void MemoryFence() { asm volatile("dmb sy" ::: "memory"); }

#else
#error unsupported architecture: only __x86_64__ or __aarch64__ supported at this time
#endif

struct PauseSpin {
  inline void operator()(void) { Pause(); }
};

template<size_t N>
static inline mword multiscan_next(const mword* data, size_t idx, bool findset = true) {
  if (idx >= N * bitsize<mword>()) return N * bitsize<mword>();
  size_t i = idx / bitsize<mword>();
  mword result = align_down(idx, bitsize<mword>());
  mword datafield = (findset ? data[i] : ~data[i]);
  datafield &= ~bitmask<mword>(idx % bitsize<mword>());
  for (;;) {
    mword b = lsb(datafield);
    result += b;
    if (b < bitsize<mword>()) break;
    i += 1;
    if (i == N) break;
    datafield = (findset ? data[i] : ~data[i]);
  }
  return result;
}

#if defined(__x86_64__)

// depending on the overall code complexity, the loop can be unrolled at -O3
// "=&r"(scan) to mark as 'earlyclobber': modified before all input processed
// "+r"(newmask) to keep newmask = mask
template<size_t N>
static inline mword multiscan(const mword* data, bool findset = true) {
  mword result = 0;
  mword mask = ~mword(0);
  mword newmask = mask;
  size_t i = 0;
  do {
    mword scan;
    mword datafield = (findset ? data[i] : ~data[i]);
    asm volatile("\
      bsfq %2, %0\n\t\
      cmovzq %3, %0\n\t\
      cmovnzq %4, %1"
    : "=&r"(scan), "+r"(newmask)
    : "rm"(datafield), "r"(bitsize<mword>()), "r"(mword(0))
    : "cc");
    result += scan & mask;
    mask = newmask;
    i += 1;
  } while (i < N);

  return result;
}

// depending on the overall code complexity, the loop can be unrolled at -O3
// "=&r"(scan) to mark as 'earlyclobber': modified before all input processed
// "+r"(newmask) to keep newmask = mask
template<size_t N>
static inline mword multiscan_rev(const mword* data, bool findset = true) {
  mword result = 0;
  mword mask = ~mword(0);
  mword newmask = mask;
  size_t i = N;
  do {
    i -= 1;
    mword scan;
    mword datafield = (findset ? data[i] : ~data[i]);
    asm volatile("\
      bsrq %2, %0\n\t\
      cmovzq %3, %0\n\t\
      cmovnzq %4, %1"
    : "=&r"(scan), "+r"(newmask)
    : "rm"(datafield), "r"(mword(0)), "r"(mword(0))
    : "cc");
    result += (scan & mask) + (bitsize<mword>() & ~mask);
    mask = newmask;
  } while (i > 0);
  return result;
}

#else

template<size_t N>
static inline mword multiscan(const mword* data, bool findset = true) {
  return multiscan_next<N>(data, 0, findset);
}

#endif

#endif /* _Platform_h_ */
