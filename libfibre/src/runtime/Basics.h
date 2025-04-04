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
#ifndef _Basics_h_
#define _Basics_h_ 1

#include "runtime/Assertions.h"
#include "runtime/FloatingPoint.h"
#include "runtime/Platform.h"

#include <ctime> // struct timespec

typedef void (*funcvoid0_t)();
typedef void (*funcvoid1_t)(ptr_t);
typedef void (*funcvoid2_t)(ptr_t, ptr_t);
typedef void (*funcvoid3_t)(ptr_t, ptr_t, ptr_t);

typedef void* (*funcptr0_t)();
typedef void* (*funcptr1_t)(ptr_t);
typedef void* (*funcptr2_t)(ptr_t, ptr_t);
typedef void* (*funcptr3_t)(ptr_t, ptr_t, ptr_t);

class NoObject {
  NoObject() = delete;                           // no creation
  NoObject(const NoObject&) = delete;            // no copy
  NoObject& operator=(const NoObject&) = delete; // no assignment
};

template<typename Friend> class _friend {
  friend Friend;
  _friend() {}
};

template <typename T, unsigned int Pos, unsigned int Width>
struct BitString {
  static_assert( Pos + Width <= 8*sizeof(T), "illegal parameters" );
  constexpr T operator()() const { return bitmask<T>(Pos,Width); }
  constexpr T put(T f) const { return (f & bitmask<T>(Width)) << Pos; }
  constexpr T get(T f) const { return (f >> Pos) & bitmask<T>(Width); }
  constexpr T excl(T f) const { return f & ~bitmask<T>(Pos,Width); }
};

class Time : public timespec {
  friend std::ostream& operator<<(std::ostream&, const Time&);
public:
  static const long long NSEC = 1000000000ll;
  static const long long USEC = 1000000ll;
  static const long long MSEC = 1000ll;
  Time() {}
  Time(const volatile Time& t) : timespec({t.tv_sec, t.tv_nsec}) {}
  Time(const Time& t) : timespec({t.tv_sec, t.tv_nsec}) {}
  Time(time_t sec, long nsec) : timespec({sec, nsec}) {}
  Time(const timespec& t) : timespec(t) {}
  static const Time zero() { return Time(0,0); }
  static Time fromMS(long long ms) {
    return Time(ms / 1000, (ms % 1000) * 1000000);
  }
  long long toMS() const {
    return 1000ll * tv_sec + tv_nsec / 1000000;
  }
  static Time fromUS(long long us) {
    return Time(us / 1000000, (us % 1000000) * 1000);
  }
  long long toUS() const {
    return 1000000ll * tv_sec + tv_nsec / 1000;
  }
  static Time fromNS(long long ns) {
    return Time(ns / 1000000000, ns % 1000000000);
  }
  long long toNS() const {
    return 1000000000ll * tv_sec + tv_nsec;
  }
  Time& operator=(const Time& t) {
    tv_sec  = t.tv_sec;
    tv_nsec = t.tv_nsec;
    return *this;
  }
  Time& operator+=(const Time& t) {
    tv_sec  += t.tv_sec;
    tv_nsec += t.tv_nsec;
    if (tv_nsec > NSEC) { tv_sec += 1; tv_nsec -= NSEC; }
    return *this;
  }
  Time operator+(const Time& t) const {
    Time v = {tv_sec + t.tv_sec, tv_nsec + t.tv_nsec};
    if (v.tv_nsec > NSEC) { v.tv_sec += 1; v.tv_nsec -= NSEC; }
    return v;
  }
  Time& operator-=(const Time& t) {
    tv_nsec -= t.tv_nsec;
    tv_sec  -= t.tv_sec;
    if (tv_nsec < 0) { tv_sec -= 1; tv_nsec += NSEC; }
    return *this;
  }
  Time operator-(const Time& t) const {
    Time v = {tv_sec - t.tv_sec, tv_nsec - t.tv_nsec};
    if (v.tv_nsec < 0) { v.tv_sec -= 1; v.tv_nsec += NSEC; }
    return v;
  }
  bool operator==(const Time& t) const {
    return tv_sec == t.tv_sec && tv_nsec == t.tv_nsec;
  }
  bool operator<(const Time& t) const {
    return (tv_sec == t.tv_sec) ? tv_nsec < t.tv_nsec : tv_sec < t.tv_sec;
  }
  bool operator<=(const Time& t) const {
    return (tv_sec == t.tv_sec) ? tv_nsec <= t.tv_nsec : tv_sec <= t.tv_sec;
  }
  bool operator>(const Time& t) const {
    return (tv_sec == t.tv_sec) ? tv_nsec > t.tv_nsec : tv_sec > t.tv_sec;
  }
  bool operator>=(const Time& t) const {
    return (tv_sec == t.tv_sec) ? tv_nsec >= t.tv_nsec : tv_sec >= t.tv_sec;
  }
};

extern inline std::ostream& operator<<(std::ostream& os, const Time& t) {
  os << t.tv_sec << '.' << std::setw(9) << std::setfill('0') << t.tv_nsec;
  return os;
}

#endif /* _Basics_h_ */
