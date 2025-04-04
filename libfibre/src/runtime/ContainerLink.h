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
#ifndef _ContainerLink_h_
#define _ContainerLink_h_ 1

#include "runtime/Platform.h"

template<typename T,size_t CNT=1> class SingleLink {
  struct {
    union {
      T* next;
      T* volatile vnext;
    };
  } link[CNT];
  template<size_t NUM=0> static void check() { static_assert(NUM < CNT, "NUM >= CNT"); }
public:
  SingleLink() { for (size_t i = 0; i < CNT; i++) link[i].next = nullptr; }
  template<size_t NUM=0>        inline bool         valid() const { check<NUM>(); return link[NUM].vnext; }
  template<size_t NUM=0> static inline T*&          Next(T& elem) { check<NUM>(); return elem.link[NUM].next; }
  template<size_t NUM=0> static inline T*volatile& VNext(T& elem) { check<NUM>(); return elem.link[NUM].vnext; }
};

template<typename T,size_t CNT=1> class DoubleLink {
  friend class SingleLink<T,CNT>;
  struct {
    union {
      T* next;
      T* volatile vnext;
    };
    union {
      T* prev;
      T* volatile vprev;
    };
  } link[CNT];
  template<size_t NUM=0> static void check() { static_assert(NUM < CNT, "NUM >= CNT"); }
public:
  DoubleLink() { for (size_t i = 0; i < CNT; i++) link[i].next = link[i].prev = nullptr; }
  template<size_t NUM=0>        inline bool         valid() const { check<NUM>(); return link[NUM].vnext; }
  template<size_t NUM=0> static inline T*&          Next(T& elem) { check<NUM>(); return elem.link[NUM].next; }
  template<size_t NUM=0> static inline T*volatile& VNext(T& elem) { check<NUM>(); return elem.link[NUM].vnext; }
  template<size_t NUM=0> static inline T*&          Prev(T& elem) { check<NUM>(); return elem.link[NUM].prev; }
  template<size_t NUM=0> static inline T*volatile& VPrev(T& elem) { check<NUM>(); return elem.link[NUM].vprev; }
};

#endif /* _ContainerLink_h_ */
