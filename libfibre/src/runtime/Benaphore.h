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
#ifndef _Benaphore_h_
#define _Benaphore_h_ 1

#include "runtime/Assertions.h"
#include <sys/types.h> // ssize_t

template<bool Binary = false>
class Benaphore {
protected:
  volatile ssize_t counter;

public:
  explicit Benaphore(ssize_t c = 0) : counter(c) {}
  void reset(ssize_t c = 0) { counter = c; }
  ssize_t getValue() const { return counter; }

  bool P() { // true: success (no blocking needed)
    return __atomic_fetch_sub(&counter, 1, __ATOMIC_SEQ_CST) > 0;
  }

  bool tryP() { // true: success
    ssize_t c = counter;
    return (c >= 1) && __atomic_compare_exchange_n(&counter, &c, c-1, false, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);
  }

  bool V() { // true: success (no resume needed)
    if (!Binary) return __atomic_add_fetch(&counter, 1, __ATOMIC_SEQ_CST) > 0;
    // short cut (counter == 1)? no memory synchronization then...
    for (ssize_t c = 0;;) {
      if (__atomic_compare_exchange_n(&counter, &c, c+1, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        if (c == 0) return true;
        RASSERT(c < 0, c);
        return false;
      } else {
        if (c == 1) return true;
        RASSERT(c < 1, c);
        Pause();
      }
    }
  }
};

#endif /* _Benaphore_h_ */
