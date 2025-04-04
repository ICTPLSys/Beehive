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
#ifndef _ScopedLocks_h_
#define _ScopedLocks_h_ 1

#include "runtime/SpinLocks.h"

struct DummyLock {
  void acquire() {}
  void release() {}
};

template <typename Lock>
class ScopedLock {
  Lock& lk;
public:
  ScopedLock(Lock& lk) : lk(lk) { lk.acquire(); }
  ~ScopedLock() { lk.release(); }
};

template<>
class ScopedLock<MCSLock> {
  MCSLock& lk;
  MCSLock::Node node;
public:
  ScopedLock(MCSLock& lk) : lk(lk) { lk.acquire(node); }
  ~ScopedLock() { lk.release(node); }
};

template <typename Lock>
class ScopedLockRead {
  Lock& lk;
public:
  ScopedLockRead(Lock& lk) : lk(lk) { lk.acquireRead(); }
  ~ScopedLockRead() { lk.release(); }
};

template <typename Lock>
class ScopedLockWrite {
  Lock& lk;
public:
  ScopedLockWrite(Lock& lk) : lk(lk) { lk.acquireWrite(); }
  ~ScopedLockWrite() { lk.release(); }
};

#endif /* _ScopedLocks_h_ */
