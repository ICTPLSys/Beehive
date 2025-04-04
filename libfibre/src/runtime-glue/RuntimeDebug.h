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
#ifndef _RuntimeDebug_h_
#define _RuntimeDebug_h_ 1

#include "runtime/ScopedLocks.h"
#include "runtime-glue/RuntimeLock.h"

#if defined(__FreeBSD__)
#include <sys/thr.h>     // thr_self
#else // __linux__ below
#include <unistd.h>      // syscall
#include <sys/syscall.h> // __NR_gettid
#endif

enum DBG::Level : size_t {
  Basic = 0,
  Blocking,
  Polling,
  Scheduling,
  Threads,
  Warning,
  MaxLevel
};

static inline void dprint() {}

template<typename T, typename... Args>
inline void dprint(T x, const Args&... a) {
  std::cerr << x;
  dprint(a...);
}

static inline void dprintl() {
  std::cerr << std::endl;
}

static inline void dprinttid() {
#if defined(__FreeBSD__)
  long tid;
  thr_self(&tid);
  dprint(tid, ' ');
#else // __linux__ below
  dprint(syscall(__NR_gettid), ' ');
#endif
}

extern WorkerLock* _lfDebugOutputLock;

#if 1
template <typename... Args>
inline void DBG::out1(DBG::Level, const Args&...) {}

template <typename... Args>
inline void DBG::outs(DBG::Level, const Args&...) {}

template <typename... Args>
inline void DBG::outl(DBG::Level, const Args&...) {}

inline void DBG::outl(DBG::Level) {}
#else
template<typename... Args>
inline void DBG::out1(DBG::Level c, const Args&... a) {
  if (c && !test(c)) return;
  dprint(a...);
}

template<typename... Args>
inline void DBG::outs(DBG::Level c, const Args&... a) {
  if (c && !test(c)) return;
  dprinttid();
  dprint(a...);
}

template<typename... Args>
inline void DBG::outl(DBG::Level c, const Args&... a) {
  if (c && !test(c)) return;
  ScopedLock<WorkerLock> sl(*_lfDebugOutputLock);
  dprinttid();
  dprint(a...);
  dprintl();
}

inline void DBG::outl(DBG::Level c) {
  if (c && !test(c)) return;
  dprintl();
}
#endif

#endif /* _RuntimeDebug_h_ */
