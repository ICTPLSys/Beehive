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
#ifndef _HaltSemaphore_h_
#define _HaltSemaphore_h_ 1

#include "runtime-glue/RuntimeLock.h"

class BaseProcessor;

#if TESTING_WORKER_IO_URING || TESTING_WORKER_POLLER

extern bool RuntimeWorkerPoll(BaseProcessor&);
extern bool RuntimeWorkerTrySuspend(BaseProcessor&);
extern void RuntimeWorkerSuspend(BaseProcessor&);
extern void RuntimeWorkerResume(BaseProcessor&);

struct HaltSemaphore {
  HaltSemaphore(size_t c)     { RASSERT(c == 0, c); }
  bool tryP(BaseProcessor& p) { return RuntimeWorkerTrySuspend(p); }
  bool    P(BaseProcessor& p) { RuntimeWorkerSuspend(p); return true; }
  void    V(BaseProcessor& p) { RuntimeWorkerResume(p); }
};

#else

static inline bool RuntimeWorkerPoll(BaseProcessor&) { return false; }

struct HaltSemaphore : public WorkerSemaphore {
  HaltSemaphore(size_t c) : WorkerSemaphore(c) {}
  bool tryP(BaseProcessor&) { return WorkerSemaphore::tryP(); }
  bool    P(BaseProcessor&) { return WorkerSemaphore::P(); }
  void    V(BaseProcessor&) { WorkerSemaphore::V(); }
};

#endif

#endif /* _HaltSemaphore_h_ */
