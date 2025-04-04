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
#ifndef _Scheduler_h_
#define _Scheduler_h_ 1

#include "runtime/BaseProcessor.h"

#if TESTING_LOADBALANCING

#if TESTING_GO_IDLEMANAGER

class IdleManager {
  volatile size_t spinCounter;
  volatile size_t waitCounter;
  WorkerLock      procLock;
  ProcessorList   waitingProcs;

public:
  void incSpinning() { __atomic_add_fetch(&spinCounter, 1, __ATOMIC_SEQ_CST); }
  void decSpinning() { __atomic_sub_fetch(&spinCounter, 1, __ATOMIC_SEQ_CST); }
  void incWaiting()  { __atomic_add_fetch(&waitCounter, 1, __ATOMIC_SEQ_CST); }
  void decWaiting()  { __atomic_sub_fetch(&waitCounter, 1, __ATOMIC_SEQ_CST); }
  void unblockSpin() {
    if (__atomic_sub_fetch(&spinCounter, 1, __ATOMIC_SEQ_CST) == 0) unblock();
  }

  void block(BaseProcessor& proc) {
    procLock.acquire();
    proc.setHalting(true, _friend<IdleManager>());
    waitingProcs.push_front(proc);
    procLock.release();
    proc.halt(_friend<IdleManager>());
  }

  void unblock(BaseProcessor* nextProc = nullptr) {
    while (waitCounter > 0) {
      if (!procLock.tryAcquire()) {
        Pause();
      } else if (waitingProcs.empty()) {
        procLock.release();
        Pause();
      } else {
        if (nextProc && nextProc->isHalting(_friend<IdleManager>())) {
          waitingProcs.remove(*nextProc);
        } else {
          nextProc = waitingProcs.pop_front();
        }
        nextProc->setHalting(false, _friend<IdleManager>());
        procLock.release();
        decWaiting();
        nextProc->wake(nullptr, _friend<IdleManager>());
        return;
      }
    }
  }

  IdleManager(cptr_t) : spinCounter(0), waitCounter(0) {}
  void reset(cptr_t, _friend<EventScope>) {}
};

#else /* TESTING_GO_IDLEMANAGER */

class IdleManager {
  volatile ssize_t         fredCounter;
  WorkerLock               procLock;
  ProcessorList            waitingProcs;
  FredQueue<FredReadyLink> waitingFreds;

  FredStats::IdleManagerStats* stats;

  Fred* block(BaseProcessor& proc) {
    procLock.acquire();
    if (waitingFreds.empty()) {
      proc.setHalting(true, _friend<IdleManager>());
      waitingProcs.push_front(proc);
      procLock.release();
      return proc.halt(_friend<IdleManager>());
    } else {
      Fred* nextFred = waitingFreds.pop();
      procLock.release();
      return nextFred;
    }
  }

  void unblock(Fred& fred, BaseProcessor* nextProc) {
    procLock.acquire();
    if (waitingProcs.empty()) {
      waitingFreds.push(fred);
      procLock.release();
    } else {
      if (nextProc->isHalting(_friend<IdleManager>())) {
        waitingProcs.remove(*nextProc);
      } else {
        nextProc = waitingProcs.pop_front();
      }
      nextProc->setHalting(false, _friend<IdleManager>());
      procLock.release();
      nextProc->wake(&fred, _friend<IdleManager>());
    }
  }

public:
  IdleManager(cptr_t parent) : fredCounter(0) { stats = new FredStats::IdleManagerStats(this, parent); }

  void reset(cptr_t parent, _friend<EventScope>) {
    new (stats) FredStats::IdleManagerStats(this, parent);
  }

  bool tryGetReadyFred() {
    ssize_t c = fredCounter;
    return (c > 0) && __atomic_compare_exchange_n(&fredCounter, &c, c-1, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  }

  Fred* getReadyFred(BaseProcessor& proc) {
    ssize_t fredCount = __atomic_fetch_sub(&fredCounter, 1, __ATOMIC_RELAXED);
    if (fredCount > 0) {
      stats->ready.count(fredCount);     // number of freds ready
      return nullptr;
    } else {
      stats->blocked.count(1-fredCount); // number of procs waiting including this one
      return block(proc);
    }
  }

  bool addReadyFred(Fred& f, BaseProcessor& proc) {
    if (__atomic_add_fetch(&fredCounter, 1, __ATOMIC_RELAXED) > 0) return false;
    unblock(f, &proc);
    return true;
  }
};

#endif /* TESTING_GO_IDLEMANAGER */

#else

class IdleManager {
public:
  IdleManager(cptr_t) {}
  void reset(cptr_t, _friend<EventScope>) {}
};

#endif

class Scheduler {
protected:
  WorkerLock     ringLock;
  size_t         ringCount;
  BaseProcessor* placeProc;

public:
  IdleManager idleManager;
  Scheduler() : ringCount(0), placeProc(nullptr), idleManager(this) {}
  ~Scheduler() {
    ScopedLock<WorkerLock> sl(ringLock);
    RASSERT(!ringCount, ringCount);
  }

  void addProcessor(BaseProcessor& proc) {
    ScopedLock<WorkerLock> sl(ringLock);
    if (placeProc == nullptr) {
      ProcessorRing::close(proc);
      placeProc = &proc;
    } else {
      ProcessorRing::insert_after(*placeProc, proc);
    }
    ringCount += 1;
  }

  void removeProcessor(BaseProcessor& proc) {
    ScopedLock<WorkerLock> sl(ringLock);
    RASSERT0(placeProc);
    // move placeProc, if necessary
    if (placeProc == &proc) placeProc = ProcessorRing::next(*placeProc);
    // ring empty?
    if (placeProc == &proc) placeProc = nullptr;
    ProcessorRing::remove(proc);
    ringCount -= 1;
  }

  BaseProcessor& placement(_friend<Fred>) {
    // ring insert is traversal-safe, so could use separate 'placeLock' here
    ScopedLock<WorkerLock> sl(ringLock);
    RASSERT0(placeProc);
    placeProc = ProcessorRing::next(*placeProc);
    return *placeProc;
  }
};

#endif /* _Scheduler_h_ */
