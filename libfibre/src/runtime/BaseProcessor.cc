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
#include "runtime/Scheduler.h"

inline Fred* BaseProcessor::searchAll() {
  Fred* nextFred;
  if ((nextFred = searchLocal())) return nextFred;
#if TESTING_LOADBALANCING
  if (RuntimeWorkerPoll(*this)) {
    if ((nextFred = searchLocal())) return nextFred;
  }
  if ((nextFred = searchSteal())) return nextFred;
#endif
  return nullptr;
}

inline Fred* BaseProcessor::searchLocal() {
  Fred* f = readyQueue.dequeue();
  if (f) {
    DBG::outl(DBG::Level::Scheduling, "searchLocal: ", FmtHex(this), ' ', FmtHex(f));
    stats->deq.count();
  }
  return f;
}

#if TESTING_LOADBALANCING
inline Fred* BaseProcessor::searchSteal() {
  BaseProcessor* victim = ProcessorRing::next(*this);
  for (;;) {
    if (victim == this) return nullptr;
    Fred* f = victim->readyQueue.tryDequeue();
    if (f) {
      DBG::outl(DBG::Level::Scheduling, "searchSteal: ", FmtHex(this), "<-", FmtHex(victim), ' ', FmtHex(f));
      if (f->checkAffinity(*this, _friend<BaseProcessor>())) stats->borrow.count();
      else stats->steal.count();
      return f;
    }
    victim = ProcessorRing::next(*victim);
  }
}
#endif

inline Fred* BaseProcessor::scheduleBlocking() {
  for (;;) {
    Fred* nextFred = searchAll();
    if (nextFred) return nextFred;
  }
}

inline Fred* BaseProcessor::scheduleNonblocking() {
#if TESTING_LOADBALANCING
#if TESTING_GO_IDLEMANAGER
  return searchAll();
#else
  if (!scheduler.idleManager.tryGetReadyFred()) return nullptr;
#endif
#else /* TESTING_LOADBALANCING */
  if (!readyCount.tryP()) return nullptr;
#endif
  return scheduleBlocking();
}

#if TESTING_LOADBALANCING && TESTING_GO_IDLEMANAGER

inline Fred& BaseProcessor::scheduleIdle() {
  Fred* nextFred;
  for (;;) {
    scheduler.idleManager.incSpinning();
    for (size_t i = 1; i < IdleSpinMax; i += 1) {
      nextFred = searchAll();
      if (nextFred) {
        scheduler.idleManager.unblockSpin();
        return *nextFred;
      }
    }
    scheduler.idleManager.decSpinning();
    scheduler.idleManager.incWaiting();
    nextFred = searchAll();
    if (nextFred) {
      scheduler.idleManager.decWaiting();
      return *nextFred;
    }
    scheduler.idleManager.block(*this);
  }
}

#else /* TESTING_LOADBALANCING && TESTING_GO_IDLEMANAGER */

inline Fred& BaseProcessor::scheduleIdle() {
  Fred* nextFred;
  for (size_t i = 1; i < IdleSpinMax; i += 1) {
    nextFred = scheduleNonblocking();
    if (nextFred) return *nextFred;
  }
#if TESTING_LOADBALANCING
  nextFred = scheduler.idleManager.getReadyFred(*this);
  if (nextFred) {
    DBG::outl(DBG::Level::Scheduling, "handover: ", FmtHex(this), ' ', FmtHex(nextFred));
    nextFred->checkAffinity(*this, _friend<BaseProcessor>());
    stats->handover.count();
    return *nextFred;
  }
#else  /* TESTING_LOADBALANCING */
  if (!readyCount.P()) haltSem.P(*this);
#endif /* TESTING_LOADBALANCING */
  return *scheduleBlocking();
}

#endif /* TESTING_LOADBALANCING && TESTING_GO_IDLEMANAGER */

void BaseProcessor::idleLoop(Fred* initFred) {
  if (initFred) Fred::idleYieldTo(*initFred, _friend<BaseProcessor>());
  for (;;) {
    Fred& nextFred = scheduleIdle();
    Fred::idleYieldTo(nextFred, _friend<BaseProcessor>());
  }
}

Fred* BaseProcessor::tryScheduleLocal(_friend<Fred>) {
  return searchLocal();
}

Fred* BaseProcessor::tryScheduleGlobal(_friend<Fred>) {
  return searchAll();
}

Fred* BaseProcessor::tryScheduleSteal(_friend<Fred>) { return searchSteal(); }

Fred& BaseProcessor::scheduleFull(_friend<Fred>) {
  Fred* nextFred = scheduleNonblocking();
  return nextFred ? *nextFred : *idleFred;
}

void BaseProcessor::enqueueResume(Fred& f, BaseProcessor&proc, _friend<Fred>) {
#if TESTING_LOADBALANCING
#if TESTING_GO_IDLEMANAGER
  enqueueFred(f);
  scheduler.idleManager.unblock(&proc);
#else
  if (!scheduler.idleManager.addReadyFred(f, proc)) enqueueFred(f);
#endif
#else
  (void)proc;
  enqueueFred(f);
  if (!readyCount.V()) haltSem.V(*this);
#endif
}
