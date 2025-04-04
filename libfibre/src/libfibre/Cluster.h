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
#ifndef _Cluster_h_
#define _Cluster_h_ 1

#include "runtime/Scheduler.h"
#include "libfibre/Fibre.h"
#include "libfibre/Poller.h"
#if TESTING_WORKER_IO_URING
#include "libfibre/IOUring.h"
#endif

#include <list>

#ifdef SPLIT_STACK
#include <csignal>  // sigaltstack
#endif

/**
A Cluster object provides a scheduling scope and uses processors (pthreads)
to execute fibres.  It also manages I/O pollers and provides a
simple stop-the-world pause mechanism.
*/
class Cluster : public Scheduler {
  EventScope& scope;

#if TESTING_CLUSTER_POLLER_FIBRE
  typedef PollerFibre  PollerType;
#else
  typedef PollerThread PollerType;
#endif
  PollerType* iPollVec;
  PollerType* oPollVec;
  size_t      iPollCount;
  size_t      oPollCount;

  std::list<Fibre*> pauseFibres;
  WorkerSemaphore   pauseSem;
  WorkerSemaphore   pauseConfirmSem;
  static void       pauseOperation(Cluster* cl);

  FredStats::ClusterStats* stats;

  struct Worker : public BaseProcessor {
    pthread_t     sysThreadId;
#if TESTING_WORKER_IO_URING
    IOUring*      iouring = nullptr;
#endif
#if TESTING_WORKER_POLLER
    WorkerPoller* workerPoller = nullptr;
#endif
    Worker(Cluster& c) : BaseProcessor(c) {
      c.Scheduler::addProcessor(*this);
    }
    void setIdleLoop(Fibre* f) { BaseProcessor::idleFred = f; }
    void runIdleLoop(Fibre* f) { BaseProcessor::idleLoop(f); }
    pthread_t getSysID()       { return sysThreadId; }
  };

  static Worker& CurrWorker() { return reinterpret_cast<Worker&>(Context::CurrProcessor()); }

  Cluster(EventScope& es, size_t ipcnt, size_t opcnt = 1) : scope(es), iPollCount(ipcnt), oPollCount(opcnt) {
    stats = new FredStats::ClusterStats(this, &es);
    iPollVec = (PollerType*)new char[sizeof(PollerType[iPollCount])];
    oPollVec = (PollerType*)new char[sizeof(PollerType[oPollCount])];
  }

  void start() {
    for (size_t p = 0; p < oPollCount; p += 1) {
      (new (&oPollVec[p]) PollerType(scope, this, "O-Poller   ", _friend<Cluster>()))->start();
    }
    for (size_t p = 0; p < iPollCount; p += 1) {
      (new (&iPollVec[p]) PollerType(scope, this, "I-Poller   ", _friend<Cluster>()))->start();
    }
  }

  struct Argpack {
    Cluster* cluster;
    Worker*  worker;
    Fibre*   initFibre;
  };

  inline void  setupWorker(Fibre*, Worker*);
  static void  initDummy(ptr_t);
  static void  fibreHelper(Worker*);
  static void* threadHelper(Argpack*);
  inline void  registerIdleWorker(Worker* worker, Fibre* initFibre);

public:
  /** Constructor: create Cluster in current EventScope. */
  Cluster(size_t pollerCount = 1) : Cluster(Context::CurrEventScope(), pollerCount) { start(); }

  // Dedicated constructor & helper for EventScope creation.
  Cluster(EventScope& es, size_t pollerCount, _friend<EventScope>) : Cluster(es, pollerCount) {}
  void startPolling(_friend<EventScope>) { start(); }

  ~Cluster() {
    // TODO: wait until regular fibres have left, then delete processors?
    delete [] iPollVec;
    delete [] oPollVec;
  }

  void preFork(_friend<EventScope>);
  void postFork(cptr_t parent, _friend<EventScope>);

#if TESTING_WORKER_IO_URING
  static IOUring& getWorkerUring() {
    return *CurrWorker().iouring;
  }

  static bool pollWorker(BaseProcessor& proc) {
    return reinterpret_cast<Worker&>(proc).iouring->poll(_friend<Cluster>());
  }
  static bool trySuspendWorker(BaseProcessor& proc) {
    return reinterpret_cast<Worker&>(proc).iouring->trySuspend(_friend<Cluster>());
  }
  static void suspendWorker(BaseProcessor& proc) {
    reinterpret_cast<Worker&>(proc).iouring->suspend(_friend<Cluster>());
  }
  static void resumeWorker(BaseProcessor& proc) {
    reinterpret_cast<Worker&>(proc).iouring->resume(_friend<Cluster>());
  }
#endif

#if TESTING_WORKER_POLLER
  static BasePoller& getWorkerPoller(BaseProcessor& proc) {
    return *reinterpret_cast<Worker&>(proc).workerPoller;
  }

  static bool pollWorker(BaseProcessor& proc) {
    return reinterpret_cast<Worker&>(proc).workerPoller->poll(_friend<Cluster>());
  }
  static bool trySuspendWorker(BaseProcessor& proc) {
    return reinterpret_cast<Worker&>(proc).workerPoller->trySuspend(_friend<Cluster>());
  }
  static void suspendWorker(BaseProcessor& proc) {
    reinterpret_cast<Worker&>(proc).workerPoller->suspend(_friend<Cluster>());
  }
  static void resumeWorker(BaseProcessor& proc) {
    reinterpret_cast<Worker&>(proc).workerPoller->resume(_friend<Cluster>());
  }
#endif

  // Register curent system thread (pthread) as worker.
  Fibre* registerWorker(_friend<EventScope>);

  /** Create one new worker (pthread) and add to cluster.
      Start `initFunc(initArg)` as dedicated fibre immediately after creation. */
  Worker& addWorker(funcvoid1_t initFunc = nullptr, ptr_t initArg = nullptr);
  /** Create new workers (pthreads) and add to cluster. */
  void addWorkers(size_t cnt = 1) { for (size_t i = 0; i < cnt; i += 1) addWorker(); }

  /** Obtain system-level ids for workers (pthread_t). */
  size_t getWorkerSysIDs(pthread_t* tids, size_t cnt = 0) {
    ScopedLock<WorkerLock> sl(ringLock);
    BaseProcessor* p = placeProc;
    for (size_t i = 0; i < cnt && i < ringCount; i += 1) {
      tids[i] = reinterpret_cast<Worker*>(p)->sysThreadId;
      p = ProcessorRing::next(*p);
    }
    return ringCount;
  }

  /** Get individual access to pollers. */
  PollerType&  getInputPoller(size_t hint) { return iPollVec[hint % iPollCount]; }
  PollerType& getOutputPoller(size_t hint) { return oPollVec[hint % oPollCount]; }

  /** Obtain number of pollers */
  size_t  getInputPollerCount() { return iPollCount; }
  size_t getOutputPollerCount() { return oPollCount; }

  /** Pause all OsProcessors (except caller).. */
  void pause();
  /** Resume all OsProcessors. */
  void resume();
};

#endif /* _Cluster_h_ */
