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
#ifndef _BlockingSync_h_
#define _BlockingSync_h_ 1

#include "runtime/Benaphore.h"
#include "runtime/Debug.h"
#include "runtime/Stats.h"
#include "runtime/Fred.h"
#include "runtime-glue/RuntimeContext.h"
#include "runtime-glue/RuntimeLock.h"
#include "runtime-glue/RuntimePreemption.h"
#include "runtime-glue/RuntimeTimer.h"

#include <map>

#if TRACING
#include "tracing/BlockingSyncTrace.h"
#else
#define lttng_ust_tracepoint(provider, name, ...)
#endif

/****************************** Basics ******************************/

struct Suspender { // funnel suspend calls through this class for access control
  static void prepareRace(Fred& fred) {
    fred.prepareResumeRace(_friend<Suspender>());
  }

  // returns the resumeMsg
  static ptr_t cancelRunningResumeRace(Fred& fred) {
    return fred.cancelRunningResumeRace(_friend<Suspender>());
  }

  static void cancelEarlyResume(Fred& fred) {
    fred.cancelEarlyResume(_friend<Suspender>());
  }

  template<bool DisablePreemption=true, size_t SpinStart=1, size_t SpinEnd=0>
  static ptr_t suspend(Fred& fred) {
    if (DisablePreemption) RuntimeDisablePreemption();
    ptr_t result = fred.suspend<SpinStart, SpinEnd>(_friend<Suspender>());
    DBG::outl(DBG::Level::Blocking, "Fred ", FmtHex(&fred), " continuing");
    RuntimeEnablePreemption();
    return result;
  }
};

enum SemaphoreResult : size_t { SemaphoreTimeout = 0, SemaphoreSuccess = 1, SemaphoreWasOpen = 2 };

/****************************** Timeouts ******************************/

class TimerQueue {
public:
  struct Node {
    Fred& fred;
    bool expired;
  };
  typedef std::multimap<Time, Node*>::iterator Handle;

private:
  WorkerLock lock;
  std::multimap<Time, Node*> queue;
  FredStats::TimerStats* stats;

public:
  TimerQueue(cptr_t parent = nullptr) { stats = new FredStats::TimerStats(this, parent); }
  void reinit(cptr_t parent) { new (stats) FredStats::TimerStats(this, parent); }
  bool empty() const { return queue.empty(); }

  void checkExpiry() {
    size_t cnt = 0;
    Time now = Runtime::Timer::now();
    lock.acquire();
    for (auto iter = queue.begin(); iter != queue.end(); iter = queue.erase(iter)) {
      if (iter->first > now) {
        Runtime::Timer::newTimeout(iter->first); // timeouts remaining after this run
        lock.release();
        stats->events.count(cnt);
    return;
      }
      Node* node = iter->second;
      if (node->fred.raceResume(&queue)) {
        node->fred.resume();                     // node no longer accessible after this
      } else {
        node->expired = true;                    // node no longer accessible after this
      }
      cnt += 1;
    }
    lock.release();
    stats->events.count(cnt);
  }

  ptr_t blockTimeout(Fred& cf, const Time& absTimeout) {
    // set up queue node
    Node node = {cf, false};
    auto iter = enqueue(node, absTimeout);
    // suspend
    ptr_t winner = Suspender::suspend(cf);
    if (winner == &queue) return nullptr; // timer expired
    erase(iter, node);
    return winner;                        // timer cancelled
  }

  // Warning: must NOT call if timer won race
  void erase(Handle& handle, const Node& node) {
    // optimization, try without lock + memory sync
    if (node.expired) return;
    ScopedLock<WorkerLock> sl(lock);
    if (!node.expired) queue.erase(handle);
  }

  Handle enqueue(Node& node, const Time& absTimeout) {
    ScopedLock<WorkerLock> sl(lock);
    auto iter = queue.insert({absTimeout, &node});
    if (iter == queue.begin()) Runtime::Timer::newTimeout(absTimeout);
    return iter;
  }

  bool didExpireAfterLosingRace(const Node& node) {
    // optimization, try without lock + memory sync
    if (node.expired) return true;
    ScopedLock<WorkerLock> sl(lock);
    return node.expired;
  }
};

static inline bool sleepFred(const Time& timeout, TimerQueue& tq = Runtime::Timer::CurrTimerQueue()) {
  Fred* cf = Context::CurrFred();
  DBG::outl(DBG::Level::Blocking, "Fred ", FmtHex(cf), " sleep ", timeout);
  Suspender::prepareRace(*cf);
  return tq.blockTimeout(*cf, Runtime::Timer::now() +  timeout) == nullptr;
}

/****************************** Common Locked Synchronization ******************************/

class BlockingQueue {
  struct Node : public DoubleLink<Node> { // need separate node for timeout & cancellation
    Fred& fred;
    Node(Fred& cf) : fred(cf) {}
  };
  IntrusiveList<Node> queue;

  ptr_t blockHelper(Fred& cf) {
    return Suspender::suspend(cf);
  }
  ptr_t blockHelper(Fred& cf, const Time& absTimeout, TimerQueue& tq = Runtime::Timer::CurrTimerQueue()) {
    return tq.blockTimeout(cf, absTimeout);
  }

  template<typename Lock, typename... Args>
  bool blockInternal(Lock& lock, Fred* cf, const Args&... args) {
    // set up queue node
    Node node(*cf);
    DBG::outl(DBG::Level::Blocking, "Fred ", FmtHex(cf), " blocking on ", FmtHex(&queue));
    Suspender::prepareRace(*cf);
    queue.push_back(node);
    lock.release();
    // block, potentially with timeout
    lttng_ust_tracepoint(BlockingSyncTrace, blocking, (uintptr_t)Context::CurrFred(), (uintptr_t)this, "block");
    ptr_t winner = blockHelper(*cf, args...);
    lttng_ust_tracepoint(BlockingSyncTrace, blocking, (uintptr_t)Context::CurrFred(), (uintptr_t)this, "unblock");
    if (winner == &queue) return true; // blocking completed;
    // clean up
    ScopedLock<Lock> sl(lock);
    queue.remove(node);
    return false;                      // blocking cancelled
  }

  BlockingQueue(const BlockingQueue&) = delete;            // no copy
  BlockingQueue& operator=(const BlockingQueue&) = delete; // no assignment

public:
  BlockingQueue() {
    lttng_ust_tracepoint(BlockingSyncTrace, blocking, (uintptr_t)Context::CurrFred(), (uintptr_t)this, "init");
  }
  ~BlockingQueue() { RASSERT0(empty()); }
  bool empty() const { return queue.empty(); }

  template<typename Lock>
  bool block(Lock& lock, Fred* cf, bool wait = true) {    // Note that caller must hold lock
    if (wait) return blockInternal(lock, cf);
    lock.release();
    return false;
  }

  template<typename Lock>
  bool block(Lock& lock, Fred* cf, const Time& timeout) { // Note that caller must hold lock
    if (timeout > Runtime::Timer::now()) return blockInternal(lock, cf, timeout);
    lock.release();
    return false;
  }

  template<typename Lock>
  bool block(Lock& lock, bool wait = true) {
    return block(lock, Context::CurrFred(), wait);
  }

  template<typename Lock>
  bool block(Lock& lock, const Time& timeout) {
    return block(lock, Context::CurrFred(), timeout);
  }

  template<bool Enqueue>
  Fred* unblock() {                     // Note that caller must hold lock
    lttng_ust_tracepoint(BlockingSyncTrace, blocking, (uintptr_t)Context::CurrFred(), (uintptr_t)this, "resume");
    for (Node* node = queue.front(); node != queue.edge(); node = IntrusiveList<Node>::next(*node)) {
      Fred* f = &node->fred;
      if (f->raceResume(&queue)) {
        IntrusiveList<Node>::remove(*node);
        DBG::outl(DBG::Level::Blocking, "Fred ", FmtHex(f), " resume from ", FmtHex(&queue));
        if (Enqueue) f->resume();
        lttng_ust_tracepoint(BlockingSyncTrace, blocking, (uintptr_t)f, (uintptr_t)this, "resumed");
        return f;
      }
    }
    return nullptr;
  }
};

template<typename Lock, bool Binary = false, typename BQ = BlockingQueue>
class LockedSemaphore {
  Lock lock;
  volatile ssize_t counter;
  BQ bq;

  template<typename... Args>
  SemaphoreResult internalP(const Args&... args) {
    // baton passing: counter unchanged, if blocking fails (timeout)
    if (counter < 1) return bq.block(lock, args...) ? SemaphoreSuccess : SemaphoreTimeout;
    counter -= 1;
    lock.release();
    return SemaphoreWasOpen;
  }

  void unlock() {}

  template<typename Lock1, typename...Args>
  void unlock(Lock1& l, Args&... args) {
    l.release();
    unlock(args...);
  }

public:
  explicit LockedSemaphore(ssize_t c = 0) : counter(c) {}
  ~LockedSemaphore() { reset(); }
  void reset(ssize_t c = 0) { // baton passing requires serialization at destruction
    ScopedLock<Lock> al(lock);
    RASSERT0(bq.empty());
    counter = c;
  }
  ssize_t getValue() const { return counter; }

  template<typename... Args>
  SemaphoreResult P(const Args&... args) { lock.acquire(); return internalP(args...); }

  SemaphoreResult tryP()                 { lock.acquire(); return internalP(false); }

  template<typename... Args>
  SemaphoreResult unlockP(Args&... args) { lock.acquire(); unlock(args...); return internalP(true); }

  // use condition/signal semantics
  template<typename... Args>
  SemaphoreResult wait(const Args&... args) {
    RASSERT0(Binary);
    lock.acquire();
    RASSERT0(counter >= 0);
    counter = 0;
    return internalP(args...);
  }

  template<bool Enqueue = true>
  Fred* V() {
    lock.acquire();
    Fred* next = bq.template unblock<false>();
    if (next) {
      lock.release();
      if (Enqueue) next->resume();
    } else {
      if (Binary) counter = 1;
      else counter += 1;
      lock.release();
    }
    return next;
  }

  template<bool Enqueue = true>
  Fred* tryV() {
    lock.acquire();
    Fred* next = bq.template unblock<false>();
    lock.release();
    if (Enqueue && next) next->resume();
    return next;
  }

  template<bool Enqueue = true>
  Fred* waitV() {
    for (;;) {
      Fred* next = tryV<Enqueue>();
      if (next) return next;
    }
  }

  template<bool Enqueue = true>
  Fred* release() { return V<Enqueue>(); }
};

template<typename Lock, bool Fifo, typename BQ = BlockingQueue>
class LockedMutex {
  Lock lock;
  Fred* owner;
  BQ bq;

protected:
  template<bool OwnerLock, typename... Args>
  bool internalAcquire(const Args&... args) {
    Fred* cf = Context::CurrFred();
    if (OwnerLock && cf == owner) return true;
    RASSERT(cf != owner, FmtHex(cf), FmtHex(owner));
    for (;;) {
      lock.acquire();
      if (!owner) break;
      if (!bq.block(lock, cf, args...)) return false; // timeout
      if (Fifo) return true; // owner set via baton passing
    }
    owner = cf;
    lock.release();
    return true;
  }

public:
  LockedMutex() : owner(nullptr) {}
  ~LockedMutex() { reset(); }
  void reset() { // baton passing requires serialization at destruction
    ScopedLock<Lock> al(lock);
    RASSERT(owner == nullptr, FmtHex(owner));
    RASSERT0(bq.empty());
  }

  template<typename... Args>
  bool acquire(const Args&... args) { return internalAcquire<false>(args...); }
  bool tryAcquire() { return acquire(false); }

  void release() {
    RASSERT(owner == Context::CurrFred(), FmtHex(owner));
    lock.acquire();
    Fred* next = bq.template unblock<false>();
    owner = Fifo ? next : nullptr;
    lock.release();
    if (next) next->resume();
  }
};

// condition variable with external lock
template<typename BQ = BlockingQueue>
class Condition {
  BQ bq;

public:
  ~Condition() { reset(); }
  void reset() { RASSERT0(bq.empty()); }

  template<typename Lock>
  bool wait(Lock& lock) { return bq.block(lock); }

  template<typename Lock>
  bool wait(Lock& lock, const Time& timeout) { return bq.block(lock, timeout); }

  template<bool Broadcast = false>
  void signal() { while (bq.template unblock<true>() && Broadcast); }
};

template<typename Lock, typename BQ = BlockingQueue>
class LockedBarrier {
  Lock lock;
  size_t target;
  size_t counter;
  BQ bq;
public:
  explicit LockedBarrier(size_t t = 1) : target(t), counter(0) { RASSERT0(t > 0); }
  ~LockedBarrier() { reset(); }
  void reset() {
    ScopedLock<Lock> al(lock);
    RASSERT0(bq.empty())
  }

  bool wait() {
    lock.acquire();
    counter += 1;
    if (counter == target) {
      for ( ;counter > 0; counter -= 1) bq.template unblock<true>();
      lock.release();
      return true;
    } else {
      bq.block(lock);
      return false;
    }
  }
};

// simple blocking RW lock: release alternates; new readers block when writer waits -> no starvation
template<typename Lock, typename BQ = BlockingQueue>
class LockedRWLock {
  Lock lock;
  ssize_t state;                    // -1 writer, 0 open, >0 readers
  BQ bqR;
  BQ bqW;

  template<typename... Args>
  bool internalAR(const Args&... args) {
    lock.acquire();
    if (state >= 0 && bqW.empty()) {
      state += 1;
      lock.release();
    } else {
      if (!bqR.block(lock, args...)) return false;
      lock.acquire();
      Fred* next = bqR.template unblock<false>();  // all waiting readers can barge after writer
      if (next) state += 1;
      lock.release();
      if (next) next->resume();
    }
    return true;
  }

  template<typename... Args>
  bool internalAW(const Args&... args) {
    lock.acquire();
    if (state == 0) {
      state -= 1;
      lock.release();
    } else {
      if (!bqW.block(lock, args...)) return false;
    }
    return true;
  }

public:
  LockedRWLock() : state(0) {}
  ~LockedRWLock() { reset(); }
  void reset() {
    ScopedLock<Lock> al(lock);
    RASSERT(state == 0, state);
    RASSERT0(bqR.empty());
    RASSERT0(bqW.empty());
  }

  template<typename... Args>
  bool acquireRead(const Args&... args) { return internalAR(args...); }
  bool tryAcquireRead() { return acquireRead(false); }

  template<typename... Args>
  bool acquireWrite(const Args&... args) { return internalAW(args...); }
  bool tryAcquireWrite() { return acquireWrite(false); }

  void release() {
    Fred* next;
    lock.acquire();
    RASSERT0(state != 0);
    if (state > 0) {             // reader leaves; if open -> writer next
      state -= 1;
      if (state > 0) {
        next = nullptr;
      } else {
        next = bqW.template unblock<false>();
        if (next) state -= 1;
        else {
          next = bqR.template unblock<false>();
          if (next) state += 1;
        }
      }
    } else {                     // writer leaves -> readers next
      RASSERT(state == -1, state);
      state += 1;
      next = bqR.template unblock<false>();
      if (next) state += 1;
      else {
        next = bqW.template unblock<false>();
        if (next) state -= 1;
      }
    }
    lock.release();
    if (next) next->resume();
  }
};

/****************************** Special Locked Synchronization ******************************/

// synchronization flag with with external lock
class SynchronizedFlag {

public:
  enum State : uintptr_t { Running = 0, Dummy = 1, Posted = 2, Detached = 4 };

protected:
  union {                             // 'waiter' set <-> 'state == Waiting'
    Fred* waiter;
    State state;
  };

public:
  static const State Invalid = Dummy; // dummy bit never set (for ManagedArray)
  explicit SynchronizedFlag(State s = Running) : state(s) {}
  bool posted()   const { return state == Posted; }
  bool detached() const { return state == Detached; }

  template<typename Lock>
  bool wait(Lock& lock) {             // returns false, if detached
    if (state == Running) {
      waiter = Context::CurrFred();
      Fred* temp = waiter;            // use a copy, since waiter might be...
      lock.release();                 // ... overwritten after releasing lock
      Suspender::suspend(*temp);
      lock.acquire();                 // reacquire lock to check state
    }
    if (state == Posted) return true;
    if (state == Detached) return false;
    RABORT(FmtHex(state));
  }

  bool post() {                       // returns false, if already detached
    RASSERT0(state != Posted);        // check for spurious posts
    if (state == Detached) return false;
    if (state != Running) waiter->resume();
    state = Posted;
    return true;
  }

  void detach() {                     // returns false, if already posted or detached
    RASSERT0(state != Detached && state != Posted);
    if (state != Running) waiter->resume();
    state = Detached;
  }
};

// synchronization (with result) with external lock
template<typename Runner, typename Result>
class Joinable : public SynchronizedFlag {
  union {
    Runner* runner;
    Result  result;
  };

public:
  Joinable(Runner* t) : runner(t) {}

  template<typename Lock>
  bool wait(Lock& bl, Result& r) {
    bool retcode = SynchronizedFlag::wait(bl);
    r = retcode ? result : 0; // result is available after returning from wait
    return retcode;
  }

  bool post(Result r) {
    bool retcode = SynchronizedFlag::post();
    if (retcode) result = r;  // still holding lock while setting result
    return retcode;
  }

  Runner* getRunner() const { return runner; }
};

// synchronization flag with automatic locking
template<typename Lock>
class SyncPoint : public SynchronizedFlag {
  Lock lock;

public:
  SyncPoint(typename SynchronizedFlag::State s = SynchronizedFlag::Running) : SynchronizedFlag(s) {}
  bool wait()   { ScopedLock<Lock> al(lock); return SynchronizedFlag::wait(lock); }
  bool post()   { ScopedLock<Lock> al(lock); return SynchronizedFlag::post(); }
  void detach() { ScopedLock<Lock> al(lock); SynchronizedFlag::detach(); }
};

/****************************** (Almost-)Lock-Free Synchronization ******************************/

template<typename Lock = DummyLock, int SpinStart = 1, int SpinEnd = 128>
class LimitedSemaphore0 {
  Lock lock;
  FredMPSC<FredReadyLink> queue;

public:
  explicit LimitedSemaphore0(ssize_t c = 0) { RASSERT(c == 0, c); }
  ~LimitedSemaphore0() { reset(); }
  void reset(ssize_t c = 0) {
    RASSERT(c == 0, c);
    RASSERT0(queue.empty());
  }

  SemaphoreResult P(bool wait = true) {
    RASSERT0(wait);
    Fred* cf = Context::CurrFred();
    RuntimeDisablePreemption();
    queue.push(*cf);
    Suspender::suspend<false>(*cf);
    return SemaphoreSuccess;
  }

  SemaphoreResult P(const Time&) {
    RABORT("timeout not implemented for LimitedSemaphore0");
  }

  template<bool Enqueue = true>
  Fred* V() {
    for (;;) {
      for (int s = SpinStart; s <= SpinEnd; s += 1) {
        Fred* next = queue.pop(lock);
        if (next) {
          if (Enqueue) next->resume();
          return next;
        }
      }
      Fred::yield(); // yield() needed to avoid circular deadlock between all workers in V()
    }
  }

  template<bool Enqueue = true>
  Fred* tryV() {
    Fred* next = queue.pop(lock);
    if (Enqueue && next) next->resume();
    return next;
  }

  template<bool Enqueue = true>
  Fred* waitV() {
    return V<Enqueue>();
  }
};

template<bool DirectSwitch>
class SimpleMutex0 {
  Benaphore<> ben;
  LimitedSemaphore0<> sem;

public:
  SimpleMutex0() : ben(1), sem(0) {}
  void reset() {
    RASSERT(ben.getValue() == 1, FmtHex(this));
    sem.reset(0);
  }
  bool acquire()    { return ben.P() || sem.P(); }
  bool tryAcquire() { return ben.tryP(); }
  bool acquire(bool wait) { return wait ? acquire() : tryAcquire(); }
  bool acquire(const Time&) { RABORT("timeout not implemented for SimpleMutex0"); }
  void release()    {
    if (ben.V()) return;
    Fred* next = sem.V<false>();
    next->resume<DirectSwitch>();
  }
};

template<typename Lock>
class FastBarrier {
  size_t target;
  volatile size_t counter;
  FredMPSC<FredReadyLink> queue;
  Lock lock;

public:
  explicit FastBarrier(size_t t = 1) : target(t), counter(0) { RASSERT0(t > 0); }
  ~FastBarrier() { reset(); }
  void reset() { RASSERT0(queue.empty()); }
  bool wait() {
    // There's a race between counter and queue.  A thread can be in a
    // different position in the queue relative to the counter.  The
    // notifier is determined by the counter, so a thread might notify a
    // group of other threads, but itself not continue.  However, one thread
    // of each group must be receive a "special" return code.  With POSIX
    // threads, this is PTHREAD_BARRIER_SERIAL_THREAD - here it's 'true'.
    Fred* cf = Context::CurrFred();
    Suspender::prepareRace(*cf);
    RuntimeDisablePreemption();
    queue.push(*cf);
    bool park = __atomic_add_fetch(&counter, 1, __ATOMIC_RELAXED) % target;
    if (!park) {
      __atomic_sub_fetch(&counter, target, __ATOMIC_RELAXED);
      park = true;
      ScopedLock<Lock> sl(lock);
      for (size_t i = 0; i < target; i += 1) {
        Fred* next;
        for (;;) {
          next = queue.pop();
          if (next) break;
          Pause();
        }
        if (next == cf) {
          park = false; // don't suspend self
        } else {
          // if caller ends up suspending, set special return code for last waiter in loop
          if ((i == target - 1) && park) next->raceResume(cf);
          next->resume();
        }
      }
    }
    if (park) return Suspender::suspend<false>(*cf);
    else RuntimeEnablePreemption();
    return cf;
  }
};

/****************************** Compound Types ******************************/

template<typename Semaphore, bool Binary = false>
class FredBenaphore {
  Benaphore<Binary> ben;
  Semaphore sem;

public:
  explicit FredBenaphore(ssize_t c = 0) : ben(c), sem(0) {}
  void reset(ssize_t c = 0) {
    sem.reset(0);
    ben.reset(c);
  }
  ssize_t getValue() const { return ben.getValue(); }

  SemaphoreResult P()          { return ben.P() ? SemaphoreWasOpen : sem.P(); }
  SemaphoreResult tryP()       { return ben.tryP() ? SemaphoreWasOpen : SemaphoreTimeout; }
  SemaphoreResult P(bool wait) { return wait ? P() : tryP(); }
  SemaphoreResult P(const Time& timeout) {
    if (ben.P()) return SemaphoreWasOpen;
    if (sem.P(timeout)) return SemaphoreSuccess;
    ben.V();
    return SemaphoreTimeout;
  }
  // use condition/signal semantics
  SemaphoreResult wait()                    { ben.reset(); return P(); }
  SemaphoreResult wait(const Time& timeout) { ben.reset(); return P(timeout); }

  template<bool Enqueue = true>
  Fred* V() {
    if (ben.V()) return nullptr;
    return sem.template V<Enqueue>();
  }
};

struct YieldSpin {
  inline void operator()(void) { Fred::yield(); }
};

template<int SpinStart, int SpinEnd, int SpinCount, int YieldCount, typename SpinOp, typename T, typename Func>
static inline bool Spin(Fred* cf, T* This, Func&& tryLock) {
  SpinOp spinOp;
  int ycnt = 0;
  int scnt = 0;
  int spin = SpinStart;
  for (;;) {
    if (tryLock(This, cf)) return true;
    if (scnt < SpinCount) {
      for (int i = 0; i < spin; i += 1) spinOp();
      if (spin < SpinEnd) spin += spin;
      else scnt += 1;
    } else if (ycnt < YieldCount) {
      ycnt += 1;
      Fred::yield();
    } else {
      return false;
    }
  }
}

template<typename Semaphore, int SpinStart, int SpinEnd, int SpinCount, int YieldCount, typename SpinOp = PauseSpin>
class SpinSemMutex {
  Fred* volatile owner;
  Semaphore sem;

  template<typename... Args>
  bool tryOnly(const Args&...) { return false; }

  template<typename... Args>
  bool tryOnly(bool wait) { return !wait; }

  static bool tryLock(SpinSemMutex* This, Fred* cf) {
    Fred* exp = nullptr;
    return __atomic_compare_exchange_n(&This->owner, &exp, cf, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
  }

protected:
  template<bool OwnerLock, typename... Args>
  bool internalAcquire(const Args&... args) {
    Fred* cf = Context::CurrFred();
    if (OwnerLock && cf == owner) return true;
    RASSERT(cf != owner, FmtHex(cf), FmtHex(owner));
    if (tryOnly(args...)) return tryLock(this, cf);
    if (Spin<SpinStart,SpinEnd,SpinCount,YieldCount,SpinOp>(cf, this, tryLock)) return true;
    while (sem.P(args...)) if (tryLock(this, cf)) return true;
    return false;
  }

public:
  SpinSemMutex() : owner(nullptr), sem(1) {}
  ~SpinSemMutex() { RASSERT(owner == nullptr, FmtHex(owner)); }
  void reset() {
    RASSERT(owner == nullptr, FmtHex(owner));
    sem.reset(1);
  }

  template<typename... Args>
  bool acquire(const Args&... args) { return internalAcquire<false>(args...); }
  bool tryAcquire() { return acquire(false); }

  void release() {
    RASSERT(owner == Context::CurrFred(), FmtHex(owner));
    __atomic_store_n(&owner, nullptr, __ATOMIC_RELEASE);
    Fred* next = sem.template V<false>();
    if (next) next->resume();
  }
};

// poor man's futex
template<typename Lock>
class ConditionalQueue : public BlockingQueue {
  Lock lock;

public:
  template<typename Func, typename... Args>
  bool block(Fred* cf, Func&& func, const Args&... args) {
    lock.acquire();
    if (func()) return BlockingQueue::block(lock, cf, args...);
    lock.release();
    return true;
  }

  template<bool Enqueue>
  Fred* unblock() {
    lock.acquire();
    Fred* next = BlockingQueue::unblock<Enqueue>();
    lock.release();
    return next;
  }
};

// inspired by Linux pthread mutex/futex implementation
template<typename CQ, int SpinStart, int SpinEnd, int SpinCount, int YieldCount, typename SpinOp = PauseSpin>
class SpinCondMutex {
  CQ queue;
  volatile size_t value;
  Fred* volatile owner;

  template<typename... Args>
  bool tryOnly(const Args&...) { return false; }

  template<typename... Args>
  bool tryOnly(bool wait) { return !wait; }

  bool tryLock1(Fred* cf, size_t& exp) {
    if (__atomic_compare_exchange_n(&value, &exp, 1, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
      owner = cf;
      return true;
    }
    return false;
  }

  static bool tryLock2(SpinCondMutex* This, Fred* cf) {
    if (__atomic_exchange_n(&This->value, 2, __ATOMIC_ACQUIRE) == 0) {
      This->owner = cf;
      return true;
    }
    return false;
  }

protected:
  template<bool OwnerLock, typename... Args>
  bool internalAcquire(const Args&... args) {
    SpinOp spinOp;
    Fred* cf = Context::CurrFred();
    if (OwnerLock && cf == owner) return true;
    RASSERT(cf != owner, FmtHex(cf), FmtHex(owner));
    size_t exp = 0;
    if (tryOnly(args...)) return tryLock1(cf, exp);
    int spin = SpinStart;
    for (;;) {                            // spin one round on value 1 while contention is low
      exp = 0;
      if (tryLock1(cf, exp)) return true;
      if (exp == 2) break;                // high contention detected
      for (int i = 0; i < spin; i += 1) spinOp();
      if (spin < SpinEnd) spin += spin;
      else break;
    }
    if (Spin<SpinStart,SpinEnd,SpinCount,YieldCount,SpinOp>(cf, this, tryLock2)) return true;
    while (queue.block(cf, [this]() { return this->value == 2; }, args...)) if (tryLock2(this, cf)) return true;
    return false;
  }

public:
  SpinCondMutex() : value(0), owner(nullptr) {}
  ~SpinCondMutex() { RASSERT(value == 0, value); }
  void reset() {}

  template<typename... Args>
  bool acquire(const Args&... args) { return internalAcquire<false>(args...); }
  bool tryAcquire() { return acquire(false); }

  template<bool DirectSwitch=false>
  void release() {
    RASSERT(value > 0, value);
    owner = nullptr;
    if (__atomic_exchange_n(&value, 0, __ATOMIC_RELEASE) == 1) return;
    Fred* next = queue.template unblock<false>();
    if (next) next->resume<DirectSwitch>();
  }
};

template<typename BaseMutex>
class OwnerMutex : private BaseMutex {
  size_t counter;
  bool recursion;

public:
  OwnerMutex() : counter(0), recursion(false) {}
  void reset() { BaseMutex::reset(); }
  void enableRecursion() { recursion = true; }

  template<typename... Args>
  size_t acquire(const Args&... args) {
    bool success = recursion
      ? BaseMutex::template internalAcquire<true>(args...)
      : BaseMutex::template internalAcquire<false>(args...);
    if (success) return ++counter; else return 0;
  }
  size_t tryAcquire() { return acquire(false); }

  size_t release() {
    if (--counter > 0) return counter;
    BaseMutex::release();
    return 0;
  }
};

#include "runtime/MCSTimeoutSemaphore.h"
#include "runtime/ConditionalNemesisQueue.h"

#if defined(FAST_MUTEX_TYPE)
typedef FAST_MUTEX_TYPE FastMutex;
#else
typedef SpinSemMutex<FredBenaphore<LimitedSemaphore0<MCSLock>,true>, 4, 1024, 16, 0> FastMutex;
#endif

#if defined(FRED_MUTEX_TYPE)
typedef FRED_MUTEX_TYPE FredMutex;
#else
typedef SpinCondMutex<ConditionalQueue<WorkerLock>, 4, 1024, 16, 0> FredMutex;
#endif

typedef Condition<>                 FredCondition;
typedef LockedSemaphore<WorkerLock> FredSemaphore;
typedef LockedRWLock<WorkerLock>    FredRWLock;
typedef LockedBarrier<WorkerLock>   FredBarrier;

#endif /* _BlockingSync_h_ */
