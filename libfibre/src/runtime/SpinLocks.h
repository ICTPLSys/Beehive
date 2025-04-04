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
#ifndef _SpinLocks_h_
#define _SpinLocks_h_ 1

#include "runtime/Basics.h"

template<typename T>
static inline bool _CAS(T volatile *ptr, T expected, T desired, int success_memorder = __ATOMIC_ACQUIRE, int failure_memorder = __ATOMIC_RELAXED) {
  T* exp = &expected;
  return __atomic_compare_exchange_n(ptr, exp, desired, false, success_memorder, failure_memorder);
}

// pro: simple
// con: unfair, cache contention
template<size_t SpinStart = 4, size_t SpinEnd = 1024>
class BinaryLock {
protected:
  volatile bool locked;
public:
  BinaryLock() : locked(false) {}
  bool tryAcquire() {
    if (locked) return false;
    return !__atomic_test_and_set(&locked, __ATOMIC_ACQUIRE);
  }
  void acquire() {
    size_t spin = SpinStart;
    for (;;) {
      if fastpath(!__atomic_test_and_set(&locked, __ATOMIC_ACQUIRE)) break;
      for (size_t i = 0; i < spin; i += 1) Pause();
      if (spin < SpinEnd) spin += spin;
      while (locked) Pause();
    }
  }
  void release() {
    RASSERT0(locked);
    __atomic_clear(&locked, __ATOMIC_RELEASE);
  }
} __caligned;

// pro: simple, owner locking
// con: unfair, cache contention
template<size_t SpinStart = 4, size_t SpinEnd = 1024, typename T = uintptr_t, T noOwner=limit<uintptr_t>()>
class BinaryOwnerLock {
  volatile T owner;
  size_t counter;
public:
  BinaryOwnerLock() : owner(noOwner), counter(0) {}
  size_t tryAcquire(T caller) {
    if (owner != caller) {
      if (owner != noOwner) return 0;
      if (!_CAS((T*)&owner, noOwner, caller)) return 0;
    }
    counter += 1;
    return counter;
  }
  size_t acquire(T caller) {
    if (owner != caller) {
      size_t spin = SpinStart;
      for (;;) {
        if fastpath(_CAS((T*)&owner, noOwner, caller)) break;
        for (size_t i = 0; i < spin; i += 1) Pause();
        if (spin < SpinEnd) spin += spin;
        while (owner != noOwner) Pause();
      }
    }
    counter += 1;
    return counter;
  }
  template<bool full = false>
  size_t release(T caller) {
    RASSERT0(owner == caller);
    counter = full ? 0 : (counter - 1);
    if (counter == 0) __atomic_store_n(&owner, noOwner, __ATOMIC_RELEASE);
    return counter;
  }
} __caligned;

// pro: fair
// con: cache contention, cache line bouncing
class TicketLock {
  volatile size_t serving;
  size_t ticket;
public:
  TicketLock() : serving(0), ticket(0) {}
  bool tryAcquire() {
    if (serving != ticket) return false;
    size_t tryticket = serving;
    return _CAS(&ticket, tryticket, tryticket + 1);
  }
  void acquire() {
    size_t myticket = __atomic_fetch_add(&ticket, 1, __ATOMIC_RELAXED);
    while slowpath(myticket != __atomic_load_n(&serving, __ATOMIC_ACQUIRE)) Pause();
  }
  void release() {
    RASSERT0(sword(ticket-serving) > 0);
    __atomic_fetch_add(&serving, 1, __ATOMIC_RELEASE);
  }
} __caligned;

// https://doi.org/10.1145/103727.103729
// pro: no cache contention -> scalability
// con: storage node, lock bouncing -> use cohorting?
// tested acquire/release memory ordering -> failure?
class MCSLock {
public:
  struct Node {
    Node* volatile next;
    volatile bool wait;
  };
private:
  Node* volatile tail;
public:
  MCSLock() : tail(nullptr) {}
  bool tryAcquire(Node& n) {
    n.next = nullptr;
    return (tail == nullptr) && _CAS(&tail, (Node*)nullptr, &n, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);
  }
  void acquire(Node& n) {
    n.next = nullptr;
    Node* prev = __atomic_exchange_n(&tail, &n, __ATOMIC_ACQUIRE);
    if (!prev) return;
    n.wait = true;
    __atomic_store_n(&prev->next, &n, __ATOMIC_RELEASE);
    while slowpath(__atomic_load_n(&n.wait, __ATOMIC_ACQUIRE)) Pause();
  }
  void release(Node& n) {
    RASSERT0(tail != nullptr);
    // could check 'n.next' first, but no memory consistency then
    if (_CAS(&tail, &n, (Node*)nullptr, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED)) return;
    while slowpath(!__atomic_load_n(&n.next, __ATOMIC_ACQUIRE)) Pause();
    __atomic_store_n(&n.next->wait, false, __ATOMIC_RELEASE);
  }
} __caligned;

// simple spinning RW lock: all racing - starvation possible
class SpinLockRW {
  volatile ssize_t state;                    // -1 writer, 0 open, >0 readers
public:
  SpinLockRW() : state(0) {}
  bool tryAcquireRead() {
    ssize_t s = state;
    return (s >= 0) && __atomic_compare_exchange_n(&state, &s, s+1, false, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);
  }
  void acquireRead() {
    while slowpath(!tryAcquireRead()) Pause();
  }
  bool tryAcquireWrite() {
    ssize_t s = state;
    return (s == 0) && __atomic_compare_exchange_n(&state, &s, s-1, false, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);
  }
  void acquireWrite() {
    while slowpath(!tryAcquireWrite()) Pause();
  }
  void release() {
    RASSERT0(state != 0);
    if (state < 0) __atomic_add_fetch(&state, 1, __ATOMIC_RELEASE);
    else __atomic_sub_fetch(&state, 1, __ATOMIC_RELEASE);
  }
} __caligned;

// simple spinning barrier for testing
class SpinBarrier {
  size_t target;
  volatile size_t counter;
public:
  explicit SpinBarrier(size_t t = 1) : target(t), counter(0) { RASSERT0(t > 0); }
  void init(size_t t = 1) {
    target = t;
    counter = 0;
  }
  bool wait() {
    size_t cnt = __atomic_fetch_add(&counter, 1, __ATOMIC_SEQ_CST);
    size_t tgt = cnt + target - (cnt % target);
    while slowpath(ssize_t(tgt - counter) > 0) Pause(); // works with overflow
    return (cnt == tgt - 1);
  }
  void reset() {}
} __caligned;

static inline void AcquireSpinLock() {}
static inline void ReleaseSpinLock() {}

template<typename Lock>
static inline void AcquireSpinLock(Lock& lk) { lk.acquire(); }
template<typename Lock>
static inline void ReleaseSpinLock(Lock& lk) { lk.release(); }

template<typename Lock>
static inline void AcquireSpinLock(Lock& lk, typename Lock::Node& node) { lk.acquire(node); }
template<typename Lock>
static inline void ReleaseSpinLock(Lock& lk, typename Lock::Node& node) { lk.release(node); }

#endif /* _SpinLocks_h_ */
