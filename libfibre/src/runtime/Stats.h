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
#ifndef _Stats_h_
#define _Stats_h_ 1

#include "runtime/Container.h"

#include <ostream>
using std::ostream;

typedef long long Number;

#ifdef KERNEL
static inline Number sqrt(Number x) { return x; }
#else
#include <cmath>
#endif

namespace FredStats {

void StatsClear(int = 0);
void StatsReset();

#if TESTING_ENABLE_STATISTICS

void StatsPrint(ostream&, bool);

struct Base : public SingleLink<Base> {
  cptr_t const object;
  cptr_t const parent;
  const char* const name;
  const size_t sort;

  Base(cptr_t const o, cptr_t const p, const char* const n, const size_t s);
  virtual ~Base();
  virtual void reset();
  virtual void print(ostream& os) const;
};

class Counter {
protected:
  volatile Number cnt;
public:
  Counter() : cnt(0) {}
  operator Number() const { return cnt; }
  void count(Number n = 1) {
    __atomic_add_fetch( &cnt, n, __ATOMIC_RELAXED);
  }
  void aggregate(const Counter& x) {
    cnt += x.cnt;
  }
  void reset() {
    cnt = 0;
  }
}; 

inline ostream& operator<<(ostream& os, const Counter& x) {
  os << std::fixed << (Number)x;
  return os;
}

class Average : public Counter {
  using Counter::cnt;
  volatile Number sum;
  volatile Number sqsum;
public:
  Number average() const {
    if (!cnt) return 0;
    return sum/cnt;
  }
  Number variance() const {
    if (!cnt) return 0;
    return sqrt((sqsum - (sum*sum) / cnt) / cnt);
  }
public:
  Average() : sum(0), sqsum(0) {}
  Number operator()() const { return average(); }
  void count(Number val) {
    Counter::count();
    __atomic_add_fetch( &sum, val, __ATOMIC_RELAXED);
    __atomic_add_fetch( &sqsum, val*val, __ATOMIC_RELAXED);
  }
  void aggregate(const Average& x) {
    Counter::aggregate(x);
    sum += x.sum;
    sqsum += x.sqsum;
  }
  void reset() {
    cnt = 0;
    sum = 0;
    sqsum = 0;
  }
};

inline ostream& operator<<(ostream& os, const Average& x) {
  os << ' ' << (const Counter&)x;
  os << ' ' << std::fixed << x.average() << '/' << x.variance();
  return os;
}

template<size_t N>
struct HashTable {
  Counter bucket[N];
public:
  Number operator[](size_t n) const { return bucket[n]; }
  void count(size_t n) {
    bucket[n % N].count();
  }
  void aggregate(const HashTable<N>& x) {
    for (size_t n = 0; n < N; n += 1) bucket[n].aggregate(x.bucket[n]);
  }
  void reset() {
    for (size_t n = 0; n < N; n += 1) bucket[n].reset();
  }
};

template<size_t N>
inline ostream& operator<<(ostream& os, const HashTable<N>& x) {
  for (size_t n = 0; n < N; n += 1) {
    if (x[n]) os << ' ' << n << ":" << x[n];
  }
  return os;
}

struct Distribution {
  Average average;
  Counter zero;
  HashTable<bitsize<Number>()> hashTable;
public:
  void count(size_t n) {
    average.count(n);
    if (n) hashTable.count(floorlog2(n));
    else zero.count();
  }
  void aggregate(const Distribution& x) {
    average.aggregate(x.average);
    zero.aggregate(x.zero);
    hashTable.aggregate(x.hashTable);
  }
  void reset() {
    average.reset();
    zero.reset();
    hashTable.reset();
  }
};

inline ostream& operator<<(ostream& os, const Distribution& x) {
  os << x.average;
  if (x.zero) os << " Z:" << x.zero;
  os << x.hashTable;
  return os;
}

struct Queue {
  volatile Number qlen;
  Counter fails;
  Counter tryfails;
  Distribution qdist;
public:
  Queue() : qlen(0) {}
  void add(Number n = 1) {
    __atomic_add_fetch( &qlen, n, __ATOMIC_RELAXED);
    qdist.count(qlen);
  }
  void remove(Number n = 1) {
    __atomic_sub_fetch( &qlen, n, __ATOMIC_RELAXED);
  }
  void fail() {
    fails.count();
  }
  void tryfail() {
    tryfails.count();
  }
  void aggregate(const Queue& x) {
    qlen += x.qlen;
    fails.aggregate(x.fails);
    tryfails.aggregate(x.tryfails);
    qdist.aggregate(x.qdist);
  }
  void reset() {
    qlen = 0;
    fails.reset();
    tryfails.reset();
    qdist.reset();
  }
};

inline ostream& operator<<(ostream& os, const Queue& x) {
  if (x.qlen != 0) os << " Q:" << x.qlen;
  os << " F: " << x.fails << " T: " << x.tryfails << " L:" << x.qdist;
  return os;
}

#else

static inline void StatsPrint(ostream&, bool) {}

struct Base {
  Base(cptr_t, cptr_t, const char*, const size_t) {}
};

struct Counter {
  void count(Number = 1) {}
  void aggregate(const Counter&) {}
  void reset() {}
};

struct Average {
  void count(Number) {}
  void aggregate(const Average&) {}
  void reset() {}
};

template<size_t N>
struct HashTable {
  void count(Number) {}
  void aggregate(const HashTable<N>&) {}
  void reset() {}
};

struct Distribution {
  void count(Number) {}
  void aggregate(const Distribution&) {}
  void reset() {}
};

struct Queue {
  void add(Number = 1) {}
  void remove(Number = 1) {}
  void fail() {}
  void tryfail() {}
  void aggregate(const Queue&) {}
  void reset() {}
};

#endif /* TESTING_ENABLE_STATISTICS */

struct EventScopeStats : public Base {
  Counter srvconn;
  Counter cliconn;
  Counter resets;
  Counter calls;
  Counter fails;
  EventScopeStats(cptr_t o, cptr_t p, const char* n = "EventScope   ") : Base(o, p, n, 0) {}
  void print(ostream& os) const;
  void aggregate(const EventScopeStats& x) {
    srvconn.aggregate(x.srvconn);
    cliconn.aggregate(x.cliconn);
    resets.aggregate(x.resets);
    calls.aggregate(x.calls);
    fails.aggregate(x.fails);
  }
  virtual void reset() {
    srvconn.reset();
    cliconn.reset();
    resets.reset();
    calls.reset();
    fails.reset();
  }
};

struct PollerStats : public Base {
  Counter regs;
  Distribution eventsB;
  Distribution eventsNB;
  PollerStats(cptr_t o, cptr_t p, const char* n = "Poller") : Base(o, p, n, 1) {}
  void print(ostream& os) const;
  void aggregate(const PollerStats& x) {
    regs.aggregate(x.regs);
    eventsB.aggregate(x.eventsB);
    eventsNB.aggregate(x.eventsNB);
  }
  virtual void reset() {
    regs.reset();
    eventsB.reset();
    eventsNB.reset();
  }
};

struct IOUringStats : public Base {
  Distribution attempts;
  Distribution submits;
  Distribution eventsB;
  Distribution eventsNB;
  IOUringStats(cptr_t o, cptr_t p, const char* n = "IOUring") : Base(o, p, n, 1) {}
  void print(ostream& os) const;
  void aggregate(const IOUringStats& x) {
    attempts.aggregate(x.attempts);
    submits.aggregate(x.submits);
    eventsB.aggregate(x.eventsB);
    eventsNB.aggregate(x.eventsNB);
  }
  virtual void reset() {
    attempts.reset();
    submits.reset();
    eventsB.reset();
    eventsNB.reset();
  }
};

struct TimerStats : public Base {
  Distribution events;
  TimerStats(cptr_t o, cptr_t p, const char* n = "Timer       ") : Base(o, p, n, 1) {}
  void print(ostream& os) const;
  void aggregate(const TimerStats& x) {
    events.aggregate(x.events);
  }
  virtual void reset() {
    events.reset();
  }
};

struct ClusterStats : public Base {
  Counter pause;
  ClusterStats(cptr_t o, cptr_t p, const char* n = "Cluster     ") : Base(o, p, n, 2) {}
  void print(ostream& os) const;
  void aggregate(const ClusterStats& x) {
    pause.aggregate(x.pause);
  }
  virtual void reset() {
    pause.reset();
  }
};

struct IdleManagerStats : public Base {
  Distribution ready;
  Distribution blocked;
  IdleManagerStats(cptr_t o, cptr_t p, const char* n = "IdleManager") : Base(o, p, n, 1) {}
  void print(ostream& os) const;
  void aggregate(const IdleManagerStats& x) {
    ready.aggregate(x.ready);
    blocked.aggregate(x.blocked);
  }
  virtual void reset() {
    ready.reset();
    blocked.reset();
  }
};

struct ProcessorStats : public Base {
  Counter create;
  Counter start;
  Counter deq;
  Counter handover;
  Counter borrow;
  Counter steal;
  Counter idle;
  Counter wake;
  ProcessorStats(cptr_t o, cptr_t p, const char* n = "Processor  ") : Base(o, p, n, 2) {}
  void print(ostream& os) const;
  void aggregate(const ProcessorStats& x) {
    create.aggregate(x.create);
    start.aggregate(x.start);
    deq.aggregate(x.deq);
    handover.aggregate(x.handover);
    borrow.aggregate(x.borrow);
    steal.aggregate(x.steal);
    idle.aggregate(x.idle);
    wake.aggregate(x.wake);
  }
  virtual void reset() {
    create.reset();
    start.reset();
    deq.reset();
    handover.reset();
    borrow.reset();
    steal.reset();
    idle.reset();
    wake.reset();
  }
};

struct ReadyQueueStats : public Base {
  Queue queue;
  ReadyQueueStats(cptr_t o, cptr_t p, const char* n = "ReadyQueue") : Base(o, p, n, 0) {}
  void print(ostream& os) const;
  void aggregate(const ReadyQueueStats& x) {
    queue.aggregate(x.queue);
  }
  virtual void reset() {
    queue.reset();
  }
};

} // namespace FredStats

/*
  sort order for output:
  0 EventScope
  0 Poller
  0 IOUring
  1 Timer
  2 Cluster
  0  Poller
  1  IdleManager
  2  Processor
  0   ReadyQueue
  1   IOUring
  1   Poller
*/

#endif /* _Stats_h_ */
