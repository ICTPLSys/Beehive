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
#ifndef _Poller_h_
#define _Poller_h_ 1

#include "runtime/BlockingSync.h"

class BaseProcessor;
class Cluster;

#include <pthread.h>
#include <unistd.h>      // close
#if defined(__FreeBSD__)
#include <sys/event.h>
#else // __linux__ below
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#endif

class EventScope;
class Fibre;
class Fred;

struct Poller {
#if defined(__FreeBSD__)
  typedef struct kevent EventType;
  enum Operation : ssize_t { Create = EV_ADD, Modify = EV_ADD, Remove = EV_DELETE };
  enum Direction : ssize_t { Input = EVFILT_READ, Output = EVFILT_WRITE };
  enum Variant   : ssize_t { Level = 0, Edge = EV_CLEAR, Oneshot = EV_ONESHOT };
#else // __linux__ below
#define EPOLLONDEMAND (1u << 24)
  typedef epoll_event   EventType; // man 2 epoll_ctl: EPOLLERR, EPOLLHUP not needed
  enum Operation : ssize_t { Create = EPOLL_CTL_ADD, Modify = EPOLL_CTL_MOD, Remove = EPOLL_CTL_DEL };
  enum Direction : ssize_t { Input = EPOLLIN | EPOLLPRI | EPOLLRDHUP, Output = EPOLLOUT };
  enum Variant   : ssize_t { Level = 0, Edge = EPOLLET, Oneshot = EPOLLONESHOT, OnDemand = EPOLLONESHOT | EPOLLONDEMAND };
#endif
  typedef LockedSemaphore<WorkerLock,true> SyncSem;
};

class BasePoller : public Poller {
protected:
  static const int MaxPoll = 256;
  EventType events[MaxPoll];
  int       pollFD;
#if defined(__FreeBSD__)
  SyncSem   userEvent;
#endif

  EventScope&   eventScope;
  volatile bool pollTerminate;

  FredStats::PollerStats* stats;

  template<bool Blocking>
  inline int doPoll(bool CountAsBlocking = Blocking);

  template<bool Enqueue = true>
  inline Fred* notifyOne(EventType& ev);

  inline void notifyAll(int evcnt);

public:
  BasePoller(EventScope& es, cptr_t parent, const char* n = "BasePoller") : eventScope(es), pollTerminate(false) {
    stats = new FredStats::PollerStats(this, parent, n);
#if defined(__FreeBSD__)
    pollFD = SYSCALLIO(kqueue());
#else // __linux__ below
    pollFD = SYSCALLIO(epoll_create1(EPOLL_CLOEXEC));
#endif
    DBG::outl(DBG::Level::Polling, "Poller ", FmtHex(this), " create ", pollFD);
  }

  ~BasePoller() {
    SYSCALL(close(pollFD));
  }

  void setupFD(int fd, Operation op, Direction dir, Variant var) {
    DBG::outl(DBG::Level::Polling, "Poller ", FmtHex(this), " setup ", fd, " at ", pollFD, " with ", op, '/', dir, '/', var);
    stats->regs.count(op != Remove);
#if defined(__FreeBSD__)
    struct kevent ev;
    EV_SET(&ev, fd, dir, op | (op == Remove ? 0 : var), 0, 0, 0);
    SYSCALL(kevent(pollFD, &ev, 1, nullptr, 0, nullptr));
#else // __linux__ below
    epoll_event ev;
    ev.events = (uint32_t)dir | (uint32_t)var;
    ev.data.fd = fd;
    SYSCALL(epoll_ctl(pollFD, op, fd, op == Remove ? nullptr : &ev));
#endif
  }
};

#if TESTING_WORKER_POLLER
class WorkerPoller : public BasePoller {
#if defined(__linux__)
  int haltFD;
#endif
  enum PollType : size_t { Poll, Suspend, Try };
  template<PollType PT> inline size_t internalPoll();
public:
  WorkerPoller(EventScope& es, cptr_t parent, const char* n = "W-Poller  ") : BasePoller(es, parent, n) {
#if defined(__linux__)
    haltFD = SYSCALLIO(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    setupFD(haltFD, Poller::Create, Poller::Input, Poller::Level);
#else
    struct kevent ev;
    EV_SET(&ev, 0, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, 0);
    SYSCALL(kevent(pollFD, &ev, 1, nullptr, 0, nullptr));
#endif
  }
#if defined(__linux__)
  ~WorkerPoller() { SYSCALL(close(haltFD)); }
#endif
  size_t poll(_friend<Cluster>);
  size_t trySuspend(_friend<Cluster>);
  void suspend(_friend<Cluster>);
  void resume(_friend<Cluster>) {
#if defined(__linux__)
    uint64_t val = 1;
    SYSCALL_EQ(write(haltFD, &val, sizeof(val)), sizeof(val));
#else
    struct kevent ev;
    EV_SET(&ev, 0, EVFILT_USER, EV_ENABLE, NOTE_TRIGGER, 0, 0);
    SYSCALL(kevent(pollFD, &ev, 1, nullptr, 0, nullptr));
#endif
  }
};
#endif

class PollerFibre : public BasePoller {
  Fibre* pollFibre;
  inline void pollLoop();
  static void pollLoopSetup(PollerFibre*);

public:
  PollerFibre(EventScope&, cptr_t parent, const char* n, _friend<Cluster>);
  ~PollerFibre();
  void start();
};

class BaseThreadPoller : public BasePoller {
  pthread_t pollThread;

protected:
  BaseThreadPoller(EventScope& es, cptr_t parent, const char* n) : BasePoller(es, parent, n) {}
  void start(void *(*loopSetup)(void*)) {
    SYSCALL(pthread_create(&pollThread, nullptr, loopSetup, this));
  }

  template<typename T>
  static inline void pollLoop(T& This);

public:
  pthread_t getSysThreadId() { return pollThread; }
  void terminate(_friend<EventScope>);
};

class PollerThread : public BaseThreadPoller {
  static void* pollLoopSetup(void*);

public:
  PollerThread(EventScope& es, cptr_t parent, const char* n, _friend<Cluster>) : BaseThreadPoller(es, parent, n) {}
  void prePoll(_friend<BaseThreadPoller>) {}
  void start() { BaseThreadPoller::start(pollLoopSetup); }
};

class MasterPoller : public BaseThreadPoller {
  int timerFD;
  static void* pollLoopSetup(void*);

public:
#if defined(__FreeBSD__)
  static const int extraTimerFD = 1;
#else
  static const int extraTimerFD = 0;
#endif

  MasterPoller(EventScope& es, unsigned long fdCount, _friend<EventScope>) : BaseThreadPoller(es, &es, "MasterPoller") {
    BaseThreadPoller::start(pollLoopSetup);
#if defined(__FreeBSD__)
    timerFD = fdCount - 1;
#else
    (void)fdCount;
    timerFD = SYSCALLIO(timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK | TFD_CLOEXEC));
    setupFD(timerFD, Create, Input, Level);
#endif
  }

#if defined(__linux__)
  ~MasterPoller() { SYSCALL(close(timerFD)); }
#endif

  inline void prePoll(_friend<BaseThreadPoller>);

  void setTimer(const Time& timeout) {
#if defined(__FreeBSD__)
    struct kevent ev;
    EV_SET(&ev, timerFD, EVFILT_TIMER, EV_ADD | EV_ONESHOT, NOTE_USECONDS | NOTE_ABSTIME, timeout.toUS(), 0);
    SYSCALL(kevent(pollFD, &ev, 1, nullptr, 0, nullptr));
#else
    itimerspec tval = { {0,0}, timeout };
    SYSCALL(timerfd_settime(timerFD, TFD_TIMER_ABSTIME, &tval, nullptr));
#endif
  }
};

#endif /* _Poller_h_ */
