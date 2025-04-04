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
#ifndef _EventScope_h_
#define _EventScope_h_ 1

/** @file */

#include "libfibre/Cluster.h"
#include "libfibre/Fibre.h"

#include <fcntl.h>        // O_NONBLOCK
#include <limits.h>       // PTHREAD_STACK_MIN
#include <sys/resource.h> // getrlimit
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h> // various syscalls

#ifdef __GNUC__
#define restrict __restrict__
#else
#define restrict
#endif

/**
 An EventScope object holds a set of Clusters and provides a common I/O
 scope.  Multiple EventScope objects can be used to take advantage of
 partitioned kernel file descriptor tables on Linux.
*/
class EventScope {
  // A vector for FDs works well here in principle, because POSIX guarantees
  // lowest-numbered FDs:
  // http://pubs.opengroup.org/onlinepubs/9699919799/functions/V2_chap02.html#tag_15_14
  // A fixed-size array based on 'getrlimit' is somewhat brute-force, but simple
  // and fast.
  struct SyncFD {
    Poller::SyncSem sync[2];
    BasePoller *poller[2];
    bool blocking;
    bool useUring;
    SyncFD() : poller{nullptr, nullptr}, blocking(false), useUring(false) {}
  } *fdSyncVector;

  int fdCount;

  EventScope *parentScope;
  MasterPoller *masterPoller; // runs without cluster
  TimerQueue timerQueue;      // scope-global timer queue

  // on Linux, file I/O cannot be monitored via select/poll/epoll
  // therefore, all file operations are executed on dedicated processor(s)
  Cluster *diskCluster;

  // main fibre, cluster
  Fibre *mainFibre;
  Cluster *mainCluster;

  // simple kludge to provide event-scope-local data
  void *clientData;

  FredStats::EventScopeStats *stats;
  pthread_t *tids;

  // TODO: not available until cluster deletion implemented
  ~EventScope() {
    delete mainFibre;
    delete mainCluster;
    masterPoller->terminate(_friend<EventScope>());
    delete masterPoller;
    delete[] fdSyncVector;
    delete tids;
  }

  static void cloneInternal(EventScope *This) {
    This->initSync();
    RASSERT0(This->parentScope);
    for (int f = 0; f < This->fdCount; f += 1) {
      This->fdSyncVector[f].blocking =
          This->parentScope->fdSyncVector[f].blocking;
      This->fdSyncVector[f].useUring =
          This->parentScope->fdSyncVector[f].useUring;
    }
#if defined(__linux__)
    SYSCALL(unshare(CLONE_FILES));
#else
    (void)This->parentScope;
#endif
    This->start();
  }

  EventScope(size_t pollerCount, EventScope *ps = nullptr)
      : parentScope(ps), timerQueue(this), diskCluster(nullptr), tids(nullptr) {
    RASSERT0(pollerCount > 0);
    stats = new FredStats::EventScopeStats(this, nullptr);
    mainCluster = new Cluster(*this, pollerCount,
                              _friend<EventScope>()); // create main cluster
  }

  void initSync() {
    struct rlimit rl;
    SYSCALL(getrlimit(RLIMIT_NOFILE,
                      &rl));   // get hard limit for file descriptor count
    rl.rlim_max = rl.rlim_cur; // firm up current FD limit
    SYSCALL(setrlimit(RLIMIT_NOFILE, &rl)); // and install maximum
    fdCount = rl.rlim_max +
              MasterPoller::extraTimerFD; // add fake timer fd, if necessary
    fdSyncVector = new SyncFD[fdCount];   // create vector of R/W sync points
  }

  void start() {
    masterPoller = new MasterPoller(
        *this, fdCount,
        _friend<EventScope>()); // start master poller & timer handling
    mainCluster->startPolling(
        _friend<EventScope>()); // start polling now (potentially new event
                                // scope)
  }

  void cleanupFD(int fd) {
    RASSERT0(fd >= 0 && fd < fdCount);
    SyncFD &fdsync = fdSyncVector[fd];
    fdsync.sync[false].reset();
    fdsync.sync[true].reset();
    fdsync.poller[false] = nullptr;
    fdsync.poller[true] = nullptr;
    fdsync.blocking = false;
    fdsync.useUring = false;
  }

  template <bool Input, bool Cluster> BasePoller &getPoller(int fd) {
    if (!Input)
      return Context::CurrCluster().getOutputPoller(fd);
#if TESTING_WORKER_POLLER
    if (!Cluster)
      return Cluster::getWorkerPoller(
          CurrFibre()->getProcessor(_friend<EventScope>()));
#endif
    return Context::CurrCluster().getInputPoller(fd);
  }

  template <bool Input> inline bool TestEAGAIN() {
    int serrno = _SysErrno();
    stats->resets.count((int)(serrno == ECONNRESET));
#if defined(__FreeBSD__)
    // workaround - suspect:
    // https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=129169 - or similar?
    return serrno == EAGAIN || (Input == false && serrno == ENOTCONN);
#else // __linux__
    return serrno == EAGAIN;
#endif
  }

  template <bool Input, typename T, class... Args>
  bool tryIO(T &ret, T (*iofunc)(int, Args...), int fd, Args... a) {
    stats->calls.count();
    ret = iofunc(fd, a...);
    if (ret >= 0 || !TestEAGAIN<Input>())
      return true;
    stats->fails.count();
    return false;
  }

  template <bool Input, bool Accept, typename T, class... Args>
  T syncIO(T (*iofunc)(int, Args...), int fd, Args... a) {
    T ret;
    static const bool Read = Input && !Accept;
    static const Poller::Direction direction =
        Input ? Poller::Input : Poller::Output;
#if TESTING_EVENTPOLL_EDGE
    static const Poller::Variant variant =
        Input ? Poller::Edge : Poller::Oneshot;
#elif TESTING_EVENTPOLL_ONESHOT
    static const Poller::Variant variant = Poller::Oneshot;
#elif TESTING_EVENTPOLL_ONDEMAND
    static const Poller::Variant variant =
        Input ? Poller::OnDemand : Poller::Oneshot;
#else // level
    static const Poller::Variant variant =
        Read ? Poller::Level : Poller::Oneshot;
#endif
    if (Read) {
#if TESTING_EVENTPOLL_TRYREAD
      Fibre::yield();
      if (tryIO<Input>(ret, iofunc, fd, a...))
        return ret;
#endif
    } else {
      if (tryIO<Input>(ret, iofunc, fd, a...))
        return ret;
    }
    BasePoller *&poller = fdSyncVector[fd].poller[Input];
    if (!poller) {
      poller = &getPoller<Input, Accept>(fd);
      poller->setupFD(fd, Poller::Create, direction, variant);
    } else if (variant == Poller::Oneshot) {
      poller->setupFD(fd, Poller::Modify, direction, variant);
    }
    Poller::SyncSem &sync = fdSyncVector[fd].sync[Input];
    for (;;) {
      if (variant == Poller::Level)
        sync.wait();
      else
        sync.P();
      if (tryIO<Input>(ret, iofunc, fd, a...))
        return ret;
      if (variant == Poller::Oneshot) {
        poller->setupFD(fd, Poller::Modify, direction, variant);
      }
    }
  }

  int checkAsyncCompletion(int fd) {
    SyncFD &fdsync = fdSyncVector[fd];
    fdsync.poller[false] = &getPoller<false, false>(fd);
    fdsync.poller[false]->setupFD(fd, Poller::Create, Poller::Output,
                                  Poller::Oneshot); // register immediately
    fdsync.sync[false].P();                         // wait for completion
    int err;
    socklen_t sz = sizeof(err);
    SYSCALL(getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &sz));
    return err;
  }

  template <typename T, class... Args>
  T blockingInput(T (*readfunc)(int, Args...), int fd, Args... a) {
    return syncIO<true, false>(readfunc, fd, a...); // yield before read
  }

  template <typename T, class... Args>
  T blockingOutput(T (*writefunc)(int, Args...), int fd, Args... a) {
    return syncIO<false, false>(writefunc, fd, a...); // no yield before write
  }

public:
  pthread_t *get_tids() const { return tids; }
  /** Create an event scope during bootstrap. */
  static EventScope *bootstrap(std::list<size_t> &cpulist,
                               size_t pollerCount = 1, size_t workerCount = 1) {
    EventScope *es = new EventScope(pollerCount);
    es->mainFibre = es->mainCluster->registerWorker(_friend<EventScope>());
    if (workerCount > 1)
      es->mainCluster->addWorkers(workerCount - 1);
    pthread_t *tids = new pthread_t[workerCount];
    es->mainCluster->getWorkerSysIDs(tids, workerCount);
    cpu_set_t onecpu;
    CPU_ZERO(&onecpu);
    auto it = cpulist.begin();
    for (size_t i = 0; i < workerCount && it != cpulist.end(); i++, it++) {
      CPU_SET(*it, &onecpu);
      SYSCALL(pthread_setaffinity_np(tids[i], sizeof(cpu_set_t), &onecpu));
      CPU_CLR(*it, &onecpu);
    }
    // delete[] tids;
    // store tids
    es->tids = tids;
    es->initSync();
    es->start();
    return es;
  }

  /** Create an event scope during bootstrap. */
  static EventScope *bootstrap(size_t pollerCount = 1, size_t workerCount = 1) {
    std::list<size_t> dummy;
    return bootstrap(dummy, pollerCount, workerCount);
  }

  /** Create a event scope by cloning the current one.
      The new event scope automatically starts with a single worker (pthread)
      and a separate kernel file descriptor table where supported (Linux).
      `mainFunc(mainArg)` is invoked as main fibre of the new scope. */
  EventScope *clone(funcvoid1_t mainFunc, ptr_t mainArg,
                    size_t pollerCount = 1) {
    EventScope *es = new EventScope(pollerCount, this);
    es->mainCluster->addWorker((funcvoid1_t)cloneInternal,
                               (ptr_t)es); // calls initSync()/start()
    es->mainFibre = new Fibre(*es->mainCluster);
    es->mainFibre->setName("u:Main");
    es->mainFibre->run(mainFunc, mainArg);
    return es;
  }

  void preFork() {
    // TODO: assert globalClusterCount == 1
    // TODO: test for other fibres?
    RASSERT0(CurrFibre() == mainFibre);
    RASSERT0(timerQueue.empty());
    RASSERT0(diskCluster == nullptr);
    mainCluster->preFork(_friend<EventScope>());
    for (int f = 0; f < fdCount; f += 1) {
      RASSERT(fdSyncVector[f].sync[false].getValue() >= 0, f);
      RASSERT(fdSyncVector[f].sync[true].getValue() >= 0, f);
      RASSERT(fdSyncVector[f].poller[false] == 0, f);
      RASSERT(fdSyncVector[f].poller[true] == 0, f);
    }
  }

  void postFork() {
    new (stats) FredStats::EventScopeStats(this, nullptr);
    timerQueue.reinit(this);
#if defined(__linux__)
    delete masterPoller; // FreeBSD does not copy kqueue across fork()
#endif
    masterPoller = new MasterPoller(
        *this, fdCount,
        _friend<EventScope>()); // start master poller & timer handling
    mainCluster->postFork(this, _friend<EventScope>());
    mainCluster->startPolling(_friend<EventScope>());
  }

  /** Wait for the main routine of a cloned event scope. */
  void join() { mainFibre->join(); }

  /** Create disk cluster (if needed for application). */
  Cluster &addDiskCluster(size_t cnt = 1) {
    RASSERT0(!diskCluster);
    diskCluster = new Cluster;
    diskCluster->addWorkers(cnt);
    return *diskCluster;
  }

  /** Set event-scope-local data. */
  void setClientData(void *cd) { clientData = cd; }

  /** Get event-scope-local data. */
  void *getClientData() { return clientData; }

  TimerQueue &getTimerQueue() { return timerQueue; }

  void setTimer(const Time &timeout) { masterPoller->setTimer(timeout); }

  bool tryblock(int fd, _friend<MasterPoller>) {
    RASSERT0(fd >= 0 && fd < fdCount);
    return fdSyncVector[fd].sync[true].tryP();
  }

#if TESTING_WORKER_POLLER
  bool tryblock(int fd, _friend<WorkerPoller>) {
    RASSERT0(fd >= 0 && fd < fdCount);
    return fdSyncVector[fd].sync[true].tryP();
  }
#endif

  template <bool Input, bool Enqueue = true>
  Fred *unblock(int fd, _friend<BasePoller>) {
    RASSERT0(fd >= 0 && fd < fdCount);
    return fdSyncVector[fd].sync[Input].V<Enqueue>();
  }

  void registerPollFD(int fd, _friend<PollerFibre>) {
    RASSERT0(fd >= 0 && fd < fdCount);
    masterPoller->setupFD(fd, Poller::Create, Poller::Input, Poller::Oneshot);
  }

  void blockPollFD(int fd, _friend<PollerFibre>) {
    RASSERT0(fd >= 0 && fd < fdCount);
    masterPoller->setupFD(fd, Poller::Modify, Poller::Input, Poller::Oneshot);
    fdSyncVector[fd].sync[true].P();
  }

  void unblockPollFD(int fd, _friend<PollerFibre>) {
    RASSERT0(fd >= 0 && fd < fdCount);
    fdSyncVector[fd].sync[true].V();
  }

  template <typename T, class... Args>
  T directIO(T (*diskfunc)(Args...), Args... a) {
    RASSERT0(diskCluster);
    BaseProcessor &proc = Fibre::migrate(*diskCluster);
    int result = diskfunc(a...);
    Fibre::migrate(proc);
    return result;
  }

  template <typename T, class... Args>
  T syncInput(T (*readfunc)(int, Args...), int fd, Args... a) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return readfunc(fd, a...);
    return blockingInput(readfunc, fd, a...);
  }

  template <typename T, class... Args>
  T syncOutput(T (*writefunc)(int, Args...), int fd, Args... a) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return writefunc(fd, a...);
    return blockingOutput(writefunc, fd, a...);
  }

#if defined(__linux__)
  int epoll_wait(int epfd, struct epoll_event *events, int maxevents,
                 int timeout) {
    RASSERT0(epfd >= 0 && epfd < fdCount);
    stats->calls.count((int)(timeout != 0));
    int ret = ::epoll_wait(epfd, events, maxevents, 0);
    if (ret != 0 || timeout == 0)
      return ret;
    stats->fails.count();
    BasePoller *&poller = fdSyncVector[epfd].poller[true];
    if (!poller) {
      poller = &getPoller<true, false>(epfd);
      poller->setupFD(epfd, Poller::Create, Poller::Input, Poller::Oneshot);
    } else {
      poller->setupFD(epfd, Poller::Modify, Poller::Input, Poller::Oneshot);
    }
    Poller::SyncSem &sync = fdSyncVector[epfd].sync[true];
    Time absTimeout;
    if (timeout > 0)
      absTimeout = Runtime::Timer::now() + Time::fromMS(timeout);
    for (;;) {
      if (timeout < 0)
        sync.P();
      else if (!sync.P(absTimeout))
        return 0;
      stats->calls.count();
      ret = ::epoll_wait(epfd, events, maxevents, 0);
      if (ret != 0)
        return ret;
      stats->fails.count();
      poller->setupFD(epfd, Poller::Modify, Poller::Input, Poller::Oneshot);
    }
  }
#endif

  int socket(int domain, int type, int protocol, bool useUring) {
    int ret = ::socket(domain, type | (useUring ? 0 : SOCK_NONBLOCK), protocol);
    if (ret < 0)
      return ret;
    fdSyncVector[ret].blocking = !(type & SOCK_NONBLOCK);
    fdSyncVector[ret].useUring = useUring;
    return ret;
  }

#if TESTING_WORKER_IO_URING
  inline bool uring(int fd) { return fdSyncVector[fd].useUring; }
#endif

  int bind(int fd, const sockaddr *addr, socklen_t addrlen) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::bind(fd, addr, addrlen);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return ::bind(fd, addr, addrlen);
#endif
    int ret = ::bind(fd, addr, addrlen);
    if (ret < 0) {
      if (_SysErrno() != EINPROGRESS)
        return ret;
      ret = checkAsyncCompletion(fd);
      if (ret != 0) {
        _SysErrnoSet() = ret;
        return -1;
      }
    }
    return ret;
  }

  int connect(int fd, const sockaddr *addr, socklen_t addrlen) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::connect(fd, addr, addrlen);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_connect, fd, addr,
                                              addrlen);
#endif
    int ret = ::connect(fd, addr, addrlen);
    if (ret < 0) {
      if (_SysErrno() != EINPROGRESS)
        return ret;
      ret = checkAsyncCompletion(fd);
      if (ret != 0) {
        _SysErrnoSet() = ret;
        return -1;
      }
    }
    stats->cliconn.count();
    return ret;
  }

  int accept4(int fd, sockaddr *addr, socklen_t *addrlen, int flags) {
    RASSERT0(fd >= 0 && fd < fdCount);
    int ret;
#if TESTING_WORKER_IO_URING
    if (uring(fd)) {
      ret = fdSyncVector[fd].blocking
                ? Cluster::getWorkerUring().syncIO(io_uring_prep_accept, fd,
                                                   addr, addrlen, flags)
                : ::accept4(fd, addr, addrlen, flags);
    } else
#endif
      ret = fdSyncVector[fd].blocking
                ? syncIO<true, true>(::accept4, fd, addr, addrlen,
                                     flags | SOCK_NONBLOCK)
                : ::accept4(fd, addr, addrlen, flags | SOCK_NONBLOCK);
    if (ret < 0)
      return ret;
    fdSyncVector[ret].blocking = !(flags & SOCK_NONBLOCK);
    fdSyncVector[ret].useUring = fdSyncVector[fd].useUring;
    stats->srvconn.count();
    return ret;
  }

  int dup(int fd) {
    int ret = ::dup(fd);
    if (ret < 0)
      return ret;
    fdSyncVector[ret].blocking = fdSyncVector[fd].blocking;
    fdSyncVector[ret].useUring = fdSyncVector[fd].useUring;
    return ret;
  }

  int pipe2(int pipefd[2], int flags, bool useUring) {
    int ret = ::pipe2(pipefd, flags | (useUring ? 0 : O_NONBLOCK));
    if (ret < 0)
      return ret;
    fdSyncVector[pipefd[0]].blocking = !(flags & O_NONBLOCK);
    fdSyncVector[pipefd[0]].useUring = useUring;
    fdSyncVector[pipefd[1]].blocking = !(flags & O_NONBLOCK);
    fdSyncVector[pipefd[1]].useUring = useUring;
    return ret;
  }

  int fcntl(int fd, int cmd, int flags) {
    RASSERT0(fd >= 0 && fd < fdCount);
    int ret =
        ::fcntl(fd, cmd, flags | (fdSyncVector[fd].useUring ? 0 : O_NONBLOCK));
    if (ret < 0)
      return ret;
    fdSyncVector[fd].blocking = !(flags & O_NONBLOCK);
    return ret;
  }

  int close(int fd) {
    cleanupFD(fd);
    return ::close(fd);
  }

  int read(int fd, void *buf, size_t nbyte) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::read(fd, buf, nbyte);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(
          io_uring_prep_read, fd, buf, (unsigned)nbyte, (UringOffsetType)0);
#endif
    return blockingInput(::read, fd, buf, nbyte);
  }

  int pread(int fd, void *buf, size_t nbyte, off_t offset) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::pread(fd, buf, nbyte, offset);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_read, fd, buf,
                                              (unsigned)nbyte,
                                              (UringOffsetType)offset);
#endif
    return blockingInput(::pread, fd, buf, nbyte, offset);
  }

  int readv(int fd, const struct iovec *iovecs, int nr_vecs) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::readv(fd, iovecs, nr_vecs);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_readv, fd, iovecs,
                                              (unsigned)nr_vecs,
                                              (UringOffsetType)0);
#endif
    return blockingInput(::readv, fd, iovecs, nr_vecs);
  }

  int preadv(int fd, const struct iovec *iovecs, int nr_vecs, off_t offset) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::preadv(fd, iovecs, nr_vecs, offset);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_readv, fd, iovecs,
                                              (unsigned)nr_vecs,
                                              (UringOffsetType)offset);
#endif
    return blockingInput(::preadv, fd, iovecs, nr_vecs, offset);
  }

  int write(int fd, const void *buf, size_t nbyte) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::write(fd, buf, nbyte);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(
          io_uring_prep_write, fd, buf, (unsigned)nbyte, (UringOffsetType)0);
#endif
    return blockingOutput(::write, fd, buf, nbyte);
  }

  int pwrite(int fd, const void *buf, size_t nbyte, off_t offset) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::pwrite(fd, buf, nbyte, offset);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_write, fd, buf,
                                              (unsigned)nbyte,
                                              (UringOffsetType)offset);
#endif
    return blockingOutput(::pwrite, fd, buf, nbyte, offset);
  }

  int writev(int fd, const struct iovec *iovecs, int nr_vecs) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::writev(fd, iovecs, nr_vecs);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_writev, fd, iovecs,
                                              (unsigned)nr_vecs,
                                              (UringOffsetType)0);
#endif
    return blockingOutput(::writev, fd, iovecs, nr_vecs);
  }

  int pwritev(int fd, const struct iovec *iovecs, int nr_vecs, off_t offset) {
    RASSERT0(fd >= 0 && fd < fdCount);
    if (!fdSyncVector[fd].blocking)
      return ::pwritev(fd, iovecs, nr_vecs, offset);
#if TESTING_WORKER_IO_URING
    if (uring(fd))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_writev, fd, iovecs,
                                              (unsigned)nr_vecs,
                                              (UringOffsetType)offset);
#endif
    return blockingOutput(::pwritev, fd, iovecs, nr_vecs, offset);
  }

  ssize_t sendmsg(int socket, const struct msghdr *message, int flags) {
    RASSERT0(socket >= 0 && socket < fdCount);
    if (!fdSyncVector[socket].blocking)
      return ::sendmsg(socket, message, flags);
#if TESTING_WORKER_IO_URING
    if (uring(socket))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_sendmsg, socket,
                                              message, (unsigned)flags);
#endif
    return blockingOutput(::sendmsg, socket, message, flags);
  }

  ssize_t sendto(int socket, const void *message, size_t length, int flags,
                 const struct sockaddr *dest_addr, socklen_t dest_len) {
    RASSERT0(socket >= 0 && socket < fdCount);
    if (!fdSyncVector[socket].blocking)
      return ::sendto(socket, message, length, flags, dest_addr, dest_len);
#if TESTING_WORKER_IO_URING
    if (uring(socket)) {
      struct iovec iov = {.iov_base = (void *)message, .iov_len = length};
      const struct msghdr msg = {.msg_name = (struct sockaddr *)dest_addr,
                                 .msg_namelen = dest_len,
                                 .msg_iov = &iov,
                                 .msg_iovlen = 1,
                                 .msg_control = nullptr,
                                 .msg_controllen = 0,
                                 .msg_flags = 0};
      return sendmsg(socket, &msg, flags);
    }
#endif
    return blockingOutput(::sendto, socket, message, length, flags, dest_addr,
                          dest_len);
  }

  ssize_t send(int socket, const void *buffer, size_t length, int flags) {
    RASSERT0(socket >= 0 && socket < fdCount);
    if (!fdSyncVector[socket].blocking)
      return ::send(socket, buffer, length, flags);
#if TESTING_WORKER_IO_URING
    if (uring(socket))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_send, socket,
                                              buffer, length, flags);
#endif
    return blockingOutput(::send, socket, buffer, length, flags);
  }

  ssize_t recvmsg(int socket, struct msghdr *message, int flags) {
    RASSERT0(socket >= 0 && socket < fdCount);
    if (!fdSyncVector[socket].blocking)
      return ::recvmsg(socket, message, flags);
#if TESTING_WORKER_IO_URING
    if (uring(socket))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_recvmsg, socket,
                                              message, (unsigned)flags);
#endif
    return blockingInput(::recvmsg, socket, message, flags);
  }

  ssize_t recvfrom(int socket, void *restrict buffer, size_t length, int flags,
                   struct sockaddr *restrict address,
                   socklen_t *restrict address_len) {
    RASSERT0(socket >= 0 && socket < fdCount);
    if (!fdSyncVector[socket].blocking)
      return ::recvfrom(socket, buffer, length, flags, address, address_len);
#if TESTING_WORKER_IO_URING
    if (uring(socket)) {
      struct iovec iov = {.iov_base = buffer, .iov_len = length};
      struct msghdr msg = {.msg_name = address,
                           .msg_namelen = address_len ? *address_len : 0,
                           .msg_iov = &iov,
                           .msg_iovlen = 1,
                           .msg_control = nullptr,
                           .msg_controllen = 0,
                           .msg_flags = 0};
      ssize_t ret = recvmsg(socket, &msg, flags);
      if (ret >= 0 && address_len)
        *address_len = msg.msg_namelen;
      return ret;
    }
#endif
    return blockingInput(::recvfrom, socket, buffer, length, flags, address,
                         address_len);
  }

  ssize_t recv(int socket, void *buffer, size_t length, int flags) {
    RASSERT0(socket >= 0 && socket < fdCount);
    if (!fdSyncVector[socket].blocking)
      return ::recv(socket, buffer, length, flags);
#if TESTING_WORKER_IO_URING
    if (uring(socket))
      return Cluster::getWorkerUring().syncIO(io_uring_prep_recv, socket,
                                              buffer, length, flags);
#endif
    return blockingInput(::recv, socket, buffer, length, flags);
  }
};

/** @brief Generic input wrapper. User-level-block if file descriptor not ready
 * for reading. */
template <typename T, class... Args>
inline T lfInput(T (*readfunc)(int, Args...), int fd, Args... a) {
  return Context::CurrEventScope().syncInput(readfunc, fd,
                                             a...); // yield before read
}

/** @brief Generic output wrapper. User-level-block if file descriptor not ready
 * for writing. */
template <typename T, class... Args>
inline T lfOutput(T (*writefunc)(int, Args...), int fd, Args... a) {
  return Context::CurrEventScope().syncOutput(writefunc, fd,
                                              a...); // no yield before write
}

/** @brief Generic wrapper for I/O that cannot be polled. Fibre is migrated to
 * disk cluster for execution. */
template <typename T, class... Args>
inline T lfDirectIO(T (*diskfunc)(int, Args...), int fd, Args... a) {
  return Context::CurrEventScope().directIO(diskfunc, fd, a...);
}

#if TESTING_WORKER_IO_URING && TESTING_IO_URING_DEFAULT
static const bool DefaultUring = true;
#else
static const bool DefaultUring = false;
#endif

#if defined(__linux__)
static inline int lfEpollWait(int epfd, struct epoll_event *events,
                              int maxevents, int timeout) {
  return Context::CurrEventScope().epoll_wait(epfd, events, maxevents, timeout);
}
#endif

/** @brief Create new socket. */
static inline int lfSocket(int domain, int type, int protocol,
                           bool useUring = DefaultUring) {
  return Context::CurrEventScope().socket(domain, type, protocol, useUring);
}

/** @brief Set up socket listen queue. */
static inline int lfListen(int fd, int backlog) { return listen(fd, backlog); }

/** @brief Bind socket to local name. */
static inline int lfBind(int fd, const sockaddr *addr, socklen_t addrlen) {
  return Context::CurrEventScope().bind(fd, addr, addrlen);
}

/** @brief Accept new connection. New file descriptor registered for I/O events.
 */
static inline int lfAccept(int fd, sockaddr *addr, socklen_t *addrlen,
                           int flags = 0) {
  return Context::CurrEventScope().accept4(fd, addr, addrlen, flags);
}

/** @brief Create new connection. */
static inline int lfConnect(int fd, const sockaddr *addr, socklen_t addrlen) {
  return Context::CurrEventScope().connect(fd, addr, addrlen);
}

/** @brief Clone file descriptor. */
static inline int lfDup(int fd) { return Context::CurrEventScope().dup(fd); }

/** @brief Create pipe. */
static inline int lfPipe(int pipefd[2], int flags = 0,
                         bool useUring = DefaultUring) {
  return Context::CurrEventScope().pipe2(pipefd, flags, useUring);
}

/** @brief Close file descriptor. */
static inline int lfClose(int fd) {
  return Context::CurrEventScope().close(fd);
}

/** @brief Set file descriptor flags. */
static inline int lfFcntl(int fd, int cmd, int flags) {
  return Context::CurrEventScope().fcntl(fd, cmd, flags);
}

static inline int lfRead(int fd, void *buf, size_t nbyte) {
  return Context::CurrEventScope().read(fd, buf, nbyte);
}

static inline int lfPread(int fd, void *buf, size_t nbyte, off_t offset) {
  return Context::CurrEventScope().pread(fd, buf, nbyte, offset);
}

static inline int lfReadv(int fd, const struct iovec *iovecs, int nr_vecs) {
  return Context::CurrEventScope().readv(fd, iovecs, nr_vecs);
}

static inline int lfPreadv(int fd, const struct iovec *iovecs, int nr_vecs,
                           off_t offset) {
  return Context::CurrEventScope().preadv(fd, iovecs, nr_vecs, offset);
}

static inline int lfWrite(int fd, const void *buf, size_t nbyte) {
  return Context::CurrEventScope().write(fd, buf, nbyte);
}

static inline int lfPwrite(int fd, const void *buf, size_t nbyte,
                           off_t offset) {
  return Context::CurrEventScope().pwrite(fd, buf, nbyte, offset);
}

static inline int lfWritev(int fd, const struct iovec *iovecs, int nr_vecs) {
  return Context::CurrEventScope().writev(fd, iovecs, nr_vecs);
}

static inline int lfPwritev(int fd, const struct iovec *iovecs, int nr_vecs,
                            off_t offset) {
  return Context::CurrEventScope().pwritev(fd, iovecs, nr_vecs, offset);
}

static inline ssize_t lfSendmsg(int socket, const struct msghdr *message,
                                int flags) {
  return Context::CurrEventScope().sendmsg(socket, message, flags);
}

static inline ssize_t lfSendto(int socket, const void *message, size_t length,
                               int flags, const struct sockaddr *dest_addr,
                               socklen_t dest_len) {
  return Context::CurrEventScope().sendto(socket, message, length, flags,
                                          dest_addr, dest_len);
}

static inline ssize_t lfSend(int socket, const void *buffer, size_t length,
                             int flags) {
  return Context::CurrEventScope().send(socket, buffer, length, flags);
}

static inline ssize_t lfRecvmsg(int socket, struct msghdr *message, int flags) {
  return Context::CurrEventScope().recvmsg(socket, message, flags);
}

static inline ssize_t lfRecvfrom(int socket, void *restrict buffer,
                                 size_t length, int flags,
                                 struct sockaddr *restrict address,
                                 socklen_t *restrict address_len) {
  return Context::CurrEventScope().recvfrom(socket, buffer, length, flags,
                                            address, address_len);
}

static inline ssize_t lfRecv(int socket, void *buffer, size_t length,
                             int flags) {
  return Context::CurrEventScope().recv(socket, buffer, length, flags);
}

#endif /* _EventScope_h_ */
