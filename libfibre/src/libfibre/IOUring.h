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
#ifndef _IOUring_h_
#define _IOUring_h_ 1

#include "runtime/BlockingSync.h"
#include "libfibre/Fibre.h"

#include <cstring>
#include <liburing.h>
#include <sys/eventfd.h>

#if defined(OLDURING)
typedef off_t UringOffsetType; // liburing version 2.0 and lower
#else
typedef __u64 UringOffsetType; // liburing version 2.1 and higher
#endif

class IOUring {
  int haltFD;
  uint64_t count;
  struct io_uring ring;
  size_t sqe_count;
  static const int BatchSize = 64;
  static const int NumEntries = 4096;
  struct io_uring_cqe* cqe[NumEntries];

  FredStats::IOUringStats* stats;

  struct Block {
    Fibre* fibre;
    int retcode;
    Block(Fibre* f) : fibre(f) {}
  };

  void processCQE(struct io_uring_cqe* cqe, size_t& evcnt, size_t& resume) {
    Block* b = (Block*)io_uring_cqe_get_data(cqe);
    if (b) {
      b->retcode = cqe->res;
      b->fibre->resume();
      evcnt += 1;
    } else {
      RASSERT(resume == 0, resume);
      resume += 1;
    }
  }

  enum PollType : size_t { Poll, Suspend, Try, Check };

  template<PollType PT>
  size_t internalPoll() {
    if (PT != Check && sqe_count > 0) submitRing();
    size_t resume = 0;
    size_t evcnt = 0;
    if (PT == Suspend) {
      while (TRY_SYSCALL(io_uring_wait_cqe(&ring, cqe), EINTR) < 0);
      processCQE(cqe[0], evcnt, resume);
      io_uring_cq_advance(&ring, 1);
    }
    size_t cnt = io_uring_peek_batch_cqe(&ring, cqe, NumEntries);
    for (size_t idx = 0; idx < cnt; idx += 1) processCQE(cqe[idx], evcnt, resume);
    io_uring_cq_advance(&ring, cnt);
    if (PT == Suspend) stats->eventsB.count(evcnt);
    else stats->eventsNB.count(evcnt);
    return (PT == Poll) ? evcnt : resume;
  }

  bool submitRing() {
    stats->attempts.count(sqe_count);
    int submitted = TRY_SYSCALL_GE2(io_uring_submit(&ring), 1, EBUSY, EAGAIN);
    if (submitted < 0) return false;
    stats->submits.count(submitted);
    sqe_count -= submitted;
    return true;
  }

  template<class... Args>
  void submit(Block* b, void (*prepfunc)(struct io_uring_sqe *sqe, Args...), Args... a) {
    struct io_uring_sqe* sqe;
    for (;;) {
      sqe = io_uring_get_sqe(&ring);
      if (sqe) break;
      if (!submitRing()) internalPoll<Check>();
    }
    sqe_count += 1;
    prepfunc(sqe, a...);
    io_uring_sqe_set_data(sqe, b);
    if (sqe_count < BatchSize) return;
    while (!submitRing()) internalPoll<Check>();
  }

public:
  IOUring(cptr_t parent, const char* n) : sqe_count(0) {
    stats = new FredStats::IOUringStats(this, parent, n);
    haltFD = SYSCALLIO(eventfd(0, EFD_CLOEXEC));
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));
    SYSCALLIO(io_uring_queue_init_params(NumEntries, &ring, &p));
    DBG::outl(DBG::Level::Polling, "SQE: ", p.sq_entries, " CQE: ", p.cq_entries);
    submit(nullptr, io_uring_prep_read, haltFD, (void*)&count, (unsigned)sizeof(count), (UringOffsetType)0);
  }

  ~IOUring() {
    io_uring_queue_exit(&ring);
    SYSCALL(close(haltFD));
  }

  size_t poll(_friend<Cluster>) {
    return internalPoll<Poll>();
  }

  size_t trySuspend(_friend<Cluster>) {
    size_t ret = internalPoll<Try>();
    if (ret) {
      RASSERT(count == 1, count);
      submit(nullptr, io_uring_prep_read, haltFD, (void*)&count, (unsigned)sizeof(count), (UringOffsetType)0);
    }
    return ret;
  }

  void suspend(_friend<Cluster>) {
    while (internalPoll<Suspend>() == 0) {}
    RASSERT(count == 1, count);
    submit(nullptr, io_uring_prep_read, haltFD, (void*)&count, (unsigned)sizeof(count), (UringOffsetType)0);
  }

  void resume(_friend<Cluster>) {
    uint64_t val = 1;
    SYSCALL_EQ(write(haltFD, &val, sizeof(val)), sizeof(val));
  }

  template<class... Args>
  int syncIO( void (*prepfunc)(struct io_uring_sqe *sqe, Args...), Args... a) {
    Block b(CurrFibre());
    RuntimeDisablePreemption();
    submit(&b, prepfunc, a...);
    Suspender::suspend<false>(*b.fibre);
    int ret = (volatile int)b.retcode; // uring conveys errno via result code
    if (ret < 0) _SysErrnoSet() = -ret;
    return ret;
  }
};

#endif /* _IOUring_h_ */
