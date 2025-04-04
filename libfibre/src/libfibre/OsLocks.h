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
#ifndef _OsLocks_h_
#define _OsLocks_h_ 1

#include "runtime/Basics.h"
#include "runtime/ScopedLocks.h"

extern "C" {
#include "errnoname/errnoname.h"
}
#define SYSCALL_HAVE_ERRNONAME 1

// errno is TLS, so must not be inlined
// see, for example, http://www.crystalclearsoftware.com/soc/coroutine/coroutine/coroutine_thread.html
extern int _SysErrno() __no_inline;
extern int& _SysErrnoSet() __no_inline;
#define SYSCALL_HAVE_SYSERRNO 1

extern void _SYSCALLabort() __noreturn;
extern void _SYSCALLabortLock();
extern void _SYSCALLabortUnlock();
#define	SYSCALL_HAVE_ABORT 1

#include "libfibre/syscall_macro.h"

#include <pthread.h>
#include <semaphore.h>

template<int SpinStart, int SpinEnd, int SpinCount>
class OsLock {
  pthread_mutex_t mutex;
  friend class OsCondition;
public:
  OsLock() : mutex(PTHREAD_MUTEX_INITIALIZER) {}
  ~OsLock() {
    SYSCALL(pthread_mutex_destroy(&mutex));
  }
  bool tryAcquire() {
    return pthread_mutex_trylock(&mutex) == 0;
  }
  void acquire() {
    for (int cnt = 0; cnt < SpinCount; cnt += 1) {
      for (int spin = SpinStart; spin < SpinEnd; spin += spin) {
        if fastpath(tryAcquire()) return;
        for (int i = 0; i < spin; i += 1) Pause();
      }
    }
    SYSCALL(pthread_mutex_lock(&mutex));
  }
  bool acquire(const Time& timeout) {
    return TRY_SYSCALL(pthread_mutex_timedlock(&mutex, &timeout), ETIMEDOUT) == 0;
  }
  void release() {
    SYSCALL(pthread_mutex_unlock(&mutex));
  }
};

class OsCondition {
  pthread_cond_t cond;
public:
  OsCondition() : cond(PTHREAD_COND_INITIALIZER) {}
  ~OsCondition() {
    SYSCALL(pthread_cond_destroy(&cond));
  }
  template<typename Lock>
  void clear(Lock& lock) {
    SYSCALL(pthread_cond_broadcast(&cond));
    lock.release();
  }
  template<typename Lock>
  void wait(Lock& lock) {
    SYSCALL(pthread_cond_wait(&cond, &lock.mutex));
  }
  template<typename Lock>
  bool wait(Lock& lock, const Time& timeout) {
    return TRY_SYSCALL(pthread_cond_timedwait(&cond, &lock.mutex, &timeout), ETIMEDOUT) == 0;
  }
  void signal() {
    SYSCALL(pthread_cond_signal(&cond));
  }
};

template<size_t SpinStartRead, size_t SpinEndRead, size_t SpinCountRead, size_t SpinStartWrite, size_t SpinEndWrite, size_t SpinCountWrite>
class OsLockRW {
  pthread_rwlock_t rwlock;
public:
  OsLockRW() {
    SYSCALL(pthread_rwlock_init(&rwlock, nullptr));
  }
  ~OsLockRW() {
    SYSCALL(pthread_rwlock_destroy(&rwlock));
  }
  bool tryAcquireRead() {
    return pthread_rwlock_tryrdlock(&rwlock) == 0;
  }
  void acquireRead() {
    for (size_t cnt = 0; cnt < SpinCountRead; cnt += 1) {
      for (size_t spin = SpinStartRead; spin <= SpinEndRead; spin += spin) {
        if fastpath(tryAcquireRead()) return;
        for (size_t i = 0; i < spin; i += 1) Pause();
      }
    }
    SYSCALL(pthread_rwlock_rdlock(&rwlock));
  }
  bool acquireRead(const Time& timeout) {
    return TRY_SYSCALL(pthread_rwlock_timedrdlock(&rwlock, &timeout), ETIMEDOUT) == 0;
  }
  bool tryAcquireWrite() {
    return pthread_rwlock_trywrlock(&rwlock) == 0;
  }
  void acquireWrite() {
    for (size_t cnt = 0; cnt < SpinCountWrite; cnt += 1) {
      for (size_t spin = SpinStartWrite; spin <= SpinEndWrite; spin += spin) {
        if fastpath(tryAcquireWrite()) return;
        for (size_t i = 0; i < spin; i += 1) Pause();
      }
    }
    SYSCALL(pthread_rwlock_wrlock(&rwlock));
  }
  bool acquireWrite(const Time& timeout) {
    return TRY_SYSCALL(pthread_rwlock_timedwrlock(&rwlock, &timeout), ETIMEDOUT) == 0;
  }
  void release() {
    SYSCALL(pthread_rwlock_unlock(&rwlock));
  }
};

class OsSemaphore {
  sem_t sem;
public:
  OsSemaphore(size_t c = 0) {
    SYSCALL(sem_init(&sem, 0, c));
  }
  ~OsSemaphore() {
    SYSCALL(sem_destroy(&sem));
  }
  bool empty() {
    int val;
    SYSCALL(sem_getvalue(&sem, &val));
    return val >= 0;
  }
  bool open() {
    int val;
    SYSCALL(sem_getvalue(&sem, &val));
    return val > 0;
  }
  bool tryP() {
    for (;;) {
      int ret = sem_trywait(&sem);
      if (ret == 0) return true;
      if (_SysErrno() == EAGAIN) return false;
      RASSERT(_SysErrno() == EINTR, _SysErrno());
    }
  }
  bool P(bool wait = true) {
    if (!wait) return tryP();
    for (;;) {
      int ret = sem_wait(&sem);
      if (ret == 0) return true;
      RASSERT(_SysErrno() == EINTR, _SysErrno());
    }
  }
  bool P(const Time& timeout) {
    for (;;) {
      int ret = sem_timedwait(&sem, &timeout);
      if (ret == 0) return true;
      if (_SysErrno() == ETIMEDOUT) return false;
      RASSERT(_SysErrno() == EINTR, _SysErrno());
    }
  }
  void V() {
    SYSCALL(sem_post(&sem));
  }
};

#endif /* _OsLocks_h_ */
