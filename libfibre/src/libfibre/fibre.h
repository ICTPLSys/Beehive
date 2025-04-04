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
#ifndef _fibre_h_
#define _fibre_h_ 1

/** @file */

#ifndef __LIBFIBRE__
#define __LIBFIBRE__ 1
#endif

#include "libfibre/EventScope.h" // EventScope.h pulls in everything else

typedef Fibre*                    fibre_t;
typedef FredCondition             fibre_cond_t;
typedef FredSemaphore             fibre_sem_t;
typedef FredRWLock                fibre_rwlock_t;
typedef FredBarrier               fibre_barrier_t;
typedef SpinBarrier               spin_barrier_t;
typedef FastBarrier<BinaryLock<>> fast_barrier_t;

#if TESTING_LOCK_RECURSION
typedef OwnerMutex<FredMutex> fibre_mutex_t;
typedef OwnerMutex<FastMutex> fibre_fastmutex_t;
#else
typedef FredMutex fibre_mutex_t;
typedef FastMutex fibre_fastmutex_t;
#endif

struct fibre_attr_t {
  Cluster* cluster;
  size_t stackSize;
  size_t guardSize;
  size_t priority;
  size_t affinity;
  bool detached;
  void init() {
    cluster = &Context::CurrCluster();
    stackSize = Fibre::DefaultStackSize;
    guardSize = Fibre::DefaultStackGuard;
    priority = Fibre::DefaultPriority;
    affinity = Fibre::DefaultAffinity;
    detached = false;
  }
};

enum {
  FIBRE_MUTEX_RECURSIVE  = PTHREAD_MUTEX_RECURSIVE,
  FIBRE_MUTEX_ERRORCHECK = PTHREAD_MUTEX_ERRORCHECK,
  FIBRE_MUTEX_DEFAULT    = PTHREAD_MUTEX_DEFAULT
};

struct fibre_mutexattr_t {
  int type;
  fibre_mutexattr_t() : type(FIBRE_MUTEX_DEFAULT) {}
};

struct fibre_condattr_t {};
struct fibre_rwlockattr_t {};
struct fibre_barrierattr_t {};
struct spin_barrierattr_t {};
struct fast_barrierattr_t {};

struct fibre_fastmutexattr_t {
  int type;
  fibre_fastmutexattr_t() : type(FIBRE_MUTEX_DEFAULT) {}
};

typedef size_t fibre_key_t;

typedef pthread_once_t fibre_once_t;
static const fibre_once_t FIBRE_ONCE_INIT = PTHREAD_ONCE_INIT;

/** @brief One-time initialization (`pthread_once`). */
static inline int fibre_once(fibre_once_t *once_control, void (*init_routine)(void)) {
  return pthread_once(once_control, init_routine);
}

#ifdef __GNUC__
#define restrict __restrict__
#else
#define restrict
#endif

/** @brief Bootstrap routine should be called early in main(). */
extern EventScope* FibreInit(size_t pollerCount = 1, size_t workerCount = 1);

/** @brief Fork process (with restrictions) and re-initialize runtime in child process (`fork`). */
extern pid_t FibreFork();

struct __FibreBootstrap {
  static int counter;
  __FibreBootstrap() {
    if (__atomic_fetch_add(&counter, 1, __ATOMIC_SEQ_CST) == 0) FibreInit();
  }
};

#if defined(__FIBRE_BOOTSTRAP__)
static __FibreBootstrap __fibreBootstrap;
#endif

/** @brief Initialize attributes for fibre creation (`pthread_attr_init`). */
inline int fibre_attr_init(fibre_attr_t *attr) {
  attr->init();
  return 0;
}

/** @brief Destroy attributes for fibre creation. (`pthread_attr_destroy`). */
inline int fibre_attr_destroy(fibre_attr_t *) {
  return 0;
}

/** @brief Set cluster attribute for fibre creation. */
inline int fibre_attr_setcluster(fibre_attr_t *attr, Cluster *cluster) {
  attr->cluster = cluster;
  return 0;
}

/** @brief Get cluster attribute for fibre creation. */
inline int fibre_attr_getcluster(const fibre_attr_t *attr, Cluster **cluster) {
  *cluster = attr->cluster;
  return 0;
}

/** @brief Set stack size for fibre creation. (`pthread_attr_setstacksize`). */
inline int fibre_attr_setstacksize(fibre_attr_t *attr, size_t stacksize) {
  attr->stackSize = stacksize;
  return 0;
}

/** @brief Get stack size for fibre creation. (`pthread_attr_getstacksize`). */
inline int fibre_attr_getstacksize(const fibre_attr_t *attr, size_t *stacksize) {
  *stacksize = attr->stackSize;
  return 0;
}

/** @brief Set guard size attribute for fibre creation. (`pthread_attr_setguardsize`) */
inline int fibre_attr_setguardsize(fibre_attr_t *attr, size_t guardsize) {
  attr->guardSize = guardsize;
  return 0;
}

/** @brief Get guard size attribute for fibre creation. (`pthread_attr_getguardsize`) */
inline int fibre_attr_getguardsize(const fibre_attr_t *attr, size_t *guardsize) {
  *guardsize = attr->guardSize;
  return 0;
}

/** @brief Set priority attribute for fibre creation. */
inline int fibre_attr_setpriority(fibre_attr_t *attr, int priority) {
  attr->priority = priority;
  return 0;
}

/** @brief Get priority attribute for fibre creation. */
inline int fibre_attr_getpriority(const fibre_attr_t *attr, int *priority) {
  *priority = attr->priority;
  return 0;
}

/** @brief Set affinity attribute for fibre creation. */
inline int fibre_attr_setaffinity(fibre_attr_t *attr, int affinity) {
  attr->affinity = affinity;
  return 0;
}

/** @brief Get affinity attribute for fibre creation. */
inline int fibre_attr_getaffinity(const fibre_attr_t *attr, int *affinity) {
  *affinity = attr->affinity;
  return 0;
}

/** @brief Set detach attribute for fibre creation. (`pthread_attr_setdetachstate`) */
inline int fibre_attr_setdetachstate(fibre_attr_t *attr, int detachstate) {
  attr->detached = detachstate;
  return 0;
}

/** @brief Get detach attribute for fibre creation. (`pthread_attr_getdetachstate`) */
inline int fibre_attr_getdetachstate(const fibre_attr_t *attr, int *detachstate) {
  *detachstate = attr->detached;
  return 0;
}

/** @brief Create and start fibre. (`pthread_create`) */
inline int fibre_create(fibre_t *thread, const fibre_attr_t *attr, void *(*start_routine) (void *), void *arg) {
  Fibre* f;
  if (!attr) {
    f = new Fibre;
  } else {
    f = new Fibre(*attr->cluster, attr->stackSize, attr->guardSize);
    f->setPriority(Fibre::Priority(attr->priority));
    f->setAffinity(attr->affinity);
    if (attr->detached) f->detach();
  }
  *thread = f->run(start_routine, arg);
  return 0;
}

/** @brief Wait for fibre to complete execution and retrieve return value. (`pthread_join`) */
inline int fibre_join(fibre_t thread, void **retval) {
  if (retval) *retval = thread->join();
  delete thread;
  return 0;
}

/** @brief Detach fibre. (`pthread_detach`) */
inline int fibre_detach(fibre_t thread) {
  thread->detach();
  return 0;
}

inline void fibre_exit() __noreturn;
inline void fibre_exit() {
  Fibre::exit();
}

/** @brief Obtain fibre ID of currently running fibre. (`pthread_self`) */
inline fibre_t fibre_self(void) {
  return CurrFibre();
}

/** @brief Compare fibre IDs. (`pthread_equal`) */
inline int fibre_equal(fibre_t thread1, fibre_t thread2) {
  return thread1 == thread2;
}

/** @brief Voluntarily yield execution. (`pthread_yield`) */
inline int fibre_yield(void) {
  Fibre::yield();
  return 0;
}

/** @brief Create key for thread-specific storage. (`pthread_key_create`) */
inline int fibre_key_create(fibre_key_t *key, void (*destructor)(void*)) {
  *key = Fibre::key_create(destructor);
  return 0;
}

/** @brief Delete key for thread-specific storage. (`pthread_key_delete`) */
inline int fibre_key_delete(fibre_key_t key) {
  Fibre::key_delete(key);
  return 0;
}

/** @brief Store thread-specific value. (`pthread_setspecific`) */
inline int fibre_setspecific(fibre_key_t key, const void *value) {
  CurrFibre()->setspecific(key, value);
  return 0;
}

/** @brief Read thread-specific value for key. (`pthread_getspecific`) */
inline void *fibre_getspecific(fibre_key_t key) {
  return CurrFibre()->getspecific(key);
}

/** @brief Park fibre (indefinite sleep). */
inline int fibre_park() {
  Suspender::suspend(*CurrFibre());
  return 0;
}

/** @brief Wake up parked fibre. */
inline int fibre_unpark(fibre_t thread) {
  thread->resume();
  return 0;
}

/** @brief Migrate fibre to a different cluster. */
inline int fibre_migrate(Cluster *cluster) {
  RASSERT0(cluster);
  Fibre::migrate(*cluster);
  return 0;
}

/** @brief Initialize semaphore object. (`sem_init`) */
inline int fibre_sem_init(fibre_sem_t *sem, int pshared, unsigned int value) {
  RASSERT0(pshared == 0);
  new (sem) fibre_sem_t(value);
  return 0;
}

/** @brief Destroy semaphore object. (`sem_destroy`) */
inline int fibre_sem_destroy(fibre_sem_t *sem) {
  sem->reset(0);
  return 0;
}

/** @brief Perform P operation. (`sem_wait`) */
inline int fibre_sem_wait(fibre_sem_t *sem) {
  sem->P();
  return 0;
}

/** @brief Perform non-blocking P attempt. (`sem_trywait`) */
inline int fibre_sem_trywait(fibre_sem_t *sem) {
  return sem->tryP() ? 0 : EAGAIN;
}

/** @brief Perform P attempt with timeout. (`sem_timedwait`) */
inline int fibre_sem_timedwait(fibre_sem_t *sem, const struct timespec *abs_timeout) {
  return sem->P(*abs_timeout) ? 0 : ETIMEDOUT;
}

/** @brief Perform V operation. (`sem_post`) */
inline int fibre_sem_post(fibre_sem_t *sem) {
  sem->V();
  return 0;
}

/** @brief Read current semaphore value. Not synchronized. (`sem_getvalue`) */
inline int fibre_sem_getvalue(fibre_sem_t *sem, int *sval) {
  *sval = sem->getValue();
  return 0;
}

/** @brief Initialize the mutex attributes object. (`pthread_mutexattr_init`) */
inline int fibre_mutexattr_init(fibre_mutexattr_t *) {
  return 0;
}

/** @brief Destroy the mutex attributes object. (`pthread_mutexattr_destroy`) */
inline int fibre_mutexattr_destroy(fibre_mutexattr_t *) {
  return 0;
}

/** @brief Set the mutex type attribute. (`pthread_mutexattr_settype`) */
inline int fibre_mutexattr_settype(fibre_mutexattr_t *attr, int type) {
  attr->type = type;
  return 0;
}

/** @brief Initialize mutex lock. (`pthread_mutex_init`) */
inline int fibre_mutex_init(fibre_mutex_t *restrict mutex, const fibre_mutexattr_t *restrict attr) {
  new (mutex) fibre_mutex_t;
#if TESTING_LOCK_RECURSION
  if (attr && attr->type == PTHREAD_MUTEX_RECURSIVE) mutex->enableRecursion();
#else
  if (attr && attr->type == PTHREAD_MUTEX_RECURSIVE) return -1;
#endif
  return 0;
}

/** @brief Destroy mutex lock. (`pthread_mutex_destroy`) */
inline int fibre_mutex_destroy(fibre_mutex_t *mutex) {
  mutex->reset();
  return 0;
}

/** @brief Acquire mutex lock. Block, if necessary. (`pthread_mutex_lock`) */
inline int fibre_mutex_lock(fibre_mutex_t *mutex) {
  mutex->acquire();
  return 0;
}

/** @brief Perform non-blocking attempt to acquire mutex lock. (`pthread_mutex_trylock`) */
inline int fibre_mutex_trylock(fibre_mutex_t *mutex) {
  return mutex->tryAcquire() ? 0 : EBUSY;
}

/** @brief Perform attempt to acquire mutex lock with timeout. (`pthread_mutex_timedlock`) */
inline int fibre_mutex_timedlock(fibre_mutex_t *restrict mutex, const struct timespec *restrict abstime) {
  return mutex->acquire(*abstime) ? 0 : ETIMEDOUT;
}

/** @brief Release mutex lock. Block, if necessary. (`pthread_mutex_unlock`) */
inline int fibre_mutex_unlock(fibre_mutex_t *mutex) {
  mutex->release();
  return 0;
}

inline int fibre_condattr_init(fibre_condattr_t*) {
  return 0;
}

inline int fibre_condattr_destroy(fibre_condattr_t*) {
  return 0;
}

/** @brief Initialize condition variable. (`pthread_cond_init`) */
inline int fibre_cond_init(fibre_cond_t *restrict cond, const fibre_condattr_t *restrict attr) {
  RASSERT0(attr == nullptr);
  new (cond) fibre_cond_t;
  return 0;
}

/** @brief Destroy condition variable. (`pthread_cond_init`) */
inline int fibre_cond_destroy(fibre_cond_t *cond) {
  cond->reset();
  return 0;
}

/** @brief Wait on condition variable. (`pthread_cond_wait`) */
inline int fibre_cond_wait(fibre_cond_t *restrict cond, fibre_mutex_t *restrict mutex) {
  cond->wait(*mutex);
  mutex->acquire();
  return 0;
}

/** @brief Perform wait attempt on condition variable with timeout. (`pthread_cond_timedwait`) */
inline int fibre_cond_timedwait(fibre_cond_t *restrict cond, fibre_mutex_t *restrict mutex, const struct timespec *restrict abstime) {
  int retcode = cond->wait(*mutex, *abstime) ? 0 : ETIMEDOUT;
  mutex->acquire();
  return retcode;
}

/** @brief Signal one waiting fibre on condition variable. (`pthread_cond_signal`) */
inline int fibre_cond_signal(fibre_cond_t *cond) {
  cond->signal();
  return 0;
}

/** @brief Signal all waiting fibres on condition variable. (`pthread_cond_broadcast`) */
inline int fibre_cond_broadcast(fibre_cond_t *cond) {
  cond->signal<true>();
  return 0;
}

/** @brief Initialize rw-lock. (`pthread_rwlock_init`) */
inline int fibre_rwlock_init(fibre_rwlock_t *restrict rwlock, const fibre_rwlockattr_t *restrict attr) {
  RASSERT0(attr == nullptr);
  new (rwlock) fibre_rwlock_t;
  return 0;
}

/** @brief Destroy rw-lock. (`pthread_rwlock_init`) */
inline int fibre_rwlock_destroy(fibre_rwlock_t *rwlock) {
  rwlock->reset();
  return 0;
}

/** @brief Acquire reader side of rw-lock. Block, if necessary. (`pthread_rwlock_rdlock`) */
inline int fibre_rwlock_rdlock(fibre_rwlock_t *rwlock){
  rwlock->acquireRead();
  return 0;
}

/** @brief Perform non-blocking attempt to acquire reader side of rw-lock. (`pthread_rwlock_tryrdlock`) */
inline int fibre_rwlock_tryrdlock(fibre_rwlock_t *rwlock){
  return rwlock->tryAcquireRead() ? 0 : EBUSY;
}

/** @brief Perform attempt to acquire reader side of rw-lock with timeout. (`pthread_rwlock_timedrdlock`) */
inline int fibre_rwlock_timedrdlock(fibre_rwlock_t *restrict rwlock, const struct timespec *restrict abstime){
  return rwlock->acquireRead(*abstime) ? 0 : ETIMEDOUT;
}

/** @brief Acquire writer side of rw-lock. Block, if necessary. (`pthread_rwlock_wrlock`) */
inline int fibre_rwlock_wrlock(fibre_rwlock_t *rwlock){
  rwlock->acquireWrite();
  return 0;
}

/** @brief Perform non-blocking attempt to acquire writer side of rw-lock. (`pthread_rwlock_trywrlock`) */
inline int fibre_rwlock_trywrlock(fibre_rwlock_t *rwlock){
  return rwlock->tryAcquireWrite() ? 0 : EBUSY;
}

/** @brief Perform attempt to acquire writer side of rw-lock with timeout. (`pthread_rwlock_timedwrlock`) */
inline int fibre_rwlock_timedwrlock(fibre_rwlock_t *restrict rwlock, const struct timespec *restrict abstime){
  return rwlock->acquireWrite(*abstime) ? 0 : ETIMEDOUT;
}

/** @brief Release rw-lock. (`pthread_rwlock_unlock`) */
inline int fibre_rwlock_unlock(fibre_rwlock_t *rwlock){
  rwlock->release();
  return 0;
}

/** @brief Initialize barrier. (`pthread_barrier_init`) */
inline int fibre_barrier_init(fibre_barrier_t *restrict barrier, const fibre_barrierattr_t *restrict attr, unsigned count) {
  RASSERT0(attr == nullptr);
  new (barrier) fibre_barrier_t(count);
  return 0;
}

/** @brief Destroy barrier. (`pthread_barrier_destroy`) */
inline int fibre_barrier_destroy(fibre_barrier_t *barrier) {
  barrier->reset();
  return 0;
}

/** @brief Wait on barrier. Block, if necessary. (`pthread_barrier_wait`) */
inline int fibre_barrier_wait(fibre_barrier_t *barrier) {
  return barrier->wait() ? PTHREAD_BARRIER_SERIAL_THREAD : 0;
}

/** @brief Initialize barrier. (`pthread_barrier_init`) */
inline int spin_barrier_init(spin_barrier_t *restrict barrier, const spin_barrierattr_t *restrict attr, unsigned count) {
  RASSERT0(attr == nullptr);
  new (barrier) spin_barrier_t(count);
  return 0;
}

/** @brief Destroy barrier. (`pthread_barrier_destroy`) */
inline int spin_barrier_destroy(spin_barrier_t *barrier) {
  barrier->reset();
  return 0;
}

/** @brief Wait on barrier. Block, if necessary. (`pthread_barrier_wait`) */
inline int spin_barrier_wait(spin_barrier_t *barrier) {
  return barrier->wait() ? PTHREAD_BARRIER_SERIAL_THREAD : 0;
}

/** @brief Initialize barrier. (`pthread_barrier_init`) */
inline int fast_barrier_init(fast_barrier_t *restrict barrier, const fast_barrierattr_t *restrict attr, unsigned count) {
  RASSERT0(attr == nullptr);
  new (barrier) fast_barrier_t(count);
  return 0;
}

/** @brief Destroy barrier. (`pthread_barrier_destroy`) */
inline int fast_barrier_destroy(fast_barrier_t *barrier) {
  barrier->reset();
  return 0;
}

/** @brief Wait on barrier. Block, if necessary. (`pthread_barrier_wait`) */
inline int fast_barrier_wait(fast_barrier_t *barrier) {
  return barrier->wait() ? PTHREAD_BARRIER_SERIAL_THREAD : 0;
}

/** @brief Initialize the fastmutex attributes object. (`pthread_mutexattr_init`) */
inline int fibre_fastmutexattr_init(fibre_fastmutexattr_t *) {
  return 0;
}

/** @brief Destroy the fastmutex attributes object. (`pthread_mutexattr_destroy`) */
inline int fibre_fastmutexattr_destroy(fibre_fastmutexattr_t *) {
  return 0;
}

/** @brief Set the fastmutex type attribute. (`pthread_mutexattr_settype`) */
inline int fibre_fastmutexattr_settype(fibre_fastmutexattr_t *attr, int type) {
  attr->type = type;
  return 0;
}

/** @brief Initialize mutex lock. (`pthread_mutex_init`) */
inline int fibre_fastmutex_init(fibre_fastmutex_t *restrict mutex, const fibre_fastmutexattr_t *restrict attr) {
  new (mutex) fibre_fastmutex_t;
#if TESTING_LOCK_RECURSION
  if (attr && attr->type == PTHREAD_MUTEX_RECURSIVE) mutex->enableRecursion();
#else
  if (attr && attr->type == PTHREAD_MUTEX_RECURSIVE) return -1;
#endif
  return 0;
}

/** @brief Destroy mutex lock. (`pthread_mutex_destroy`) */
inline int fibre_fastmutex_destroy(fibre_fastmutex_t *) {
  return 0;
}

/** @brief Acquire mutex lock. Block, if necessary. (`pthread_mutex_lock`) */
inline int fibre_fastmutex_lock(fibre_fastmutex_t *mutex) {
#if TESTING_LOCK_RECURSION
  return mutex->acquire() ? 0 : -1;
#else
  mutex->acquire();
  return 0;
#endif
}

/** @brief Perform non-blocking attempt to acquire mutex lock. (`pthread_mutex_trylock`) */
inline int fibre_fastmutex_trylock(fibre_fastmutex_t *mutex) {
  return mutex->tryAcquire() ? 0 : EBUSY;
}

/** @brief Release mutex lock. Block, if necessary. (`pthread_mutex_unlock`) */
inline int fibre_fastmutex_unlock(fibre_fastmutex_t *mutex) {
  mutex->release();
  return 0;
}

/** @brief Wait on condition variable using fast mutex. (`pthread_cond_wait`) */
inline int fibre_fastcond_wait(fibre_cond_t *restrict cond, fibre_fastmutex_t *restrict mutex) {
  cond->wait(*mutex);
  mutex->acquire();
  return 0;
}

/** @brief Perform wait attempt on condition variable with timeout using fast mutex. (`pthread_cond_timedwait`) */
inline int fibre_fastcond_timedwait(fibre_cond_t *restrict cond, fibre_fastmutex_t *restrict mutex, const struct timespec *restrict abstime) {
  int retcode = cond->wait(*mutex, *abstime) ? 0 : ETIMEDOUT;
  mutex->acquire();
  return retcode;
}

#endif /* _fibre_h_ */

/**
@mainpage

The fibre runtime system provides M:N user-level threading.  A <i>fibre</i>
is an independent execution context backed by a stack.  A <i>processor</i>
represents an OS-level thread, typically implemented as `pthread`.  Multiple
fibres are transparently executed in the scheduling scope of a
<i>cluster</i> using one or multiple processors.  Fibres are cooperatively
scheduled and do not preempt each other.  In other words, a fibre occupies a
processor until the fibre is blocked or yields execution.

The fibre runtime system supports blocking synchronization using mutex,
condition, semaphore, rwlock, and barrier.  In addition, I/O wrapper
routines automatically block a fibre, if the I/O system call would otherwise
block the underlying OS-level thread.  This is facilitated via <i>poller</i>
fibres and/or threads that perform I/O event monitoring on behalf of
application fibres in the scope of a cluster.  Multiple clusters can be used
to structure the scheduling and I/O handling of fibres and processors within
an <i>event scope</i>.  More than one event scope can be created to take
advantage of partitioned kernel file descriptor tables (on Linux, see `man 2
clone`).  Before the execution of the regular `main()` routine, the runtime
system automatically creates one of each default objects: Fibre, Cluster,
EventScope.


Application source code must include fibre declarations via either fibre.h
(for C++) or cfibre.h (for C).  The runtime provides both C and C++ APIs:

- Regular C++ API
  - classes (Fibre, Cluster, EventScope)
  - global routines (Fibre.h, EventScope.h)
- Subset of API corresponding to the POSIX Threads API (fibre.h)
- C wrapper/bindings (cfibre.h)
  - all `fibre_` routines also available as `cfibre_`
  - other functionality from class-based API

*/
