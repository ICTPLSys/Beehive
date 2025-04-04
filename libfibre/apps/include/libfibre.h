#ifndef _tt_libfibre_h_
#define _tt_libfibre_h_ 1

#include "libfibre/fibre.h"

#define CurrCluster   Context::CurrCluster

typedef Fibre         shim_thread_t;
typedef FredCondition shim_cond_t;
typedef FredBarrier   shim_barrier_t;

#if TESTING_LOCK_RECURSION
typedef OwnerMutex<FredMutex> shim_mutex_t;
#else
typedef FredMutex shim_mutex_t;
#define HASTRYLOCK 1
#define HASTIMEDLOCK 1
#endif

static inline shim_thread_t* shim_thread_create(void (*start_routine)(void *), void* arg) {
  Fibre* f = new Fibre(CurrCluster());
  f->run(start_routine, arg);
  return f;
}
static inline void shim_thread_destroy(shim_thread_t* tid) { delete tid; }
static inline void shim_yield() { Fibre::yield(); }

#if TESTING_LOCK_RECURSION
static inline void shim_mutex_init(shim_mutex_t* mtx)    {
  new (mtx) shim_mutex_t;
  mtx->enableRecursion();
}
static inline void shim_mutex_lock(shim_mutex_t* mtx)    {
  size_t x;
  x = mtx->acquire(); RASSERT0(x == 1);
  x = mtx->acquire(); RASSERT0(x == 2);
}
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  {
  size_t x;
  x = mtx->release(); RASSERT0(x == 1);
  x = mtx->release(); RASSERT0(x == 0);
}
#else
static inline void shim_mutex_init(shim_mutex_t* mtx)    { new (mtx) shim_mutex_t; }
template<typename... Args>
static inline void shim_mutex_lock(shim_mutex_t* mtx, Args&&... args)    { mtx->acquire(args...); }
template<typename... Args>
static inline void shim_mutex_unlock(shim_mutex_t* mtx, Args&&... args)  { mtx->release(args...); }
#if HASTRYLOCK
template<typename... Args>
static inline bool shim_mutex_trylock(shim_mutex_t* mtx, Args&&... args) { return mtx->tryAcquire(args...); }
#endif
#if HASTIMEDLOCK
static inline bool shim_mutex_timedlock(shim_mutex_t* mtx, unsigned int timeout) {
  return mtx->acquire(Runtime::Timer::now() + Time::fromNS(timeout));
}
#endif
#endif

static inline void shim_mutex_destroy(shim_mutex_t*) {}

static inline void shim_cond_init(shim_cond_t* cond)                    { new (cond) shim_cond_t; }
static inline void shim_cond_destroy(shim_cond_t*)                      {}
static inline void shim_cond_wait(shim_cond_t* cond, shim_mutex_t* mtx) { cond->wait(*mtx); }
static inline void shim_cond_signal(shim_cond_t* cond)                  { cond->signal(); }

static inline shim_barrier_t* shim_barrier_create(size_t cnt) { return new shim_barrier_t(cnt); }
static inline void shim_barrier_destroy(shim_barrier_t* barr) { delete barr; }
static inline void shim_barrier_wait(shim_barrier_t* barr)    { barr->wait(); }

#endif /* _tt_libfibre_h_ */
