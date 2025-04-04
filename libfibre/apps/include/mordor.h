#ifndef _tt_mordor_h_
#define _tt_mordor_h_ 1

#include "mordor/predef.h"
#include "mordor/fiber.h"
#include "mordor/fibersynchronization.h"
#include "mordor/main.h"
#include "mordor/workerpool.h"

using namespace Mordor;

static void mworker(void (*start_routine)(void *), void* arg) {
  start_routine(arg);
}

static WorkerPool* poolScheduler = nullptr;

struct shim_thread_t {
 Fiber::ptr f;
 shim_thread_t(void (*start_routine)(void *), void* arg) : f(new Fiber(std::bind(mworker, start_routine, arg))) {
    poolScheduler->schedule(f);
  }
};

typedef FiberMutex     shim_mutex_t;

struct shim_barrier_t {
  shim_mutex_t mtx;
  FiberCondition cond;
  size_t target;
  size_t counter;
  shim_barrier_t(size_t t) : cond(mtx), target(t), counter(0) {}
};

static inline shim_thread_t* shim_thread_create(void (*start_routine)(void *), void* arg, bool = false) {
  return new shim_thread_t(start_routine, arg);
}
static inline void shim_thread_destroy(shim_thread_t* tid) { delete tid; }
static inline void shim_yield() { Scheduler::yield(); }

static inline void shim_mutex_init(shim_mutex_t* mtx)    { new (mtx) shim_mutex_t; }
static inline void shim_mutex_destroy(shim_mutex_t* mtx) {}
static inline void shim_mutex_lock(shim_mutex_t* mtx)    { mtx->lock(); }
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  { mtx->unlock(); }

static inline shim_barrier_t* shim_barrier_create(size_t cnt) { return new shim_barrier_t(cnt); }
static inline void shim_barrier_destroy(shim_barrier_t* barr) { delete barr; }
static inline void shim_barrier_wait(shim_barrier_t* barr)    {
  barr->mtx.lock();
  barr->counter += 1;
  if (barr->counter < barr->target) barr->cond.wait();
  else barr->cond.broadcast();
  barr->mtx.unlock();
}

#endif /* _tt_mordor_h_ */
