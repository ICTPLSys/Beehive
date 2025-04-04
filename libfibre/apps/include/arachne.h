#ifndef _tt_arachne_h_
#define _tt_arachne_h_ 1

#include "Arachne/Arachne.h"

#define HASTRYLOCK 1

typedef Arachne::ThreadId  shim_thread_t;
typedef Arachne::SleepLock shim_mutex_t;
//typedef Arachne::SleepLock shim_mutex_t;
struct shim_barrier_t {
  shim_mutex_t lock;
  Arachne::ConditionVariable queue;
  size_t target;
  size_t counter;
  shim_barrier_t(size_t cnt) : target(cnt), counter(0) {}
};

static inline shim_thread_t* shim_thread_create(void (*start_routine)(void *), void* arg, bool = false) {
  shim_thread_t* tid = new shim_thread_t;
  *tid = Arachne::createThread(start_routine, arg);
  Arachne::yield();
  return tid;
}

static inline void shim_thread_destroy(shim_thread_t* tid) {
  Arachne::join(*tid);
  delete tid;
}

static inline void shim_yield() { Arachne::yield(); }

static inline void shim_mutex_init(shim_mutex_t* mtx)    { new (mtx) shim_mutex_t; }
static inline void shim_mutex_destroy(shim_mutex_t* mtx) {}
static inline void shim_mutex_lock(shim_mutex_t* mtx)    { mtx->lock(); }
static inline bool shim_mutex_trylock(shim_mutex_t* mtx) { return mtx->try_lock(); }
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  { mtx->unlock(); }

static inline shim_barrier_t* shim_barrier_create(size_t cnt) { return new shim_barrier_t(cnt); }
static inline void shim_barrier_destroy(shim_barrier_t* barr) { delete barr; }
static inline void shim_barrier_wait(shim_barrier_t* barr) {
  barr->lock.lock();
  barr->counter += 1;
  if (barr->counter == barr->target) {
    barr->queue.notifyAll();
    barr->counter = 0;
  } else {
    Arachne::yield();
    barr->queue.wait(barr->lock);
  }
  barr->lock.unlock();
}

#endif /* _tt_arachne_h_ */
