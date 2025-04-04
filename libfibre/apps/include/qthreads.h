#ifndef _tt_qthreads_h_
#define _tt_qthreads_h_ 1

#include "qthread/barrier.h"

struct shim_thread_t {
  void (*start_routine)(void* arg);
  void* arg;
  syncvar_t sync;
  static aligned_t qworker(void* f) {
    shim_thread_t* This = (shim_thread_t*)f;
    This->start_routine(This->arg);
    qthread_syncvar_writeF_const(&This->sync, 0);
    return 0;
  }
  shim_thread_t(void (*start_routine)(void *), void* arg) : start_routine(start_routine), arg(arg),
    sync(SYNCVAR_STATIC_INITIALIZER) {
    qthread_syncvar_empty(&sync);
    qthread_spawn(qworker, this, 0, nullptr, 0, nullptr, NO_SHEPHERD, 0);
  }
  ~shim_thread_t() {
    uint64_t dummy;
    qthread_syncvar_readFE(&dummy, &sync);
  }
};

typedef aligned_t    shim_mutex_t;
typedef qt_barrier_t shim_barrier_t;

static inline shim_thread_t* shim_thread_create(void (*start_routine)(void *), void* arg, bool = false) {
  return new shim_thread_t(start_routine, arg);
}

static inline void shim_thread_destroy(shim_thread_t* tid) { delete tid; }

static inline void shim_yield() { qthread_yield(); }

static inline void shim_mutex_init(shim_mutex_t* mtx)    { new (mtx) shim_mutex_t; }
static inline void shim_mutex_destroy(shim_mutex_t* mtx) {}
static inline void shim_mutex_lock(shim_mutex_t* mtx)    { qthread_lock(mtx); }
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  { qthread_unlock(mtx); }

static inline shim_barrier_t* shim_barrier_create(size_t cnt) { return qt_barrier_create(cnt, REGION_BARRIER); }
static inline void shim_barrier_destroy(shim_barrier_t* barr) { qt_barrier_destroy(barr); }
static inline void shim_barrier_wait(shim_barrier_t* barr)    { qt_barrier_enter(barr); }

#endif /* _tt_qthreads_h_ */
