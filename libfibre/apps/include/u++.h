#ifndef _tt_upp_h_
#define _tt_upp_h_

#ifndef UPP_SEMAPHORE
#define UPP_SEMAPHORE 0
#endif

#include "uBarrier.h"
#include "uRealTime.h"
#include "uSemaphore.h"

#define HASTRYLOCK 1

typedef uCluster Cluster;
#define CurrCluster   uThisCluster

_Task shim_thread_t {
  void (*start_routine)(void *);
  void* arg;
  void main() { start_routine(arg); }
public:
  shim_thread_t(void (*start_routine)(void *), void* arg) : start_routine(start_routine), arg(arg) {}
};

#if UPP_SEMAPHORE
typedef uSemaphore shim_mutex_t;
#else
typedef uOwnerLock shim_mutex_t;
#endif

typedef uSemaphore shim_cond_t;
typedef uBarrier   shim_barrier_t;

static inline shim_thread_t* shim_thread_create(void (*start_routine)(void *), void* arg, bool bg = false) {
  return new shim_thread_t(start_routine, arg);
}
static inline void shim_thread_destroy(shim_thread_t* tid) { delete tid; }
static inline void shim_yield() { uThisTask().uYieldNoPoll(); }

#if UPP_SEMAPHORE
static inline void shim_mutex_init(shim_mutex_t* mtx)    { new (mtx) shim_mutex_init; }
static inline void shim_mutex_destroy(shim_mutex_t* mtx) {}
static inline void shim_mutex_lock(shim_mutex_t* mtx)    { mtx->P(); }
static inline bool shim_mutex_trylock(shim_mutex_t* mtx) { return mtx->TryP(); }
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  { mtx->V(); }
#else
static inline void shim_mutex_init(shim_mutex_t* mtx)    { new (mtx) uOwnerLock; }
static inline void shim_mutex_destroy(shim_mutex_t* mtx) {}
static inline void shim_mutex_lock(shim_mutex_t* mtx)    { mtx->acquire(); }
static inline bool shim_mutex_trylock(shim_mutex_t* mtx) { return mtx->tryacquire(); }
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  { mtx->release(); }
#endif

static inline void shim_cond_init(shim_cond_t* cond)                    { new (cond) shim_cond_t; }
static inline void shim_cond_destroy(shim_cond_t* cond)                 {}
static inline void shim_cond_wait(shim_cond_t* cond, shim_mutex_t* mtx) { shim_mutex_unlock(mtx); cond->P(); }
static inline void shim_cond_signal(shim_cond_t* cond)                  { cond->V(); }

static inline shim_barrier_t* shim_barrier_create(size_t cnt) { return new shim_barrier_t(cnt); }
static inline void shim_barrier_destroy(shim_barrier_t* barr) { delete barr; }
static inline void shim_barrier_wait(shim_barrier_t* barr)    { barr->block(); }

unsigned int uDefaultPreemption() { // timeslicing not required
    return 0;
} // uDefaultPreemption
unsigned int uDefaultSpin() {       // kernel schedule-spinning off
    return 0;
} // uDefaultPreemption
unsigned int uMainStackSize() {     // reduce, default 500K
    return 60 * 1000;
} // uMainStackSize

#endif /* _tt_upp_h_ */
