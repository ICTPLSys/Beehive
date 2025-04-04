#ifndef _tt_libfiber_h_
#define _tt_libfiber_h_ 1

#include "fiber.h"
#include "fiber_mutex.h"
#include "fiber_cond.h"
#include "fiber_barrier.h"
#include "fiber_io.h"
#include "fiber_manager.h"

#define HASTRYLOCK 1

typedef fiber_t         shim_thread_t;
typedef fiber_mutex_t   shim_mutex_t;
typedef fiber_cond_t    shim_cond_t;
typedef fiber_barrier_t shim_barrier_t;

static inline shim_thread_t* shim_thread_create(void (*start_routine)(void *), void* arg, bool = false) {
  return fiber_create(65536, (fiber_run_function_t)(void*)start_routine, arg);
}

static inline void shim_thread_destroy(shim_thread_t* tid) {
  void* dummy;
  fiber_join(tid, &dummy);
}

static inline void shim_yield() { fiber_yield(); }

static inline void shim_mutex_init(shim_mutex_t* mtx)    { fiber_mutex_init(mtx); }
static inline void shim_mutex_destroy(shim_mutex_t* mtx) { fiber_mutex_destroy(mtx); }
static inline void shim_mutex_lock(shim_mutex_t* mtx)    { fiber_mutex_lock(mtx); }
static inline bool shim_mutex_trylock(shim_mutex_t* mtx) { return fiber_mutex_trylock(mtx) == FIBER_SUCCESS; }
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  { fiber_mutex_unlock(mtx); }

static inline void shim_cond_init(shim_cond_t* cond)                    { fiber_cond_init(cond); }
static inline void shim_cond_destroy(shim_cond_t* cond)                 { fiber_cond_destroy(cond); }
static inline void shim_cond_wait(shim_cond_t* cond, shim_mutex_t* mtx) { fiber_cond_wait(cond, mtx); }
static inline void shim_cond_signal(shim_cond_t* cond)                  { fiber_cond_signal(cond); }

static inline shim_barrier_t* shim_barrier_create(size_t cnt) {
  fiber_barrier_t* barr = new fiber_barrier_t;
  fiber_barrier_init(barr, cnt);
  return barr;
}

static inline void shim_barrier_destroy(shim_barrier_t* barr) {
  fiber_barrier_destroy(barr);
  delete barr;
}

static inline void shim_barrier_wait(shim_barrier_t* barr) {
  fiber_barrier_wait(barr);
}

#endif /* _tt_qthreads_h_ */
