#ifndef _tt_pthreads_h_
#define _tt_pthreads_h_ 1

extern "C" { // needed for __cforall
#include <limits.h> // PTHREAD_STACK_MIN
#include <pthread.h>
}

#define HASTRYLOCK 1
#define HASTIMEDLOCK 1

typedef pthread_t         shim_thread_t;
typedef pthread_mutex_t   shim_mutex_t;
typedef pthread_cond_t    shim_cond_t;
typedef pthread_barrier_t shim_barrier_t;

static inline shim_thread_t* shim_thread_create(void (*start_routine)(void *), void* arg, bool = false, size_t stacksize = PTHREAD_STACK_MIN) {
  typedef void* (*TSR)(void*);
  pthread_t* tid = (pthread_t*)malloc(sizeof(pthread_t));
  pthread_attr_t attr;
  SYSCALL(pthread_attr_init(&attr));
  SYSCALL(pthread_attr_setstacksize(&attr, stacksize));
#if defined(__cforall)
  SYSCALL(pthread_create(tid, &attr, (TSR)start_routine, arg));
#else
  SYSCALL(pthread_create(tid, &attr, (TSR)(void*)start_routine, arg));
#endif
  SYSCALL(pthread_attr_destroy(&attr));
  return tid;
}

static inline void shim_thread_destroy(shim_thread_t* tid) {
  SYSCALL(pthread_join(*tid, nullptr));
  free(tid);
}

static inline void shim_yield() { sched_yield(); }

static inline void shim_mutex_init(shim_mutex_t* mtx)    { SYSCALL(pthread_mutex_init(mtx, nullptr)); }
static inline void shim_mutex_destroy(shim_mutex_t* mtx) { SYSCALL(pthread_mutex_destroy(mtx)); }
static inline void shim_mutex_lock(shim_mutex_t* mtx)    { SYSCALL(pthread_mutex_lock(mtx)); }
static inline bool shim_mutex_trylock(shim_mutex_t* mtx) { return pthread_mutex_trylock(mtx) == 0; }
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  { SYSCALL(pthread_mutex_unlock(mtx)); }
static inline bool shim_mutex_timedlock(shim_mutex_t* mtx, unsigned int timeout) {
  struct timespec ts;
  if (timeout < 1000000000) {
    ts.tv_sec = 0;
    ts.tv_nsec = timeout;
  } else {
    ts.tv_sec = timeout / 1000000000;
    ts.tv_nsec = timeout % 1000000000;
  }
  return TRY_SYSCALL(pthread_mutex_timedlock(mtx, &ts), ETIMEDOUT) == 0;
}

static inline void shim_cond_init(shim_cond_t* cond)                    { SYSCALL(pthread_cond_init(cond, nullptr)); }
static inline void shim_cond_destroy(shim_cond_t* cond)                 { SYSCALL(pthread_cond_destroy(cond)); }
static inline void shim_cond_wait(shim_cond_t* cond, shim_mutex_t* mtx) { SYSCALL(pthread_cond_wait(cond, mtx)); SYSCALL(pthread_mutex_unlock(mtx)); }
static inline void shim_cond_signal(shim_cond_t* cond)                  { SYSCALL(pthread_cond_signal(cond)); }

static inline shim_barrier_t* shim_barrier_create(size_t cnt) {
  shim_barrier_t* barr = (shim_barrier_t*)malloc(sizeof(shim_barrier_t));
  SYSCALL(pthread_barrier_init(barr, nullptr, cnt));
  return barr;
}

static inline void shim_barrier_destroy(shim_barrier_t* barr) {
  SYSCALL(pthread_barrier_destroy(barr));
  free(barr);
}

static inline void shim_barrier_wait(shim_barrier_t* barr) {
  int ret = pthread_barrier_wait(barr);
  assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
}

#endif /* _tt_pthreads_h_ */
