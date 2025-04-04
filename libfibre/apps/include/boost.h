#ifndef _tt_boost_h_
#define _tt_boost_h_ 1

#include <boost/version.hpp> 
#include <boost/fiber/all.hpp>

#define HASTRYLOCK 1

typedef boost::fibers::fiber   shim_thread_t;
typedef boost::fibers::mutex   shim_mutex_t;
typedef boost::fibers::barrier shim_barrier_t;

static inline shim_thread_t* shim_thread_create(void (*start_routine)(void *), void* arg, bool = false) {
  return new boost::fibers::fiber(start_routine, arg);
}

static inline void shim_thread_destroy(shim_thread_t* tid) {
  tid->join();
  delete tid;
}

static inline void shim_yield() { boost::this_fiber::yield(); }

static inline void shim_mutex_init(shim_mutex_t* mtx)    { new (mtx) shim_mutex_t; }
static inline void shim_mutex_destroy(shim_mutex_t* mtx) {}
static inline void shim_mutex_lock(shim_mutex_t* mtx)    { mtx->lock(); }
static inline bool shim_mutex_trylock(shim_mutex_t* mtx) { return mtx->try_lock(); }
static inline void shim_mutex_unlock(shim_mutex_t* mtx)  { mtx->unlock(); }

static inline shim_barrier_t* shim_barrier_create(size_t cnt) { return new boost::fibers::barrier(cnt); }
static inline void shim_barrier_destroy(shim_barrier_t* barr) { delete barr; }
static inline void shim_barrier_wait(shim_barrier_t* barr)    { barr->wait(); }

static pthread_t* btids = nullptr;
static pthread_barrier_t tbar;
static boost::fibers::barrier* fbar = nullptr;

static void* bthread(void* cnt) {
  // set up work spin-based stealing scheduler
//  boost::fibers::use_scheduling_algorithm< boost::fibers::algo::work_stealing>((uintptr_t)cnt, false);
  boost::fibers::use_scheduling_algorithm< boost::fibers::algo::shared_work>();
  // wait for all pthreads to arrive
  pthread_barrier_wait(&tbar);
  // suspend until experiment is done
  fbar->wait();
  return nullptr;
}

static void boost_init(uintptr_t cnt) {
  // set up synchronization
  pthread_barrier_init(&tbar, nullptr, cnt);
  fbar = new boost::fibers::barrier(cnt);
  btids = new pthread_t[cnt - 1];
  // start threads
  for (uintptr_t i = 0; i < cnt - 1; i += 1) {
    pthread_create(&btids[i], nullptr, bthread, (void*)cnt);
  }
  // set up spin-based work stealing scheduler after threads are started
//  boost::fibers::use_scheduling_algorithm< boost::fibers::algo::work_stealing>(cnt, false);
  boost::fibers::use_scheduling_algorithm< boost::fibers::algo::shared_work>();
  // synchronize pthread arrival
  pthread_barrier_wait(&tbar);
}

static void boost_finalize(uintptr_t cnt) {
  // synchronize experiment done
  fbar->wait();
  // finish all pthreads
  for (uintptr_t i = 0; i < cnt - 1; i += 1) {
    pthread_join(btids[i], nullptr);
  }
  delete [] btids;
  delete fbar;
  pthread_barrier_destroy(&tbar);
}

#endif /* _tt_boost_h_ */
