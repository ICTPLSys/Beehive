#pragma once
#include <sched.h>

#include <memory>

#include "libfibre/fibre.h"
#include "utils/stats.hpp"

namespace FarLib {

namespace uthread {

using UThread = Fibre;
using Mutex = FredMutex;
using Condition = FredCondition;
using UThreadLocal = FibreLocalVariables;
extern EventScope *es;

inline void runtime_init(size_t workers = 1) { es = FibreInit(1, workers); }

inline void runtime_destroy() {}

inline size_t get_worker_count() {
    return ::Context::CurrCluster().getWorkerSysIDs(nullptr, 0);
}
/* clang-format off */
inline size_t get_thread_count() { return 16; }
/* clang-format on */

inline std::unique_ptr<UThread> create(void (*func)()) {
    auto uthread = std::make_unique<UThread>();
    uthread->run(func);
    return uthread;
}

template <bool HighPriority = false, typename T>
inline std::unique_ptr<UThread> create(void (*func)(T *), T *arg,
                                       std::string name = "") {
    auto uthread = std::make_unique<UThread>();
    if constexpr (HighPriority) {
        uthread->setPriority(Fibre::TopPriority);
    }
    uthread->setName(name);
    uthread->run(func, arg);
    return uthread;
}

inline void join(std::unique_ptr<UThread> uthread) { uthread.reset(); }

inline bool yield() {
    bool suspended = profile::suspend_work();
    profile::start_yield();
    bool yielded = Fibre::yieldGlobal();
    profile::end_yield();
    profile::resume_work(suspended);
    return yielded;
}

inline void lock(Mutex *mutex) { mutex->acquire(); }

inline bool try_lock(Mutex *mutex) { return mutex->tryAcquire(); }

inline void unlock(Mutex *mutex) { mutex->release(); }

inline void wait_locked(Condition *cond, Mutex *mutex) {
    // printf("%p wait\n", fibre_self());
    cond->wait(*mutex);
    // printf("%p wake up\n", fibre_self());
}

inline void wait(Condition *cond, Mutex *mutex) {
    // printf("%p wait\n", fibre_self());
    mutex->acquire();
    cond->wait(*mutex);
    // printf("%p wake up\n", fibre_self());
}

template <class Pred>
inline void wait_for(Condition *cond, Mutex *mutex, Pred pred) {
    mutex->acquire();
    while (!pred()) {
        cond->wait(*mutex);
        mutex->acquire();
    }
    mutex->release();
}

inline void notify(Condition *cond, Mutex *mutex) {
    mutex->acquire();
    cond->signal();
    mutex->release();
}

inline void notify_all(Condition *cond, Mutex *mutex) {
    mutex->acquire();
    cond->signal<true>();
    mutex->release();
}

inline void notify_locked(Condition *cond) { cond->signal(); }

inline void notify_all_locked(Condition *cond) { cond->signal<true>(); }

inline UThreadLocal *get_tls() {
    return static_cast<UThreadLocal *>(fibre_self());
}

inline void set_default_priority() {
    fibre_self()->setPriority(Fibre::DefaultPriority);
}

inline void set_high_priority() {
    fibre_self()->setPriority(Fibre::TopPriority);
}

}  // namespace uthread

using uthread::UThread;

}  // namespace FarLib
