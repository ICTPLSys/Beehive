#include "cfibre.h"

#include <pthread.h>

#include <array>
#include <atomic>
#include <iostream>
#include <string>
#include <vector>

#include "libfibre/fibre.h"

void say(const std::string s) { std::cout << s << std::endl; }
namespace cfibre {
using UThread = Fibre;
using Mutex = FredMutex;
using Condition = FredCondition;
using UThreadLocal = FibreLocalVariables;
EventScope *es = nullptr;

// strongly bound to kernel
constexpr size_t TID_OFFS = 0x2d0;
std::atomic_size_t thread_counter(0);
thread_local size_t thread_idx = thread_counter.fetch_add(1);
extern "C" {
size_t get_worker_count() { 
    return ::Context::CurrCluster().getWorkerSysIDs(nullptr, 0); 
}

size_t get_thread_idx() { return thread_idx; }
pid_t get_thread_tid(size_t idx) {
    struct pthread_fake {
        char padding[TID_OFFS];
        pid_t tid;
    };
    return ((pthread_fake *)(es->get_tids()[idx]))->tid;
}
void say_hi() { say("hi"); }

void uthread_init(size_t workers) { es = FibreInit(1, workers); }
// void cfibre_destroy() { delete es; }

Mutex *new_mutex() { return new Mutex(); }

Condition *new_condition() { return new Condition(); }

void destroy_mutex(Mutex *mutex) { delete mutex; }

void destroy_condition(Condition *cond) { delete cond; }

bool uthread_yield() { return Fibre::yieldGlobal(); }

UThread *uthread_create(void (*func)(void *), void *arg, bool low_priority) {
    auto *uthread = new UThread();
    if (low_priority) {
        uthread->setPriority(Fibre::LowPriority);
    }
    uthread->run(func, arg);
    return uthread;
}
void uthread_join(UThread *uthread) { uthread->join(); }
void uthread_destroy(UThread *uthread) { delete uthread; }

void lock(Mutex *mutex) { mutex->acquire(); }

bool try_lock(Mutex *mutex) { return mutex->tryAcquire(); }

void unlock(Mutex *mutex) { return mutex->release(); }

void wait_locked(Condition *cond, Mutex *mutex) { cond->wait(*mutex); }
void wait(Condition *cond, Mutex *mutex) {
    mutex->acquire();
    wait_locked(cond, mutex);
}

void notify_locked(Condition *cond) { cond->signal(); }

void notify_all_locked(Condition *cond) { cond->signal<true>(); }

void notify(Condition *cond, Mutex *mutex) {
    mutex->acquire();
    notify_locked(cond);
    mutex->release();
}

void notify_all(Condition *cond, Mutex *mutex) {
    mutex->acquire();
    notify_all_locked(cond);
    mutex->release();
}

UThreadLocal *get_tls() { return static_cast<UThreadLocal *>(fibre_self()); }

void uthread_run(size_t thread_count, task_func tf, void *closure) {
    struct UThreadTask {
        task_func func;
        void *closure;
        size_t arg;
    };
    std::vector<UThreadTask> tasks(thread_count);
    std::vector<UThread> uthreads(thread_count);
    typedef void (*uthread_fn_t)(UThreadTask *);
    uthread_fn_t uthread_run = [](UThreadTask *task) {
        (task->func)(task->closure, task->arg);
    };
    for (size_t i = 0; i < thread_count; i++) {
        tasks[i].func = tf;
        tasks[i].closure = closure;
        tasks[i].arg = i;
        uthreads[i].run(uthread_run, &tasks[i]);
    }
    for (auto &t : uthreads) {
        t.join();
    }
}
}
}  // namespace cfibre
