#include <array>
#include <functional>
#include <memory>
#include <thread>

#include "libfibre/fibre.h"

namespace Beehive {

template <size_t ThreadCount>
void pthread_run(std::function<void(size_t)> task) {
    std::array<std::thread, ThreadCount> threads;
    for (size_t i = 0; i < ThreadCount; i++) {
        threads[i] = std::thread(task, i);
    }
    for (auto& t : threads) {
        t.join();
    }
}

template <size_t ThreadCount>
void uthread_run(std::function<void(size_t)> task) {
    struct UThreadTask {
        std::function<void(size_t)>* func;
        size_t arg;
    };
    typedef void (*uthread_fn_t)(UThreadTask*);
    uthread_fn_t uthread_run = [](UThreadTask* task) {
        (*task->func)(task->arg);
    };

    std::array<UThreadTask, ThreadCount> tasks;
    std::array<Fibre, ThreadCount> uthreads;
    for (size_t i = 0; i < ThreadCount; i++) {
        tasks[i].func = &task;
        tasks[i].arg = i;
        uthreads[i].run(uthread_run, &tasks[i]);
    }
    for (auto& t : uthreads) {
        t.join();
    }
}

}  // namespace Beehive