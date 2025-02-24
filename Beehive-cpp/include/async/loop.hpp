#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <stack>

#include "async.hpp"
#include "cache/cache.hpp"
#include "stack_allocator.hpp"
#include "task.hpp"

namespace Beehive {
namespace async {
static constexpr size_t default_max_exec_ahead = 16;
#ifdef NEW_SCHEDULE
template <typename State, typename Cond, typename Step, typename Run,
          size_t MaxExecAhead = default_max_exec_ahead,
          bool KeepSeqCons = false>
class LoopRunner {
private:
    State state;
    Cond cond;
    Step step;
    Run run;

    RunnerTaskManager manager;

public:
    using ull = unsigned long long;
    LoopRunner(State &&init, Cond &&cond, Step &&step, Run &&run)
        : state(std::forward<State>(init)),
          cond(std::forward<Cond>(cond)),
          step(std::forward<Step>(step)),
          run(std::forward<Run>(run)),
          manager(MaxExecAhead) {}

    void loop() {
        ON_MISS_BEGIN
            yield(__entry__, __ddl__);
        ON_MISS_END
        while (cond(state)) {
            State current = state;
            step(state);
            run(current, __on_miss__, [this]() { this->manager.sync(); });
            if constexpr (KeepSeqCons) {
                this->keep_seq_cons();
            }
        }
        run_unfinished();
    }

private:
    void run_xstack_until_ready() {
        while (!manager.full()) {
            bool loop_continue = step_xstack();
            if (!loop_continue) break;
            if (manager.current_ready()) return;
        }
        if (manager.empty()) return;
        // run each task in queue until in_order_ready
        while (!manager.current_ready() && !manager.empty()) {
            TaskBase *task = manager.next_task();
            if (task == manager.current_task) {
                task = manager.next_task();
                if (task == manager.current_task) {
                    // only self in queue, return
                    return;
                }
            }
            switch (task->state) {
            case FETCH:
                // Cache::get_default()->check_cq();
                if (!task->blocked_entry->is_local()) break;
                [[fallthrough]];
            case READY:
                manager.resume(task);
                break;
            case DONE:
                ERROR("done task detected!");
            default:
                break;
            }
        }
    }

    void yield(cache::FarObjectEntry *entry, cache::fetch_ddl_t ddl) {
        manager.current_task->blocked_entry = entry;
        manager.current_task->blocked_ddl = ddl;
        if (!manager.current_in_order()) {
            // ahead task yield
            if constexpr (KeepSeqCons) {
                if (manager.in_order_sync()) {
                    manager.yield_back_in_order(FETCH);
                    return;
                }
            }
            if (manager.in_order_ready()) {
                manager.yield_back_in_order(FETCH);
                return;
            }
        }
        run_xstack_until_ready();
    }

    void run_xstack() {
        auto fn = [this] {
            ON_MISS_BEGIN
                yield(__entry__, __ddl__);
            ON_MISS_END
            while (cond(state)) {
                State current = state;
                step(state);
                run(current, __on_miss__, [this]() { this->manager.sync(); });
                if constexpr (KeepSeqCons) {
                    this->keep_seq_cons();
                }
            }
            this->done_now_task();
            return;
        };
        // 1. allocate stack
        char *ex_stack = StackAllocator<STACK_SIZE>::allocate_stack();
        // char *ex_stack = stacks[idx++];
        sp_t ex_sp = ex_stack + STACK_SIZE;
        // 2. construct the closure to the stack
        using task_t = Task<decltype(fn)>;
        static_assert(STACK_SIZE > sizeof(task_t));
        static_assert(std::is_trivially_destructible_v<task_t>);
        task_t *task_ptr = reinterpret_cast<task_t *>(ex_stack);
        new (task_ptr) task_t(manager, fn);
        manager.insert_tail(task_ptr);
        // 3. invoke on stack
        manager.call_on_stack(task_ptr, ex_sp);
    }

    bool step_xstack() {
        if (cond(state)) {
            run_xstack();
            return true;
        } else {
            return false;
        }
    }

    void run_unfinished() {
        while (!manager.empty()) [[unlikely]] {
            if constexpr (KeepSeqCons) {
                if (manager.current_in_order()) {
                    auto task = manager.first_task();
                    if (task->state != DONE) {
                        manager.template resume<true>(task);
                    }
                } else {
                    auto task = manager.in_order_task;
                    if (task->state != DONE) {
                        manager.template resume<false>(task);
                    }
                }
            } else {
                auto task = manager.first_task();
                if (task->state != DONE) {
                    manager.template resume<true>(task);
                }
            }
        }
    }

    void done_now_task() {
        manager.current_task->done();
        manager.remove(manager.current_task);
        // TODO depend on stack allocator
        StackAllocator<STACK_SIZE>::deallocate_stack(
            reinterpret_cast<char *>(manager.current_task));
    }

    void keep_seq_cons() { manager.keep_seq_cons(); }
};
#else
template <typename State, typename Cond, typename Step, typename Run,
          size_t MaxExecAhead = 16>
class LoopRunner {
public:
    LoopRunner(State &&init, Cond &&cond, Step &&step, Run &&run)
        : state(std::forward<State>(init)),
          cond(std::forward<Cond>(cond)),
          step(std::forward<Step>(step)),
          run(std::forward<Run>(run)) {}

    void loop() {
        ON_MISS_BEGIN
            yield(__entry__, __ddl__);
        ON_MISS_END
        while (cond(state)) {
            State current = state;
            step(state);
            run(current, __on_miss__);
            run_unfinished();
        }
    }

private:
    void run_xstack_until_ready() {
        while (!task_queue_full()) {
            bool loop_continue = step_xstack();
            if (!loop_continue) break;
            if (main_ready()) return;
        }
        if (task_queue_empty()) return;
        // run each task in queue until main_ready()
        size_t task_idx = task_queue_head;
        while (!main_ready()) {
            TaskBase *task = task_queue[task_idx];
            switch (task->state) {
            case FETCH:
                // Cache::get_default()->check_cq();
                if (!task->blocked_entry->is_local()) break;
                [[fallthrough]];
            case READY:
                out_of_order_task = task;
                task->resume();
                out_of_order_task = nullptr;
                break;
            default:
                break;
            }
            task_idx = next_task_idx(task_idx);
            if (task_idx == task_queue_tail) task_idx = task_queue_head;
        }
    }

    // only call this on main task!
    void yield(cache::FarObjectEntry *entry, cache::fetch_ddl_t ddl) {
        main_block_entry = entry;
        main_block_ddl = ddl;
        run_xstack_until_ready();
    }

    bool main_ready() {
        return cache::check_fetch(main_block_entry, main_block_ddl);
    }

    void run_xstack(State current) {
        auto fn = [current, this] {
            ON_MISS_BEGIN
                yield(__entry__, __ddl__);
            ON_MISS_END
            run(current, __on_miss__);
        };
        // 1. allocate stack
        char *ex_stack = StackAllocator<STACK_SIZE>::allocate_stack();
        using task_t = Task<decltype(fn)>;
        static_assert(STACK_SIZE > sizeof(task_t));
        static_assert(std::is_trivially_destructible_v<task_t>);
        sp_t ex_sp = ex_stack + STACK_SIZE;
        // 2. construct the closure to the stack
        task_t *task_ptr = reinterpret_cast<task_t *>(ex_stack);
        new (task_ptr) task_t(fn);
        out_of_order_task = task_ptr;
        // 3. invoke on stack
        bool finished = task_ptr->invoke(ex_sp) == DONE;
        // 4. record current task
        if (finished) {
            StackAllocator<STACK_SIZE>::deallocate_stack(ex_stack);
        } else {
            push_task(task_ptr);
        }
        out_of_order_task = nullptr;
    }

    bool step_xstack() {
        if (cond(state)) {
            State current = state;
            step(state);
            run_xstack(current);
            return true;
        } else {
            return false;
        }
    }

    void run_unfinished() {
        while (!task_queue_empty()) [[unlikely]] {
            auto task = pop_task();
            if (task->state != DONE) {
                auto state = task->resume();
                ASSERT(state == DONE);
            }
            StackAllocator<STACK_SIZE>::deallocate_stack(
                reinterpret_cast<char *>(task));
        }
    }

    static size_t next_task_idx(size_t idx) {
        return idx == MaxExecAhead - 1 ? 0 : idx + 1;
    }

    size_t task_queue_size() {
        if (task_queue_tail > task_queue_head) {
            return task_queue_tail - task_queue_head;
        } else {
            return task_queue_tail + MaxExecAhead - task_queue_head;
        }
    }

    void push_task(TaskBase *task) {
        assert(!task_queue_full());
        task_queue[task_queue_tail] = task;
        task_queue_tail = next_task_idx(task_queue_tail);
    }

    TaskBase *pop_task() {
        TaskBase *front = task_queue[task_queue_head];
        task_queue_head++;
        if (task_queue_head == MaxExecAhead) [[unlikely]] {
            task_queue_head = 0;
        }
        return front;
    }

    bool task_queue_full() {
        return next_task_idx(task_queue_tail) == task_queue_head;
    }

    bool task_queue_empty() { return (task_queue_tail == task_queue_head); }

private:
    State state;
    Cond cond;
    Step step;
    Run run;

    std::array<TaskBase *, MaxExecAhead> task_queue;
    size_t task_queue_head = 0;
    size_t task_queue_tail = 0;
};
#endif
template <typename State, typename Cond, typename Step, typename Run,
          size_t MaxExecAhead = default_max_exec_ahead>
using SyncLoopRunner = LoopRunner<State, Cond, Step, Run, MaxExecAhead, true>;
}  // namespace async
}  // namespace Beehive

#define ASYNC_FOR(TYPE, I, INIT, COND, STEP)                               \
    Beehive::async::LoopRunner((TYPE)(INIT), [&](TYPE I) { return COND; }, \
                       [](TYPE &I) { STEP; },                             \
                       [&](TYPE I, auto __on_miss__, auto __sync__) {
#define ASYNC_FOR_END \
    }).loop();

#define SYNC_FOR(TYPE, I, INIT, COND, STEP)                                    \
    Beehive::async::SyncLoopRunner((TYPE)(INIT), [&](TYPE I) { return COND; }, \
                       [](TYPE &I) { STEP; },                             \
                       [&](TYPE I, auto __on_miss__, auto __sync__) {
#define SYNC_FOR_END \
    }).loop();
