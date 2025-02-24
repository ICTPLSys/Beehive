#pragma once
#include <cstddef>
#include <type_traits>
#include <utility>

#include "async/select_schedule.hpp"
#ifdef NEW_SCHEDULE
#include "cache/cache.hpp"
#else
#include "cache/handler.hpp"
#endif
#include "utils/debug.hpp"

constexpr size_t STACK_SIZE = 8 * 1024;

extern "C" {

typedef void *sp_t;

#ifdef NEW_SCHEDULE
typedef struct {
    sp_t current_sp;
} context_t;
typedef context_t *(*func_t)(context_t *);
#else
typedef struct {
    sp_t caller_sp;
    sp_t current_sp;
} context_t;
typedef context_t *(*func_t)(context_t *);
#endif
}

namespace Beehive {

namespace async {
#ifndef NEW_SCHEDULE
extern thread_local cache::FarObjectEntry *main_block_entry;
extern thread_local cache::fetch_ddl_t main_block_ddl;
#endif
}  // namespace async
}  // namespace Beehive

extern "C" {

/*
    1. save regs
    2. save current stack pointer to ctx
    3. switch stack to sp
    4. call func
    5. restore stack & regs

    return 0x1 if finished, see `TaskState` below
*/
#ifdef NEW_SCHEDULE
int call_xstack(context_t *ctx, sp_t sp, context_t *caller_task, func_t func);
#else
int call_xstack(context_t *ctx, sp_t sp, func_t func);
#endif
/*
    1. save regs
    2. save current stack pointer to current_sp
    3. switch stack back to sp
    4. restore regs

    return 0x1 if finished, see `TaskState` below
*/
#ifdef NEW_SCHEDULE
int yield_xstack(sp_t sp, int state, void *_task);
#else
int yield_xstack(sp_t *current_sp, sp_t sp, int state);
#endif
}

namespace Beehive {

namespace async {

enum TaskState {
    READY = 0x0,
    DONE = 0x1,
    FETCH = 0x2,
    SYNC = 0x3,
};
}  // namespace async
#ifdef NEW_SCHEDULE
namespace async {
struct TaskBase : public context_t {
    static constexpr uint64_t MAGIC_PADDING = 0x12345678;
    static constexpr uint64_t PADDING_SIZE = 16;
    TaskState state;
    cache::FarObjectEntry *blocked_entry;
    cache::fetch_ddl_t blocked_ddl;
    TaskBase *prev;
    TaskBase *next;
#ifndef NDEBUG
    uint64_t padding[PADDING_SIZE];
#endif

    TaskBase() : prev(nullptr), next(nullptr), blocked_entry(nullptr) {
        current_sp = 0;
        state = READY;
#ifndef NDEBUG
        for (size_t i = 0; i < PADDING_SIZE; i++) {
            padding[i] = MAGIC_PADDING;
        }
#endif
    }

    void done() { state = DONE; }

    void remove_self() {
        this->next->prev = this->prev;
        this->prev->next = this->next;
#ifndef NDEBUG
        for (size_t i = 0; i < PADDING_SIZE; i++) {
            ASSERT(padding[i] == MAGIC_PADDING);
        }
#endif
    }

    void insert_back(TaskBase *task) {
        task->prev = this;
        task->next = this->next;
        this->next->prev = task;
        this->next = task;
    }

    void insert_front(TaskBase *task) {
        task->prev = this->prev;
        task->next = this;
        this->prev->next = task;
        this->prev = task;
    }
};

class TaskQueue {
protected:
    TaskBase *task_cursor;
    TaskBase *current_task;
    TaskBase *in_order_task;
    size_t task_count;
    const size_t max_count;
    TaskBase main_task_node;
    TaskBase dummy_head;
    TaskBase dummy_tail;

public:
    void insert_tail(TaskBase *task) {
        dummy_tail.insert_front(task);
        task_count++;
    }

    void insert_head(TaskBase *task) {
        main_task_node.insert_back(task);
        task_count++;
    }

    TaskBase *next_task() {
        if (empty()) {
            task_cursor = static_cast<TaskBase *>(in_order_task);
            return nullptr;
        } else {
            task_cursor = task_cursor->next == &dummy_tail ? &main_task_node
                                                           : task_cursor->next;
        }
        return task_cursor;
    }

    TaskBase *first_task() { return main_task_node.next; }

    bool full() { return task_count == max_count; }

    bool empty() {
        return (in_order_task == &main_task_node ||
                current_task == &main_task_node)
                   ? task_count == 0
                   : task_count == 1;
    }

    size_t size() { return task_count; }

    void remove(TaskBase *task) {
        task->remove_self();
        task_count--;
        if (task == task_cursor) {
            task_cursor = task_cursor->prev;
        }
    }

    void keep_seq_cons() {
        if (current_task == in_order_task) {
            TaskBase *task = static_cast<TaskBase *>(in_order_task);
            in_order_task =
                task->next == &dummy_tail ? &main_task_node : task->next;
            task_cursor = static_cast<TaskBase *>(in_order_task)->prev;
        }
        TaskBase *cur_task = static_cast<TaskBase *>(current_task);
        cur_task->remove_self();
        dummy_tail.insert_front(cur_task);
    }

    TaskQueue(size_t max_cnt)
        : task_cursor(&main_task_node),
          task_count(0),
          max_count(max_cnt),
          current_task(&main_task_node),
          in_order_task(current_task) {
        dummy_head.next = &main_task_node;
        main_task_node.prev = &dummy_head;
        main_task_node.next = &dummy_tail;
        dummy_tail.prev = &main_task_node;
    }

    template <typename Fn, size_t StackSize>
    friend class Task;
};

template <typename Fn, size_t StackSize = STACK_SIZE>
class Task : public TaskBase {
    using Self = Task<Fn, StackSize>;

private:
    Fn fn;
    TaskQueue &task_in_q;

public:
    Task(TaskQueue &task_in_q, Fn &&fn) : task_in_q(task_in_q), fn(fn) {}
    Task(TaskQueue &task_in_q, const Fn &fn) : task_in_q(task_in_q), fn(fn) {}

    void call() { fn(); }

    static context_t *do_invoke(context_t *task) {
        static_cast<Self *>(task)->call();
        return static_cast<Self *>(task)->task_in_q.current_task ==
                       static_cast<Self *>(task)->task_in_q.in_order_task
                   ? &(static_cast<Self *>(task)->task_in_q.main_task_node)
                   : static_cast<Self *>(task)->task_in_q.in_order_task;
    }
};

class RunnerTaskManager : public TaskQueue {
public:
    RunnerTaskManager(size_t max_cnt) : TaskQueue(max_cnt) {}

    inline bool current_in_order() const {
        return current_task == in_order_task;
    }

    inline bool current_ready() {
        return cache::check_fetch(current_task->blocked_entry,
                                  current_task->blocked_ddl);
    }

    inline bool in_order_ready() {
        return cache::check_fetch(in_order_task->blocked_entry,
                                  in_order_task->blocked_ddl);
    }

    inline bool in_order_sync() { return in_order_task->state == SYNC; }

    void sync() {
        while (current_task != in_order_task) {
            // TODO can yield other to improve performance
            yield_back_in_order(SYNC);
        }
    }

    template <typename Fn, size_t StackSize>
    void call_on_stack(Task<Fn, StackSize> *task, sp_t sp) {
        using task_t = Task<Fn, StackSize>;
        auto temp_task = current_task;
        current_task = task;
        call_xstack(task, sp, temp_task, Task<Fn, StackSize>::do_invoke);
        current_task = temp_task;
    }

    void yield_back_in_order(TaskState state) {
        current_task->state = state;
        auto temp_task = current_task;
        current_task = in_order_task;
        yield_xstack(in_order_task->current_sp, state, temp_task);
        current_task = temp_task;
    }

    template <bool main_priv_pass = false>
    void resume(TaskBase *task) {
        task->state = READY;
        if constexpr (main_priv_pass) {
            in_order_task = task;
        }
        auto temp_task = current_task;
        current_task = task;
        yield_xstack(task->current_sp, READY, temp_task);
        current_task = temp_task;
        if constexpr (main_priv_pass) {
            in_order_task = temp_task;
        }
    }
    template <typename State, typename Cond, typename Step, typename Run,
              size_t MaxExecAhead, bool KeepSeqCons>
    friend class LoopRunner;

    template <typename State, typename Cond, typename Step, typename Run,
              size_t MaxExecAhead, bool KeepSeqCons>
    friend class ScopeLoopRunner;
};
}  // namespace async
#else
namespace async {
struct TaskBase : public context_t {
    TaskState state = READY;
    cache::FarObjectEntry *blocked_entry;

    TaskState call_on_stack(sp_t sp, func_t func) {
        state = static_cast<TaskState>(call_xstack(this, sp, func));
        return state;
    }

    // yield back to the caller,
    TaskState yield(TaskState state) {
        state =
            static_cast<TaskState>(yield_xstack(&current_sp, caller_sp, state));
        return state;
    }

    TaskState resume() {
        state =
            static_cast<TaskState>(yield_xstack(&caller_sp, current_sp, READY));
        return state;
    }
};

template <typename Fn, size_t StackSize = STACK_SIZE>
class Task : public TaskBase {
    using Self = Task<Fn, StackSize>;

public:
    Task(Fn &&fn) : fn(fn) {}
    Task(const Fn &fn) : fn(fn) {}

    void call() { fn(); }

    TaskState invoke(sp_t ex_sp) {
        return this->call_on_stack(ex_sp, do_invoke);
    }

private:
    static context_t *do_invoke(context_t *task) {
        static_cast<Self *>(task)->fn();
        return task;
    }

private:
    Fn fn;
};
}  // namespace async
#endif
}  // namespace Beehive
