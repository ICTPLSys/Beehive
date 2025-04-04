#pragma once
#include <array>
#include <cstddef>
#include <deque>
#include <iostream>
#include <type_traits>

#include "async/context.hpp"
#include "async/stream_runner.hpp"
#include "cache/cache.hpp"
#include "utils/cpu_cycles.hpp"
#include "utils/debug.hpp"

namespace FarLib {
namespace async {
/*
    Co-design with dereference scope:
    The RootDereferenceScope should not be constructed in OoO contexts.
    The evictor pins all contexts and call methods in the parent scope.
*/
static constexpr size_t DEFAULT_BATCH_SIZE = 32;

template <ScopedRunnableContext Context, typename State, typename Cond,
          typename Step, typename Generate,
          size_t BatchSize = DEFAULT_BATCH_SIZE>
class ScopedInlineLoopRunnerHelper {
private:
    using ContextBuffer = std::array<char, sizeof(Context)>;
    size_t run_cnt;
    struct TaskNode {
        ContextBuffer buffer;
        TaskNode *next;

        Context *get_context() {
            return reinterpret_cast<Context *>(buffer.data());
        }

        bool run(DereferenceScope &scope) { return get_context()->run(scope); }

        bool fetched() { return get_context()->fetched(); }

        Context *construct(Context &&context) {
            Context *c = get_context();
            new (c) Context(std::move(context));
            return c;
        }

        void deconstruct() { get_context()->~Context(); }

        void pin() { get_context()->pin(); }

        void unpin() { get_context()->unpin(); }
    };

    struct Scope : public DereferenceScope {
        ScopedInlineLoopRunnerHelper *runner;

        Scope(DereferenceScope *parent, ScopedInlineLoopRunnerHelper *runner)
            : DereferenceScope(parent), runner(runner) {}

        void pin() const override {
            runner->current_task->pin();
            for (TaskNode *task = runner->task_front(); task != nullptr;
                 task = task->next) {
                task->pin();
            }
        }
        void unpin() const override {
            runner->current_task->unpin();
            for (TaskNode *task = runner->task_front(); task != nullptr;
                 task = task->next) {
                task->unpin();
            }
        }
    };

private:
    // the loop status and functions
    Scope scope;
    State state;
    Cond cond;
    Step step;
    Generate gen;

    // the task list
    TaskNode *task_list_head;
    TaskNode *task_list_last;
    size_t task_list_size;

    // the task buffers
    TaskNode *free_list;
    std::deque<TaskNode> buffer_list;

    // current_task is a preallocated buffer
    // use this to avoid reduce allocation & deallocation
    TaskNode *current_task;

private:
    TaskNode *allocate_task() {
        if (free_list == nullptr) {
            return &buffer_list.emplace_back();
        } else {
            TaskNode *node = free_list;
            free_list = free_list->next;
            return node;
        }
    }

    void deallocate_task(TaskNode *node) {
        node->next = free_list;
        free_list = node;
    }

    void task_enqueue(TaskNode *node) {
        node->next = nullptr;
        if (task_list_size == 0) [[unlikely]] {
            task_list_head = node;
            task_list_last = node;
        } else {
            task_list_last->next = node;
            task_list_last = node;
        }
        task_list_size++;
    }

    TaskNode *task_front() { return task_list_head; }

    void task_dequeue() {
        task_list_head = task_list_head->next;
        task_list_size--;
    }

public:
    ScopedInlineLoopRunnerHelper(DereferenceScope &parent_scope, State &&init,
                                 Cond &&cond, Step &&step, Generate &&gen)
        : scope(&parent_scope, this),
          state(std::forward<State>(init)),
          cond(std::forward<Cond>(cond)),
          step(std::forward<Step>(step)),
          gen(std::forward<Generate>(gen)),
          task_list_head(nullptr),
          task_list_last(nullptr),
          task_list_size(0),
          free_list(nullptr),
          run_cnt(0) {
        ERROR("deprecated");
    }

    void loop() {
        // allocate the first buffer for context
        current_task = allocate_task();
        while (cond(state)) {
            State current = state;
            current_task->construct(gen(current));
            bool finished = current_task->run(scope);
            run_cnt++;
            if (!finished) [[unlikely]] {
                task_enqueue(current_task);
                if (task_list_size >= BatchSize) schedule();
                current_task = allocate_task();
            } else {
                current_task->deconstruct();
            }
            step(state);
        }
        while (task_list_size > 0) schedule();
    }

    ~ScopedInlineLoopRunnerHelper() {
        std::cout << "run count: " << run_cnt << std::endl;
    }

private:
    // select and run unfinished tasks from the task list
    // TODO: should we limit the max size of task list?
    // TODO: should we suspend scheduling when there is too few tasks?
    //      (to leave more parallel oppotunities)
    void schedule() {
        cache::check_cq();
        while (task_list_size > 0) [[likely]] {
            TaskNode *task = task_front();
            // if the first task is not ready
            // in most cases other tasks are not ready, too
            if (!task->fetched()) return;
            // pop the first task from the list and run it
            task_dequeue();
            current_task = task;
            bool finished = task->run(scope);
            run_cnt++;
            if (finished) [[likely]] {
                task->deconstruct();
                deallocate_task(task);
            } else {
                task_enqueue(task);
            }
        }
    }
};

template <ScopedRunnableContext ContextType, typename State, typename Cond,
          typename Step, typename Generate>
struct LoopStream {
    using Context = ContextType;
    State state;
    Cond cond;
    Step step;
    Generate gen;

    LoopStream(State &&init, Cond &&cond, Step &&step, Generate &&gen)
        : state(init), cond(cond), step(step), gen(gen) {}

    StreamState get(Context *ctx) {
        if (cond(state)) {
            new (ctx) Context(gen(state));
            step(state);
            return StreamState::READY;
        } else {
            return StreamState::FINISHED;
        }
    }
};

template <ScopedRunnableContext Context, bool Ordered = false>
struct ScopedInlineLoopRunner {
    template <typename State, typename Cond, typename Step, typename Generate>
    static void run(DereferenceScope &scope, State &&init, Cond &&cond,
                    Step &&step, Generate &&gen) {
        using Stream = LoopStream<Context, State, Cond, Step, Generate>;
        process_pararoutine_stream<Stream, Ordered>(
            Stream(std::forward<State>(init), std::forward<Cond>(cond),
                   std::forward<Step>(step), std::forward<Generate>(gen)),
            scope);
    }
};

template <bool Ordered = false, typename Generate>
void for_range(DereferenceScope &scope, size_t start, size_t end,
               Generate &&gen) {
    using Context = decltype(gen(0));
    auto cond = [end](size_t i) { return i < end; };
    auto step = [](size_t &i) { i++; };
    using Stream =
        LoopStream<Context, size_t, decltype(cond), decltype(step), Generate>;
    process_pararoutine_stream<Stream, Ordered>(
        Stream(std::move(start), std::move(cond), std::move(step),
               std::forward<Generate>(gen)),
        scope);
}

}  // namespace async
}  // namespace FarLib

#define SCOPED_INLINE_ASYNC_FOR(CONTEXT, TYPE, I, INIT, COND, STEP, SCOPE) \
    FarLib::async::ScopedInlineLoopRunner<CONTEXT>::run(          \
        SCOPE, (TYPE)(INIT), [&](TYPE I) { return COND; },  \
                       [&](TYPE &I) { STEP; },              \
                       [&](TYPE I) {
#define SCOPED_INLINE_ASYNC_FOR_END \
    });
