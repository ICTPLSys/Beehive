#include <array>
#include <concepts>
#include <cstddef>
#include <deque>
#include <type_traits>

#include "cache/cache.hpp"
#include "utils/debug.hpp"

namespace Beehive {
namespace async {

template <typename Context>
concept RunnableContext =
    std::is_move_assignable_v<Context> && requires(Context context) {
        // return true if finished
        // return false if data miss
        { context.run() } -> std::convertible_to<bool>;

        // return true if fetched
        { context.fetched() } -> std::convertible_to<bool>;
    };

template <RunnableContext Context, typename State, typename Cond, typename Step,
          typename Generate, size_t BatchSize = 32>
class InlineLoopRunnerHelper {
private:
    using ContextBuffer = std::array<char, sizeof(Context)>;

    struct TaskNode {
        ContextBuffer buffer;
        TaskNode *next;

        Context *get_context() {
            return reinterpret_cast<Context *>(buffer.data());
        }

        bool run() { return get_context()->run(); }

        bool fetched() { return get_context()->fetched(); }

        Context *construct(Context &&context) {
            Context *c = get_context();
            new (c) Context(std::move(context));
            return c;
        }

        void deconstruct() { get_context()->~Context(); }
    };

private:
    // the loop status and functions
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
    InlineLoopRunnerHelper(State &&init, Cond &&cond, Step &&step,
                           Generate &&gen)
        : state(std::forward<State>(init)),
          cond(std::forward<Cond>(cond)),
          step(std::forward<Step>(step)),
          gen(std::forward<Generate>(gen)),
          task_list_head(nullptr),
          task_list_last(nullptr),
          task_list_size(0),
          free_list(nullptr) {}

    void loop() {
        // allocate the first buffer for context
        current_task = allocate_task();
        while (cond(state)) {
            State current = state;
            current_task->construct(gen(current));
            bool finished = current_task->run();
            if (!finished) {
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
            bool finished = task->run();
            if (finished) [[likely]] {
                task->deconstruct();
                deallocate_task(task);
            } else {
                task_enqueue(task);
            }
        }
    }
};

template <RunnableContext Context>
struct InlineLoopRunner {
    template <typename State, typename Cond, typename Step, typename Generate>
    static void run(State &&init, Cond &&cond, Step &&step, Generate &&gen) {
        InlineLoopRunnerHelper<Context, State, Cond, Step, Generate>(
            std::forward<State>(init), std::forward<Cond>(cond),
            std::forward<Step>(step), std::forward<Generate>(gen))
            .loop();
    }
};

}  // namespace async
}  // namespace Beehive

#define INLINE_ASYNC_FOR(CONTEXT, TYPE, I, INIT, COND, STEP) \
    Beehive::async::InlineLoopRunner<CONTEXT>::run(          \
        (TYPE)(INIT), [&](TYPE I) { return COND; },         \
                       [&](TYPE &I) { STEP; },              \
                       [&](TYPE I) {
#define INLINE_ASYNC_FOR_END \
    });
