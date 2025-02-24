#pragma once
#include <x86intrin.h>

#include <array>
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <deque>
#include <type_traits>

#include "async/context.hpp"
#include "cache/cache.hpp"
#include "utils/linked_list.hpp"

#define PROFILE_STREAM_RUNNER_SCHEDULE

namespace Beehive {
namespace async {

enum class StreamState { READY, WAITING, FINISHED };

struct StreamRunnerProfiler {
#ifdef PROFILE_STREAM_RUNNER_SCHEDULE
public:
    inline void timer_begin() { total_cycles -= __rdtsc(); }
    inline void timer_end() { total_cycles += __rdtsc(); }
    inline void app_begin() { /* app_cycles -= __rdtsc(); */ }
    inline void app_end() { /* app_cycles += __rdtsc(); */ }
    inline void sched_begin() { sched_cycles -= __rdtsc(); }
    inline void sched_end() { sched_cycles += __rdtsc(); }
    inline void poll_cq_begin() { sched_cycles += __rdtsc(); }
    inline void poll_cq_end() { sched_cycles -= __rdtsc(); }
    static inline int64_t get_total_cycles() { return global_total_cycles; }
    static inline int64_t get_app_cycles() { return global_app_cycles; }
    static inline int64_t get_sched_cycles() { return global_sched_cycles; }
    static inline int64_t get_poll_cq_cycles() { return global_poll_cq_cycles; }
    static inline void reset() {
        global_total_cycles = 0;
        global_app_cycles = 0;
        global_sched_cycles = 0;
        global_poll_cq_cycles = 0;
    }

    ~StreamRunnerProfiler() {
        ASSERT(total_cycles >= 0);
        ASSERT(app_cycles >= 0);
        ASSERT(sched_cycles >= 0);
        ASSERT(poll_cq_cycles >= 0);
        global_total_cycles.fetch_add(total_cycles, std::memory_order::relaxed);
        global_app_cycles.fetch_add(app_cycles, std::memory_order::relaxed);
        global_sched_cycles.fetch_add(sched_cycles, std::memory_order::relaxed);
        global_poll_cq_cycles.fetch_add(poll_cq_cycles,
                                        std::memory_order::relaxed);
    }

private:
    int64_t total_cycles = 0;
    int64_t app_cycles = 0;
    int64_t sched_cycles = 0;
    int64_t poll_cq_cycles = 0;

    static std::atomic_int64_t global_total_cycles;
    static std::atomic_int64_t global_app_cycles;
    static std::atomic_int64_t global_sched_cycles;
    static std::atomic_int64_t global_poll_cq_cycles;

    struct Warn {
        Warn() {
            WARN(
                "Schedule profiler enabled, "
                "App performance will be affected.");
        }
    };
    static Warn warn;
#else
public:
    inline void timer_begin() {}
    inline void timer_end() {}
    inline void app_begin() {}
    inline void app_end() {}
    inline void sched_begin() {}
    inline void sched_end() {}
    inline void poll_cq_begin() {}
    inline void poll_cq_end() {}
    static inline int64_t get_total_cycles() { return -1; }
    static inline int64_t get_app_cycles() { return -1; }
    static inline int64_t get_sched_cycles() { return -1; }
    static inline int64_t get_poll_cq_cycles() { return -1; }
    static inline void reset() {}
#endif
};

template <typename Stream>
concept ScopedContextStream =
    requires(Stream &stream, typename Stream::Context *ctx) {
        requires ScopedRunnableContext<typename Stream::Context>;
        { stream.get(ctx) } -> std::convertible_to<StreamState>;
    };

template <ScopedContextStream Stream, bool Ordered = false,
          bool UseTimer = false, size_t BatchSize = 32,
          size_t OrderedQueueCount = 64>
class StreamRunner {
private: /* the inner types */
    // Context: produced by Stream, executable & suspendable
    using Context = typename Stream::Context;

    // ContextBuffer: buffer for Context, used for allocator
    using ContextBuffer = std::array<char, sizeof(Context)>;

    // TaskNode: unit of management
    struct TaskNodeBase {
        ContextBuffer buffer;
        std::conditional_t<UseTimer, uint64_t, std::tuple<>> remote_access_ddl;

        Context *get_context() {
            return reinterpret_cast<Context *>(buffer.data());
        }
        bool run(cache::DereferenceScope &scope) {
            return get_context()->run(scope);
        }
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

    using OrderedTaskNode = utils::SingleLinkedNode<TaskNodeBase>;
    using TaskNode = utils::SingleLinkedNode<
        std::conditional_t<Ordered, OrderedTaskNode, TaskNodeBase>>;

    // Scope: the scope used by the runner, pins all contexts on GC
    struct Scope : public cache::DereferenceScope {
        StreamRunner *runner;

        Scope(cache::DereferenceScope *parent, StreamRunner *runner)
            : cache::DereferenceScope(parent), runner(runner) {}
        void pin() const override {
            runner->current_task->pin();
            runner->for_each_on_the_fly_task(
                [](TaskNode *task) { task->pin(); });
        }
        void unpin() const override {
            runner->current_task->unpin();
            runner->for_each_on_the_fly_task(
                [](TaskNode *task) { task->unpin(); });
        }
    };

    // TaskAllocator: pool allocator to allocate tasks
    struct TaskAllocator {
        union Node {
            TaskNode task;
            Node *next;
        };

        std::deque<Node> buffer_list;
        Node *free_list = nullptr;

        TaskNode *allocate() {
            if (free_list == nullptr) {
                return &(buffer_list.emplace_back().task);
            } else {
                Node *node = free_list;
                free_list = free_list->next;
                return &(node->task);
            }
        }
        void deallocate(TaskNode *task) {
            Node *node = reinterpret_cast<Node *>(task);
            node->next = free_list;
            free_list = node;
        }
    };

    struct TaskList {
    private:
        utils::SingleLinkedList<TaskNode> pending_list;

    public:
        using Node = TaskNode;
        size_t size = 0;

        void enqueue_pending(Node *node) {
            pending_list.push_back(node);
            size++;
        }
        void dequeue_pending() {
            pending_list.pop_front();
            size--;
        }
        Node *pending_front() {
            return static_cast<Node *>(pending_list.front());
        }
        const Node *pending_front() const {
            return static_cast<const Node *>(pending_list.front());
        }
        void enqueue(Node *node) { enqueue_pending(node); }
        bool empty() const { return pending_list.empty(); }

        template <std::invocable<Node *> Fn>
        void for_each(Fn &&fn) {
            for (auto *task = pending_list.front(); task != nullptr;
                 task = task->next) {
                fn(static_cast<Node *>(task));
            }
        }
    };

    using OrderedQueue = utils::SingleLinkedList<OrderedTaskNode>;

private: /* the fields */
    Stream stream;
    Scope scope;
    TaskAllocator task_allocator;
    TaskList task_list;
    std::conditional_t<Ordered, size_t, std::tuple<>> fenced_count;
    std::conditional_t<Ordered, OrderedQueue, std::tuple<>>
        ordered_queue[OrderedQueueCount];

    // current_task is a preallocated buffer
    // use this to avoid reduce allocation & deallocation
    TaskNode *current_task;

    StreamRunnerProfiler profiler;

public:
    StreamRunner(Stream &&stream, cache::DereferenceScope &parent_scope)
        : stream(std::move(stream)), scope(&parent_scope, this) {
        if constexpr (Ordered) {
            fenced_count = 0;
        }
        // allocate the first buffer for context
        current_task = task_allocator.allocate();
    }

    void run() {
        // the main loop
        profiler.timer_begin();
        while (true) {
            // get new task
            profiler.app_begin();
            StreamState state = stream.get(current_task->get_context());
            profiler.app_end();
            switch (state) {
            [[likely]] case StreamState::READY:
                handle_ready();
                break;
            case StreamState::WAITING:
                profiler.sched_begin();
                schedule();
                profiler.sched_end();
                break;
            case StreamState::FINISHED:
                profiler.sched_begin();
                while (!task_list.empty()) schedule();
                profiler.sched_end();
                profiler.timer_end();
                return;
            }
        }
    }

    bool resume() {
        profiler.timer_begin();
        bool waiting = false;
        // the main loop
        while (true) {
            // get new task
            profiler.app_begin();
            StreamState state = stream.get(current_task->get_context());
            profiler.app_end();
            switch (state) {
            [[likely]] case StreamState::READY:
                waiting = false;
                handle_ready();
                break;
            case StreamState::WAITING:
                if (waiting) {
                    return false;
                }
                profiler.sched_begin();
                schedule();
                profiler.sched_end();
                waiting = true;
                break;
            case StreamState::FINISHED:
                profiler.sched_begin();
                while (!task_list.empty()) {
                    schedule();
                }
                profiler.sched_end();
                if constexpr (Ordered) {
                    assert(fenced_count == 0);
                }
                profiler.timer_end();
                return true;
            }
        }
    }

    bool task_list_empty() const { return task_list.empty(); }

    uint64_t min_access_ddl() const {
        static_assert(UseTimer);
        if (task_list.empty())
            return 0;
        else
            return task_list.pending_front()->remote_access_ddl;
    }

private:
    static uint64_t get_time_ns() {
        std::chrono::nanoseconds ns =
            std::chrono::high_resolution_clock::now().time_since_epoch();
        return ns.count();
    }

    bool list_full() {
        if constexpr (Ordered) {
            return task_list.size + fenced_count >= BatchSize;
        } else {
            return task_list.size >= BatchSize;
        }
    }

    void handle_ready() {
        OrderedQueue *oq;
        if constexpr (Ordered) {
            size_t conflict_id = current_task->get_context()->conflict_id();
            oq = &ordered_queue[conflict_id % OrderedQueueCount];
            bool first_in_order = oq->empty();
            if (!first_in_order) [[unlikely]] {
                // will be scheduled when it is the frontmost
                oq->push_back(static_cast<OrderedTaskNode *>(current_task));
                fenced_count++;
                current_task = task_allocator.allocate();
                if (list_full()) {
                    profiler.sched_begin();
                    schedule();
                    profiler.sched_end();
                }
                return;
            }
        }
        profiler.app_begin();
        bool finished = current_task->run(scope);
        profiler.app_end();
        if (!finished) [[unlikely]] {
            profiler.sched_begin();
            if constexpr (UseTimer) {
                current_task->remote_access_ddl = get_time_ns() + 3000;
            }
            task_list.enqueue(current_task);
            if constexpr (Ordered) {
                assert(oq->empty());
                oq->push_back(static_cast<OrderedTaskNode *>(current_task));
                fenced_count++;
            }
            if (list_full()) schedule();
            current_task = task_allocator.allocate();
            profiler.sched_end();
        } else {
            current_task->deconstruct();
        }
    }

    // select and run unfinished tasks from the task list
    void schedule() {
        TaskNode *prev_task = current_task;
        profiler.poll_cq_begin();
        cache::check_cq();
        profiler.poll_cq_end();
        while (!task_list.empty()) [[likely]] {
            TaskNode *task = task_list.pending_front();
            current_task = task;  // to pin
            // if the first task is not ready
            // in most cases other tasks are not ready, too
            if (!task->fetched()) break;
            // pop the first task from the pending list and run it
            task_list.dequeue_pending();
            profiler.sched_end();
            profiler.app_begin();
            bool finished = task->run(scope);
            profiler.app_end();
            profiler.sched_begin();
            if (finished) [[likely]] {
                OrderedQueue *oq;
                if constexpr (Ordered) {
                    size_t conflict_id = task->get_context()->conflict_id();
                    oq = &ordered_queue[conflict_id % OrderedQueueCount];
                    assert(oq->front() == static_cast<OrderedTaskNode *>(task));
                }
                task->deconstruct();
                task_allocator.deallocate(task);
                if constexpr (Ordered) {
                    oq->pop_front();
                    while (!oq->empty()) {
                        task = static_cast<TaskNode *>(oq->front());
                        profiler.sched_end();
                        profiler.app_begin();
                        bool finished = task->run(scope);
                        profiler.app_end();
                        profiler.sched_begin();
                        if (finished) [[likely]] {
                            task->deconstruct();
                            oq->pop_front();
                        } else {
                            if constexpr (UseTimer) {
                                task->remote_access_ddl = get_time_ns() + 3000;
                            }
                            task_list.enqueue(task);
                            break;
                        }
                    }
                }
            } else {
                if constexpr (UseTimer) {
                    task->remote_access_ddl = get_time_ns() + 3000;
                }
                task_list.enqueue_pending(task);
            }
        }
        current_task = prev_task;
    }

    template <std::invocable<TaskNode *> Fn>
    void for_each_on_the_fly_task(Fn &&fn) {
        task_list.for_each(std::forward<Fn>(fn));
    }
};

}  // namespace async

template <async::ScopedContextStream Stream, bool Ordered = false,
          bool UseTimer = false, size_t BatchSize = 32>
inline void process_pararoutine_stream(Stream &&stream) {
    RootDereferenceScope scope;
    async::StreamRunner<Stream, Ordered, UseTimer, BatchSize>(std::move(stream),
                                                              scope)
        .run();
}

template <async::ScopedContextStream Stream, bool Ordered = false,
          bool UseTimer = false, size_t BatchSize = 32>
inline void process_pararoutine_stream(Stream &&stream,
                                       cache::DereferenceScope &scope) {
    async::StreamRunner<Stream, Ordered, UseTimer, BatchSize>(std::move(stream),
                                                              scope)
        .run();
}

}  // namespace Beehive
