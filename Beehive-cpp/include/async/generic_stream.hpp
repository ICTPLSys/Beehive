#pragma once
#include <array>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <deque>
#include <type_traits>

#include "async/context.hpp"
#include "cache/cache.hpp"
#include "utils/debug.hpp"
#include "utils/linked_list.hpp"

namespace Beehive {
namespace async {

template <size_t ContextSize = 256, bool Ordered = false, bool UseTimer = false,
          size_t BatchSize = 32, size_t OrderedQueueCount = 64>
class GenericStreamRunner {
private:
    using GenericContextType =
        std::conditional_t<Ordered, GenericOrderedContext, GenericContext>;
    /* definition */
    using ContextBuffer = std::array<char, ContextSize>;
    static_assert(ContextSize >= sizeof(GenericContextType));
    struct TaskNodeBase {
        ContextBuffer buffer;
        std::conditional_t<UseTimer, uint64_t, std::tuple<>> remote_access_ddl;

        GenericContextType *get_context() {
            return reinterpret_cast<GenericContextType *>(buffer.data());
        }

        const GenericContextType *get_context() const {
            return reinterpret_cast<const GenericContextType *>(buffer.data());
        }

        bool run(DereferenceScope &scope) { return get_context()->run(scope); }
        bool fetched() const { return get_context()->fetched(); }
        void pin() const { get_context()->pin(); }
        void unpin() const { get_context()->unpin(); }
        void destruct() { get_context()->destruct(); }
        size_t conflict_id() const {
            if constexpr (Ordered) {
                return reinterpret_cast<const GenericOrderedContext *>(
                           get_context())
                    ->conflict_id();
            } else {
                // will not be used when non-ordered
                return 0;
            }
        }
    };

    using OrderedTaskNode = utils::SingleLinkedNode<TaskNodeBase>;
    using TaskNode = utils::SingleLinkedNode<
        std::conditional_t<Ordered, OrderedTaskNode, TaskNodeBase>>;

    // Scope: the scope used by the runner, pins all contexts on GC
    struct Scope : public DereferenceScope {
        GenericStreamRunner *runner;

        Scope(const DereferenceScope *parent, GenericStreamRunner *runner)
            : DereferenceScope(parent), runner(runner) {}
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

private:
    /* field */
    Scope scope;
    TaskAllocator task_allocator;
    // all tasks on the fly
    TaskList task_list;
    std::conditional_t<Ordered, size_t, std::tuple<>> fenced_count;
    // all tasks are NOT on the fly, if ordered. waiting for schedule
    std::conditional_t<Ordered, OrderedQueue, std::tuple<>>
        ordered_queue[OrderedQueueCount];
    TaskNode *current_task;

    /* private help function */
    void inc_await_count(TaskNode *task) {
        int32_t *await_cnt = task->get_context()->await_cnt;
        if (await_cnt) {
            assert(*await_cnt >= 0);
            ++*await_cnt;
        }
    }

    void dec_await_count(TaskNode *task) {
        int32_t *await_cnt = task->get_context()->await_cnt;
        if (await_cnt) {
            assert(*await_cnt > 0);
            --*await_cnt;
        }
    }

    bool run_new_task(TaskNode *task) {
        // assert: ctx has set and runnable
        OrderedQueue *oq = nullptr;
        current_task = task;
        if constexpr (Ordered) {
            size_t conflict_id = current_task->get_context()->conflict_id();
            oq = &ordered_queue[conflict_id % OrderedQueueCount];
            bool first_in_order = oq->empty();
            // check if this task can run directly
            if (!first_in_order) {
                // if not, push into order queue and return false directly
                // this task need to wait for above ordered task(s)
                oq->push_back(static_cast<OrderedTaskNode *>(current_task));
                fenced_count++;
                // dont inc await count for they are blocked
                // and will be invoked in the future
                return false;
            }
        }
        // if first in order/ignore order, just run it
        if (current_task->run(scope)) {
            // done without suspend
            current_task->destruct();
            task_allocator.deallocate(current_task);
            return true;
        }
        if constexpr (UseTimer) {
            current_task->remote_access_ddl = get_time_ns() + 3000;
        }
        // else, push into task list
        task_list.enqueue(current_task);
        if constexpr (Ordered) {
            // if new task can run, it means it is at the head of oq,
            // so the oq is empty now, and this task need to be insert back
            assert(oq->empty());
            oq->push_back(static_cast<OrderedTaskNode *>(current_task));
            fenced_count++;
        }
        // and inc await count for suspend
        inc_await_count(current_task);
        return false;
    }

    void schedule() {
        cache::check_cq();
        while (!task_list.empty()) [[likely]] {
            current_task = task_list.pending_front();
            if (!current_task->fetched()) {
                break;
            }
            // dec await count for restore
            dec_await_count(current_task);
            task_list.dequeue_pending();
            if (current_task->run(scope)) [[likely]] {
                // destruct task manually
                OrderedQueue *oq = nullptr;
                if constexpr (Ordered) {
                    // modify nothing, just check correctness
                    size_t conflict_id =
                        current_task->get_context()->conflict_id();
                    oq = &ordered_queue[conflict_id % OrderedQueueCount];
                    assert(oq->front() ==
                           static_cast<OrderedTaskNode *>(current_task));
                }
                current_task->destruct();
                task_allocator.deallocate(current_task);
                if constexpr (Ordered) {
                    // remove first task, == current task
                    oq->pop_front();
                    fenced_count--;
                    while (!oq->empty()) {
                        // continue to exec tasks left in oq with best-effort
                        // they are blocked when invoke run_new_task
                        current_task = static_cast<TaskNode *>(oq->front());
                        if (current_task->run(scope)) {
                            current_task->destruct();
                            oq->pop_front();
                            fenced_count--;
                            task_allocator.deallocate(current_task);
                        } else {
                            if constexpr (UseTimer) {
                                current_task->remote_access_ddl =
                                    get_time_ns() + 3000;
                            }
                            task_list.enqueue(current_task);
                            // inc await count for suspend
                            inc_await_count(current_task);
                            break;
                        }
                    }
                }
            } else {
                if constexpr (UseTimer) {
                    current_task->remote_access_ddl = get_time_ns() + 3000;
                }
                task_list.enqueue(current_task);
                // inc await for suspending
                inc_await_count(current_task);
            }
        }
    }

    bool list_full() const {
        if constexpr (Ordered) {
            return task_list.size + fenced_count >= BatchSize;
        } else {
            return task_list.size >= BatchSize;
        }
    }

    template <std::invocable<TaskNode *> Fn>
    void for_each_on_the_fly_task(Fn &&fn) {
        task_list.for_each(std::forward<Fn>(fn));
    }

public:
    GenericStreamRunner(const DereferenceScope &scope)
        : scope(&scope, this), fenced_count() {}
    template <typename Fn>
        requires requires(Fn fn, GenericContextType *ctx) {
            { fn(ctx) };
        }
    bool async_run(Fn &&frame_constructor, int32_t *await_cnt = nullptr) {
        if (list_full()) {
            schedule();
        }
        TaskNode *task = task_allocator.allocate();
        auto ctx = reinterpret_cast<GenericContextType *>(task->get_context());
        frame_constructor(ctx);
        ctx->await_cnt = await_cnt;
        return run_new_task(task);
    }

    void await(int32_t *await_cnt) {
        while (*await_cnt > 0) {
            schedule();
        }
    }

    void await() {
        while (!task_list.empty()) {
            schedule();
        }
    }

    ~GenericStreamRunner() { await(); }
};

}  // namespace async

}  // namespace Beehive