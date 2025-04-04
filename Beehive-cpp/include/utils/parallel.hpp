#pragma once
#include <atomic>

#include "cache/cache.hpp"
#include "utils/fork_join.hpp"

namespace FarLib {
namespace uthread {
template <typename Fn>
inline void spawn(Fn &&fn) {
    std::function<void(size_t)> task_fn = [fn](size_t thread_id) { fn(); };
    fork_join(1, task_fn);
}

template <size_t Granularity, bool UseWorkList = false, typename Fn>
void parallel_for(size_t max_thread_count, size_t n, Fn &&fn) {
    if (max_thread_count == 1 || n <= Granularity) {
        for (size_t i = 0; i < n; i++) {
            fn(i);
        }
        return;
    }
    size_t work_count = UseWorkList
                            ? (n / Granularity)
                            : std::min(max_thread_count, n / Granularity);
    size_t blocks = (n + Granularity - 1) / Granularity;
    size_t stride = (blocks + work_count - 1) / work_count * Granularity;
    std::function<void(size_t)> run = [stride, n, &fn](size_t thread_id) {
        size_t start = thread_id * stride;
        size_t end = std::min(start + stride, n);
        for (size_t i = start; i < end; i++) {
            fn(i);
        }
    };
    if constexpr (UseWorkList) {
        fork_join_work_list(work_count, run);
    } else {
        fork_join(work_count, run);
    }
}

template <size_t Granularity, bool UseWorkList = false, typename Fn>
void parallel_for_with_scope(size_t max_thread_count, size_t n, Fn &&fn) {
    if (max_thread_count == 1 || n <= Granularity) {
        RootDereferenceScope scope;
        for (size_t i = 0; i < n; i++) {
            fn(i, scope);
        }
        return;
    }
    if constexpr (UseWorkList) {
        std::atomic_size_t next_i = 0;
        std::function<void(size_t)> run = [n, &next_i, &fn](size_t) {
            RootDereferenceScope scope;
            while (true) {
                size_t i = next_i.fetch_add(Granularity);
                if (i >= n) break;
                for (size_t j = i; j < std::min(i + Granularity, n); j++) {
                    fn(j, scope);
                }
            }
        };
        fork_join(max_thread_count, run);
    } else {
        size_t blocks = (n + Granularity - 1) / Granularity;
        size_t thread_count = std::min(max_thread_count, n / Granularity);
        size_t stride =
            (blocks + thread_count - 1) / thread_count * Granularity;
        std::function<void(size_t)> run = [stride, n, &fn](size_t thread_id) {
            RootDereferenceScope scope;
            size_t start = thread_id * stride;
            size_t end = std::min(start + stride, n);
            for (size_t i = start; i < end; i++) {
                fn(i, scope);
            }
        };
        fork_join(thread_count, run);
    }
}

// parallel for, with a thread local var
template <size_t Granularity, bool UseWorkList = false, typename Fn,
          typename VarInit>
void parallel_for_with_scope(size_t max_thread_count, size_t n, Fn &&fn,
                             VarInit &&init) {
    if (max_thread_count == 1 || n <= Granularity) {
        auto local_var = init(0);
        RootDereferenceScope scope;
        for (size_t i = 0; i < n; i++) {
            fn(i, local_var, scope);
        }
        return;
    }
    if constexpr (UseWorkList) {
        std::atomic_size_t next_i = 0;
        std::function<void(size_t)> run = [n, &next_i, &fn,
                                           &init](size_t thread_id) {
            auto local_var = init(thread_id);
            RootDereferenceScope scope;
            while (true) {
                size_t i = next_i.fetch_add(Granularity);
                if (i >= n) break;
                for (size_t j = i; j < std::min(i + Granularity, n); j++) {
                    fn(j, local_var, scope);
                }
            }
        };
        fork_join(max_thread_count, run);
    } else {
        size_t thread_count = std::min(max_thread_count, n / Granularity);
        size_t blocks = (n + Granularity - 1) / Granularity;
        size_t stride =
            (blocks + thread_count - 1) / thread_count * Granularity;
        std::function<void(size_t)> run = [stride, n, &fn,
                                           &init](size_t thread_id) {
            auto local_var = init(thread_id);
            RootDereferenceScope scope;
            size_t start = thread_id * stride;
            size_t end = std::min(start + stride, n);
            for (size_t i = start; i < end; i++) {
                fn(i, local_var, scope);
            }
        };
        fork_join(thread_count, run);
    }
}

}  // namespace uthread

}  // namespace FarLib
