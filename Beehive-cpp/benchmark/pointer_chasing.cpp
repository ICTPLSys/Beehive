// use async for loop to alleviate remote access delays caused by pointer
// chasing

#include <chrono>
#include <cstddef>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "async/loop.hpp"
#include "async/scoped_inline_task.hpp"
#include "cache/cache.hpp"
#include "data_structure/vector.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

static constexpr size_t REPEAT = 4;
#define LOCAL
// #define BASELINE
// #define ASYNC
#define UTHREAD
// #define PREFETCH
// #define SCOPE
// #define ASCOPE

void run(size_t size, size_t depth, size_t n) {
    std::vector<uint64_t> local_vec;
    DenseVector<uint64_t, 64> vec;
    std::default_random_engine re;
    std::uniform_int_distribution<uint64_t> dist(0, size - 1);
    std::vector<uint64_t> initial_idxs;
    // initialization
    for (size_t i = 0; i < size; i++) {
        uint64_t v = dist(re);
        local_vec.push_back(v);
        vec.emplace_back(v);
    }
    ASSERT(vec.size() == size);
    initial_idxs.resize(n);
    auto reload = [&] {
        for (size_t i = 0; i < n; i++) {
            initial_idxs[i] = dist(re);
        }
    };
    for (size_t i = 0; i < 4; i++) {
        int sum_std = 0;
        int sum_baseline = 0;
        int sum_scope = 0;
        int sum_async_scope = 0;
        int sum_prefetch = 0;
        int sum_async = 0;
        int sum_async_sync = 0;
#ifdef LOCAL
        // local
        {
            reload();
            uint64_t start = get_cycles();
            for (size_t i = 0; i < n; i++) {
                size_t idx = initial_idxs[i];
                for (size_t d = 0; d < depth; d++) {
                    idx = local_vec[idx];
                }
                sum_std += idx;
            }
            uint64_t end = get_cycles();
            std::cout << "local   : cycles: " << end - start << std::endl;
        }
#endif
#ifdef BASELINE
        // baseline
        {
            for (size_t cnt = 0; cnt < REPEAT; cnt++) {
                reload();
                sum_baseline = 0;
                uint64_t start = get_cycles();
                RootDereferenceScope scope;
                for (size_t i = 0; i < n; i++) {
                    ON_MISS_BEGIN
                    ON_MISS_END
                    size_t idx = initial_idxs[i];
                    for (size_t d = 0; d < depth; d++) {
                        idx = *vec.get_lite(idx, __on_miss__, scope);
                    }
                    sum_baseline += idx;
                }
                uint64_t end = get_cycles();
                std::cout << "baseline: cycles: " << end - start << std::endl;
                ASSERT(sum_baseline == sum_std);
            }
        }
#endif
#ifdef ASYNC
        // async
        for (size_t cnt = 0; cnt < REPEAT; cnt++) {
            reload();
            {
                sum_async = 0;
                uint64_t start = get_cycles();
                ASYNC_FOR(size_t, i, 0, i < n, i++)
                    size_t idx = initial_idxs[i];
                    for (size_t d = 0; d < depth; d++) {
                        idx = *vec.get(idx, __on_miss__);
                    }
                    sum_async += idx;
                ASYNC_FOR_END
                uint64_t end = get_cycles();
                std::cout << "async   : cycles: " << end - start << std::endl;
                ASSERT(sum_async == sum_std);
            }
        }
#endif
#ifdef UTHREAD
        // uthread
        for (size_t cnt = 0; cnt < REPEAT; cnt++) {
            reload();
            {
                int sum_uth = 0;
                uint64_t start = get_cycles();
                uthread::parallel_for_with_scope<1>(
                    8, n, [&](size_t i, DereferenceScope &scope) {
                        size_t idx = initial_idxs[i];
                        ON_MISS_BEGIN
                            uthread::yield();
                        ON_MISS_END
                        for (size_t d = 0; d < depth; d++) {
                            idx = *vec.get_lite(idx, __on_miss__, scope);
                        }
                        sum_uth += idx;
                    });
                uint64_t end = get_cycles();
                std::cout << "uthread cycles: " << end - start << std::endl;
                std::cout << (sum_uth == sum_std) << std::endl;
            }
        }
#endif
#if 0
        // async + sync
        {
                reload();
            uint64_t start = get_cycles();
            ASYNC_FOR(size_t, i, 0, i < n, i++)
                size_t idx = initial_idxs[i];
                for (size_t d = 0; d < depth; d++) {
                    idx = *vec.get(idx, __on_miss__);
                }
                __sync__();
                sum_async_sync += idx;
            ASYNC_FOR_END
            uint64_t end = get_cycles();
            std::cout << "async2  : cycles: " << end - start << std::endl;
            ASSERT(sum_async_sync == sum_std);
        }
#endif
#ifdef PREFETCH
        // only prefetch
        {
            RootDereferenceScope scope;
            for (size_t cnt = 0; cnt < REPEAT; cnt++) {
                reload();
                uint64_t start = get_cycles();
                sum_prefetch = 0;
                size_t miss_cnt = 0;
                size_t acc_cnt = 0;
                for (size_t i = 0; i < n; i++) {
                    ON_MISS_BEGIN
                        miss_cnt++;
                        cache::OnMissScope oms(__entry__, &scope);
                        // ERROR("trigger miss in full memory");
                        for (int j = i + 1; j <= n; j++) {
                            vec.prefetch(initial_idxs[j], oms);
                            if (cache::check_fetch(__entry__, __ddl__)) return;
                        }
                    ON_MISS_END
                    size_t idx = initial_idxs[i];
                    for (size_t d = 0; d < depth; d++) {
                        acc_cnt++;
                        idx = *vec.get_lite(idx, __on_miss__, scope);
                        // std::cout << idx << std::endl;
                    }
                    // std::cout << std::endl;
                    sum_prefetch += idx;
                }
                uint64_t end = get_cycles();
                std::cout << "prefetch: cycles: " << end - start << std::endl;
                std::cout << miss_cnt << " / " << acc_cnt << std::endl;
                std::cout << (sum_std == sum_prefetch) << std::endl;
            }
        }
#endif
#ifdef SCOPE
        // scope
        {
            struct Scope : public RootDereferenceScope {
                // this accessor will be held during the scope
                LiteAccessor<uint64_t> v0;
                void pin() const override { v0.pin(); }
                void unpin() const override { v0.unpin(); }
            };

            Scope scope;
            for (size_t cnt = 0; cnt < REPEAT; cnt++) {
                reload();
                uint64_t start = get_cycles();
                sum_scope = 0;
                scope.v0 = vec.get_lite(0, scope);
                uint64_t v0_value = *scope.v0;
                for (size_t i = 0; i < n; i++) {
                    size_t idx = initial_idxs[i];
                    for (size_t d = 0; d < depth; d++) {
                        idx = *vec.get_lite(idx, scope);
                    }
                    sum_scope += idx;
                }
                uint64_t end = get_cycles();
                ASSERT(*scope.v0 == v0_value);
                std::cout << "scope   : cycles: " << end - start << std::endl;
                ASSERT(sum_scope == sum_std);
            }
        }
#endif
#ifdef ASCOPE
        // scope + async
        RootDereferenceScope scope;
        for (size_t cnt = 0; cnt < REPEAT; cnt++) {
            reload();
            uint64_t start = get_cycles();
            sum_async_scope = 0;
            {
                struct Context {
                    DenseVector<uint64_t, 64> *vec;
                    int *sum;
                    size_t idx;
                    size_t depth;
                    LiteAccessor<uint64_t> accessor;

                    bool fetched() { return cache::at_local(accessor); }
                    void pin() { accessor.pin(); }
                    void unpin() { accessor.unpin(); }

                    bool run(DereferenceScope &scope) {
                        while (depth > 0) {
                            if (!vec->async_get_lite(idx, accessor, scope))
                                return false;
                            idx = *accessor;
                            accessor = {};
                            depth--;
                        }
                        *sum += idx;
                        return true;
                    }
                };
                SCOPED_INLINE_ASYNC_FOR(Context, size_t, i, 0, i < n, i++,
                                        scope)
                    return Context{.vec = &vec,
                                   .sum = &sum_async_scope,
                                   .idx = initial_idxs[i],
                                   .depth = depth,
                                   .accessor = {}};
                SCOPED_INLINE_ASYNC_FOR_END
                uint64_t end = get_cycles();
                std::cout << "as-scope: cycles: " << end - start << std::endl;
                std::cout << (sum_async_scope == sum_std) << std::endl;
            }
        }
#endif
        std::cout << std::endl;
    }
}

int main(int argc, const char *const argv[]) {
    if (argc != 5) {
        std::cout << "usage: " << argv[0]
                  << " <configure file> <memory size> <chase depth> <count>"
                  << std::endl;
        return -1;
    }
    Beehive::rdma::Configure config;
    config.from_file(argv[1]);
    Beehive::runtime_init(config);
    int mem_size = std::atoi(argv[2]);
    int depth = std::atoi(argv[3]);
    int count = std::atoi(argv[4]);
    ASSERT(mem_size > 0);
    ASSERT(depth > 0);
    ASSERT(count > 0);
    run(mem_size / sizeof(uint64_t), depth, count);
    Beehive::runtime_destroy();
    return 0;
}
