#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "async/scoped_inline_task.hpp"
#include "cache/accessor.hpp"
#include "cache/cache.hpp"
#include "data_structure/vector.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

constexpr size_t GroupSize = 1024 / sizeof(int);
constexpr size_t VecSize = 1024 * GroupSize - 3;
constexpr size_t TestCount = VecSize;

struct Ctx {
    DenseVector<int, GroupSize> *vec;
    size_t idx;
    size_t i;
    LiteAccessor<int, true> acc;
    int label = 0;

    bool fetched() const { return cache::at_local(acc); }
    void pin() { acc.pin(); }
    void unpin() { acc.unpin(); }
    size_t conflict_id() const { return idx; }
    bool run(DereferenceScope &scope) {
        // trace.push_back({i, label});
        if (label == 1) goto L1;
        if (!vec->async_get_lite(idx, acc, scope)) {
            label = 1;
            return false;
        }
    L1:
        *acc = i;
        return true;
    }
};

void test_fence() {
    std::vector<int> local_vec;
    DenseVector<int, GroupSize> vec;
    {
        RootDereferenceScope scope;
        for (size_t i = 0; i < VecSize; i++) {
            local_vec.push_back(i);
            vec.emplace_back(scope, i);
        }
    }
    ASSERT(vec.size() == VecSize);

    std::vector<size_t> index_vec;
    std::default_random_engine re(0);
    std::uniform_int_distribution<size_t> idx_dist(0, VecSize - 1);
    for (size_t i = 0; i < TestCount; i += 8) {
        size_t x = idx_dist(re);
        size_t y = idx_dist(re);
        size_t z = idx_dist(re);
        index_vec.push_back(x);
        index_vec.push_back(y);
        index_vec.push_back(x);
        index_vec.push_back(z);
        index_vec.push_back(y);
        index_vec.push_back(y);
        index_vec.push_back(z);
        index_vec.push_back(x);
    }

    // test
    for (size_t i = 0; i < TestCount; i++) {
        size_t idx = index_vec[i];
        local_vec[idx] = i;
    }
    {
        RootDereferenceScope scope;
        auto start = std::chrono::high_resolution_clock::now();
        async::for_range<true>(scope, 0, TestCount, [&](size_t i) {
            return Ctx{.vec = &vec, .idx = index_vec[i], .i = i};
        });
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << std::setw(24) << "run time: " << (end - start).count()
                  << std::endl;
    }

    // verify
    {
        RootDereferenceScope scope;
        for (size_t i = 0; i < VecSize; i++) {
            auto acc = vec.get_lite<false>(i, scope);
            ASSERT(*acc == local_vec[i]);
        }
    }

    {
        RootDereferenceScope scope;
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < TestCount; i++) {
            *(vec.get_lite<true>(index_vec[i], scope)) = i;
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << std::setw(24) << "seq run time: " << (end - start).count()
                  << std::endl;
    }

    {
        RootDereferenceScope scope;
        auto start = std::chrono::high_resolution_clock::now();
        async::for_range<false>(scope, 0, TestCount, [&](size_t i) {
            return Ctx{.vec = &vec, .idx = index_vec[i], .i = i};
        });
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << std::setw(24)
                  << "unordered run time: " << (end - start).count()
                  << std::endl;
    }
}

int main() {
    Configure config;
    config.server_addr = "127.0.0.1";
    config.server_port = "1234";
    config.server_buffer_size = 1024L * 1024 * 2;
    config.client_buffer_size = 512 * 1024;
    config.max_thread_cnt = 1;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    runtime_init(config);
    test_fence();
    runtime_destroy();
    server_thread.join();
    return 0;
}
