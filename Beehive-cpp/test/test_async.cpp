#include <chrono>
#include <cstddef>
#include <iostream>
#include <thread>
#include <vector>

#include "async/loop.hpp"
#include "cache/cache.hpp"
#include "data_structure/vector.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

struct DummyObject {
    int value;
    char dummy[1024 - sizeof(int)];

    DummyObject(int value) : value(value) {}
};

static_assert(sizeof(DummyObject) == 1024);

void test_for() {
    constexpr size_t VecSize = 1024 * 256;

    int sum_expect = 0;
    for (size_t i = 0; i < VecSize; i++) {
        sum_expect += i * i - i;
    }

    Vector<DummyObject> vec;
    // allocation
    for (size_t i = 0; i < VecSize; i++) {
        vec.emplace_back(i);
    }
    ASSERT(vec.size() == VecSize);
    // baseline
    {
        uint64_t start = get_cycles();
        int sum = 0;
        for (size_t i = 0; i < vec.size(); i++) {
            auto accessor = vec.get(i);
            int x = accessor->value;
            sum += x * x - x;
        }
        uint64_t end = get_cycles();
        ASSERT(sum == sum_expect);
        std::cout << "cycles: " << end - start << std::endl;
    }
    // read only, nothing
    {
        uint64_t start = get_cycles();
        int sum = 0;
        ASYNC_FOR(size_t, i, 0, i < vec.size(), i++)
            auto accessor = vec.get(i);
            int x = accessor->value;
            sum += x * x - x;
        ASYNC_FOR_END
        uint64_t end = get_cycles();
        ASSERT(sum == sum_expect);
        std::cout << "cycles: " << end - start << std::endl;
    }
    // read only, yield
    {
        uint64_t start = get_cycles();
        int sum = 0;
        ASYNC_FOR(size_t, i, 0, i < vec.size(), i++)
            auto accessor = vec.get(i, __on_miss__);
            int x = accessor->value;
            sum += x * x - x;
        ASYNC_FOR_END
        uint64_t end = get_cycles();
        ASSERT(sum == sum_expect);
        std::cout << "cycles: " << end - start << std::endl;
    }
    // read only, sync
    {
        uint64_t start = get_cycles();
        int sum = 0;
        ASYNC_FOR(size_t, i, 0, i < vec.size(), i++)
            auto accessor = vec.get(i, __on_miss__);
            int x = accessor->value;
            __sync__();
            sum += x * x - x;
        ASYNC_FOR_END
        uint64_t end = get_cycles();
        ASSERT(sum == sum_expect);
        std::cout << "cycles: " << end - start << std::endl;
    }
}

int main() {
    Configure config;
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024L * 1024 * 1024 * 2;
    config.client_buffer_size = 64 * 1024 * 1024;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    Beehive::runtime_init(config);
    test_for();
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}
