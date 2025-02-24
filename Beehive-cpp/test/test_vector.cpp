#include <chrono>
#include <thread>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/vector.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

Configure config;

struct DummyObject {
    int value;
    char dummy[1024 - sizeof(int)];

    DummyObject(int value) : value(value) {}
};

void test_vector() {
    Vector<DummyObject> vec;
    // allocation
    for (size_t i = 0; i < 1024 * 1024; i++) {
        vec.emplace_back(i);
    }
    ASSERT(vec.size() == 1024 * 1024);
    // write
    for (size_t i = 0; i < vec.size(); i++) {
        auto accessor = vec.get_mut(i);
        accessor->value *= 2;
    }
    // read
    int sum = 0;
    for (size_t i = 0; i < vec.size(); i++) {
        auto accessor = vec.get(i);
        sum += accessor->value;
    }
    int expected_sum = 0;
    for (size_t i = 0; i < 1024 * 1024; i++) {
        expected_sum += i * 2;
    }
    ASSERT(sum == expected_sum);
}

void test_dense_vector() {
    constexpr size_t GroupSize = 1024 / sizeof(int);
    constexpr size_t VecSize = 2 * GroupSize - 3;
    DenseVector<int, GroupSize> vec;
    // allocation
    for (size_t i = 0; i < VecSize; i++) {
        vec.emplace_back(i);
    }
    ASSERT(vec.size() == VecSize);
    // write
    for (size_t i = 0; i < vec.size(); i++) {
        auto accessor = vec.get_mut(i);
        *accessor *= 2;
    }
    // read
    int sum = 0;
    for (size_t i = 0; i < vec.size(); i++) {
        auto accessor = vec.get(i);
        sum += *accessor;
    }
    int expected_sum = 0;
    for (size_t i = 0; i < VecSize; i++) {
        expected_sum += i * 2;
    }
    ASSERT(sum == expected_sum);
}

int main() {
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024L * 1024 * 1024 * 2;
    config.client_buffer_size = 64 * 1024 * 1024;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    Beehive::runtime_init(config);
    test_vector();
    test_dense_vector();
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}
