#include <algorithm>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/bplus_tree_map.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

Configure config;

void test() {
    size_t count = 1024 * 1024 * 2;
    std::vector<int> keys;
    std::minstd_rand0 re(0);
    std::uniform_int_distribution<int> dist(0, count - 1);
    for (int i = 0; i < count; i++) {
        keys.push_back(i);
    }
    for (int i = 0; i < count; i++) {
        keys.push_back(dist(re));
    }
    std::shuffle(keys.begin(), keys.end(), re);
    BPlusTreeMap<int, int, 64> map;
    for (int i : keys) {
        auto [_, accessor] = map.get_or_insert(i);
        *accessor = i;
    }
    ASSERT(map.size() == count);
    map.iterate([](int k, int &v) { v = v * v; });
    int sum = 0;
    size_t __i = 0;
    map.const_iterate([&sum, &__i](int k, int v) {
        ASSERT(k == __i);
        ASSERT(k * k == v);
        __i++;
        sum += v;
    });
    ASSERT(__i == count);
    int expected_sum = 0;
    for (int i = 0; i < count; i++) {
        expected_sum += i * i;
    }
    ASSERT(sum == expected_sum);
}

int main() {
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024 * 1024 * 1024;
    config.client_buffer_size = 32 * 1024 * 1024;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    Beehive::runtime_init(config);
    test();
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}
