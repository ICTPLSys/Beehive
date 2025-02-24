#include <chrono>
#include <thread>
#include <vector>

#include "cache/cache.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace Beehive;
using namespace Beehive::cache;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

Configure config;

void test() {
    auto &local_cache = *Cache::get_default();
    std::vector<far_obj_t> objs;
    // allocation
    for (size_t i = 0; i < 1024 * 128; i++) {
        auto [obj, ptr] = local_cache.allocate(1024, true);
        *static_cast<size_t *>(ptr) = i;
        local_cache.release_cache(obj, true);
        objs.push_back(obj);
    }
    // sync_fetch & write back
    for (size_t i = 0; i < objs.size(); i++) {
        auto obj = objs[i];
        void *ptr = local_cache.sync_fetch(obj);
        size_t v = *static_cast<size_t *>(ptr);
        ASSERT(v == i);
        *static_cast<size_t *>(ptr) = i * i;
        local_cache.release_cache(obj, true);
    }
    for (size_t i = 0; i < objs.size(); i++) {
        auto obj = objs[i];
        void *ptr = local_cache.sync_fetch(obj);
        size_t v = *static_cast<size_t *>(ptr);
        ASSERT(v == i * i);
        local_cache.release_cache(obj, false);
    }
    // accessor
    size_t sum = 0;
    for (size_t i = 0; i < objs.size(); i++) {
        auto obj = objs[i];
        cache::Accessor<size_t> accessor(obj);
        sum += *accessor;
    }
    size_t expected_sum = 0;
    for (size_t i = 0; i < 1024 * 128; i++) {
        expected_sum += i * i;
    }
    ASSERT(sum == expected_sum);
    // cache miss handler
    {
        size_t sum = 0;
        size_t on_miss = 0;
        ON_MISS_BEGIN
            on_miss++;
        ON_MISS_END
        for (size_t i = 0; i < objs.size(); i++) {
            auto obj = objs[i];
            auto accessor = cache::Accessor<size_t>(obj, __on_miss__);
            sum += *accessor;
        }
        size_t expected_sum = 0;
        for (size_t i = 0; i < 1024 * 128; i++) {
            expected_sum += i * i;
        }
        ASSERT(sum == expected_sum);
        ASSERT(on_miss >= 0 && on_miss <= objs.size());
    }
}

int main() {
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024 * 1024 * 1024;
    config.client_buffer_size =
        64 * 1024 * 1024;  // Allocator needs enough memory
    config.evict_batch_size = 4 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    Beehive::runtime_init(config);
    test();
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}