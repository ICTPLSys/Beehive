#include <cstdlib>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/hopscotch.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

Configure config;

void test_hopscotch() {
    constexpr size_t ObjectCount = 1024 * 1024;
    constexpr size_t TestCount = 1024;

    struct HopscotchObject {
        int key;
        int value;

        bool equals(int cmp) const { return cmp == key; }
    };

    Hopscotch<int, HopscotchObject> hashmap(22);
    std::unordered_map<int, int> std_hashmap;
    std::default_random_engine re(0);
    std::uniform_int_distribution<int> dist(0, ObjectCount * 16);

    ON_MISS_BEGIN
    ON_MISS_END

    // prepare data
    std::vector<int> test_data;
    {
        RootDereferenceScope scope;
        for (int i = 0; i < ObjectCount; i++) {
            int key = dist(re);
            int value = dist(re);
            auto [accessor, b] =
                hashmap.put(key, sizeof(HopscotchObject), __on_miss__, scope);
            accessor->key = key;
            accessor->value = value;
            auto [iterator, b_expected] =
                std_hashmap.insert_or_assign(key, value);
            ASSERT(b == b_expected);
            test_data.push_back(key);
        }
    }

    // test read
    std::uniform_int_distribution<int> idx_dist(0, ObjectCount - 1);
    {
        RootDereferenceScope scope;
        for (int i = 0; i < TestCount; i++) {
            int idx = idx_dist(re);
            int test_key = test_data[idx];
            HopscotchObject obj = *hashmap.get(test_key, __on_miss__, scope);
            ASSERT(obj.key == test_key);
            int value_expected = std_hashmap[test_key];
            ASSERT(obj.value == value_expected);
        }
    }

    // test modify
    {
        RootDereferenceScope scope;
        std::vector<size_t> modified_idxs;
        for (int i = 0; i < TestCount; i++) {
            int idx = idx_dist(re);
            int test_key = test_data[idx];
            int new_value = dist(re);
            auto accessor = hashmap.get(test_key, __on_miss__, scope).as_mut();
            accessor->value = new_value;
            std_hashmap[test_key] = new_value;
            modified_idxs.push_back(idx);
        }

        for (int i = 0; i < TestCount; i++) {
            int idx = modified_idxs[i];
            int test_key = test_data[idx];
            int value = hashmap.get(test_key, __on_miss__, scope)->value;
            int value_expected = std_hashmap[test_key];
            ASSERT(value == value_expected);
        }
    }

    // test erase
    {
        RootDereferenceScope scope;
        std::vector<size_t> erased_idxs;
        for (int i = 0; i < TestCount; i++) {
            int idx = idx_dist(re);
            int test_key = test_data[idx];
            bool existed = hashmap.remove(test_key, __on_miss__, scope);
            size_t erased_count = std_hashmap.erase(test_key);
            ASSERT(existed == (erased_count != 0));
            erased_idxs.push_back(idx);
        }

        for (int i = 0; i < TestCount; i++) {
            int idx = erased_idxs[i];
            int test_key = test_data[idx];
            auto accessor = hashmap.get(test_key, __on_miss__, scope);
            ASSERT(accessor.is_null());
        }
    }
}

int main() {
    srand(time(NULL));
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024 * 1024 * 1024;
    config.client_buffer_size = 1024 * 1024 * 32;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    FarLib::runtime_init(config);
    test_hopscotch();
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}
