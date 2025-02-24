#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/hashtable.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "test/fixed_size_string.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

Configure config;

void test_hashtable() {
    constexpr size_t ObjectCount = 1024 * 1024;
    constexpr size_t TestCount = 1024;
    constexpr size_t StringSize = 64;
    using TestObject = FixedSizeString<StringSize>;
    HashTable<int, TestObject, ObjectCount> ht;
    std::unordered_map<int, TestObject> cmp_ht;
    // prepare data
    std::vector<int> testdatas;
    for (int i = 0; i < ObjectCount; i++) {
        int k = rand();
        TestObject v = TestObject::random();
        ht.insert(k, v);
        cmp_ht[k] = v;
        testdatas.push_back(k);
    }

    // test read
    bool success = true;
    for (int i = 0; i < TestCount; i++) {
        int idx = rand() % ObjectCount;
        int test_key = testdatas[idx];
        auto v_acc = ht.get(test_key);
        TestObject& test_value = cmp_ht[test_key];
        if (*v_acc != test_value) {
            success = false;
            break;
        }
    }

    if (success) {
        std::cout << "test_read success" << std::endl;
    } else {
        std::cout << "test read failed" << std::endl;
    }

    std::vector<int> modified_idx;
    success = true;
    for (int i = 0; i < TestCount; i++) {
        int idx = rand() % ObjectCount;
        int test_key = testdatas[idx];
        auto v_acc = ht.get_mutable(test_key);
        TestObject& test_value = cmp_ht[test_key];
        char modify_val = rand() % 0xFF;
        (*v_acc)[i % StringSize] = modify_val;
        test_value[i % StringSize] = modify_val;
        modified_idx.push_back(idx);
    }

    ASSERT(modified_idx.size() == TestCount);
    for (int i = 0; i < TestCount; i++) {
        int idx = modified_idx[i];
        int test_key = testdatas[idx];
        auto v_acc = ht.get(test_key);
        TestObject& test_value = cmp_ht[test_key];
        if (*v_acc != test_value) {
            success = false;
            break;
        }
    }
    if (success) {
        std::cout << "test write success" << std::endl;
    } else {
        std::cout << "test write failed" << std::endl;
    }
}

int main() {
    srand(time(NULL));
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024 * 1024 * 1024;
    config.client_buffer_size = 64 * 1024 * 1024;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    Beehive::runtime_init(config);
    std::cout << "test_hashtable start" << std::endl;
    test_hashtable();
    std::cout << "test_hashtable end" << std::endl;
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}
