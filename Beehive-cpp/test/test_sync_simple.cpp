#include <chrono>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <string>
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

#define TEST_ASYNC
#define TEST_SYNC
#define TEST_COND_SYNC

static constexpr size_t TEST_SIZE = 1024 * 1024;

std::vector<uint64_t> build_test_data_uint64(size_t size = TEST_SIZE) {
    std::vector<uint64_t> datas(size, 1);
    return datas;
}

template <typename T>
void check(std::vector<T>& local, Beehive::Vector<T>& remote) {
    ASSERT(local.size() == remote.size());
    for (int i = 0; i < TEST_SIZE; i++) {
        T& local_data = local[i];
        auto remote_data = remote.get(i);
        ASSERT(local_data == *remote_data);
    }
}

template <typename T>
void check(std::vector<T>& local, std::vector<T>& remote) {
    ASSERT(local.size() == remote.size());
    for (int i = 0; i < local.size(); i++) {
        ASSERT(local[i] == remote[i]);
    }
}

void test() {
#ifdef TEST_ASYNC
    {
        std::cout << "test async" << std::endl;
        auto local_vector = build_test_data_uint64();
        Beehive::Vector<uint64_t> remote_vector;
        for (auto data : local_vector) {
            remote_vector.push_back(data);
        }
        for (int i = 1; i < TEST_SIZE; i++) {
            auto& data1 = local_vector[i];
            data1 = i;
        }

        ASYNC_FOR(int, i, 1, i < TEST_SIZE, i++)
            auto data1 = remote_vector.get_mut(i, __on_miss__);
            *data1 = i;
        ASYNC_FOR_END

        check<uint64_t>(local_vector, remote_vector);
    }
#endif
#ifdef TEST_SYNC
    {
        std::cout << "test sync" << std::endl;
        auto local_vector = build_test_data_uint64();
        Beehive::Vector<uint64_t> remote_vector;
        for (auto data : local_vector) {
            remote_vector.push_back(data);
        }
        for (int i = 1; i < TEST_SIZE; i++) {
            auto& data0 = local_vector[i - 1];
            auto& data1 = local_vector[i];
            data1 += data0;
        }

        SYNC_FOR(int, i, 1, i < TEST_SIZE, i++)
            auto data0 = remote_vector.get_mut(i - 1, __on_miss__);
            auto data1 = remote_vector.get_mut(i, __on_miss__);
            __sync__();
            *data1 += *data0;
        SYNC_FOR_END

        check<uint64_t>(local_vector, remote_vector);
    }
#endif
#ifdef TEST_COND_SYNC
    {
        std::cout << "test cond sync" << std::endl;
        auto local_vector = build_test_data_uint64();
        Beehive::Vector<uint64_t> remote_vector;
        for (auto data : local_vector) {
            remote_vector.push_back(data);
        }
        std::vector<uint64_t> local_ans;
        for (auto md : local_vector) {
            if (md % 2 == 0) {
                local_ans.push_back(md);
            }
        }
        std::vector<uint64_t> remote_ans;
        SYNC_FOR(int, i, 0, i < TEST_SIZE, i++)
            auto md = remote_vector.get(i, __on_miss__);
            if (*md % 2 == 0) {
                __sync__();
                remote_ans.push_back(*md);
            }
        SYNC_FOR_END

        check<uint64_t>(local_ans, remote_ans);
    }
#endif
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
    test();
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}