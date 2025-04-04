#include <chrono>
#include <cstdlib>
#include <iostream>
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

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

#define TEST_SYNC
#define TEST_COND_SYNC

static constexpr double DIV = 9982.44353;
static constexpr size_t TEST_SIZE = 1024 * 256;
static constexpr size_t DATA_SIZE = 1024;
struct MyData {
    int i;
    double d;
    char pad[DATA_SIZE - sizeof(int) - sizeof(double)];

    static MyData rand_data() { return {rand(), (double)rand() / DIV}; }

    bool operator==(const MyData& that) { return i == that.i && d == that.d; }
};

std::vector<MyData> build_test_data(size_t size = TEST_SIZE) {
    std::vector<MyData> datas;
    for (int i = 0; i < size; i++) {
        datas.push_back(MyData::rand_data());
    }
    return datas;
}

template <typename T>
void check(std::vector<T>& local, FarLib::Vector<T>& remote) {
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
    srand(time(NULL));
#ifdef TEST_SYNC
    {
        auto local_vector = build_test_data();
        FarLib::Vector<MyData> remote_vector;
        for (auto data : local_vector) {
            remote_vector.push_back(data);
        }
        for (int i = 1; i < TEST_SIZE; i++) {
            MyData& data0 = local_vector[i - 1];
            MyData& data1 = local_vector[i];
            data1.i = data1.i * 2 + data0.i;
            data1.d = data0.d * data1.i;
        }

        SYNC_FOR(int, i, 1, i < TEST_SIZE, i++)
            auto data0 = remote_vector.get_mut(i - 1, __on_miss__);
            auto data1 = remote_vector.get_mut(i, __on_miss__);
            __sync__();
            data1->i = data1->i * 2 + data0->i;
            data1->d = data0->d * data1->i;
        SYNC_FOR_END

        check<MyData>(local_vector, remote_vector);
    }
#endif
#ifdef TEST_COND_SYNC
    {
        auto local_vector = build_test_data();
        FarLib::Vector<MyData> remote_vector;
        for (auto data : local_vector) {
            remote_vector.push_back(data);
        }
        std::vector<double> local_ans;
        for (auto md : local_vector) {
            if (md.i % 2 == 0) {
                local_ans.push_back(md.i * md.d);
            }
        }
        std::vector<double> remote_ans;
        SYNC_FOR(int, i, 0, i < TEST_SIZE, i++)
            auto md = remote_vector.get(i, __on_miss__);
            if (md->i % 2 == 0) {
                __sync__();
                remote_ans.push_back(md->i * md->d);
            }
        SYNC_FOR_END

        check<double>(local_ans, remote_ans);
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
    ASSERT(config.client_buffer_size < TEST_SIZE * DATA_SIZE);
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    FarLib::runtime_init(config);
    test();
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}