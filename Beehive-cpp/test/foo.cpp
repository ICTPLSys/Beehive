#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/far_vector.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

constexpr size_t VEC_SIZE = 1024 * 1024;
constexpr size_t THREAD_COUNT = 16;
constexpr size_t X = 4096, Y = 4096;
template <typename Fn>
void profile(const char *name, Fn &&fn) {
    // warm up
    constexpr size_t WARMUP_TURN = 10;
    for (int i = 0; i < WARMUP_TURN; i++) {
        fn();
    }
    auto start = std::chrono::high_resolution_clock::now();
    fn();
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << std::string(name) << ": "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                      start)
                     .count()
              << " ns" << std::endl;
}

void test_float_vec() {
    std::vector<float> u(VEC_SIZE), v(VEC_SIZE);
    for (int i = 0; i < VEC_SIZE; i++) {
        u[i] = rand();
        v[i] = rand();
    }
    float val = 0;
    profile("test_float_vec", [&] {
        for (int i = 0; i < VEC_SIZE; i++) {
            val += v[i] * u[i];
        }
    });
    std::cout << "val = " << val << std::endl;
}

void test_c_arr() {
    float *u = new float[VEC_SIZE], *v = new float[VEC_SIZE];
    for (int i = 0; i < VEC_SIZE; i++) {
        u[i] = rand();
        v[i] = rand();
    }
    float val = 0;
    profile("test_c_arr", [&] {
        for (int i = 0; i < VEC_SIZE; i++) {
            val += v[i] * u[i];
        }
    });
    std::cout << "val = " << val << std::endl;
    delete[] u;
    delete[] v;
}

void test_obj_mul() {
    Beehive::FarVector<float> fv(VEC_SIZE);
    float *u = new float[VEC_SIZE];
    {
        Beehive::RootDereferenceScope scope;
        auto it = fv.lbegin(scope);
        for (int i = 0; i < VEC_SIZE; i++, it.next(scope)) {
            u[i] = rand();
            *it = rand();
        }
    }
    float val;
    profile("test_obj_mul", [&] { val = fv.vecmul(u, VEC_SIZE); });
    std::cout << "val = " << val << std::endl;
    delete[] u;
}

void test_far_vector() {
    Beehive::FarVector<float> fv(VEC_SIZE);
    float *u = new float[VEC_SIZE];
    {
        Beehive::RootDereferenceScope scope;
        auto it = fv.lbegin(scope);
        for (int i = 0; i < VEC_SIZE; i++, it.next(scope)) {
            u[i] = rand();
            *it = rand();
        }
    }
    Beehive::RootDereferenceScope scope;
    float val = 0;
    profile("test_far_vector", [&] {
        auto it = fv.clbegin(scope);
        for (int i = 0; i < VEC_SIZE; i++, it.next(scope)) {
            val += *it * u[i];
        }
    });
    std::cout << "val = " << val << std::endl;
    delete[] u;
}

void test_vec_matmul() {
    std::vector<float> u(X * Y), v(Y), w(X);
    for (auto &e : u) {
        e = rand();
    }
    for (auto &e : v) {
        e = rand();
    }
    const size_t slice = (X + THREAD_COUNT - 1) / THREAD_COUNT;
    profile("test_vec_matmul", [&] {
        std::vector<std::thread> threads;
        for (int id = 0; id < THREAD_COUNT; id++) {
            threads.emplace_back([&] {
                const size_t istart = id * slice;
                const size_t iend = std::min(istart + slice, X);
                if (istart >= iend) {
                    return;
                }
                for (int i = istart; i < iend; i++) {
                    float val = 0.0;
                    for (int j = 0; j < Y; j++) {
                        val += u[i * X + j] * v[j];
                    }
                    w[i] += val;
                }
            });
        }
        for (auto &t : threads) {
            t.join();
        }
    });
    std::cout << w[rand() % X] << std::endl;
}

void test_far_matmul() {
    Beehive::FarVector<float> fv(X * Y);
    float *u = new float[X];
    float *res = new float[Y];
    {
        Beehive::RootDereferenceScope scope;
        auto it = fv.lbegin(scope);
        for (int i = 0; i < X * Y; i++, it.next(scope)) {
            *it = rand();
        }
        for (int i = 0; i < X; i++) {
            u[i] = rand();
        }
    }
    Beehive::RootDereferenceScope scope;
    const size_t slice = (X + THREAD_COUNT - 1) / THREAD_COUNT;
    profile("test_far_matmul", [&] {
        Beehive::uthread::parallel_for_with_scope<1>(
            THREAD_COUNT, THREAD_COUNT,
            [&](size_t id, Beehive::DereferenceScope &scope) {
                const size_t istart = id * slice;
                const size_t iend = std::min(istart + slice, X);
                if (istart >= iend) {
                    return;
                }
                const int idx_start = istart * X;
                const int idx_end = iend * X;
                auto it = fv.get_const_lite_iter(idx_start, scope, idx_start,
                                                 idx_end);
                for (int i = istart; i < iend; i++) {
                    float val = 0;
                    for (int j = 0; j < Y; j++, it.next(scope)) {
                        val += *it * u[j];
                    }
                    res[i] = val;
                }
            });
    });
    std::cout << res[rand() % X] << std::endl;
    delete[] u;
    delete[] res;
}

void test() {
    test_float_vec();
    test_c_arr();
    test_obj_mul();
    test_far_vector();
    // test_vec_matmul();
    // test_far_matmul();
}

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;
int main() {
    Configure config;
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024L * 1024 * 1024 * 4L;
    config.client_buffer_size = 4 * 1024 * 1024 * 1024L;
    config.evict_batch_size = 64 * 1024;
    config.max_thread_cnt = THREAD_COUNT;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    Beehive::runtime_init(config);
    test();
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}