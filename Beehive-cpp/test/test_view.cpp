#include <x86intrin.h>

#include <iostream>
#include <ranges>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/far_vector.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

#define TEST_DROP
#define TEST_TAKE
#define TEST_FILTER_MAP

static constexpr size_t TEST_SIZE = 1024 * 1024 * 256;

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

void test() {
    std::vector<int> std_vec;
    FarLib::FarVector<int> far_vec;
    {
        RootDereferenceScope scope;
        for (int i = 0; i < TEST_SIZE; i++) {
            int num = i;
            std_vec.push_back(num);
            far_vec.push_back(num, scope);
        }
    }
    ASSERT(std_vec.size() == far_vec.size());
#ifdef TEST_FILTER_MAP
    {
        std::cout << "test filter && map" << std::endl;
        std::vector<int> std_res;
        std::vector<int> far_res;
        auto even = [](const int &i) { return i % 2 == 0; };
        auto square = [](const int &i) { return i * i; };
        auto start = __rdtsc();
        for (int i : std_vec | std::views::filter(even) |
                         std::views::transform(square)) {
            std_res.push_back(i);
        }
        auto end = __rdtsc();
        std::cout << "std view time: " << end - start << std::endl;
        start = __rdtsc();
        RootDereferenceScope scope;
        far_vec.get_view<false>().filter(even).map(square).for_each(
            [&far_res](int i) { far_res.push_back(i); }, scope);
        end = __rdtsc();
        std::cout << "far view time: " << end - start << std::endl;
        ASSERT(std_res == far_res);
    }
#endif
#ifdef TEST_DROP
    {
        std::cout << "test drop" << std::endl;
        std::vector<int> std_res;
        std::vector<int> far_res;
        auto start = __rdtsc();
        for (int i : std_vec | std::views::drop(10)) {
            std_res.push_back(i);
        }
        auto end = __rdtsc();
        std::cout << "std view time: " << end - start << std::endl;
        start = __rdtsc();
        RootDereferenceScope scope;
        far_vec.get_view<false>().drop(10).for_each(
            [&far_res](const int i) { far_res.push_back(i); }, scope);
        end = __rdtsc();
        std::cout << "far view time: " << end - start << std::endl;
    }
#endif
#ifdef TEST_TAKE
    {
        std::cout << "test take" << std::endl;
        std::vector<int> std_res;
        std::vector<int> far_res;
        auto start = __rdtsc();
        for (int i : std_vec | std::views::take(10)) {
            std_res.push_back(i);
        }
        auto end = __rdtsc();
        std::cout << "std view time: " << end - start << std::endl;
        start = __rdtsc();
        RootDereferenceScope scope;
        far_vec.get_view<false>().take(10).for_each(
            [&far_res](const int i) { far_res.push_back(i); }, scope);
        end = __rdtsc();
        std::cout << "far view time: " << end - start << std::endl;
    }
#endif
}

int main() {
    Configure config;
    config.server_addr = "127.0.0.1";
    config.server_port = "1234";
    config.server_buffer_size = 1024L * 1024 * 1024 * 2;
    config.client_buffer_size = TEST_SIZE / 2;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    FarLib::runtime_init(config);
    test();
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}