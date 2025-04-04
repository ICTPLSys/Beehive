#include <chrono>
#include <cstdlib>
#include <memory>
#include <random>
#include <thread>

#include "data_structure/ordered_vector.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

Configure config;

void test_insert(size_t m, size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    OrderedVector<int>* vectors = static_cast<OrderedVector<int>*>(
        std::malloc(sizeof(OrderedVector<int>) * m));
    for (int j = 0; j < m; j++) {
        new (&vectors[j]) OrderedVector<int>(scope);
    }
    // allocation
    for (int j = 0; j < m; j++) {
        for (size_t i = 0; i < n; i++) {
            vectors[j].insert(i, scope);
        }
        vectors[j].for_each([&](size_t i, int x) { ASSERT(i == x); }, scope);
    }
    std::free(vectors);
}

void test_insert_f(size_t m, size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    OrderedVector<int>* vectors = static_cast<OrderedVector<int>*>(
        std::malloc(sizeof(OrderedVector<int>) * m));
    for (int j = 0; j < m; j++) {
        new (&vectors[j]) OrderedVector<int>(scope);
    }
    // allocation
    for (int j = 0; j < m; j++) {
        for (size_t i = 0; i < n; i++) {
            vectors[j].insert_f(i, scope, j);
        }
        vectors[j].for_each([&](size_t i, int x) { ASSERT(i == x); }, scope);
    }
    std::free(vectors);
}

void test_rand_insert(size_t m, size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    OrderedVector<int>* vectors = static_cast<OrderedVector<int>*>(
        std::malloc(sizeof(OrderedVector<int>) * m));
    for (int j = 0; j < m; j++) {
        new (&vectors[j]) OrderedVector<int>(scope);
    }
    std::vector<int> arr(n);
    for (int i = 0; i < n; ++i) {
        arr[i] = i;
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    // allocation
    for (int j = 0; j < m; j++) {
        std::shuffle(arr.begin(), arr.end(), gen);
        for (size_t i = 0; i < n; i++) {
            vectors[j].insert(arr[i], scope);
        }
        vectors[j].for_each([&](size_t i, int x) { ASSERT(i == x); }, scope);
        // std::cout << "pass " << j << "th" << std::endl;
    }
    std::free(vectors);
}

void test_rand_insert_f(size_t m, size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    OrderedVector<int>* vectors = static_cast<OrderedVector<int>*>(
        std::malloc(sizeof(OrderedVector<int>) * m));
    for (int j = 0; j < m; j++) {
        new (&vectors[j]) OrderedVector<int>(scope);
    }
    std::vector<int> arr(n);
    for (int i = 0; i < n; ++i) {
        arr[i] = i;
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    // allocation
    for (int j = 0; j < m; j++) {
        std::shuffle(arr.begin(), arr.end(), gen);
        for (size_t i = 0; i < n; i++) {
            vectors[j].insert_f(arr[i], scope, j);
        }
        vectors[j].for_each([&](size_t i, int x) { ASSERT(i == x); }, scope);
        // std::cout << "pass " << j << "th" << std::endl;
    }
    std::free(vectors);
}

void test_erase(size_t m, size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    OrderedVector<int>* vectors = static_cast<OrderedVector<int>*>(
        std::malloc(sizeof(OrderedVector<int>) * m));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, n);
    for (int j = 0; j < m; j++) {
        new (&vectors[j]) OrderedVector<int>(scope);
    }
    // allocation
    for (int j = 0; j < m; j++) {
        for (size_t i = 0; i < n; i++) {
            vectors[j].insert(i, scope);
        }
        int random_number_1 = distrib(gen);
        vectors[j].erase(random_number_1, scope);
        int random_number_2 = distrib(gen);
        vectors[j].erase(random_number_2, scope);
        vectors[j].for_each(
            [&](size_t i, int x) {
                ASSERT(x != random_number_1);
                ASSERT(x != random_number_2);
            },
            scope);
    }
    std::free(vectors);
}

void test_erase_f(size_t m, size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    OrderedVector<int>* vectors = static_cast<OrderedVector<int>*>(
        std::malloc(sizeof(OrderedVector<int>) * m));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, n);
    for (int j = 0; j < m; j++) {
        new (&vectors[j]) OrderedVector<int>(scope);
    }
    // allocation
    for (int j = 0; j < m; j++) {
        for (size_t i = 0; i < n; i++) {
            vectors[j].insert_f(i, scope, j);
        }
        int random_number_1 = distrib(gen);
        vectors[j].erase_f(random_number_1, scope);
        int random_number_2 = distrib(gen);
        vectors[j].erase_f(random_number_2, scope);
        vectors[j].for_each(
            [&](size_t i, int x) {
                ASSERT(x != random_number_1);
                ASSERT(x != random_number_2);
            },
            scope);
    }
    std::free(vectors);
}

void test_get_range(size_t m, size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    OrderedVector<int>* vectors = static_cast<OrderedVector<int>*>(
        std::malloc(sizeof(OrderedVector<int>) * m));
    for (int j = 0; j < m; j++) {
        new (&vectors[j]) OrderedVector<int>(scope);
    }
    std::vector<int> arr(n);
    for (int i = 0; i < n; ++i) {
        arr[i] = i;
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    // allocation
    for (int j = 0; j < m; j++) {
        std::shuffle(arr.begin(), arr.end(), gen);
        for (size_t i = 0; i < n; i++) {
            vectors[j].insert_f(arr[i], scope, j);
        }
        for (size_t i = 0; i < n / 2; i++) {
            vectors[j].erase_f(i, scope);
        }
        std::vector<int> elements =
            vectors[j].get_f(n / 2, n, scope);
        for (int i = 0; i < elements.size(); i++) {
            ASSERT(elements[i] == n / 2 + i + 1);
        }
        // std::cout << "pass " << j << "th" << std::endl;
    }
    std::free(vectors);
}

void test_erase_mult(size_t m, size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    OrderedVector<int>* vectors = static_cast<OrderedVector<int>*>(
        std::malloc(sizeof(OrderedVector<int>) * m));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, n);
    for (int j = 0; j < m; j++) {
        new (&vectors[j]) OrderedVector<int>(scope);
    }
    // allocation
    for (int j = 0; j < m; j++) {
        for (size_t i = 0; i < n; i++) {
            vectors[j].insert_f(i, scope, j);
        }
        for (size_t i = 0; i < n - 128; i++) {
            vectors[j].erase_f(i, scope);
        }
        for (size_t i = 0; i < n - 128; i++) {
            vectors[j].insert_f(i, scope, j);
        }
        vectors[j].for_each([&](size_t i, int x) { ASSERT(i == x); }, scope);
    }
    std::free(vectors);
}

int main() {
    config.server_addr = "127.0.0.1";
    config.server_port = "50001";
    config.server_buffer_size = 1024L * 1024 * 1024 * 16;
    config.client_buffer_size = 1024L * 1024 * 64;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    FarLib::runtime_init(config);
    // test_insert(512 * 1024, 1024);
    // test_rand_insert(512 * 1024, 1024);
    // test_erase(512 * 1024, 1024);
    // test_insert_f(512 * 1024,1024);
    // test_rand_insert_f(512 * 1024, 1024);
    // test_erase_f(512 * 1024, 1024);
    // test_get_range(64 * 1024, 1024);
    test_erase_mult(1,32 * 1024);
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}
