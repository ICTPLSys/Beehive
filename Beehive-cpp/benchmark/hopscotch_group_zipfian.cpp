#include <algorithm>
#include <array>
#include <boost/timer/progress_display.hpp>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "async/loop.hpp"
#include "async/scoped_inline_task.hpp"
#include "cache/cache.hpp"
#include "data_structure/hopscotch.hpp"
#include "libfibre/fibre.h"
#include "test/fixed_size_string.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/threads.hpp"
#include "utils/zipfian.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

#define DISPLAY_PROGRESS
#define STD_BASELINE
#define REMOTE_SEQ
#define REMOTE_ASYNC
#define REMOTE_UTHREAD
#define COMMIT_DATA

using str_key_t = FixedSizeString<16>;
using str_value_t = FixedSizeString<64>;

void commit_item_impl(str_value_t) {}

void (*commit_item)(str_value_t) = commit_item_impl;

void commit(const str_value_t* data, size_t size) {
    for (size_t i = 0; i < size; i++) {
        commit_item(data[i]);
    }
}

struct HopscotchObject {
    str_key_t key;
    str_value_t value;
    bool equals(const str_key_t& cmp) const { return cmp == key; }
};

constexpr size_t RequestCount = 1024 * 1024;
constexpr size_t DataSizeShift = 20;
constexpr size_t DataSize = 1 << DataSizeShift;
constexpr size_t ProccessorCount = 4;

using RemoteHashTable = Hopscotch<str_key_t, HopscotchObject>;

std::vector<HopscotchObject> data;
std::unordered_map<str_key_t, str_value_t> std_hash_table;
RemoteHashTable remote_hash_table(DataSizeShift + 2);

void init_data() {
    ON_MISS_BEGIN
    ON_MISS_END
    data.reserve(DataSize);
#ifdef DISPLAY_PROGRESS
    std::cout << "Loading Data..." << std::endl;
    boost::timer::progress_display progress(DataSize);
#endif
    for (size_t i = 0; i < DataSize; i++) {
        str_key_t key = str_key_t::random();
        str_value_t value = str_value_t::random();
        data.push_back({.key = key, .value = value});
        auto [accessor, inserted] =
            remote_hash_table.put(key, sizeof(HopscotchObject), __on_miss__);
        accessor->key = key;
        accessor->value = value;
#ifdef STD_BASELINE
        std_hash_table[key] = value;
#endif
#ifdef DISPLAY_PROGRESS
        ++progress;
#endif
    }
}

void request_local(size_t idx, str_value_t* result) {
    auto& req = data[idx];
    auto iterator = std_hash_table.find(req.key);
    ASSERT(iterator != std_hash_table.end());
    if (result) *result = iterator->second;
}

void request(size_t idx, str_value_t* result, __DMH__,
             DereferenceScope& scope) {
    auto& req = data[idx];
    auto accessor = remote_hash_table.get(req.key, __on_miss__, scope);
    ASSERT(!accessor.is_null());
    if (result) *result = accessor->value;
}

void request(size_t idx, str_value_t* result, __DMH__) {
    auto& req = data[idx];
    auto accessor = remote_hash_table.get(req.key, __on_miss__);
    ASSERT(!accessor.is_null());
    if (result) *result = accessor->value;
}

void run() {
    std::random_device random_device;
    std::default_random_engine random_engine(random_device());
    srand(random_device());
    init_data();

    double zipfian_constants[] = {0, 0.2, 0.4, 0.6, 0.8, 0.99};

    std::cout << std::setw(24) << "zipfian constant";
    std::cout << std::setw(24) << "group size";
#ifdef STD_BASELINE
    std::cout << std::setw(24) << "local std time (ms)";
#endif
#ifdef REMOTE_SEQ
    std::cout << std::setw(24) << "sequential time (ms)";
#endif
#ifdef REMOTE_ASYNC
    std::cout << std::setw(24) << "ooo (stack) time (ms)";
#endif
#ifdef REMOTE_ASYNC
    std::cout << std::setw(24) << "ooo (inline) time (ms)";
#endif
#ifdef REMOTE_UTHREAD
    std::cout << std::setw(24) << "uthread time (ms)";
#endif
    std::cout << std::endl;

    size_t group_size = 65536;
    for (double z : zipfian_constants) {
        std::cout << std::setw(24) << z;
        std::cout << std::setw(24) << group_size;
        ZipfianGenerator<false> idx_generator(DataSize, z);
#ifdef STD_BASELINE
        {
            auto buffer = new str_value_t[group_size];
            auto start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < RequestCount; i += group_size) {
                for (size_t j = 0; j < group_size; j++) {
                    int req_idx = idx_generator(random_engine);
                    request_local(req_idx, buffer + j);
                }
                commit(buffer, group_size);
            }
            auto end = std::chrono::high_resolution_clock::now();
            delete[] buffer;
            std::chrono::duration<double, std::milli> duration = end - start;
            std::cout << std::setw(24) << duration.count() << std::flush;
        }
#endif
#ifdef REMOTE_SEQ
        {
            RootDereferenceScope scope;
            auto buffer = new str_value_t[group_size];
            auto start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < RequestCount; i += group_size) {
                ON_MISS_BEGIN
                ON_MISS_END
                for (size_t j = 0; j < group_size; j++) {
                    int req_idx = idx_generator(random_engine);
                    request(req_idx, buffer + j, __on_miss__, scope);
                }
                commit(buffer, group_size);
            }
            auto end = std::chrono::high_resolution_clock::now();
            delete[] buffer;
            std::chrono::duration<double, std::milli> duration = end - start;
            std::cout << std::setw(24) << duration.count() << std::flush;
        }
#endif
#ifdef REMOTE_ASYNC
        {
            auto buffer = new str_value_t[group_size];
            auto start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < RequestCount; i += group_size) {
                ASYNC_FOR(size_t, j, 0, j < group_size, j++)
                    int req_idx = idx_generator(random_engine);
                    request(req_idx, buffer + j, __on_miss__);
                ASYNC_FOR_END
                commit(buffer, group_size);
            }
            auto end = std::chrono::high_resolution_clock::now();
            delete[] buffer;
            std::chrono::duration<double, std::milli> duration = end - start;
            std::cout << std::setw(24) << duration.count() << std::flush;
        }
#endif
#ifdef REMOTE_ASYNC
        {
            struct BaseContext {
                RemoteHashTable* hash_table;
                HopscotchObject* request_object;
                str_value_t* result_value;

                RemoteHashTable* get_this() { return hash_table; }
                const str_key_t& get_key() const { return request_object->key; }
                void make_result(LiteAccessor<HopscotchObject>&& result) {
                    *result_value = result->value;
                    result = {};
                }
            };

            using Context = RemoteHashTable::GetContext<BaseContext>;

            auto buffer = new str_value_t[group_size];
            auto start = std::chrono::high_resolution_clock::now();
            RootDereferenceScope scope;
            for (size_t i = 0; i < RequestCount; i += group_size) {
                SCOPED_INLINE_ASYNC_FOR(Context, size_t, j, 0, j < group_size,
                                        j++, scope)
                    int req_idx = idx_generator(random_engine);
                    Context context;
                    context.hash_table = &remote_hash_table;
                    context.request_object = &(data[req_idx]);
                    context.result_value = buffer + j;
                    return context;
                SCOPED_INLINE_ASYNC_FOR_END

                commit(buffer, group_size);
            }
            auto end = std::chrono::high_resolution_clock::now();
            delete[] buffer;
            std::chrono::duration<double, std::milli> duration = end - start;
            std::cout << std::setw(24) << duration.count() << std::flush;
        }
#endif
#ifdef REMOTE_UTHREAD
        {
            constexpr size_t ThreadCount = 16;
            auto buffer = new str_value_t[group_size];
            auto start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < RequestCount; i += group_size) {
                uthread_run<ThreadCount>([&](size_t thread_idx) {
                    RootDereferenceScope scope;
                    ON_MISS_BEGIN
                        Fibre::yieldGlobal();
                    ON_MISS_END
                    for (size_t j = thread_idx; j < group_size;
                         j += ThreadCount) {
                        int req_idx = idx_generator(random_engine);
                        request(req_idx, buffer + j, __on_miss__, scope);
                    }
                });
                commit(buffer, group_size);
            }
            auto end = std::chrono::high_resolution_clock::now();
            delete[] buffer;
            std::chrono::duration<double, std::milli> duration = end - start;
            std::cout << std::setw(24) << duration.count() << std::flush;
        }
#endif
        std::cout << std::endl;
    }

#if 0

    double z = 0.99;
    std::cout << "==== large batch, multi pthreads ====" << std::endl;
    std::cout << std::setw(24) << z;
    std::cout << std::setw(24) << RequestCount;
    ZipfianGenerator<false> idx_generator(DataSize, z);
#ifdef STD_BASELINE
    {
        auto start = std::chrono::high_resolution_clock::now();
        pthread_run<ProccessorCount>([&](size_t thread_idx) {
            std::random_device random_device;
            std::default_random_engine random_engine(random_device());
            for (size_t i = thread_idx; i < RequestCount;
                 i += ProccessorCount) {
                int req_idx = idx_generator(random_engine);
                request_local(req_idx, nullptr);
            }
        });
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << std::setw(24) << duration.count() << std::flush;
    }
#endif
#ifdef REMOTE_SEQ
    {
        auto start = std::chrono::high_resolution_clock::now();
        pthread_run<ProccessorCount>([&](size_t thread_idx) {
            std::random_device random_device;
            std::default_random_engine random_engine(random_device());
            ON_MISS_BEGIN
            ON_MISS_END
            for (size_t i = thread_idx; i < RequestCount;
                 i += ProccessorCount) {
                int req_idx = idx_generator(random_engine);
                request(req_idx, nullptr, __on_miss__);
            }
        });
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << std::setw(24) << duration.count() << std::flush;
    }
#endif
#ifdef REMOTE_ASYNC
    {
        auto start = std::chrono::high_resolution_clock::now();
        pthread_run<ProccessorCount>([&](size_t thread_idx) {
            std::random_device random_device;
            std::default_random_engine random_engine(random_device());
            ASYNC_FOR(size_t, i, thread_idx, i < RequestCount,
                      i += ProccessorCount)
                int req_idx = idx_generator(random_engine);
                request(req_idx, nullptr, __on_miss__);
            ASYNC_FOR_END
        });

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << std::setw(24) << duration.count() << std::flush;
    }
#endif
#ifdef REMOTE_UTHREAD
    {
        Context::CurrCluster().addWorkers(ProccessorCount - 1);
        constexpr size_t ThreadCount = 16 * ProccessorCount;
        auto& cluster = Context::CurrCluster();
        auto start = std::chrono::high_resolution_clock::now();
        uthread_run<ThreadCount>([&](size_t thread_idx) {
            std::random_device random_device;
            std::default_random_engine random_engine(random_device());
            ON_MISS_BEGIN
                Fibre::yieldGlobal();
            ON_MISS_END
            for (size_t j = thread_idx; j < RequestCount; j += ThreadCount) {
                int req_idx = idx_generator(random_engine);
                request(req_idx, nullptr, __on_miss__);
            }
        });
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << std::setw(24) << duration.count() << std::flush;
    }
#endif
    std::cout << std::endl;
#endif
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file> " << std::endl;
        return -1;
    }

    Configure config;
    config.from_file(argv[1]);
    Beehive::runtime_init(config);
    run();
    Beehive::runtime_destroy();
    return 0;
}