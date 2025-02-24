#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/hashtable.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "test/fixed_size_string.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/timer.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

#define STD_BASELINE
#define REMOTE_SEQ
#define REMOTE_PIPE
#define REMOTE_CORO
#define REMOTE_ASYNC
#define REMOTE_THREAD
#define REMOTE_THREAD_CORO
#define REMOTE_THREAD_ASYNC

namespace Beehive {

using str_key_t = FixedSizeString<16>;
using str_value_t = FixedSizeString<64>;

constexpr size_t OBJ_N = 1024 * 1024;

void test_multi_find() {
    HashTable<str_key_t, str_value_t, OBJ_N> remote_hash_table;
    std::unordered_map<str_key_t, str_value_t> std_hash_table;
    std::vector<str_key_t> keys;
    for (int i = 0; i < OBJ_N; i++) {
        str_key_t k = str_key_t::random();
        str_value_t v = str_value_t::random();
        remote_hash_table.insert(k, v);
        std_hash_table[k] = v;
        keys.push_back(k);
    }

    std::random_shuffle(keys.begin(), keys.end());
    std::cout << "hashtable ready, start test" << std::endl;
    CpuTimer timer;
#ifdef STD_BASELINE
    {
        timer.setTimer();
        size_t count = 0;
        for (auto k : keys) {
            count += (std_hash_table.find(k) != std_hash_table.end()) ? 1 : 0;
        }
        timer.stopTimer();
        std::cout << "std multi find time: " << timer.getDurationStr()
                  << std::endl;
        ASSERT(count == OBJ_N);
    }
#endif

#ifdef REMOTE_SEQ
    {
        timer.setTimer();
        size_t count = 0;
        remote_hash_table.multi_find_sequence(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find sequence time: "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }
#endif

#ifdef REMOTE_PIPE
    {
        timer.setTimer();
        size_t count = 0;
        remote_hash_table.multi_find_buffered_pipeline(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find pipeline time: "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }
#endif

#ifdef REMOTE_CORO
    {
        timer.setTimer();
        size_t count = 0;
        remote_hash_table.multi_find_coroutine<true>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find coroutine (with sync) time: "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }

    {
        timer.setTimer();
        size_t count = 0;
        remote_hash_table.multi_find_coroutine<false>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find coroutine (without sync) time: "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }
#endif

#ifdef REMOTE_ASYNC
    {
        timer.setTimer();
        size_t count = 0;
        remote_hash_table.multi_find_async_for<true>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find async for (with sync) time: "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }

    {
        timer.setTimer();
        size_t count = 0;
        remote_hash_table.multi_find_async_for<false>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find async for (without sync) time: "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }
#endif

#ifdef REMOTE_THREAD
    {
        timer.setTimer();
        std::atomic_size_t count = 0;
        remote_hash_table.multi_find_thread<false>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find thread time (without sync): "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }

    {
        timer.setTimer();
        std::atomic_size_t count = 0;
        remote_hash_table.multi_find_thread<true>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find thread time (with sync): "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }
#endif

#ifdef REMOTE_THREAD_CORO
    {
        timer.setTimer();
        std::atomic_size_t count = 0;
        remote_hash_table.multi_find_thread_coro<false>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find thread+coro time (without sync): "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }

    {
        timer.setTimer();
        std::atomic_size_t count = 0;
        remote_hash_table.multi_find_thread_coro<true>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find thread+coro time (with sync): "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }
#endif

#ifdef REMOTE_THREAD_ASYNC
    {
        timer.setTimer();
        std::atomic_size_t count = 0;
        remote_hash_table.multi_find_thread_async_for<false>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find thread async for time (without sync): "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }

    {
        timer.setTimer();
        std::atomic_size_t count = 0;
        remote_hash_table.multi_find_thread_async_for<true>(
            keys, [&count](const str_key_t* key,
                           std::optional<const str_value_t*> value) {
                if (value.has_value()) count++;
            });
        timer.stopTimer();
        std::cout << "remote multi find thread async for time (with sync): "
                  << timer.getDurationStr() << std::endl;
        ASSERT(count == OBJ_N);
    }
#endif
}

}  // namespace Beehive

int main(int argc, char* argv[]) {
    srand(0);
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file> " << std::endl;
        return -1;
    }
    Configure config;
    config.from_file(argv[1]);
    Beehive::runtime_init(config);
    test_multi_find();
    Beehive::runtime_destroy();
    return 0;
}