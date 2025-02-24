#include <algorithm>
#include <array>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "async/loop.hpp"
#include "cache/cache.hpp"
#include "data_structure/hopscotch.hpp"
#include "test/fixed_size_string.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/timer.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

#define STD_BASELINE
#define REMOTE_SEQ
#define REMOTE_ASYNC
#define REMOTE_PTHREAD
#define REMOTE_UTHREAD

namespace Beehive {

using str_key_t = FixedSizeString<16>;
using str_value_t = FixedSizeString<64>;

constexpr size_t OBJ_N = 1024 * 1024;

void test_multi_find() {
    std::unordered_map<str_key_t, str_value_t> std_hash_table;

    struct HopscotchObject {
        str_key_t key;
        str_value_t value;
        bool equals(const str_key_t& cmp) const { return cmp == key; }
    };
    Hopscotch<str_key_t, HopscotchObject> remote_hopscotch(22);

    std::vector<str_key_t> keys;
    for (int i = 0; i < OBJ_N; i++) {
        str_key_t k = str_key_t::random();
        str_value_t v = str_value_t::random();
        std_hash_table[k] = v;
        {
            ON_MISS_BEGIN
            ON_MISS_END
            auto [accessor, _] =
                remote_hopscotch.put(k, sizeof(HopscotchObject), __on_miss__);
            accessor->key = k;
            accessor->value = v;
        }
        keys.push_back(k);
    }

    std::random_shuffle(keys.begin(), keys.end());
    std::cout << "hashtable ready, start test" << std::endl;
    CpuTimer timer;

    for (size_t i = 0; i < 4; i++) {
#ifdef STD_BASELINE
        {
            timer.setTimer();
            size_t count = 0;
            for (auto k : keys) {
                count +=
                    (std_hash_table.find(k) != std_hash_table.end()) ? 1 : 0;
            }
            timer.stopTimer();
            std::cout << "std multi find time: " << timer.getDurationStr()
                      << std::endl;
            ASSERT(count == OBJ_N);
        }
#endif

#ifdef REMOTE_SEQ
        {
            ON_MISS_BEGIN
            ON_MISS_END
            timer.setTimer();
            size_t count = 0;
            for (auto k : keys) {
                count += remote_hopscotch.get(k, __on_miss__).is_null() ? 0 : 1;
            }
            timer.stopTimer();
            std::cout << "remote multi find sequence time: "
                      << timer.getDurationStr() << std::endl;
            ASSERT(count == OBJ_N);
        }
#endif

#ifdef REMOTE_ASYNC
        {
            timer.setTimer();
            size_t count = 0;
            ASYNC_FOR(size_t, i, 0, i < keys.size(), i++)
                count += remote_hopscotch.get(keys[i], __on_miss__).is_null()
                             ? 0
                             : 1;
            ASYNC_FOR_END
            timer.stopTimer();
            std::cout << "remote multi find async for time: "
                      << timer.getDurationStr() << std::endl;
            ASSERT(count == OBJ_N);
        }
#endif

#ifdef REMOTE_PTHREAD
        {
            timer.setTimer();
            std::atomic_size_t count = 0;
            constexpr size_t ThreadCount = 16;
            auto thread_task = [&](size_t thread_idx) {
                ON_MISS_BEGIN
                    std::this_thread::yield();
                ON_MISS_END
                size_t thread_local_count = 0;
                for (size_t idx = thread_idx; idx < keys.size();
                     idx += ThreadCount) {
                    auto& k = keys[idx];
                    thread_local_count +=
                        remote_hopscotch.get(k, __on_miss__).is_null() ? 0 : 1;
                }
                count.fetch_add(thread_local_count);
            };
            std::thread threads[ThreadCount];
            for (size_t i = 0; i < ThreadCount; i++) {
                threads[i] = std::thread(thread_task, i);
            }
            for (size_t i = 0; i < ThreadCount; i++) {
                threads[i].join();
            }
            timer.stopTimer();
            std::cout << "remote multi find pthreads x" << ThreadCount
                      << " for time: " << timer.getDurationStr() << std::endl;
            ASSERT(count.load() == OBJ_N);
        }
#endif

#ifdef REMOTE_UTHREAD
        {
            struct UThreadTask {
                std::function<void(size_t)>* func;
                size_t arg;
            };
            typedef void (*uthread_fn_t)(UThreadTask*);
            uthread_fn_t uthread_run = [](UThreadTask* task) {
                (*task->func)(task->arg);
            };
            constexpr size_t ThreadCount = 16;
            auto uthread_run_func = [&](std::function<void(size_t)> fn) {
                std::unique_ptr<Fibre> uthreads[ThreadCount];
                UThreadTask tasks[ThreadCount];
                for (size_t i = 0; i < ThreadCount; i++) {
                    tasks[i].func = &fn;
                    tasks[i].arg = i;
                    auto thread = new Fibre();
                    thread->run(uthread_run, &tasks[i]);
                    uthreads[i].reset(thread);
                }
                for (size_t i = 0; i < ThreadCount; i++) {
                    uthreads[i]->join();
                }
            };

            timer.setTimer();
            std::atomic_size_t count = 0;
            auto thread_task = [&](size_t thread_idx) {
                ON_MISS_BEGIN
                    Fibre::yieldGlobal();
                ON_MISS_END
                size_t thread_local_count = 0;
                for (size_t idx = thread_idx; idx < keys.size();
                     idx += ThreadCount) {
                    auto& k = keys[idx];
                    thread_local_count +=
                        remote_hopscotch.get(k, __on_miss__).is_null() ? 0 : 1;
                }
                count.fetch_add(thread_local_count);
            };
            uthread_run_func(thread_task);
            timer.stopTimer();
            std::cout << "remote multi find uthreads x" << ThreadCount
                      << " for time: " << timer.getDurationStr() << std::endl;
            ASSERT(count.load() == OBJ_N);
        }
#endif
    }
}

}  // namespace Beehive

int main(int argc, char* argv[]) {
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