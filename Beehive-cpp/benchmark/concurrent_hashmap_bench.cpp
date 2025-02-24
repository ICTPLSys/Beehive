#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <random>
#include <unordered_map>
#include <vector>

#include "async/scoped_inline_task.hpp"
#include "data_structure/concurrent_hashmap.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"

using namespace Beehive;

struct Key {
    char str[32];
    bool operator==(const Key &other) const {
        return memcmp(str, other.str, 32) == 0;
    }
};
struct Value {
    char str[32];
    std::byte padding[176];

    bool operator==(const Value &other) const {
        return memcmp(str, other.str, 32) == 0;
    }
};
namespace std {
template <>
struct hash<Key> {
    std::size_t operator()(const Key &key) const {
        return std::hash<std::string_view>{}(std::string_view(key.str, 32));
    }
};

};  // namespace std

using STLHashMap = std::unordered_map<Key, Value>;
using HashMap = Beehive::ConcurrentHashMap<Key, Value>;

uthread::Mutex print_mutex;
std::vector<std::pair<Key, Value>> pairs;

inline std::pair<Key, Value> random_pair(auto &re) {
    std::uniform_int_distribution<char> dist('a', 'z');
    std::pair<Key, Value> pair;
    for (size_t i = 0; i < 31; i++) {
        pair.first.str[i] = dist(re);
    }
    pair.first.str[31] = '\0';
    memcpy(pair.second.str, pair.first.str, 32);
    return pair;
}

constexpr size_t InsertCount = 10'000'000;
constexpr size_t NThreads = 256;

template <typename Fn>
void time_for(Fn &&fn, const char *name, size_t n = 1) {
    auto start = std::chrono::high_resolution_clock::now();
    fn();
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> duration_us = end - start;
    uthread::lock(&print_mutex);
    std::cout << "time for " << name << ": " << duration_us.count() / n << " us"
              << std::endl;
    uthread::unlock(&print_mutex);
}

void prepare_data(int32_t seed = 0) {
    std::default_random_engine re(seed);
    for (size_t i = 0; i < InsertCount; i++) {
        pairs.push_back(random_pair(re));
    }
}

void test_stl_insert(STLHashMap &map) {
    for (const auto p : pairs) {
        map.insert(p);
    }
}

void test_stl_find_success(STLHashMap &map) {
    for (const auto p : pairs) {
        auto it = map.find(p.first);
        ASSERT(it != map.end());
        ASSERT(it->second == p.second);
    }
}

void test_insert(HashMap &map) {
    RootDereferenceScope scope;
    for (const auto p : pairs) {
        ASSERT(map.put(p.first, p.second, scope));
    }
}

void test_insert_async(HashMap &map) {
    RootDereferenceScope scope;
    struct Context {
        HashMap *hash_map;
        const std::pair<Key, Value> *pair;
        size_t *pn_finished;
        HashMap *get_hash_map() { return hash_map; }
        const Key *get_key() const { return &(pair->first); }
        Value get_value() const { return pair->second; }
        void make_result(bool r) {
            ASSERT(r);
            (*pn_finished)++;
        }
    };
    using Frame = HashMap::PutFrame<Context>;
    size_t n_finished = 0;
    async::for_range<true>(scope, 0, pairs.size(), [&](size_t i) {
        const auto &p = pairs[i];
        Frame frame;
        frame.pn_finished = &n_finished;
        frame.hash_map = &map;
        frame.pair = &p;
        frame.init();
        return frame;
    });
    ASSERT(n_finished == pairs.size());
}

void test_find_success(HashMap &map) {
    RootDereferenceScope scope;
    for (const auto p : pairs) {
        Value val;
        ASSERT(map.get(p.first, &val, scope));
        ASSERT(val == p.second);
    }
}

void test_find_success_async(HashMap &map) {
    RootDereferenceScope scope;
    struct Context {
        HashMap *hash_map;
        const Key *key;
        const Value *expected;
        size_t *pn_finished;
        HashMap *get_hash_map() { return hash_map; }
        const Key *get_key() const { return key; }
        void make_result(const Value *v) {
            ASSERT(*v == *expected);
            (*pn_finished)++;
        }
    };
    using Frame = HashMap::GetFrame<Context>;
    size_t n_finished = 0;
    async::for_range(scope, 0, pairs.size(), [&](size_t i) {
        const auto &p = pairs[i];
        Frame frame;
        frame.expected = &(p.second);
        frame.pn_finished = &n_finished;
        frame.hash_map = &map;
        frame.key = &(p.first);
        return frame;
    });
    ASSERT(n_finished == pairs.size());
}

void test_remove_success_async(HashMap &map) {
    RootDereferenceScope scope;
    struct Context {
        HashMap *hash_map;
        const Key *key;
        size_t *pn_finished;
        HashMap *get_hash_map() { return hash_map; }
        const Key *get_key() const { return key; }
        void make_result(bool r) {
            ASSERT(r);
            (*pn_finished)++;
        }
    };
    using Frame = HashMap::RemoveFrame<Context>;
    size_t n_finished = 0;
    async::for_range<true>(scope, 0, pairs.size(), [&](size_t i) {
        const auto &p = pairs[i];
        Frame frame;
        frame.pn_finished = &n_finished;
        frame.hash_map = &map;
        frame.key = &(p.first);
        frame.init();
        return frame;
    });
    ASSERT(n_finished == pairs.size());
    ASSERT(map.size == 0);
}

void test_remove_success(HashMap &map) {
    RootDereferenceScope scope;
    for (const auto p : pairs) {
        ASSERT(map.remove(p.first, scope));
    }
    ASSERT(map.size == 0);
}

void test_concurrent_insert(HashMap &map) {
    size_t step = (pairs.size() + NThreads - 1) / NThreads;
    step = (step + 7) & (~7);  // aligned to cache line
    std::function<void(size_t)> fn = [&](size_t tid) {
        size_t start = step * tid;
        size_t end = std::min(step * (tid + 1), pairs.size());
        ON_MISS_BEGIN
            uthread::yield();
        ON_MISS_END
        RootDereferenceScope scope;
        for (size_t i = start; i < end; i++) {
            ASSERT(
                map.put(pairs[i].first, pairs[i].second, __on_miss__, scope));
        }
    };
    uthread::fork_join(NThreads, fn);
}

void test_concurrent_insert_async(HashMap &map) {
    struct Context {
        HashMap *hash_map;
        const std::pair<Key, Value> *pair;
        size_t *pn_finished;
        HashMap *get_hash_map() { return hash_map; }
        const Key *get_key() const { return &(pair->first); }
        Value get_value() const { return pair->second; }
        void make_result(bool r) {
            ASSERT(r);
            (*pn_finished)++;
        }
    };
    using Frame = HashMap::PutFrame<Context>;
    size_t step = (pairs.size() + NThreads - 1) / NThreads;
    step = (step + 7) & (~7);  // aligned to cache line
    std::function<void(size_t)> fn = [&](size_t tid) {
        size_t start = step * tid;
        size_t end = std::min(step * (tid + 1), pairs.size());
        RootDereferenceScope scope;
        size_t n_finished = 0;
        async::for_range<true>(scope, start, end, [&](size_t i) {
            const auto &p = pairs[i];
            Frame frame;
            frame.pn_finished = &n_finished;
            frame.hash_map = &map;
            frame.pair = &p;
            frame.init();
            return frame;
        });
        ASSERT(start + n_finished == std::max(start, end));
    };
    uthread::fork_join(NThreads, fn);
}

void test_concurrent_find(HashMap &map) {
    size_t step = (pairs.size() + NThreads - 1) / NThreads;
    step = (step + 7) & (~7);  // aligned to cache line
    std::function<void(size_t)> fn = [&](size_t tid) {
        size_t start = step * tid;
        size_t end = std::min(step * (tid + 1), pairs.size());
        RootDereferenceScope scope;
        ON_MISS_BEGIN
            uthread::yield();
        ON_MISS_END
        for (size_t i = start; i < end; i++) {
            Value val;
            ASSERT(map.get(pairs[i].first, &val, __on_miss__, scope));
            ASSERT(val == pairs[i].second);
        }
    };
    uthread::fork_join(NThreads, fn);
}

void test_concurrent_find_async(HashMap &map) {
    size_t step = (pairs.size() + NThreads - 1) / NThreads;
    step = (step + 7) & (~7);  // aligned to cache line
    std::function<void(size_t)> fn = [&](size_t tid) {
        size_t start = step * tid;
        size_t end = std::min(step * (tid + 1), pairs.size());
        size_t n_finished = 0;
        RootDereferenceScope scope;
        struct Context {
            HashMap *hash_map;
            const Key *key;
            const Value *expected;
            size_t *pn_finished;
            HashMap *get_hash_map() { return hash_map; }
            const Key *get_key() const { return key; }
            void make_result(const Value *v) {
                ASSERT(*v == *expected);
                (*pn_finished)++;
            }
        };
        using Frame = HashMap::GetFrame<Context>;
        async::for_range(scope, start, end, [&](size_t i) {
            const auto &p = pairs[i];
            Frame frame;
            frame.expected = &(p.second);
            frame.pn_finished = &n_finished;
            frame.hash_map = &map;
            frame.key = &(p.first);
            return frame;
        });
        ASSERT(start + n_finished == std::max(start, end));
    };
    uthread::fork_join(NThreads, fn);
}

void test_concurrent_remove_success_async(HashMap &map) {
    struct Context {
        HashMap *hash_map;
        const Key *key;
        size_t *pn_finished;
        HashMap *get_hash_map() { return hash_map; }
        const Key *get_key() const { return key; }
        void make_result(bool r) {
            ASSERT(r);
            (*pn_finished)++;
        }
    };
    using Frame = HashMap::RemoveFrame<Context>;

    size_t step = (pairs.size() + NThreads - 1) / NThreads;
    step = (step + 7) & (~7);  // aligned to cache line
    std::function<void(size_t)> fn = [&](size_t tid) {
        size_t start = step * tid;
        size_t end = std::min(step * (tid + 1), pairs.size());
        RootDereferenceScope scope;
        size_t n_finished = 0;
        async::for_range<true>(scope, start, end, [&](size_t i) {
            const auto &p = pairs[i];
            Frame frame;
            frame.pn_finished = &n_finished;
            frame.hash_map = &map;
            frame.key = &(p.first);
            frame.init();
            return frame;
        });
        ASSERT(start + n_finished == std::max(start, end));
    };
    uthread::fork_join(NThreads, fn);
    ASSERT(map.size == 0);
}

void test_concurrent_remove_success(HashMap &map) {
    size_t step = (pairs.size() + NThreads - 1) / NThreads;
    step = (step + 7) & (~7);  // aligned to cache line
    std::function<void(size_t)> fn = [&](size_t tid) {
        size_t start = step * tid;
        size_t end = std::min(step * (tid + 1), pairs.size());
        RootDereferenceScope scope;
        ON_MISS_BEGIN
            uthread::yield();
        ON_MISS_END
        for (size_t i = start; i < end; i++) {
            ASSERT(map.remove(pairs[i].first, scope));
        }
    };
    uthread::fork_join(NThreads, fn);
}

int main(int argc, const char *const argv[]) {
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file>" << std::endl;
        return -1;
    }
    rdma::Configure config;
    config.from_file(argv[1]);
    runtime_init(config);

    size_t final_map_size;

    {
        STLHashMap map;
        prepare_data();
        time_for([&] { test_stl_insert(map); }, "STL insert", InsertCount);
        ASSERT(map.size() <= InsertCount);
        final_map_size = map.size();
        time_for([&] { test_stl_find_success(map); }, "STL find success",
                 InsertCount);
    }

    {
        HashMap map(int(log2(InsertCount)) + 4);
        time_for([&] { test_insert(map); }, "insert", InsertCount);
        ASSERT(map.size == final_map_size);
        time_for([&] { test_find_success(map); }, "find success", InsertCount);
        time_for([&] { test_remove_success(map); }, "remove success ",
                 InsertCount);
    }

    {
        HashMap map(int(log2(InsertCount)) + 4);
        time_for([&] { test_insert_async(map); }, "insert async", InsertCount);
        ASSERT(map.size == final_map_size);
        time_for([&] { test_find_success_async(map); }, "find async",
                 InsertCount);
        time_for([&] { test_remove_success_async(map); }, "remove async",
                 InsertCount);
    }

    {
        HashMap map(int(log2(InsertCount)) + 4);
        profile::reset_all();
        profile::start_work();
        time_for([&] { test_concurrent_insert(map); }, "concurrent insert",
                 InsertCount / uthread::get_worker_count());
        ASSERT(map.size == final_map_size);
        time_for([&] { test_concurrent_find(map); }, "concurrent find",
                 InsertCount / uthread::get_worker_count());
        time_for([&] { test_concurrent_remove_success(map); },
                 "concurrent remove",
                 InsertCount / uthread::get_worker_count());
        profile::end_work();
        profile::print_profile_data();
    }

    {
        HashMap map(int(log2(InsertCount)) + 4);
        profile::reset_all();
        profile::start_work();
        time_for([&] { test_concurrent_insert_async(map); },
                 "concurrent async insert", InsertCount);
        ASSERT(map.size == final_map_size);
        time_for([&] { test_concurrent_find_async(map); },
                 "concurrent find async",
                 InsertCount / uthread::get_worker_count());
        time_for([&] { test_concurrent_remove_success_async(map); },
                 "concurrent remove async",
                 InsertCount / uthread::get_worker_count());
        profile::end_work();
        profile::print_profile_data();
    }

    runtime_destroy();

    return 0;
}
