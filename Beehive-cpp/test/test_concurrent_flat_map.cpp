#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "data_structure/ConcurrentFlatHashmap.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"

using namespace Beehive;

static constexpr size_t MapEntriesShift = 25;
static constexpr size_t InitElemCount = 1024;
static constexpr size_t TestElemCount = 1024 * 1024 * 16 - InitElemCount;
static constexpr size_t WorkerCount = 8;
static constexpr size_t ThreadCount = 32;
static_assert(TestElemCount + InitElemCount <= (1ULL << MapEntriesShift));
static_assert(ThreadCount >= WorkerCount);
using KeyType = int32_t;
using ValueType = int32_t;
using MapType = ConcurrentFlatMap<KeyType, ValueType>;
using StdMapType = std::unordered_map<KeyType, ValueType>;

struct KeyGenerator {
private:
    std::unordered_set<KeyType> key_set;
    std::vector<KeyType> keys;
    uthread::Mutex mtx;

public:
    void insert_key(const KeyType& k) {
        if (key_set.emplace(k).second) {
            keys.push_back(k);
        }
    }
    KeyType random_gen() {
        uthread::lock(&mtx);
        auto r = rand();
        KeyType res;
        if (r % 2) {
            res = keys[r % keys.size()];
        } else {
            insert_key(r);
            res = r;
        }
        uthread::unlock(&mtx);
        return res;
    }
};

void init(KeyGenerator& generator, MapType& map) {
    for (int i = 0; i < InitElemCount; i++) {
        ValueType val = rand();
        map.put(std::forward<KeyType>(i), std::forward<ValueType>(val),
                nullptr);
        generator.insert_key(i);
    }
}
void init(KeyGenerator& generator, MapType& map, StdMapType& std_map) {
    for (int i = 0; i < InitElemCount; i++) {
        ValueType val = rand();
        map.put(i, val, nullptr);
        std_map.insert({i, val});
        ValueType v;
        map.get(
            i, [&](const KeyType& key, const ValueType& val) { v = val; },
            nullptr);
        generator.insert_key(i);
    }
}

enum Op { GET = 0, PUT, REMOVE, UPDATE, COUNT };

void do_random_op(KeyGenerator& generator, MapType& map) {
    KeyType k = generator.random_gen();
    switch (rand() % COUNT) {
    case GET: {
        ValueType v = -1;
        map.get(
            k, [&](const KeyType& key, const ValueType& val) { v = val; },
            nullptr);
        break;
    }
    case PUT: {
        ValueType v = rand();
        bool res = map.put(k, v, nullptr);
        break;
    }
    case REMOVE: {
        bool res = map.remove(k, nullptr);
        break;
    }
    case UPDATE: {
        map.update(
            k, [&](const KeyType& k, ValueType& v) { v = 123456; }, nullptr);
        break;
    }
    default:
        break;
    }
}
void single_thread_test() {
    std::cout << "single thread test" << std::endl;
    MapType map(MapEntriesShift);
    StdMapType std_map;
    KeyGenerator generator;
    init(generator, map, std_map);
    for (int i = 0; i < TestElemCount; i++) {
        KeyType k = generator.random_gen();
        switch (rand() % COUNT) {
        case GET: {
            ValueType v = -1;
            map.get(
                k, [&](const KeyType& key, const ValueType& val) { v = val; },
                nullptr);
            if (!(v == -1 && std_map.count(k) == 0 ||
                  std_map.count(k) != 0 && v == std_map[k])) {
                std::cout << "v = " << v << std::endl;
                std::cout << "std map = " << std_map[k] << std::endl;
                abort();
            }
            break;
        }
        case PUT: {
            ValueType v = rand();
            bool res = map.put(k, v, nullptr);
            std_map[k] = v;
            break;
        }
        case REMOVE: {
            bool res = map.remove(k, nullptr);
            if (!(res && std_map.count(k) > 0 ||
                  !res && std_map.count(k) == 0)) {
                std::cout << "res: " << res << std::endl;
                std::cout << "std map count: " << std_map.count(k) << std::endl;
                abort();
            }
            std_map.erase(k);
            break;
        }
        case UPDATE: {
            map.update(
                k, [&](const KeyType& k, ValueType& v) { v = 123456; },
                nullptr);
            std_map[k] = 123456;
            break;
        }
        default:
            break;
        }
    }
    ASSERT(map.size() == std_map.size());
    std::cout << "single thread test end" << std::endl;
}
void test() {
    std::cout << "test" << std::endl;
    using namespace uthread;
    ConcurrentFlatMap<int32_t, int32_t> map(MapEntriesShift);
    KeyGenerator generator;
    init(generator, map);
    parallel_for<1>(ThreadCount, TestElemCount,
                    [&](int i) { do_random_op(generator, map); });
    std::cout << "test end" << std::endl;
}

void multithread_test() {
    std::cout << "multithread test" << std::endl;
    using namespace uthread;
    MapType map(MapEntriesShift);
    std::vector<std::pair<KeyType, ValueType>> datas;
    std::unordered_set<KeyType> sets;
    for (int i = 0; i < TestElemCount; i++) {
        while (true) {
            auto r = rand();
            if (sets.emplace(r).second) {
                datas.push_back({r, rand()});
                break;
            }
        }
    }
    parallel_for<1>(ThreadCount, TestElemCount, [&](int i) {
        map.put(datas[i].first, datas[i].second, nullptr);
    });
    for (auto [k, v] : datas) {
        map.get(
            k,
            [&](const KeyType& key, const ValueType& val) { ASSERT(v == val); },
            nullptr);
    }
    std::cout << "multithread test end" << std::endl;
}

int main() {
    srand(0);
    uthread::runtime_init(WorkerCount);
    // single_thread_test();
    // test();
    multithread_test();
    uthread::runtime_destroy();
    return 0;
}