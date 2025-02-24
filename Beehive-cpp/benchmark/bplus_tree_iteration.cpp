#include <algorithm>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/bplus_tree_map.hpp"
#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/cpu_cycles.hpp"
#include "utils/debug.hpp"
#include "utils/stats.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

typedef BPlusTreeMap<int, int, 64> Map;

void run(const Map &map, int expected_sum) {
    int sum = 0;
    map.const_iterate([&sum](int k, int v) { sum += v; });
    ASSERT(sum == expected_sum);
}

void run_prefetch(const Map &map, int expected_sum) {
    int sum = 0;
    map.const_iterate_prefetch([&sum](int k, int v) { sum += v; });
    ASSERT(sum == expected_sum);
}

void run_recursion(const Map &map, int expected_sum) {
    int sum = 0;
    map.const_iterate_recursion([&sum](int k, int v) { sum += v; });
    ASSERT(sum == expected_sum);
}

void run(size_t count = 1024 * 1024) {
    std::vector<int> keys;
    std::minstd_rand0 re(0);
    for (int i = 0; i < count; i++) {
        keys.push_back(i);
    }
    std::shuffle(keys.begin(), keys.end(), re);
    Map map;
    for (int k : keys) {
        auto [_, accessor] = map.get_or_insert(k);
        *accessor = k * k;
    }
    ASSERT(map.size() == count);
    int expected_sum = 0;
    for (int i = 0; i < count; i++) {
        expected_sum += i * i;
    }
    // warm up
    run(map, expected_sum);
    for (size_t i = 0; i < 16; i++) {
        while (Cache::get_default()->check_cq());
        {
            uint64_t cycles_start = get_cycles();
            run(map, expected_sum);
            uint64_t cycles_end = get_cycles();
            std::cout << (cycles_end - cycles_start) / 1e6 << "\t";
        }
        while (Cache::get_default()->check_cq());
        {
            uint64_t cycles_start = get_cycles();
            run_prefetch(map, expected_sum);
            uint64_t cycles_end = get_cycles();
            std::cout << (cycles_end - cycles_start) / 1e6 << "\t";
        }
        while (Cache::get_default()->check_cq());
        {
            uint64_t cycles_start = get_cycles();
            run_recursion(map, expected_sum);
            uint64_t cycles_end = get_cycles();
            std::cout << (cycles_end - cycles_start) / 1e6 << std::endl;
        }
    }

    // break down
    {
        std::cout << "break down: origin" << std::endl;
        uint64_t cycles_start = get_cycles();
        run(map, expected_sum);
        uint64_t cycles_end = get_cycles();
        std::cout << "total cycles:" << cycles_end - cycles_start << std::endl;
    }
    {
        std::cout << "break down: manual prefetch" << std::endl;
        uint64_t cycles_start = get_cycles();
        run_prefetch(map, expected_sum);
        uint64_t cycles_end = get_cycles();
        std::cout << "total cycles:" << cycles_end - cycles_start << std::endl;
    }
    {
        std::cout << "break down: recursion prefetch" << std::endl;
        uint64_t cycles_start = get_cycles();
        run_recursion(map, expected_sum);
        uint64_t cycles_end = get_cycles();
        std::cout << "total cycles:" << cycles_end - cycles_start << std::endl;
    }
}

int main(int argc, const char *const argv[]) {
    if (argc != 3) {
        std::cout << "usage: " << argv[0] << " <configure file> <count>"
                  << std::endl;
        return -1;
    }
    Configure config;
    config.from_file(argv[1]);
    int size = std::atoi(argv[2]);
    ASSERT(size > 0);
    Beehive::runtime_init(config);
    run(size);
    Beehive::runtime_destroy();
    return 0;
}
