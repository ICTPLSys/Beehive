#include <iostream>
#include <random>
#include <unordered_map>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/hashtable.hpp"
#include "test/fixed_size_string.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/timer.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

using str_key_t = FixedSizeString<16>;
using str_value_t = FixedSizeString<64>;

constexpr int OBJ_N = 1024 * 1024;
constexpr double CPU_FREQ_GHZ = 2.8;

void profile_single_find() {
    HashTable<str_key_t, str_value_t, OBJ_N> test_map;
    std::unordered_map<str_key_t, str_value_t> compare_map;
    std::vector<str_key_t> keys;
    for (int i = 0; i < OBJ_N; i++) {
        auto k = str_key_t::random();
        auto v = str_value_t::random();
        test_map.insert(k, v);
        compare_map[k] = v;
        keys.push_back(k);
    }

    std::cout << "map size: " << compare_map.size() << std::endl;
    std::minstd_rand0 re(0);
    std::shuffle(keys.begin(), keys.end(), re);
    size_t avg_for_test = 0, avg_for_cmp = 0;
    CpuTimer timer;
    for (auto& k : keys) {
        timer.setTimer();
        auto acc = test_map.get(k);
        timer.stopTimer();
        avg_for_test += timer.getDuration();
        timer.setTimer();
        auto v = compare_map[k];
        timer.stopTimer();
        avg_for_cmp += timer.getDuration();
        if (*acc != v) {
            ERROR("find error");
        }
    }
    avg_for_test /= keys.size();
    avg_for_cmp /= keys.size();
    std::cout << "average remote hashtable access: "
              << avg_for_test / CPU_FREQ_GHZ << "ns" << std::endl;
    std::cout << "average std hashtable access: " << avg_for_cmp / CPU_FREQ_GHZ
              << "ns" << std::endl;
}

int main(int argc, char* argv[]) {
    srand(0);
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file> " << std::endl;
        return -1;
    }
    Configure config;
    config.from_file(argv[1]);
    Beehive::runtime_init(config);
    profile_single_find();
    Beehive::runtime_destroy();
    return 0;
}