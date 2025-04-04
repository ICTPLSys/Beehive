#include <chrono>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "papi.h"
#include "utils/parallel.hpp"
#include "utils/uthreads.hpp"

using namespace FarLib;

/*
    local workload:
    generate several random strings,
    put them into std::set,
    sort and output to vector
*/
constexpr size_t StringCount = 256 * 1024;
constexpr size_t StringLength = 64;
constexpr size_t CoreCount = 16;
size_t UThreadCount = 8 * CoreCount;
constexpr size_t RepeatTime = 8;

class RandomStringGenerator {
    std::default_random_engine random_engine;
    std::uniform_int_distribution<char> dist;

public:
    RandomStringGenerator(uint32_t seed)
        : random_engine(seed), dist('a', 'z') {}

    std::string generate() {
        std::string str;
        str.resize(StringLength);
        for (size_t i = 0; i < StringLength; i++) {
            str[i] = dist(random_engine);
        }
        return str;
    }
};

template <bool Profile>
std::vector<std::string> workload(uint64_t& work_cycles,
                                  size_t yield_interval) {
    uint64_t total_cycles = 0;
    uint64_t cycles_start = get_cycles();
    RandomStringGenerator generator(0);
    std::set<std::string> str_set;
    for (size_t i = 0; i < StringCount; i++) {
        str_set.insert(generator.generate());
        if (yield_interval != 0) {
            if (i % yield_interval == 0) {
                if constexpr (Profile) {
                    total_cycles += get_cycles() - cycles_start;
                }
                uthread::yield();
                if constexpr (Profile) {
                    cycles_start = get_cycles();
                }
            }
        }
    }
    std::vector<std::string> output;
    output.reserve(str_set.size());
    size_t i = 0;
    for (auto& s : str_set) {
        output.push_back(std::move(s));
        i++;
        if (yield_interval != 0) {
            if (i % yield_interval == 0) {
                if constexpr (Profile) {
                    total_cycles += get_cycles() - cycles_start;
                }
                uthread::yield();
                if constexpr (Profile) {
                    cycles_start = get_cycles();
                }
            }
        }
    }
    total_cycles += get_cycles() - cycles_start;
    work_cycles = total_cycles;
    return output;
}

void run_local(size_t yield_interval) {
    std::vector<uint64_t> work_cycles(UThreadCount);
    std::vector<std::vector<std::string>> outputs(UThreadCount);
    std::function<void(size_t)> workload_fn = [&](size_t i) {
        outputs[i] = workload<true>(work_cycles[i], yield_interval);
    };

    constexpr size_t PAPIEventCount = 5;
    int papi_events[PAPIEventCount] = {PAPI_L1_DCM, PAPI_L1_ICM, PAPI_L2_DCM,
                                       PAPI_L2_ICM, PAPI_L3_TCM};
    int papi_event_set = PAPI_NULL;
    long long papi_values[PAPIEventCount];
    ASSERT(PAPI_create_eventset(&papi_event_set) == PAPI_OK);
    ASSERT(PAPI_add_events(papi_event_set, papi_events, PAPIEventCount) ==
           PAPI_OK);
    ASSERT(PAPI_start(papi_event_set) == PAPI_OK);

    auto start = std::chrono::high_resolution_clock::now();
    auto start_cycles = get_cycles();
    uthread::fork_join(UThreadCount, workload_fn);
    auto end_cycles = get_cycles();
    auto end = std::chrono::high_resolution_clock::now();

    ASSERT(PAPI_stop(papi_event_set, papi_values) == PAPI_OK);
    // PAPI_read(papi_event_set);
    // PAPI_reset(papi_event_set);
    // PAPI_accum(papi_event_set, papi_values);

    std::chrono::duration<double, std::milli> duration = end - start;
    uint64_t total_work_cycles = 0;
    for (auto wc : work_cycles) total_work_cycles += wc;
    std::cout << std::setw(16) << UThreadCount;
    std::cout << std::setw(16) << yield_interval;
    std::cout << std::setw(16) << duration.count();
    double total_cycles = (double)(end_cycles - start_cycles) * CoreCount;
    std::cout << std::setw(16) << total_cycles;
    std::cout << std::setw(16) << (double)total_work_cycles;
    std::cout << std::setw(16) << total_cycles - total_work_cycles;
    for (size_t i = 0; i < PAPIEventCount; i++) {
        std::cout << std::setw(16) << papi_values[i];
    }
    std::cout << std::endl;
    for (size_t i = 1; i < UThreadCount; i++) {
        ASSERT(outputs[i] == outputs[0]);
    }
}

void run_local_all() {
    std::string titles[] = {
        "uthread count",  "yield internal", "runtime (ms)",
        "total cycles",   "work cycles",    "uthread overhead",
        "L1 dcache miss", "L1 icache miss", "L2 dcache miss",
        "L2 icache miss", "L3 cache miss"};
    for (auto& t : titles) {
        std::cout << std::setw(16) << t;
    }
    std::cout << std::endl;

#if 1  // uthread count
    for (size_t i = 0; i < 3; i++) {
        for (UThreadCount = CoreCount; UThreadCount <= 64 * CoreCount; UThreadCount *= 2) {
            run_local(1024);
        }
    }
#endif

#if 0  // yield frequency
    UThreadCount = 8 * CoreCount;
    for (size_t i = 0; i < 3; i++) {
        run_local(0);
        for (size_t yi = StringCount / 4; yi >= 4; yi /= 4) {
            run_local(yi);
        }
    }
#endif
}

int main() {
    ASSERT(PAPI_library_init(PAPI_VER_CURRENT) == PAPI_VER_CURRENT);
    uthread::runtime_init(CoreCount);
    run_local_all();
    uthread::runtime_destroy();
    return 0;
}