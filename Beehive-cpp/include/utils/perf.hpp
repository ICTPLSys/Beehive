#pragma once
extern "C" {
#include <papi.h>
}

#include <chrono>
#include <iostream>

#include "utils/cpu_cycles.hpp"
#include "utils/debug.hpp"
#include "utils/uthreads.hpp"

namespace FarLib {
// this value may modified when kernel changed.
static constexpr size_t TID_OFFS = 0x2d0;
static constexpr bool RESULT_PER_TH = false;
struct pthread_fake {
    char padding[TID_OFFS];
    pid_t tid;
};
struct PerfResult {
    double runtime_ms;
    size_t total_cycles;
    size_t instructions;
    size_t l2_cache_miss;
    size_t l3_cache_miss;

    void print() const {
        std::cout << std::internal << std::setw(32) << "perf result"
                  << std::endl;
        std::cout << std::right << std::string(32, '-') << std::endl;
        std::cout << std::setw(16) << "runtime: " << std::setw(16) << std::fixed
                  << runtime_ms << " ms" << std::endl;
        std::cout << std::setw(16) << "cycles: " << std::setw(16)
                  << total_cycles << std::endl;
        std::cout << std::setw(16) << "Insttructions: " << std::setw(16)
                  << instructions << std::endl;
        std::cout << std::setw(16) << "L2 miss: " << std::setw(16)
                  << l2_cache_miss << std::endl;
        std::cout << std::setw(16) << "L3 miss: " << std::setw(16)
                  << l3_cache_miss << std::endl;
    }
};

template <typename Fn>
PerfResult perf_profile(Fn&& fn) {
    constexpr size_t PAPIEventCount = 3;
    int papi_events[PAPIEventCount] = {PAPI_TOT_INS, PAPI_L2_TCM, PAPI_L3_TCM};
    std::vector<int> events;
    long long papi_values[PAPIEventCount] = {0};
    size_t worker_num = uthread::get_worker_count();
    for (int i = 0; i < worker_num; i++) {
        auto pthread_id = uthread::es->get_tids()[i];
        auto id = ((struct pthread_fake*)pthread_id)->tid;
        int papi_event_set = PAPI_NULL;
        ASSERT(PAPI_create_eventset(&papi_event_set) == PAPI_OK);
        ASSERT(PAPI_add_events(papi_event_set, papi_events, PAPIEventCount) ==
               PAPI_OK);
        ASSERT(PAPI_attach(papi_event_set, id) == PAPI_OK);
        events.push_back(papi_event_set);
    }
    for (auto e : events) {
        ASSERT(PAPI_start(e) == PAPI_OK);
    }

    auto start = std::chrono::high_resolution_clock::now();
    auto start_cycles = get_cycles();

    fn();

    auto end_cycles = get_cycles();
    auto end = std::chrono::high_resolution_clock::now();

    for (auto e : events) {
        long long th_values[PAPIEventCount] = {0};
        ASSERT(PAPI_stop(e, th_values) == PAPI_OK);
        ASSERT(PAPI_detach(e) == PAPI_OK);
        ASSERT(PAPI_cleanup_eventset(e) == PAPI_OK);
        ASSERT(PAPI_destroy_eventset(&e) == PAPI_OK);
        for (int i = 0; i < PAPIEventCount; i++) {
            papi_values[i] += th_values[i];
        }
        if constexpr (RESULT_PER_TH) {
            PerfResult result;
            result.instructions = th_values[0];
            result.l2_cache_miss = th_values[1];
            result.l3_cache_miss = th_values[2];
            std::cout << "-------------------" << std::endl;
            result.print();
            std::cout << "-------------------" << std::endl;
        }
    }

    std::chrono::duration<double, std::milli> duration = end - start;

    PerfResult result;
    result.runtime_ms = duration.count();
    result.total_cycles = end_cycles - start_cycles;
    result.instructions = papi_values[0];
    result.l2_cache_miss = papi_values[1];
    result.l3_cache_miss = papi_values[2];
    return result;
}

inline void perf_init() {
    ASSERT(PAPI_library_init(PAPI_VER_CURRENT) == PAPI_VER_CURRENT);
}

}  // namespace FarLib
