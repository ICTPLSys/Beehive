#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <vector>

#include "cache/cache.hpp"
#include "utils/control.hpp"
#include "utils/fork_join.hpp"

using namespace FarLib;

using Value = std::array<std::byte, 4096 - allocator::BlockHeadSize>;

constexpr uint64_t WorkingSetSize = 8ULL << 30;
constexpr uint32_t CPUFreqMHz = 2800;

using ArrayType = std::vector<UniqueFarPtr<Value>>;

static inline void my_delay_cycles(int32_t cycles) {
    uint64_t start = __rdtsc();
    while (__rdtsc() < start + cycles);
}

void do_work(size_t nthreads, size_t delay_ns) {
    Cache::get_default()->invoke_eviction();
    const uint64_t NEntries = WorkingSetSize / sizeof(Value) / nthreads;
    std::vector<ArrayType> fm_arrays(nthreads);
    for (size_t i = 0; i < nthreads; i++) {
        RootDereferenceScope scope;
        fm_arrays[i].resize(NEntries);
        for (auto &p : fm_arrays[i]) {
            p.allocate_lite_uninitialized(scope);
        }
    }

    bool warm_up = true;

    std::function<void(size_t)> fn_bench = [&](size_t tid) {
        RootDereferenceScope scope;
        auto &fm_array = fm_arrays[tid];
        auto cache = FarLib::Cache::get_default();
        for (uint64_t i = 0; i < fm_array.size(); i++) {
            cache->prefetch(fm_array[i].obj(), scope);
            if (!warm_up) {
                my_delay_cycles(delay_ns / 1000.0 * CPUFreqMHz);
            }
        }
    };
    printf("warm up...\n");
    uthread::fork_join(nthreads, fn_bench);
    warm_up = false;
    // profile::reset_all();
    // profile::start_work();
    // profile::thread_start_work();
    for (size_t i = 0; i < 2; i++) {
        auto start_ts = std::chrono::steady_clock::now();
        uthread::fork_join(nthreads, fn_bench);
        auto end_ts = std::chrono::steady_clock::now();
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                           end_ts - start_ts)
                           .count();
        printf("nthreads: %lu\tdelay: %lu\ttime: %lu us\tMpps: %lf\n", nthreads,
               delay_ns, time_us, (double)(NEntries * nthreads) / time_us);
    }
    // profile::thread_end_work();
    // profile::end_work();
    // profile::print_profile_data();
}

int main(int argc, const char *argv[]) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <cfg_file>" << std::endl;
        return -EINVAL;
    }
    rdma::Configure config;
    config.from_file(argv[1]);
    runtime_init(config);

    const size_t DelayNS[] = {0, 100, 200, 400, 800, 1600, 3200, 6400};
    for (size_t delay_ns : DelayNS) {
        for (size_t nthread = 8; nthread <= 8; nthread++) {
            do_work(nthread, delay_ns);
        }
    }

    runtime_destroy();

    return 0;
}
