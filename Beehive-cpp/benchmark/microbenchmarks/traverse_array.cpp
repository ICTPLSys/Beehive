#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <vector>

#include "cache/accessor.hpp"
#include "cache/cache.hpp"
#include "cache/handler.hpp"
#include "utils/control.hpp"
#include "utils/fork_join.hpp"

using namespace Beehive;

#define DEFINE_DATA_TYPE(width)                        \
    struct Data_##width {                              \
        uint8_t buf[width - allocator::BlockHeadSize]; \
    };                                                 \
    using Data_t = Data_##width;

DEFINE_DATA_TYPE(4096);

constexpr uint64_t kCacheSize = 512ULL << 20;
// constexpr uint64_t kCacheSize = 10ULL << 30;
constexpr uint64_t kFarMemSize = 10ULL << 30;
constexpr uint64_t kWorkingSetSize = 8ULL << 30;
constexpr uint32_t kCPUFreqMHz = 2800;

#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))

template <int64_t N>
inline void read_raw_array(uint8_t *ptr) {
    if constexpr (N >= 8) {
        ACCESS_ONCE(*((uint64_t *)ptr));
        read_raw_array<N - 8>(ptr + 8);
    } else if constexpr (N >= 4) {
        ACCESS_ONCE(*((uint32_t *)ptr));
        read_raw_array<N - 4>(ptr + 4);
    } else if constexpr (N >= 2) {
        ACCESS_ONCE(*((uint16_t *)ptr));
        read_raw_array<N - 2>(ptr + 2);
    } else if constexpr (N == 1) {
        ACCESS_ONCE(*((uint8_t *)ptr));
        read_raw_array<N - 1>(ptr + 1);
    }
}

using ArrayType = std::vector<UniqueFarPtr<Data_t>>;

inline void read_array_element_api(const ArrayType &fm_array) {
    RootDereferenceScope scope;
    for (uint64_t i = 0; i < fm_array.size(); i++) {
        fm_array[i].access(scope);
    }
}

static inline void my_delay_cycles(int32_t cycles) {
    uint64_t start = __rdtsc();
    while (__rdtsc() < start + cycles);
}

void do_work(size_t nthreads, size_t delay_ns) {
    Cache::get_default()->invoke_eviction();
    const uint64_t NEntries = kWorkingSetSize / sizeof(Data_t) / nthreads;
    std::vector<ArrayType> fm_arrays(nthreads);
    for (size_t i = 0; i < nthreads; i++) {
        RootDereferenceScope scope;
        fm_arrays[i].resize(NEntries);
        for (auto &p : fm_arrays[i]) {
            p.allocate_lite_uninitialized(scope);
        }
    }

    // Flush cache.
    std::function<void(size_t)> fn_flush = [&](size_t tid) {
        read_array_element_api(fm_arrays[tid]);
    };
    uthread::fork_join(nthreads, fn_flush);
    uthread::fork_join(nthreads, fn_flush);

    std::function<void(size_t)> fn_bench = [&](size_t tid) {
        RootDereferenceScope scope;
        uint64_t i = 0;
        uint64_t prefetch_idx = 0;
        auto &fm_array = fm_arrays[tid];
        ON_MISS_BEGIN
            auto cache = Beehive::Cache::get_default();
            auto &j = prefetch_idx;
            cache::OnMissScope oms(__entry__, &scope);
            for (j = std::max(i + 1, j); j < fm_array.size() && j <= i + 1024;
                 j++) {
                cache->prefetch(fm_array[j].obj(), oms);
                if (j % 8 == 0 && cache::check_fetch(__entry__, __ddl__)) break;
            }
        ON_MISS_END
        for (i = 0; i < fm_array.size(); i++) {
            auto acc = fm_array[i].access(__on_miss__, scope);
            read_raw_array<sizeof(Data_t)>((uint8_t *)acc->buf);
            my_delay_cycles(delay_ns / 1000.0 * kCPUFreqMHz);
        }
    };
    // profile::reset_all();
    // profile::start_work();
    // profile::thread_start_work();
    auto start_ts = std::chrono::steady_clock::now();
    uthread::fork_join(nthreads, fn_bench);
    auto end_ts = std::chrono::steady_clock::now();
    // profile::thread_end_work();
    // profile::end_work();
    // profile::print_profile_data();

    auto time_us =
        std::chrono::duration_cast<std::chrono::microseconds>(end_ts - start_ts)
            .count();
    std::cout << delay_ns << ", " << nthreads << ", " << time_us << std::endl;
}

int main(int argc, const char *argv[]) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <cfg_file>" << std::endl;
        return -EINVAL;
    }
    rdma::Configure config;
    config.from_file(argv[1]);
    runtime_init(config);

    const size_t DelayNS[] = {0,    300,  600,  900,   1200,  1600,
                              1800, 2000, 3000, 4000,  5000,  6000,
                              7000, 8000, 9000, 10000, 11000, 1200};
    for (size_t delay_ns : DelayNS) {
        for (size_t nthread = 8; nthread <= 8; nthread++) {
            do_work(nthread, delay_ns);
        }
    }

    runtime_destroy();

    return 0;
}
