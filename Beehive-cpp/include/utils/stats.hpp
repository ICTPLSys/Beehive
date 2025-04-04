#pragma once
#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <tuple>
#include <utility>

#include "cpu_cycles.hpp"

namespace FarLib {

namespace profile {

constexpr bool Enabled = false;
constexpr bool TraceRDMA = false;
constexpr bool TraceAlloc = false;
constexpr bool TraceMemoryUsage = false;

namespace enabled {
constexpr bool Evacuation = Enabled || true;
constexpr bool YieldCount = Enabled || true;
constexpr bool OnMissSchedule = Enabled || false;
};  // namespace enabled

struct ProfileData {
    int64_t work_cycles;
    int64_t allocate_cycles;
    int64_t post_fetch_cycles;
    int64_t post_fetch_retry_count;
    int64_t poll_count;
    int64_t poll_cycles;
    int64_t start_yield_cycle;
    int64_t yield_cycles;
    int64_t data_miss_count;
    int64_t yield_count;
    int64_t mark_cycles;
    int64_t evict_cycles;
    int64_t evacuation_count;
    int64_t fork_join_cycles;
    int64_t check_cq_cycles;
    int64_t check_cq_count;
    int64_t mark_count;
    int64_t not_mark_count;
    int64_t on_miss_cycles;
    int64_t prefetch_cycles;

    ProfileData() { reset(); }

    void reset() {
        work_cycles = 0;
        allocate_cycles = 0;
        post_fetch_cycles = 0;
        post_fetch_retry_count = 0;
        poll_cycles = 0;
        start_yield_cycle = 0;
        yield_cycles = 0;
        data_miss_count = 0;
        yield_count = 0;
        mark_cycles = 0;
        evict_cycles = 0;
        evacuation_count = 0;
        fork_join_cycles = 0;
        check_cq_cycles = 0;
        check_cq_count = 0;
        mark_count = 0;
        not_mark_count = 0;
        on_miss_cycles = 0;
        prefetch_cycles = 0;
    }
};

template <typename T, size_t BufferSize>
struct RingBuffer {
    size_t n;
    std::unique_ptr<T[]> buffer;

    void init() { buffer = std::make_unique<T[]>(BufferSize); }

    void add(T value) {
        buffer[n] = value;
        n = (n + 1) % BufferSize;
    }

    template <typename Fn>
    void for_each(Fn &&fn) {
        for (size_t i = n; i < BufferSize; i++) {
            fn(buffer[i]);
        }
        for (size_t i = 0; i < n; i++) {
            fn(buffer[i]);
        }
    }
};

struct AllocTrace {
    enum Operation {
        AllocObject,
        DeallocObject,
        AllocRegion,
        DeallocRegion,
    };
    Operation op;
    uint64_t tsc;
    void *addr;
    size_t bin;
};

struct MemoryUsageTrace {
    int64_t previous_free_size;
    int64_t free_size_modification;
};

struct ThreadLocalProfileData : public ProfileData {
    int64_t work_start_cycle = 0;

    RingBuffer<std::tuple<uint64_t, uint64_t, uint64_t>, 1024> rdma_read_reqs;
    RingBuffer<std::pair<uint64_t, uint64_t>, 1024> rdma_read_wcs;
    RingBuffer<AllocTrace, 4096> alloc_trace;
    RingBuffer<MemoryUsageTrace, 4096> mem_usage_trace;

    ThreadLocalProfileData() {
        if constexpr (TraceRDMA) {
            rdma_read_reqs.init();
            rdma_read_wcs.init();
        }
        if constexpr (TraceAlloc) {
            alloc_trace.init();
        }
        if constexpr (TraceMemoryUsage) {
            mem_usage_trace.init();
        }
        register_thread();
    }

    ~ThreadLocalProfileData() { unregister_thread(); }

    void register_thread();
    void unregister_thread();
};

ThreadLocalProfileData &get_tlpd();
extern ProfileData global_profile_data;
extern bool working;
extern uint64_t global_start_cycles, global_cycles;

void reset_all();

int64_t collect_work_cycles();
int64_t collect_allocate_cycles();
int64_t collect_post_fetch_cycles();
int64_t collect_poll_cycles();
int64_t collect_yield_cycles();
int64_t collect_data_miss_count();
int64_t collect_yield_count();
int64_t collect_mark_cycles();
int64_t collect_evict_cycles();
int64_t collect_check_cq_cycles();
int64_t collect_check_cq_count();
int64_t collect_on_miss_cycles();
int64_t collect_prefetch_cycles();

void print_profile_data();

inline void resume_work(bool suspended) {
    if constexpr (Enabled) {
        if (working && suspended) {
            assert(get_tlpd().work_start_cycle == 0);
            get_tlpd().work_start_cycle = get_cycles();
        }
    }
}
inline bool suspend_work() {
    if constexpr (Enabled) {
        if (working && get_tlpd().work_start_cycle != 0) {
            get_tlpd().work_cycles +=
                get_cycles() - get_tlpd().work_start_cycle;
            get_tlpd().work_start_cycle = 0;
            return true;
        }
    }
    return false;
}
inline void start_work() {
    assert(!working);
    working = true;
    global_start_cycles = get_cycles();
}
inline void end_work() {
    assert(working);
    working = false;
    global_cycles = get_cycles() - global_start_cycles;
}
inline void thread_start_work() { resume_work(true); }
inline void thread_end_work() { suspend_work(); }
inline void start_allocate() {
    if constexpr (Enabled) get_tlpd().allocate_cycles -= get_cycles();
}
inline void end_allocate() {
    if constexpr (Enabled) get_tlpd().allocate_cycles += get_cycles();
}
inline void start_post_fetch() {
    if constexpr (Enabled) {
        get_tlpd().data_miss_count++;
        get_tlpd().post_fetch_cycles -= get_cycles();
    }
}
inline void end_post_fetch() {
    if constexpr (Enabled) get_tlpd().post_fetch_cycles += get_cycles();
}

inline void add_post_retry_count() {
    if constexpr (Enabled) {
        get_tlpd().post_fetch_retry_count++;
    }
}
inline void start_poll() {
    if constexpr (Enabled) get_tlpd().poll_cycles -= get_cycles();
}
inline void end_poll() {
    if constexpr (Enabled) {
        get_tlpd().poll_count++;
        get_tlpd().poll_cycles += get_cycles();
    }
}
inline void start_yield() {
    if constexpr (enabled::YieldCount) {
        get_tlpd().yield_count++;
    }
    if constexpr (Enabled) {
        get_tlpd().start_yield_cycle = get_cycles();
    }
}
inline void end_yield() {
    if constexpr (Enabled) {
        if (get_tlpd().start_yield_cycle != 0) [[likely]] {
            get_tlpd().yield_cycles +=
                get_cycles() - get_tlpd().start_yield_cycle;
            get_tlpd().start_yield_cycle = 0;
        }
    }
}
inline void count_evacuation() {
    if constexpr (enabled::Evacuation) {
        get_tlpd().evacuation_count++;
    }
}

inline void start_on_miss() {
    if constexpr (enabled::OnMissSchedule)
        get_tlpd().on_miss_cycles -= get_cycles();
}
inline void end_on_miss() {
    if constexpr (enabled::OnMissSchedule)
        get_tlpd().on_miss_cycles += get_cycles();
}
inline void start_prefetch() {
    if constexpr (enabled::OnMissSchedule)
        get_tlpd().prefetch_cycles -= get_cycles();
}
inline void end_prefetch() {
    if constexpr (enabled::OnMissSchedule)
        get_tlpd().prefetch_cycles += get_cycles();
}

inline int64_t start_mark() {
    if constexpr (enabled::Evacuation) {
        return get_cycles();
    } else {
        return 0;
    }
}
inline void end_mark(int64_t start_cycles) {
    if constexpr (enabled::Evacuation) {
        get_tlpd().mark_cycles += get_cycles() - start_cycles;
    }
}
inline int64_t start_evict() {
    if constexpr (enabled::Evacuation) {
        return get_cycles();
    } else {
        return 0;
    }
}
inline void end_evict(int64_t start_cycles) {
    if constexpr (enabled::Evacuation) {
        get_tlpd().evict_cycles += get_cycles() - start_cycles;
    }
}
inline void start_fork_join() {
    if constexpr (Enabled) get_tlpd().fork_join_cycles -= get_cycles();
}
inline void end_fork_join() {
    if constexpr (Enabled) get_tlpd().fork_join_cycles += get_cycles();
}

inline void start_check_cq() {
    if constexpr (Enabled) {
        get_tlpd().check_cq_cycles -= get_cycles();
        get_tlpd().check_cq_count++;
    }
}
inline void end_check_cq() {
    if constexpr (Enabled) {
        get_tlpd().check_cq_cycles += get_cycles();
    }
}
inline void count_mark() {
    if constexpr (Enabled) {
        get_tlpd().mark_count++;
    }
}
inline void count_not_mark() {
    if constexpr (Enabled) {
        get_tlpd().not_mark_count++;
    }
}

inline void trace_post_rdma_read(uint64_t wr_id, uint64_t remote_offset) {
    if constexpr (TraceRDMA) {
        get_tlpd().rdma_read_reqs.add({__rdtsc(), wr_id, remote_offset});
    }
}

inline void trace_rdma_read_complete(uint64_t wr_id) {
    if constexpr (TraceRDMA) {
        get_tlpd().rdma_read_wcs.add({__rdtsc(), wr_id});
    }
}

void print_rdma_trace();

inline void trace_alloc(void *p, size_t bin) {
    if constexpr (TraceAlloc) {
        get_tlpd().alloc_trace.add(
            {AllocTrace::AllocObject, __rdtsc(), p, bin});
    }
}
inline void trace_dealloc(void *p, size_t bin) {
    if constexpr (TraceAlloc) {
        get_tlpd().alloc_trace.add(
            {AllocTrace::DeallocObject, __rdtsc(), p, bin});
    }
}
inline void trace_alloc_region(void *p, size_t bin) {
    if constexpr (TraceAlloc) {
        get_tlpd().alloc_trace.add(
            {AllocTrace::AllocRegion, __rdtsc(), p, bin});
    }
}
inline void trace_dealloc_region(void *p, size_t bin) {
    if constexpr (TraceAlloc) {
        get_tlpd().alloc_trace.add(
            {AllocTrace::DeallocRegion, __rdtsc(), p, bin});
    }
}

inline void trace_dealloc_full_region(void *p, size_t bin) {
    if constexpr (TraceAlloc) {
        get_tlpd().alloc_trace.add(
            {AllocTrace::DeallocRegion, __rdtsc(), p, bin});
    }
}

void print_alloc_trace();

inline void trace_free_memory_modification(int64_t previous_free_size,
                                           int64_t modification) {
    if constexpr (TraceMemoryUsage) {
        if (modification != 0) {
            get_tlpd().mem_usage_trace.add({previous_free_size, modification});
        }
    }
}

void print_mem_usage_trace();

}  // namespace profile

}  // namespace FarLib
