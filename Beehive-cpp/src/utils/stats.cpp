#include "utils/stats.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <thread>
#include <unordered_map>

#include "async/stream_runner.hpp"
#include "rdma/client.hpp"

namespace Beehive {
namespace async {
std::atomic_int64_t StreamRunnerProfiler::global_total_cycles;
std::atomic_int64_t StreamRunnerProfiler::global_app_cycles;
std::atomic_int64_t StreamRunnerProfiler::global_sched_cycles;
std::atomic_int64_t StreamRunnerProfiler::global_poll_cq_cycles;
}  // namespace async
namespace profile {

std::unordered_map<std::thread::id, ThreadLocalProfileData *> profile_data_map;
thread_local ThreadLocalProfileData tlpd;
__attribute__((noinline)) ThreadLocalProfileData &get_tlpd() {
    asm volatile("" : : : "memory");
    return tlpd;
}
ProfileData global_profile_data;
bool working = false;
uint64_t global_start_cycles = 0, global_cycles = 0;

void ThreadLocalProfileData::register_thread() {
    static std::mutex mtx;
    mtx.lock();
    profile_data_map[std::this_thread::get_id()] = this;
    mtx.unlock();
}

void ThreadLocalProfileData::unregister_thread() {
    global_profile_data.work_cycles += work_cycles;
    global_profile_data.allocate_cycles += allocate_cycles;
    global_profile_data.post_fetch_cycles += post_fetch_cycles;
    global_profile_data.poll_cycles += poll_cycles;
    global_profile_data.yield_cycles += yield_cycles;
    global_profile_data.data_miss_count += data_miss_count;
    global_profile_data.yield_count += yield_count;
    global_profile_data.mark_cycles += mark_cycles;
    global_profile_data.evict_cycles += evict_cycles;
    global_profile_data.evacuation_count += evacuation_count;
    global_profile_data.fork_join_cycles += fork_join_cycles;
    global_profile_data.check_cq_cycles += check_cq_cycles;
    global_profile_data.check_cq_count += check_cq_count;
    global_profile_data.mark_count += mark_count;
    global_profile_data.not_mark_count += not_mark_count;
    global_profile_data.access_count += access_count;
    global_profile_data.fetch_count += fetch_count;
    global_profile_data.on_miss_enter_count += on_miss_enter_count;
    global_profile_data.prefetch_count += prefetch_count;
    global_profile_data.on_miss_cycles += on_miss_cycles;
    global_profile_data.deref_count += deref_count;
    profile_data_map.erase(std::this_thread::get_id());
}

void reset_all() {
    global_profile_data.reset();
    for (auto &it : profile_data_map) {
        it.second->reset();
    }
    global_cycles = 0;
}

#define DEFINE_COLLECT(FIELD)                    \
    int64_t collect_##FIELD() {                  \
        int64_t sum = global_profile_data.FIELD; \
        for (auto &it : profile_data_map) {      \
            sum += it.second->FIELD;             \
        }                                        \
        return sum;                              \
    }

DEFINE_COLLECT(work_cycles)
DEFINE_COLLECT(allocate_cycles);
DEFINE_COLLECT(post_fetch_cycles);
DEFINE_COLLECT(poll_cycles);
DEFINE_COLLECT(yield_cycles);
DEFINE_COLLECT(data_miss_count);
DEFINE_COLLECT(yield_count);
DEFINE_COLLECT(mark_cycles);
DEFINE_COLLECT(evict_cycles);
DEFINE_COLLECT(evacuation_count);
DEFINE_COLLECT(fork_join_cycles);
DEFINE_COLLECT(check_cq_cycles);
DEFINE_COLLECT(check_cq_count);
DEFINE_COLLECT(mark_count);
DEFINE_COLLECT(not_mark_count);
DEFINE_COLLECT(poll_count);
DEFINE_COLLECT(post_fetch_retry_count);
DEFINE_COLLECT(check_cq_success_count);
DEFINE_COLLECT(check_cq_failed_count);
DEFINE_COLLECT(access_count);
DEFINE_COLLECT(fetch_count);
DEFINE_COLLECT(on_miss_enter_count);
DEFINE_COLLECT(present_count);
DEFINE_COLLECT(enqueue_count);
DEFINE_COLLECT(prefetch_count);
DEFINE_COLLECT(on_miss_cycles);
DEFINE_COLLECT(prefetch_cycles);
DEFINE_COLLECT(deref_count);

void print_profile_data() {
    constexpr bool PrintEnabled =
        Enabled || enabled::Evacuation || enabled::OnMissSchedule;
    if constexpr (!(PrintEnabled)) return;
#define PRINT_IF(NAME, VALUE, ENABLED)                               \
    if constexpr (ENABLED) {                                         \
        std::cout << std::setw(16) << NAME << std::setw(16) << VALUE \
                  << std::endl;                                      \
    }
    PRINT_IF("wall time cycles", global_cycles, PrintEnabled);
    std::cout << std::setw(32) << std::internal << "breakdown" << std::endl;
    std::cout << std::right;
    std::cout << std::string(32, '-') << std::endl;
    PRINT_IF("work cycles", collect_work_cycles(), Enabled);
    PRINT_IF("alloc cycles", collect_allocate_cycles(), Enabled);
    PRINT_IF("post cycles", collect_post_fetch_cycles(), Enabled);
    PRINT_IF("poll count", collect_poll_count(), Enabled);
    PRINT_IF("poll cycles", collect_poll_cycles(), Enabled);
    PRINT_IF("yield cycles", collect_yield_cycles(), Enabled);
    PRINT_IF("miss count", collect_data_miss_count(), Enabled);
    PRINT_IF("yield count", collect_yield_count(), Enabled);
    PRINT_IF("mark cycles", collect_mark_cycles(), enabled::Evacuation);
    PRINT_IF("evict cycles", collect_evict_cycles(), enabled::Evacuation);
    PRINT_IF("evacuate count", collect_evacuation_count(), enabled::Evacuation);
    PRINT_IF("forkjoin cycles", collect_fork_join_cycles(), Enabled);
    PRINT_IF("check cq cycles", collect_check_cq_cycles(), Enabled);
    PRINT_IF("check cq count", collect_check_cq_count(), Enabled);
    PRINT_IF("mark count", collect_mark_count(), Enabled);
    PRINT_IF("not mark count", collect_not_mark_count(), Enabled);
    PRINT_IF("post fetch retry count", collect_post_fetch_retry_count(),
             Enabled);
    PRINT_IF("check cq success count", collect_check_cq_success_count(),
             Enabled);
    PRINT_IF("check cq failed count", collect_check_cq_failed_count(), Enabled);
    PRINT_IF("access count", collect_access_count(), Enabled);
    PRINT_IF("fetch count", collect_fetch_count(), Enabled);
    PRINT_IF("on miss enter count", collect_on_miss_enter_count(), Enabled);
    PRINT_IF("present count", collect_present_count(), Enabled);
    PRINT_IF("enqueue count", collect_enqueue_count(), Enabled);
    PRINT_IF("prefetch count", collect_prefetch_count(), Enabled);
    PRINT_IF("on miss cycles", collect_on_miss_cycles(),
             enabled::OnMissSchedule);
    PRINT_IF("prefetch cycles", collect_prefetch_cycles(),
             enabled::OnMissSchedule);
    PRINT_IF("read bandwidth",
             (static_cast<double>(global_profile_data.rbytes) / 1e9) /
                 (global_cycles / 2.8 / 1e9),
             Enabled);
    PRINT_IF("write bandwidth",
             (static_cast<double>(global_profile_data.wbytes) / 1e9) /
                 (global_cycles / 2.8 / 1e9),
             Enabled);
    PRINT_IF("deref count", collect_deref_count(), Enabled);
#undef PRINT_IF
}

void print_rdma_trace() {
    if constexpr (TraceRDMA) {
        static std::atomic_flag printing = false;
        if (printing.test_and_set()) return;
        for (auto &it : profile_data_map) {
            auto tid = it.first;
            auto data = it.second;
            std::cout << "rdma read requests for thread " << tid << std::endl;
            data->rdma_read_reqs.for_each(
                [](std::tuple<uint64_t, uint64_t, uint64_t> p) {
                    std::cout << std::setw(32) << std::get<0>(p)
                              << std::setw(32) << std::get<1>(p)
                              << std::setw(32) << std::get<2>(p) << std::endl;
                });
            std::cout << std::endl;
            std::cout << "rdma read completions for thread " << tid
                      << std::endl;
            data->rdma_read_wcs.for_each([](std::pair<uint64_t, uint64_t> p) {
                std::cout << std::setw(24) << p.first << std::setw(24)
                          << p.second << std::endl;
            });
            std::cout << std::endl;
        }
        printing.clear();
    }
}

void print_alloc_trace() {
    if constexpr (TraceAlloc) {
        static std::atomic_flag printing = false;
        if (printing.test_and_set()) return;
        for (auto &it : profile_data_map) {
            auto tid = it.first;
            auto data = it.second;
            std::cout << "allocation trace for thread " << tid << std::endl;
            data->alloc_trace.for_each([](const AllocTrace &p) {
                std::cout << std::setw(24) << p.op << std::setw(24) << p.tsc
                          << std::setw(24) << p.addr << std::setw(24) << p.bin
                          << std::endl;
            });
            std::cout << std::endl;
        }
        printing.clear();
    }
}

void print_mem_usage_trace() {
    if constexpr (TraceMemoryUsage) {
        static std::atomic_flag printing = false;
        if (printing.test_and_set()) return;
        for (auto &it : profile_data_map) {
            auto tid = it.first;
            auto data = it.second;
            std::cout << "memory usage trace for thread " << tid << std::endl;
            data->mem_usage_trace.for_each([](const MemoryUsageTrace &p) {
                std::cout << std::setw(24) << p.previous_free_size
                          << std::setw(24) << p.free_size_modification
                          << std::endl;
            });
            std::cout << std::endl;
        }
        printing.clear();
    }
}

void start_record_bandwidth() {
    global_profile_data.rbytes =
        rdma::ClientControl::get_default()->get_all_read_bytes();
    global_profile_data.wbytes =
        rdma::ClientControl::get_default()->get_all_write_bytes();
}
void end_record_bandwidth() {
    global_profile_data.rbytes =
        rdma::ClientControl::get_default()->get_all_read_bytes() -
        global_profile_data.rbytes;
    global_profile_data.wbytes =
        rdma::ClientControl::get_default()->get_all_write_bytes() -
        global_profile_data.wbytes;
}

}  // namespace profile

}  // namespace Beehive
