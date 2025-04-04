#include <iostream>
#include <x86intrin.h>
#include <vector>
#include <cstdint>
#include <thread>
#include <atomic>
#include <algorithm>
#include <mutex>

#include "cache/region_remote_allocator.hpp"
#include "utils/debug.hpp"
#include "utils/cpu_cycles.hpp"

static constexpr size_t REMOTE_BUF_SIZE = 1L << 30; // 1G
static constexpr size_t ALLOC_SIZE = 1 << 10; // 1K
static constexpr size_t THREAD_CNT = 16;
using namespace FarLib::allocator::remote;

// #define SINGLE_THREAD_TEST
#define MULTI_THREAD_TEST
int main() {
#ifdef SINGLE_THREAD_TEST
    {
        remote_global_heap.register_remote(REMOTE_BUF_SIZE);
        std::vector<uint64_t> addrs;
        for (size_t i = 0;i < REMOTE_BUF_SIZE / ALLOC_SIZE;i++) {
            uint64_t addr = remote_thread_heap.allocate(ALLOC_SIZE);
            if (addr == InvalidRemoteAddr) {
                break;
            }
            addrs.push_back(addr);
        }
        size_t available_cnt = addrs.size();
        std::cout << "available memorty ratio: " 
            << static_cast<double>(available_cnt) / (REMOTE_BUF_SIZE / ALLOC_SIZE) << std::endl;
        for (size_t i = 0;i < available_cnt - 1;i++) {
            ASSERT(addrs[i] + ALLOC_SIZE <= addrs[i + 1]);
        }
        ASSERT(addrs[available_cnt - 1] + ALLOC_SIZE <= REMOTE_BUF_SIZE);
        auto start = __rdtsc();
        for (uint64_t ad: addrs) {
            remote_thread_heap.deallocate(ad);
        }
        auto end = __rdtsc();
        std::cout << "deallocate time: " << static_cast<double>(end - start) / available_cnt << std::endl;
        // check fragments
        addrs.resize(available_cnt);
        start = __rdtsc();
        for (size_t i = 0;i < REMOTE_BUF_SIZE / ALLOC_SIZE;i++) {
            uint64_t addr = remote_thread_heap.allocate(ALLOC_SIZE);
            if (addr == InvalidRemoteAddr) {
                break;
            }
        }
        end = __rdtsc();
        ASSERT(addrs.size() == available_cnt);
        std::cout << "allocate time: " << static_cast<double>(end - start) / available_cnt << std::endl;
    }
#endif
#ifdef MULTI_THREAD_TEST
    {
        remote_global_heap.register_remote(REMOTE_BUF_SIZE);
        std::atomic_uint64_t allocate_time(0);
        std::vector<uint64_t> global_addrs;
        std::mutex mtx;
        std::vector<std::thread> threads;
        std::atomic_bool prepare(true);
        // allocate
        auto alloc_func = [&] () {
            while (prepare);
            std::vector<uint64_t> addrs;
            addrs.reserve(REMOTE_BUF_SIZE / ALLOC_SIZE);
            size_t time = 0;
            for (size_t i = 0;i < REMOTE_BUF_SIZE / ALLOC_SIZE;i++) {
                uint64_t addr = remote_thread_heap.allocate(ALLOC_SIZE);
                auto start = get_cycles();
                if (addr == InvalidRemoteAddr) {
                    break;
                }
                auto end = get_cycles();
                addrs.push_back(addr);
                time += end - start;
            }
            allocate_time.fetch_add(time);
            {
                std::lock_guard guard(mtx);
                for (uint64_t ad: addrs) {
                    global_addrs.push_back(ad);
                }
            }
        };
        for (size_t i = 0;i < THREAD_CNT;i++) {
            threads.emplace_back(alloc_func);
        }
        prepare.store(false);
        for (auto &&t: threads) {
            t.join();
        }
        std::cout << "available memory ratio: " << 
            static_cast<double>(global_addrs.size()) / (REMOTE_BUF_SIZE / ALLOC_SIZE) << std::endl;
        size_t available_cnt = global_addrs.size();
        // deallocate
        threads.clear();
        size_t dealloc_per_thread = (available_cnt + THREAD_CNT - 1) / THREAD_CNT;
        std::atomic_uint64_t dealloc_time(0);
        auto dealloc_func = [&] (size_t base) {
            uint64_t time = 0;
            while (prepare);
            for (size_t idx = base, i = 0;idx < available_cnt && i < dealloc_per_thread;i++, idx++) {
                auto start = get_cycles();
                remote_thread_heap.deallocate(global_addrs[idx]);
                auto end = get_cycles();
                time += end - start;
            }
            dealloc_time.fetch_add(time);
        };
        prepare.store(true);
        for (size_t i = 0;i < THREAD_CNT;i++) {
            threads.emplace_back(dealloc_func, i * dealloc_per_thread);
        }
        prepare.store(false);
        for (auto &&t: threads) {
            t.join();
        }
        std::cout << "allocate time: " << static_cast<double>(allocate_time.load()) / available_cnt << std::endl;
        std::cout << "deallocate time: " << 
            static_cast<double>(dealloc_time.load()) / available_cnt << std::endl;
    }
#endif
    return 0;
}