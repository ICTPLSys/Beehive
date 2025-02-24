#include <boost/lockfree/queue.hpp>
#include <cstdlib>

#include "cache/region_based_allocator.hpp"
#include "utils/cpu_cycles.hpp"
#include "utils/parallel.hpp"

using namespace Beehive;
using namespace Beehive::allocator;

int main() {
    constexpr size_t ThreadCount = 4;
    uthread::runtime_init(ThreadCount);

    constexpr size_t ObjectSize = 1024;
    constexpr size_t ObjectCount = 1024 * 32;
    constexpr size_t buffer_size = 1024 * 1024 * 32;
    void *buffer = std::aligned_alloc(1024 * 1024 * 32, buffer_size);
    global_heap.register_heap(buffer, buffer_size);
    // touch to commit memory
    memset(buffer, 0, buffer_size);

    std::atomic_uint32_t time_stamp = 0;

    // 1 thread allocate
    for (size_t n = 0; n < 4; n++) {
        uint64_t allocate_success_cycles = 0;
        uint64_t allocate_success_count = 0;
        uint64_t allocate_fail_cycles = 0;
        uint64_t allocate_fail_count = 0;
        for (size_t i = 0; i < ObjectCount; i++) {
            uint64_t begin_cycles = get_cycles();
            BlockHead *block =
                thread_local_allocate(ObjectSize, far_obj_t::null(), nullptr);
            uint64_t end_cycles = get_cycles();
            if (block != nullptr) {
                allocate_success_cycles += end_cycles - begin_cycles;
                allocate_success_count++;
            } else {
                allocate_fail_cycles += end_cycles - begin_cycles;
                allocate_fail_count++;
            }
        }
        std::cout << "1 thread allocate success: "
                  << allocate_success_cycles / allocate_success_count
                  << " cycles (#" << allocate_success_count << ")" << std::endl;
        std::cout << "1 thread allocate fail: "
                  << allocate_fail_cycles / allocate_fail_count << " cycles (#"
                  << allocate_fail_count << ")" << std::endl;

        time_stamp++;
        uint64_t mark_begin_cycles = get_cycles();
        global_heap.mark([](BlockHead *) { return cache::MARKED; }, time_stamp);
        uint64_t mark_end_cycles = get_cycles();
        time_stamp++;
        uint64_t evict_begin_cycles = get_cycles();
        global_heap.evict([](BlockHead *) { return cache::FREE; }, time_stamp);
        time_stamp++;
        global_heap.evict([](BlockHead *) { return cache::FREE; }, time_stamp);
        uint64_t evict_end_cycles = get_cycles();
        std::cout << "1 thread mark: "
                  << (mark_end_cycles - mark_begin_cycles) /
                         allocate_success_count
                  << " cycles (#" << allocate_success_count << ")" << std::endl;
        std::cout << "1 thread evict: "
                  << (evict_end_cycles - evict_begin_cycles) /
                         allocate_success_count
                  << " cycles (#" << allocate_success_count << ")" << std::endl;
    }

    // N thread allocate
    for (size_t n = 0; n < 4; n++) {
        std::atomic_uint64_t allocate_success_cycles = 0;
        std::atomic_uint64_t allocate_success_count = 0;
        std::atomic_uint64_t allocate_fail_cycles = 0;
        std::atomic_uint64_t allocate_fail_count = 0;

        uthread::parallel_for<1>(ThreadCount, ObjectCount, [&](size_t) {
            uint64_t begin_cycles = get_cycles();
            BlockHead *block =
                thread_local_allocate(ObjectSize, far_obj_t::null(), nullptr);
            uint64_t end_cycles = get_cycles();
            if (block != nullptr) {
                allocate_success_cycles += end_cycles - begin_cycles;
                allocate_success_count++;
            } else {
                allocate_fail_cycles += end_cycles - begin_cycles;
                allocate_fail_count++;
            }
        });

        std::cout << ThreadCount << " threads allocate success: "
                  << allocate_success_cycles / allocate_success_count
                  << " cycles (#" << allocate_success_count << ")" << std::endl;
        std::cout << ThreadCount << " threads allocate fail: "
                  << allocate_fail_cycles / allocate_fail_count << " cycles (#"
                  << allocate_fail_count << ")" << std::endl;

        std::atomic_uint64_t mark_cycles = 0;
        std::atomic_uint64_t evict_cycles = 0;
        time_stamp++;
        uthread::parallel_for<1>(ThreadCount, ThreadCount, [&](size_t) {
            uint64_t mark_begin_cycles = get_cycles();
            global_heap.mark([](BlockHead *) { return cache::MARKED; },
                             time_stamp);
            uint64_t mark_end_cycles = get_cycles();
            mark_cycles += mark_end_cycles - mark_begin_cycles;
        });
        time_stamp++;
        uthread::parallel_for<1>(ThreadCount, ThreadCount, [&](size_t) {
            uint64_t evict_begin_cycles = get_cycles();
            global_heap.evict([](BlockHead *) { return cache::FREE; },
                              time_stamp);
            uint64_t evict_end_cycles = get_cycles();
            evict_cycles += evict_end_cycles - evict_begin_cycles;
        });
        time_stamp++;
        uthread::parallel_for<1>(ThreadCount, ThreadCount, [&](size_t) {
            uint64_t evict_begin_cycles = get_cycles();
            global_heap.evict([](BlockHead *) { return cache::FREE; },
                              time_stamp);
            uint64_t evict_end_cycles = get_cycles();
            evict_cycles += evict_end_cycles - evict_begin_cycles;
        });
        std::cout << ThreadCount
                  << " threads mark: " << mark_cycles / allocate_success_count
                  << " cycles (#" << allocate_success_count << ")" << std::endl;
        std::cout << ThreadCount
                  << " threads evict: " << evict_cycles / allocate_success_count
                  << " cycles (#" << allocate_success_count << ")" << std::endl;
    }

    uthread::runtime_destroy();

    return 0;
}
