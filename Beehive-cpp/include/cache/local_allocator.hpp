#pragma once

#include <cstddef>

#include "utils/stats.hpp"

#define USE_REGION_BASED_ALLOCATOR

#ifdef USE_REGION_BASED_ALLOCATOR

#include "region_based_allocator.hpp"

namespace Beehive {

namespace cache {

class LocalAllocator {
private:
    size_t buffer_size;
    std::atomic_size_t allocated_size;
    const size_t memory_high_watermark;
    const size_t memory_low_watermark;

public:
    LocalAllocator(void *buffer, size_t buffer_size)
        : buffer_size(buffer_size),
          allocated_size(0),
          memory_high_watermark(buffer_size * 5 / 10),
          memory_low_watermark(buffer_size * 7 / 10) {
        allocator::global_heap.register_heap(buffer, buffer_size);
    }

    ~LocalAllocator() { allocator::global_heap.destroy(); }

    size_t get_buffer_size() const { return buffer_size; }

    bool memory_low() const {
        return allocated_size.load() > memory_low_watermark;
    }

    size_t get_size_over_mark() {
        size_t size = allocated_size.load();
        return size > memory_high_watermark ? size - memory_high_watermark : 0;
    }

    void *allocate(size_t size) {
        profile::Profiler profiler;
        void *p = allocator::get_thread_heap().allocate(size);
        if (p == nullptr) {
            profiler.end(profile::CACHE_ALLOC_FAIL);
        } else {
            allocated_size.fetch_add(size);
            profiler.end(profile::CACHE_ALLOC_SUCCESS);
        }
        return p;
    }

    // Do we need a size?
    void deallocate(void *ptr, size_t size) { ERROR("deprecated: deallocate"); }

    void debug() {
        printf("DEBUG: local buffer size: %ld\n", buffer_size);
        printf("DEBUG: allocated size: %ld\n", allocated_size.load());
        printf("DEBUG: watermark (low): %ld\n", memory_low_watermark);
        printf("DEBUG: watermark (high): %ld\n", memory_high_watermark);
    }
};

}  // namespace cache
}  // namespace Beehive

#else

#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_external_buffer.hpp>

namespace Beehive {

namespace cache {

class LocalAllocator {
private:
    boost::interprocess::managed_external_buffer local_buffer;
    size_t buffer_size;

public:
    LocalAllocator(void *buffer, size_t buffer_size)
        : buffer_size(buffer_size),
          local_buffer(boost::interprocess::create_only, buffer, buffer_size) {}

    size_t get_buffer_size() const { return buffer_size; }

    void *allocate(size_t size) {
        profile::Profiler profiler;
        try {
            void *p = local_buffer.allocate(size);
            profiler.end(profile::CACHE_ALLOC_SUCCESS);
            return p;
        } catch (boost::interprocess::bad_alloc) {
            profiler.end(profile::CACHE_ALLOC_FAIL);
            return nullptr;
        }
    }

    // Do we need a size?
    void deallocate(void *ptr) {
        profile::Profiler profiler;
        local_buffer.deallocate(ptr);
        profiler.end(profile::CACHE_DEALLOC);
    }
};

}  // namespace cache

}  // namespace Beehive

#endif
