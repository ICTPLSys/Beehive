#pragma once

#include <cstddef>
#include <cstdint>

#include "utils/debug.hpp"
#define REMOTE_REGION_ALLOCATOR

#ifdef REMOTE_REGION_ALLOCATOR
#include "cache/region_remote_allocator.hpp"
#endif
namespace FarLib {

namespace cache {

#ifdef REMOTE_REGION_ALLOCATOR
using FarLib::allocator::remote::InvalidRemoteAddr;
using FarLib::allocator::remote::remote_global_heap;
using FarLib::allocator::remote::remote_thread_heap;
class RemoteAllocator {
public:
    RemoteAllocator(size_t buffer_size) {
        remote_global_heap.register_remote(buffer_size);
    }

    uint64_t allocate(size_t size) {
        uint64_t addr = remote_thread_heap.allocate(size);
        if (addr == InvalidRemoteAddr) [[unlikely]] {
            info();
            ERROR("Out Of Remote Memory!");
        }
        return addr;
    }

    void deallocate(uint64_t addr) { remote_thread_heap.deallocate(addr); }

    void info() { remote_global_heap.info(); }
};
#else
class RemoteAllocator {
private:
    size_t buffer_size;
    std::atomic_uint64_t allocated_offset = 0;

public:
    RemoteAllocator(size_t buffer_size) : buffer_size(buffer_size) {}

    size_t get_allocated() const { return allocated_offset.load(); }

    uint64_t allocate(size_t size) {
        uint64_t ptr = allocated_offset.fetch_add(size);
        ASSERT(ptr + size <= buffer_size);
        return ptr;
    }

    // Do we need a size?
    void deallocate(uint64_t location) {
        // Do nothing now
    }
};
#endif
}  // namespace cache

}  // namespace FarLib
