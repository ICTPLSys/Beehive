#include "cache/cache.hpp"

#include <sys/cdefs.h>

#ifdef NO_REMOTE

namespace FarLib {
namespace allocator {
#ifdef USE_BUMP_ALLOCATOR
std::byte *heap = nullptr;
size_t bump_pointer_offset = 0;
#endif
}  // namespace allocator
}  // namespace FarLib

#else

#include "cache/remote_allocator.hpp"
namespace FarLib {
namespace cache {

std::unique_ptr<Cache> Cache::default_instance;
}  // namespace cache

namespace allocator {

class ThreadHeap {
public:
    ThreadHeap() {
        for (size_t i = 0; i < RegionBinCount; i++) {
            regions[i] = nullptr;
        }
    }

    ~ThreadHeap() {
        // if the global heap is dead
        // maybe the heap is freed, so do nothing
        if (global_heap.dead()) return;
        for (size_t i = 0; i < RegionBinCount; i++) {
            auto region = regions[i];
            if (region != nullptr) {
                global_heap.return_back_region(region);
            }
        }
    }

    BlockHead *allocate(size_t size, cache::far_obj_t obj,
                        cache::DereferenceScope *scope) {
        profile::start_allocate();
        BlockHead *block = allocate_block(size + sizeof(BlockHead), obj, scope);
        if (block != nullptr) {
            assert(block >= global_heap.get_heap());
            assert((char *)block + size + sizeof(BlockHead) <=
                   (char *)global_heap.get_heap() +
                       global_heap.get_heap_size());
        }
        profile::end_allocate();
        return block;
    }

private:
    BlockHead *allocate_block(size_t block_size, cache::far_obj_t obj,
                              cache::DereferenceScope *scope) {
        size_t wsize = wsize_from_size(block_size);
        size_t bin = bin_from_wsize(wsize);
        assert(get_bin_size(bin) >= block_size);
        assert(bin == 0 || get_bin_size(bin - 1) < block_size + sizeof(void *));
        BlockHead *block = allocate_block_from_bin(bin, obj, scope);
        profile::trace_alloc(block, bin);
        return block;
    }

    BlockHead *allocate_block_from_bin(size_t bin, cache::far_obj_t obj,
                                       cache::DereferenceScope *scope) {
        RegionHead *region = regions[bin];
        if (region != nullptr) [[likely]] {
            BlockHead *block = region->allocate(obj);
            if (block != nullptr) [[likely]] {
                return block;
            } else {
                global_heap.return_back_region(region);
            }
        }
        region = global_heap.allocate_region(bin);
        if (global_heap.memory_low()) [[unlikely]] {
            global_heap.on_memory_low();
            if (scope != nullptr) {
                Cache::get_default()->update_scope(*scope);
            }
        }
        regions[bin] = region;
        if (region == nullptr) [[unlikely]] {
            return nullptr;
        } else {
            region->state = IN_USE;
            return region->allocate(obj);
        }
    }

private:
    RegionHead *regions[RegionBinCount];
};

GlobalHeap global_heap;
static thread_local ThreadHeap thread_heap;
ThreadHeap &get_thread_heap() { return thread_heap; }

__attribute_noinline__ BlockHead *thread_local_allocate(
    size_t size, cache::far_obj_t obj, cache::DereferenceScope *scope) {
    return thread_heap.allocate(size, obj, scope);
}

#ifdef REMOTE_REGION_ALLOCATOR
namespace remote {
RemoteGlobalHeap remote_global_heap;
thread_local RemoteThreadHeap remote_thread_heap;
}  // namespace remote
#endif

}  // namespace allocator

}  // namespace FarLib

#endif