#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>

#include "cache/entry.hpp"
#include "cache/object.hpp"
#include "utils/debug.hpp"
#include "utils/stats.hpp"
namespace FarLib {

namespace allocator {

constexpr size_t RegionSize = 256 * 1024;

constexpr size_t RegionBinCount = 48;

constexpr size_t BinSize[RegionBinCount] = {
    1,    2,    3,    4,    5,    6,    7,    8,    10,   12,   14,   16,
    20,   24,   28,   32,   40,   48,   56,   64,   80,   96,   112,  128,
    160,  192,  224,  256,  320,  384,  448,  512,  640,  768,  896,  1024,
    1280, 1536, 1792, 2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192,
};

constexpr size_t MaxBinSize = BinSize[RegionBinCount - 1];
inline constexpr size_t get_bin_size(size_t bin) {
    return BinSize[bin] * sizeof(void *);
}

// aligned to sizeof(void *)
inline constexpr size_t wsize_from_size(size_t size) {
    return (size + sizeof(void *) - 1) / sizeof(void *);
}

// bit scan reverse: return the index of the highest bit
inline uint8_t bsr32(uint32_t x) { return 31 - __builtin_clz(x); }

// get a proper bin from a specific wsize
inline static size_t bin_from_wsize(size_t wsize) {
    if (wsize <= 8) {
        if (wsize <= 1) [[unlikely]] {
            return 0;
        }
        // round to double word sizes
        return (wsize - 1) | 1;
    } else {
        if (wsize > MaxBinSize) [[unlikely]] {
            // size >= MaxBinSize, too large
            ERROR("too large to allocate");
        }
        wsize -= 1;
        uint8_t b = bsr32(static_cast<uint32_t>(wsize));
        // (~16% worst internal fragmentation)
        return ((b << 2) + (uint8_t)((wsize >> (b - 2)) & 0x03)) - 4;
    }
}

// actually the allocator allocates blocks
// a block include its header (metadata) and the user data
struct BlockHead {
    BlockHead *next;
    std::atomic<cache::far_obj_t> obj_meta_data;

    void *get_object_ptr() { return static_cast<void *>(this + 1); }
};

enum RegionState {
    FREE,    // this region has no block allocated
    IN_USE,  // this region belongs to a thread, and is used for allocation
    USABLE,  // this region has free blocks, and is in the global region list
    FULL,    // this region is exhausted
};

struct RegionListNodeBase {
    RegionListNodeBase() = default;
    RegionListNodeBase(RegionListNodeBase *n) : next_region(n) {}

    RegionListNodeBase *next_region;
};

template <typename Fn>
concept BlockInvoker = requires(Fn &&f, BlockHead *block) {
    { f(block) } -> std::same_as<cache::EntryState>;
};

// Only one thread can allocate from the region
// But any threads can deallocate blocks in it
// A region includes a header (metadata) and blocks to be allocate
struct alignas(64) RegionHead : public RegionListNodeBase {
    RegionState state;
    BlockHead *free_list;
    BlockHead *active_list;
    BlockHead *marked_list;
    uint32_t used_count;
    uint32_t unused_offset;
    uint32_t bin;
    // used for multithread mark & evict
    uint32_t sweep_time_stamp;

    void init(size_t bin) {
        state = FREE;
        free_list = nullptr;
        active_list = nullptr;
        marked_list = nullptr;
        used_count = 0;
        unused_offset = sizeof(RegionHead);
        next_region = nullptr;
        this->bin = bin;
        sweep_time_stamp = uint32_t(-1);
    }

    // not thread safe
    // return nullptr on exhausted
    BlockHead *allocate(cache::far_obj_t obj) {
        BlockHead *block = free_list;
        if (block != nullptr) {
            assert(block->obj_meta_data == cache::far_obj_t::null());
            used_count++;
            free_list = block->next;
            block->next = active_list;
            active_list = block;
            block->obj_meta_data.store(obj, std::memory_order::relaxed);
            return block;
        }
        size_t bin_size = get_bin_size(bin);
        if (unused_offset + bin_size <= RegionSize) {
            void *r = reinterpret_cast<char *>(this) + unused_offset;
            unused_offset += bin_size;
            block = static_cast<BlockHead *>(r);
            used_count++;
            block->next = active_list;
            active_list = block;
            block->obj_meta_data.store(obj, std::memory_order::relaxed);
            return block;
        }
        return nullptr;
    }

    // not thread safe
    bool can_allocate() const {
        return free_list != nullptr ||
               unused_offset + get_bin_size(bin) <= RegionSize;
    }

    size_t free_size() const {
        size_t bin_size = get_bin_size(bin);
        size_t max_block_num = (RegionSize - sizeof(RegionHead)) / bin_size;
        return (max_block_num - used_count) * bin_size;
    }

    bool is_empty() const {
        return active_list == nullptr && marked_list == nullptr;
    }

    // not thread safe
    // traverse active list
    // return: freed size
    template <BlockInvoker Fn>
    void mark(Fn &&fn) {
        size_t bin_size = get_bin_size(bin);
        BlockHead *new_active_list = nullptr;
        BlockHead *new_marked_list = marked_list;
        BlockHead *new_free_list = free_list;
        for (BlockHead *block = active_list; block;) {
            BlockHead *next_block = block->next;
            switch (fn(block)) {
            case cache::FREE:
                block->next = new_free_list;
                new_free_list = block;
                profile::trace_dealloc(block, bin);
                used_count--;
                break;
            case cache::LOCAL:
            case cache::FETCHING:
            case cache::REMOTE:
            case cache::BUSY:
                block->next = new_active_list;
                new_active_list = block;
                break;
            case cache::MARKED:
            case cache::EVICTING:
                block->next = new_marked_list;
                new_marked_list = block;
                break;
            }
            block = next_block;
        }
        active_list = new_active_list;
        marked_list = new_marked_list;
        free_list = new_free_list;
    }

    // traverse marked list
    template <BlockInvoker Fn>
    void evict(Fn &&fn) {
        size_t bin_size = get_bin_size(bin);
        BlockHead *new_active_list = active_list;
        BlockHead *new_marked_list = nullptr;
        BlockHead *new_free_list = free_list;
        for (BlockHead *block = marked_list; block;) {
            BlockHead *next_block = block->next;
            switch (fn(block)) {
            case cache::FREE:
                block->next = new_free_list;
                new_free_list = block;
                used_count--;
                profile::trace_dealloc(block, bin);
                break;
            case cache::LOCAL:
            case cache::FETCHING:
            case cache::REMOTE:
            case cache::BUSY:
                block->next = new_active_list;
                new_active_list = block;
                break;
            case cache::MARKED:
            case cache::EVICTING:
                block->next = new_marked_list;
                new_marked_list = block;
                break;
            }
            block = next_block;
        }
        marked_list = new_marked_list;
        active_list = new_active_list;
        free_list = new_free_list;
    }
};

// concurrent double linked list
struct RegionList {
    std::atomic_flag lock;
    RegionListNodeBase head;
    RegionListNodeBase *tail;

    RegionList() : head(&head), tail(&head) {}

    void push(RegionHead *node) {
        while (lock.test_and_set());
        assert(tail->next_region == &head);
        node->next_region = &head;
        tail->next_region = node;
        tail = node;
        lock.clear();
    }

    RegionHead *pop() {
        while (lock.test_and_set());
        RegionListNodeBase *node = head.next_region;
        if (node == &head) {
            lock.clear();
            return nullptr;
        } else {
            head.next_region = node->next_region;
            if (tail == node) tail = &head;
            lock.clear();
            return static_cast<RegionHead *>(node);
        }
    }

    RegionHead *pop_unmatched(uint32_t time_stamp) {
        while (lock.test_and_set());
        RegionListNodeBase *node = head.next_region;
        if (node == &head ||
            static_cast<RegionHead *>(node)->sweep_time_stamp == time_stamp) {
            lock.clear();
            return nullptr;
        } else {
            head.next_region = node->next_region;
            if (tail == node) tail = &head;
            lock.clear();
            return static_cast<RegionHead *>(node);
        }
    }
};

class GlobalHeap {
private:
    RegionList usable_region_list[RegionBinCount];
    RegionList full_region_list;
    RegionList free_region_list;
    std::atomic_size_t unallocated_offset;
    size_t heap_size;
    size_t memory_low_water_mark;
    void *heap;
    std::atomic_bool is_dead;
    std::atomic_int64_t free_size;

public:
    std::function<void(void)> on_memory_low = [] {};

public:
    GlobalHeap()
        : unallocated_offset(0),
          heap_size(0),
          memory_low_water_mark(0),
          heap(nullptr),
          is_dead(false),
          free_size(0) {}

    void register_heap(void *buffer, size_t size) {
        ASSERT(heap == nullptr);
        unallocated_offset = 0;
        heap_size = size;
        heap = buffer;
        free_size = heap_size;
        memory_low_water_mark = heap_size * 0.2;
    }

    void destroy() { is_dead.store(true); }

    bool dead() const { return is_dead.load(); }

    void set_on_memory_low(std::function<void(void)> fn) {
        on_memory_low = std::move(fn);
    }

    // allocate a region with free blocks
    // this region may not be empty
    // return nullptr if failed to allocate
    // return a locked region if success
    RegionHead *allocate_region(size_t bin) {
        RegionHead *region = allocate_region_impl(bin);
        profile::trace_alloc_region(region, bin);
        return region;
    }

    void sub_free_size(size_t size) {
        size_t previous_free_size =
            free_size.fetch_sub(size, std::memory_order::relaxed);
        profile::trace_free_memory_modification(previous_free_size,
                                                -(int64_t)size);
    }

    RegionHead *allocate_region_impl(size_t bin) {
        RegionHead *region =
            static_cast<RegionHead *>(usable_region_list[bin].pop());
        if (region != nullptr) {
            sub_free_size(region->free_size());
            return region;
        }
        region = static_cast<RegionHead *>(free_region_list.pop());
        if (region != nullptr) {
            region->init(bin);
            sub_free_size(RegionSize);
            return region;
        }
        size_t offset = unallocated_offset.load();
    retry_alloc:
        if (offset + RegionSize <= heap_size) {
            if (!unallocated_offset.compare_exchange_weak(offset,
                                                          offset + RegionSize))
                goto retry_alloc;
            region = reinterpret_cast<RegionHead *>(static_cast<char *>(heap) +
                                                    offset);
            region->init(bin);
            sub_free_size(RegionSize);
            return region;
        }
        return nullptr;
    }

    void return_back_region(RegionHead *region) {
        profile::trace_dealloc_region(region, region->bin);
        if (region->can_allocate()) [[unlikely]] {
            uint32_t bin = region->bin;
            region->state = USABLE;
            usable_region_list[bin].push(region);
            size_t region_free_size = region->free_size();
            size_t previous_free_size = free_size.fetch_add(
                region_free_size, std::memory_order::relaxed);
            profile::trace_free_memory_modification(previous_free_size,
                                                    region_free_size);
        } else {
            region->state = FULL;
            full_region_list.push(region);
        }
    }

    void *get_heap() { return heap; }

    size_t get_heap_size() const { return heap_size; }

    template <BlockInvoker Fn>
    size_t mark_list(Fn &&fn, RegionList &list, uint32_t timestamp) {
        size_t freed_size = 0;
        while (true) {
            RegionHead *region = list.pop_unmatched(timestamp);
            if (region == nullptr) break;
            size_t origin_free_size = region->free_size();
            region->mark(fn);
            freed_size += region->free_size() - origin_free_size;
            region->sweep_time_stamp = timestamp;
            list.push(region);
        }
        return freed_size;
    }

    template <BlockInvoker Fn>
    size_t mark(Fn &&fn, uint32_t timestamp) {
        size_t freed_size = mark_list(fn, full_region_list, timestamp);
        for (size_t i = 0; i < RegionBinCount; i++) {
            freed_size += mark_list(fn, usable_region_list[i], timestamp);
        }
        int64_t previous_free_size =
            free_size.fetch_add(freed_size, std::memory_order::relaxed);
        profile::trace_free_memory_modification(previous_free_size, freed_size);
        return freed_size;
    }

    template <BlockInvoker Fn>
    size_t evict_list(Fn &&fn, RegionList &list, uint32_t timestamp) {
        size_t freed_size = 0;
        while (true) {
            RegionHead *region = list.pop_unmatched(timestamp);
            if (region == nullptr) break;
            size_t origin_free_size = region->free_size();
            region->evict(fn);
            region->sweep_time_stamp = timestamp;
            if (region->is_empty()) {
                region->state = FREE;
                free_region_list.push(region);
                freed_size += RegionSize - origin_free_size;
            } else if (region->can_allocate()) {
                region->state = USABLE;
                usable_region_list[region->bin].push(region);
                freed_size += region->free_size() - origin_free_size;
            } else {
                region->state = FULL;
                full_region_list.push(region);
                assert(origin_free_size == 0);
            }
        }
        return freed_size;
    }

    template <BlockInvoker Fn>
    size_t evict(Fn &&fn, uint32_t timestamp) {
        size_t freed_size = evict_list(fn, full_region_list, timestamp);
        for (size_t i = 0; i < RegionBinCount; i++) {
            freed_size += evict_list(fn, usable_region_list[i], timestamp);
        }
        int64_t previous_free_size =
            free_size.fetch_add(freed_size, std::memory_order::relaxed);
        // if (freed_size == 0) {
        // printf("free size: %zu -> %zu\n", previous_free_size,
        //        previous_free_size + freed_size);
        // }
        profile::trace_free_memory_modification(previous_free_size, freed_size);
        return freed_size;
    }

    bool memory_low() {
        return free_size.load(std::memory_order::relaxed) <
               memory_low_water_mark;
    }
    size_t get_free_size() {
        return free_size.load(std::memory_order::relaxed);
    }
};

extern GlobalHeap global_heap;
BlockHead *thread_local_allocate(size_t size, cache::far_obj_t obj,
                                 cache::DereferenceScope *scope);

constexpr size_t BlockHeadSize = sizeof(BlockHead);

}  // namespace allocator

}  // namespace FarLib
