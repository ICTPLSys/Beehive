#pragma once
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <vector>

#include "cache/region_based_allocator.hpp"
#include "utils/debug.hpp"

namespace FarLib {

namespace allocator {

namespace remote {
constexpr size_t RegionSize = 512 * 1024;
using FarLib::allocator::BinSize;
using FarLib::allocator::RegionBinCount;

using FarLib::allocator::bin_from_wsize;
using FarLib::allocator::get_bin_size;
using FarLib::allocator::wsize_from_size;

constexpr uint64_t InvalidRemoteAddr = std::numeric_limits<uint64_t>::max();
// definition is the same as RegionHead
// using bitmap instead to manage blocks
// when a region is being operated, it is occupied by only 1 thread
// so we dont need concurrent programming for those operation

class DoubleLinkedListHead {
protected:
    DoubleLinkedListHead *next;
    DoubleLinkedListHead *prev;

public:
    DoubleLinkedListHead() : next(nullptr), prev(nullptr) {}
    void insert_front_unsafe(DoubleLinkedListHead *node) {
        assert(node->next == nullptr && node->prev == nullptr);
        node->next = this;
        node->prev = this->prev;
        this->prev->next = node;
        this->prev = node;
    }

    void insert_back_unsafe(DoubleLinkedListHead *node) {
        assert(node->next == nullptr && node->prev == nullptr);
        node->prev = this;
        node->next = this->next;
        this->next->prev = node;
        this->next = node;
    }

    void remove_self_unsafe() {
        // check if region is "owned" by a thread
        assert(this->prev != nullptr && this->next != nullptr);
        this->prev->next = this->next;
        this->next->prev = this->prev;
        this->prev = nullptr;
        this->next = nullptr;
    }

    void reset() {
        this->next = nullptr;
        this->prev = nullptr;
    }

    friend class RemoteRegionList;
};

class RemoteRegionHead : public DoubleLinkedListHead {
private:
    std::atomic_flag flag;
    uint32_t bin;
    uint32_t last_map_idx;
    // 0 for allocated, 1 for free
    uint64_t *blockmap;
    size_t entry_size;
    uint64_t base_addr;
    uint64_t used_count;
    static constexpr size_t MapElementBitCount = sizeof(uint64_t) * 8;
    static constexpr uint64_t FreeMap = std::numeric_limits<uint64_t>::max();
    static constexpr uint32_t FreeMap32 = std::numeric_limits<uint32_t>::max();
    static constexpr uint64_t FullMap = 0;

public:
    void lock() { while (flag.test_and_set()); }

    void unlock() { flag.clear(); }

    void init(uint64_t base_addr, uint32_t bin) {
        this->base_addr = base_addr;
        reset<true>(bin);
    }

    template <bool init = false>
    void reset(uint32_t bin) {
        assert(!flag.test());
        this->DoubleLinkedListHead::reset();
        this->entry_size = RegionSize / get_bin_size(bin);
        size_t map_size =
            (this->entry_size + MapElementBitCount - 1) / MapElementBitCount;
        if constexpr (init) {
            this->blockmap = static_cast<uint64_t *>(
                std::realloc(blockmap, map_size * sizeof(uint64_t)));
        } else if (this->bin != bin) {
            this->blockmap = static_cast<uint64_t *>(
                std::realloc(blockmap, map_size * sizeof(uint64_t)));
        }
        this->bin = bin;
        // only first several bit is valid in the last map
        // if entry size is not aligned to 64
        if (entry_size % MapElementBitCount == 0) {
            std::memset(blockmap, FreeMap32, map_size * sizeof(uint64_t));
        } else {
            std::memset(blockmap, FreeMap32, (map_size - 1) * sizeof(uint64_t));
            this->blockmap[map_size - 1] =
                (1UL << (MapElementBitCount -
                         (map_size * MapElementBitCount - entry_size))) -
                1;
        }
        last_map_idx = 0;
        used_count = 0;
    }

    RemoteRegionHead() = default;

    RemoteRegionHead(const RemoteRegionHead &head) = delete;

    RemoteRegionHead(RemoteRegionHead &&head) = delete;

    ~RemoteRegionHead() {
        if (blockmap) {
            std::free(blockmap);
        }
    }

    uint64_t allocate_unsafe() {
        size_t map_size =
            (this->entry_size + MapElementBitCount - 1) / MapElementBitCount;
        for (size_t i = 0; i < map_size; i++) {
            // get the first 1 in the bitmap num
            int bit = __builtin_ffsl(blockmap[last_map_idx]);
            if (bit) {
                // set the bit at "bit" to 0, means this block is allocated
                blockmap[last_map_idx] ^= (1L << (bit - 1));
                used_count++;
                return base_addr +
                       (MapElementBitCount * last_map_idx + (bit - 1)) *
                           get_bin_size(bin);
            } else {
                last_map_idx++;
                if (last_map_idx >= map_size) [[unlikely]] {
                    last_map_idx = 0;
                }
            }
        }
        return InvalidRemoteAddr;
    }

    uint64_t allocate() {
        lock();
        uint64_t addr = allocate_unsafe();
        unlock();
        return addr;
    }

    void deallocate_unsafe(uint64_t addr) {
        assert(addr >= base_addr && addr < base_addr + RegionSize);
        uint64_t offset = (addr - base_addr) / get_bin_size(bin);
        // set 1 for free
        blockmap[offset / MapElementBitCount] |=
            (1L << (offset % MapElementBitCount));
        used_count--;
    }

    bool is_free_unsafe() const { return used_count == 0; }

    bool is_full_unsafe() const { return used_count == entry_size; }

    bool is_full_just_now_unsafe() const {
        return used_count == entry_size - 1;
    }

    uint32_t get_bin() const { return bin; }

    uint64_t get_used_count() const { return used_count; }

    bool is_in_thread_heap_unsafe() {
        assert(!((next == nullptr) ^ (prev == nullptr)));
        return next == nullptr && prev == nullptr;
    }

    void remove_self() {
        lock();
        remove_self_unsafe();
        unlock();
    }
};

class RemoteRegionList {
private:
    std::atomic_flag flag;
    DoubleLinkedListHead dummy_head;
    DoubleLinkedListHead dummy_tail;

    void lock() { while (flag.test_and_set()); }

    void unlock() { flag.clear(); }

public:
    RemoteRegionList() {
        dummy_head.next = &dummy_tail;
        dummy_tail.prev = &dummy_head;
    }

    void insert_tail(RemoteRegionHead *node) {
        lock();
        dummy_tail.insert_front_unsafe(node);
        unlock();
    }

    void insert_head(RemoteRegionHead *node) {
        lock();
        dummy_head.insert_back_unsafe(node);
        unlock();
    }

    inline bool empty() const {
        assert(!((dummy_head.next == &dummy_tail) ^
                 (dummy_tail.prev == &dummy_head)));
        return dummy_head.next == &dummy_tail;
    }

    RemoteRegionHead *pop_head() {
        lock();
        if (empty()) {
            unlock();
            return nullptr;
        }
        RemoteRegionHead *region =
            static_cast<RemoteRegionHead *>(dummy_head.next);
        region->remove_self();
        unlock();
        return region;
    }

    RemoteRegionHead *pop_tail() {
        lock();
        if (empty()) {
            unlock();
            return nullptr;
        }
        RemoteRegionHead *region =
            static_cast<RemoteRegionHead *>(dummy_tail.prev);
        region->remove_self();
        unlock();
        return region;
    }

    void remove_list_safe_region_unsafe(RemoteRegionHead *region) {
        lock();
        region->remove_self_unsafe();
        unlock();
    }
};

class RemoteGlobalHeap {
private:
    RemoteRegionList usable_region_list[RegionBinCount];
    RemoteRegionList full_region_list;
    RemoteRegionList free_region_list;
    std::unique_ptr<RemoteRegionHead[]> regions;
    size_t regions_size;
    std::atomic_size_t used_heap_idx;

    RemoteRegionHead *addr_to_region(uint64_t addr) {
        return &regions[addr / RegionSize];
    }

public:
    ~RemoteGlobalHeap() {
        std::cout << "used memory: " << used_heap_idx * RegionSize << std::endl;
    }
    // init function
    // call only once per remote allocator
    void register_remote(size_t size) {
        regions_size = size / RegionSize;
        regions.reset(new RemoteRegionHead[regions_size]());
        used_heap_idx.store(0);
    }

    RemoteRegionHead *allocate_region(size_t bin) {
        RemoteRegionHead *region = usable_region_list[bin].pop_head();
        if (region) {
            return region;
        }
        // register a new region from free list to usable list
        region = free_region_list.pop_head();
        if (region) {
            region->reset(bin);
            return region;
        }
        // lock the global region vector
        size_t idx = used_heap_idx.load();
    retry:
        if (idx < regions_size) {
            size_t new_idx = idx + 1;
            if (!used_heap_idx.compare_exchange_weak(idx, new_idx)) {
                goto retry;
            }
            region = &regions[idx];
            region->init(idx * RegionSize, bin);
            return region;
        }
        return nullptr;
    }

    void return_back_region(RemoteRegionHead *region) {
        region->lock();
        return_back_region_unsafe(region);
        region->unlock();
    }

    void return_back_region_unsafe(RemoteRegionHead *region) {
        if (region->is_full_unsafe()) {
            full_region_list.insert_tail(region);
        } else if (region->is_free_unsafe()) {
            free_region_list.insert_tail(region);
        } else {
            usable_region_list[region->get_bin()].insert_tail(region);
        }
    }

    void deallocate(uint64_t addr) {
        RemoteRegionHead *region = addr_to_region(addr);
        region->lock();
        deallocate_unsafe(addr, region);
        region->unlock();
    }

    void deallocate_unsafe(uint64_t addr, RemoteRegionHead *region) {
        region->deallocate_unsafe(addr);
        if (!region->is_in_thread_heap_unsafe()) {
            if (region->is_free_unsafe()) [[unlikely]] {
                // region in usable list
                usable_region_list[region->get_bin()]
                    .remove_list_safe_region_unsafe(region);
                free_region_list.insert_tail(region);
            } else if (region->is_full_just_now_unsafe()) [[unlikely]] {
                // region in full list
                full_region_list.remove_list_safe_region_unsafe(region);
                usable_region_list[region->get_bin()].insert_tail(region);
            }
        }
    }

    void info() {
        // #ifndef NDEBUG
        size_t allocated_size = used_heap_idx * RegionSize;
        std::cout << "allocated size: " << allocated_size << std::endl;
        std::cout << "allocated size(KB): "
                  << static_cast<double>(allocated_size) / (1L << 10)
                  << std::endl;
        std::cout << "allocated size(MB): "
                  << static_cast<double>(allocated_size) / (1L << 20)
                  << std::endl;
        std::cout << "allocated size(GB): "
                  << static_cast<double>(allocated_size) / (1L << 30)
                  << std::endl;
        // #endif
    }
};

extern RemoteGlobalHeap remote_global_heap;

class RemoteThreadHeap {
private:
    RemoteRegionHead *regions[RegionBinCount];

public:
    RemoteThreadHeap() { std::memset(regions, 0, sizeof(regions)); }

    ~RemoteThreadHeap() {
        for (size_t i = 0; i < RegionBinCount; i++) {
            if (regions[i]) {
                remote_global_heap.return_back_region(regions[i]);
            }
        }
    }

    uint64_t allocate(size_t size) {
        size_t wsize = wsize_from_size(size);
        size_t bin = bin_from_wsize(wsize);
        assert(get_bin_size(bin) >= size);
        assert(bin == 0 || get_bin_size(bin - 1) < size + sizeof(void *));
        RemoteRegionHead *region = regions[bin];
        // region will not empty for now
        // because it has >= 1 object allocated by this thread
        if (region) [[likely]] {
            region->lock();
            uint64_t addr = region->allocate_unsafe();
            if (addr != InvalidRemoteAddr) {
                region->unlock();
                return addr;
            } else {
                remote_global_heap.return_back_region_unsafe(region);
                region->unlock();
            }
        }
        region = remote_global_heap.allocate_region(bin);
        regions[bin] = region;
        if (region) [[likely]] {
            return region->allocate();
        } else {
            return InvalidRemoteAddr;
        }
    }

    void deallocate(uint64_t addr) { remote_global_heap.deallocate(addr); }
};

extern thread_local RemoteThreadHeap remote_thread_heap;

}  // namespace remote
}  // namespace allocator
}  // namespace FarLib
