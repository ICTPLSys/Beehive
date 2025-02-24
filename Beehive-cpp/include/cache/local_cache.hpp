#pragma once

#include <limits>
#include <queue>
#include <unordered_map>

#include "handler.hpp"
#include "local_allocator.hpp"
#include "object.hpp"
#include "rdma/client.hpp"
#include "remote_allocator.hpp"
#include "utils/stats.hpp"

namespace Beehive {

namespace cache {

enum FetchState {
    FETCH_PRESENT,
    FETCH_ENQUEUE,
    FETCH_QUEUE_FULL,
    FETCH_CACHE_FULL,
};

enum ObjectState {
    OBJ_LOCAL,
    OBJ_REMOTE,
    OBJ_FETCHING,
    OBJ_EVICTING,
};

struct FarObjectEntry {
    bool is_dirty : 1;
    ObjectState state : 31;
    uint16_t ref_count;
    uint16_t hotness;
    void *local_ptr;

    void inc_hotness() {
        if (hotness < std::numeric_limits<uint16_t>::max() - 1) {
            hotness++;
        }
    }

    uint16_t dec_hotness() {
        if (hotness > 0) {
            hotness--;
        }
        return hotness;
    }

    bool is_local() { return state == OBJ_LOCAL; }
} __attribute__((packed, aligned(1)));

using CacheItem = FarObjectEntry;

constexpr size_t CacheItemSize = sizeof(CacheItem);
static_assert(CacheItemSize == 16);

class LocalCache {
private:
    RemoteAllocator remote_allocator;
    LocalAllocator local_allocator;
    std::unordered_map<far_obj_t, CacheItem> cache;
    std::queue<far_obj_t> evict_queue;
    size_t evict_batch_size;

    static std::unique_ptr<LocalCache> default_instance;

private:
    size_t check_cq_impl() {
        // profile::Profiler profiler;
        auto client = rdma::Client::get_default();
        size_t count = client->check_cq();
        // profile::Profiler poll_profiler;
        const ibv_wc *wc = client->get_work_completions();
        // poll_profiler.end(profile::POLL_RDMA);
        for (size_t i = 0; i < count; i++) {
            // profile::Profiler handle_wc_profiler;
            ASSERT(wc[i].status == IBV_WC_SUCCESS);
            std::pair<far_obj_t, CacheItem> *it =
                reinterpret_cast<std::pair<far_obj_t, CacheItem> *>(
                    wc[i].wr_id);
            CacheItem *cache_item = &(it->second);
            switch (wc[i].opcode) {
            case IBV_WC_RDMA_WRITE: {
                switch (cache_item->state) {
                case OBJ_EVICTING:
                    cache_item->state = OBJ_REMOTE;
                    local_allocator.deallocate(cache_item->local_ptr);
                    cache.erase(it->first);
                    break;
                case OBJ_LOCAL:
                    // another access occurs
                    break;
                default:
                    ERROR("invalid state when swap out is done");
                }
                break;
            }
            case IBV_WC_RDMA_READ: {
                cache_item->state = OBJ_LOCAL;
                break;
            }
            }
            // handle_wc_profiler.end(profile::HANDLE_WORK_COMPLETION);
        }
        // profiler.end(profile::CHECK_CQ);
        return count;
    }

    void *allocate_local(size_t size, bool retry = true) {
    retry_allocation:
        void *local_ptr = local_allocator.allocate(size);
        if (local_ptr == nullptr) [[unlikely]] {
            ASSERT(size < local_allocator.get_buffer_size());
            // can not allocate, need eviction
            size_t completion_count = check_cq_impl();
            if (!retry && completion_count == 0) {
                return nullptr;
            }
            if (completion_count == 0) {
                try_evict_for(size);
            }
            goto retry_allocation;
        }
        return local_ptr;
    }

    auto find_in_cache(far_obj_t obj) {
        profile::Profiler profiler;
        auto it = cache.find(obj);
        profiler.end(profile::FIND_CACHE);
        return it;
    }

    void do_evict(std::unordered_map<far_obj_t, CacheItem>::iterator &it) {
        auto &obj = it->first;
        auto &cache_item = it->second;
        if (cache_item.is_dirty) {
            // dirty
            cache_item.state = OBJ_EVICTING;
            cache_item.is_dirty = false;
            uint64_t wr_id = reinterpret_cast<uint64_t>(&*it);
        retry_post:
            bool posted = rdma::Client::get_default()->post_write(
                obj.obj_id, cache_item.local_ptr, obj.size, wr_id);
            // will be deallocated when write is finished
            if (!posted) {
                while (check_cq_impl() == 0);
                goto retry_post;
            }
        } else {
            // clean
            local_allocator.deallocate(it->second.local_ptr);
            cache.erase(it);
        }
    }

    bool can_evict(CacheItem &cache_item) {
        return cache_item.ref_count == 0 && cache_item.state == OBJ_LOCAL;
    }

    size_t try_evict_one() {
        if (evict_queue.empty()) {
            WARN("can not evict, queue empty");
            std::cerr << "evict queue size: " << evict_queue.size()
                      << std::endl;
            std::cerr << "cache size: " << cache.size() << std::endl;
            return local_allocator.get_buffer_size();
        }
        far_obj_t obj = evict_queue.front();
        evict_queue.pop();
        auto it = find_in_cache(obj);
        if (it == cache.end()) {
            // maybe this object is deallocated
            return 0;
        }
        auto &cache_item = it->second;
        if (cache_item.dec_hotness() == 0 && can_evict(cache_item)) {
            do_evict(it);
            return obj.size;
        } else {
            evict_queue.push(obj);
        }
        return 0;
    }

    void try_evict_for(size_t min_size) {
        profile::Profiler profiler;
        min_size = std::max(min_size, evict_batch_size);
        size_t evicted_size = 0;
        while (evicted_size < min_size) {
            evicted_size += try_evict_one();
        }
        profiler.end(profile::BATCHED_EVICT);
    }

public:
    LocalCache(void *local_buffer, size_t local_buffer_size,
               size_t remote_buffer_size, size_t evict_batch_size)
        : remote_allocator(remote_buffer_size),
          local_allocator(local_buffer, local_buffer_size),
          evict_batch_size(evict_batch_size) {}

    static LocalCache *init_default(void *local_buffer,
                                    size_t local_buffer_size,
                                    size_t remote_buffer_size,
                                    size_t evict_batch_size) {
        default_instance.reset(new LocalCache(local_buffer, local_buffer_size,
                                              remote_buffer_size,
                                              evict_batch_size));
        return default_instance.get();
    }

    static LocalCache *get_default() { return default_instance.get(); }

    static void destroy_default() { default_instance.reset(); }

    // allocate a new object at remote and local
    // ref count = 1
    std::pair<far_obj_t, void *> allocate(uint32_t size) {
        void *local_ptr = allocate_local(size);
        uint64_t obj_id = remote_allocator.allocate(size);
        far_obj_t far_obj = {
            .size = size,
            .obj_id = obj_id,
        };
        CacheItem cache_item = {
            .is_dirty = false,
            .state = OBJ_LOCAL,
            .ref_count = 1,
            .hotness = 1,
            .local_ptr = local_ptr,
        };
        cache.insert({far_obj, cache_item});
        evict_queue.push(far_obj);
        return {far_obj, local_ptr};
    }

    // deallocate the object at remote and local
    void deallocate(far_obj_t obj) {
        auto it = find_in_cache(obj);
        if (it != cache.end()) {
            ASSERT(it->second.state == OBJ_LOCAL);  // TODO
            local_allocator.deallocate(it->second.local_ptr);
            cache.erase(it);
        }
        remote_allocator.deallocate(obj.obj_id);
    }

    FetchState prefetch(far_obj_t obj, bool retry = true) {
        profile::Profiler profiler;
        auto it = find_in_cache(obj);
        if (it != cache.end()) {
            switch (it->second.state) {
            case OBJ_EVICTING:
                it->second.state = OBJ_LOCAL;
                [[fallthrough]];
            case OBJ_LOCAL:
                profile::record_cache_access_state(profile::PREFETCH_PRESENT);
                // just call callback and return, do not change ref count
                it->second.inc_hotness();
                profiler.end(profile::POST_PREFETCH_PRESENT);
                return FETCH_PRESENT;
            case OBJ_FETCHING:
                profile::record_cache_access_state(profile::PREFETCH_FETCHING);
                it->second.inc_hotness();
                profiler.end(profile::POST_PREFETCH_FETCHING);
                return FETCH_ENQUEUE;
            default:
                ERROR("invalid object state");
            }
        } else {
            // at remote, fetch this object
            profile::record_cache_access_state(profile::PREFETCH_REMOTE);
            void *local_ptr = allocate_local(obj.size, retry);
            if (!retry && local_ptr == nullptr) {
                return FETCH_CACHE_FULL;
            }
            auto [insert_it, _] = cache.insert({obj, CacheItem{
                                                         .is_dirty = false,
                                                         .state = OBJ_FETCHING,
                                                         .ref_count = 0,
                                                         .hotness = 1,
                                                         .local_ptr = local_ptr,
                                                     }});
            evict_queue.push(obj);
            CacheItem *cache_item = &(insert_it->second);
            uint64_t wr_id = reinterpret_cast<uint64_t>(&*insert_it);
        retry_post:
            bool posted = rdma::Client::get_default()->post_read(
                obj.obj_id, local_ptr, obj.size, wr_id);
            if (!posted) {
                size_t completion_count = check_cq_impl();
                if (retry || completion_count != 0) {
                    goto retry_post;
                } else {
                    cache.erase(insert_it);
                    local_allocator.deallocate(local_ptr);
                    return FETCH_QUEUE_FULL;
                }
            }
            profiler.end(profile::POST_PREFETCH_REMOTE);
            return FETCH_ENQUEUE;
        }
    }

    // fetch the object to local and return its local pointer.
    // will increase the ref count of the cache item
    void *sync_fetch(far_obj_t obj) {
        profile::Profiler profiler;
        auto it = find_in_cache(obj);
        if (it != cache.end()) {
            switch (it->second.state) {
            case OBJ_EVICTING:
                it->second.state = OBJ_LOCAL;
                [[fallthrough]];
            case OBJ_LOCAL:
                profile::record_cache_access_state(profile::FETCH_PRESENT);
                it->second.ref_count++;
                it->second.inc_hotness();
                profiler.end(profile::SYNC_FETCH_PRESENT);
                return it->second.local_ptr;
            case OBJ_FETCHING:
                profile::record_cache_access_state(profile::FETCH_FETCHING);
                it->second.ref_count++;
                it->second.inc_hotness();
                {
                    while (it->second.state != OBJ_LOCAL) {
                        profile::Profiler fetching_check_profiler;
                        size_t count = check_cq_impl();
                        if (count == 0) {
                            fetching_check_profiler.end(
                                profile::OBJ_FETCHING_CHECK_CQ_N);
                        } else {
                            fetching_check_profiler.end(
                                profile::OBJ_FETCHING_CHECK_CQ_Y);
                        }
                    }
                }
                profiler.end(profile::SYNC_FETCH_FETCHING);
                return it->second.local_ptr;
            default:
                ERROR("invalid object state");
            }
        } else {
            // at remote, fetch this object
            profile::record_cache_access_state(profile::FETCH_REMOTE);
            void *local_ptr = allocate_local(obj.size, true);
            auto [insert_it, _] = cache.insert({obj, CacheItem{
                                                         .is_dirty = false,
                                                         .state = OBJ_FETCHING,
                                                         .ref_count = 1,
                                                         .hotness = 1,
                                                         .local_ptr = local_ptr,
                                                     }});
            evict_queue.push(obj);
            CacheItem *cache_item = &(insert_it->second);
            uint64_t wr_id = reinterpret_cast<uint64_t>(&*insert_it);
        retry_post:
            bool posted = rdma::Client::get_default()->post_read(
                obj.obj_id, local_ptr, obj.size, wr_id);
            if (!posted) {
                check_cq_impl();
                goto retry_post;
            }
            while (insert_it->second.state != OBJ_LOCAL) {
                check_cq_impl();
            }
            profiler.end(profile::SYNC_FETCH_REMOTE);
            return local_ptr;
        }
    }

    // We can use this low-level API to
    // 1. check whether object is present
    // 2. allocate local memory and post RDMA request if not present
    // 3. hold the pointer to CacheItem to check whether the request is
    // completed
    // !!! ATTENTION: this will add ref count, DO NOT use as prefetch
    CacheItem *post_fetch(far_obj_t obj) {
        profile::Profiler profiler;
        auto it = find_in_cache(obj);
        if (it != cache.end()) {
            switch (it->second.state) {
            case OBJ_EVICTING:
                it->second.state = OBJ_LOCAL;
                [[fallthrough]];
            case OBJ_LOCAL:
                profile::record_cache_access_state(profile::FETCH_PRESENT);
                // just call callback and return, do not change ref count
                it->second.ref_count++;
                it->second.inc_hotness();
                profiler.end(profile::POST_PREFETCH_PRESENT);
                return &(it->second);
            case OBJ_FETCHING:
                profile::record_cache_access_state(profile::FETCH_FETCHING);
                it->second.ref_count++;
                it->second.inc_hotness();
                profiler.end(profile::POST_PREFETCH_FETCHING);
                return &(it->second);
            default:
                ERROR("invalid object state");
            }
        } else {
            // at remote, fetch this object
            profile::record_cache_access_state(profile::FETCH_REMOTE);
            void *local_ptr = allocate_local(obj.size, true);
            auto [insert_it, _] = cache.insert({obj, CacheItem{
                                                         .is_dirty = false,
                                                         .state = OBJ_FETCHING,
                                                         .ref_count = 1,
                                                         .hotness = 1,
                                                         .local_ptr = local_ptr,
                                                     }});
            evict_queue.push(obj);
            CacheItem *cache_item = &(insert_it->second);
            uint64_t wr_id = reinterpret_cast<uint64_t>(&*insert_it);
        retry_post:
            bool posted = rdma::Client::get_default()->post_read(
                obj.obj_id, local_ptr, obj.size, wr_id);
            if (!posted) {
                size_t completion_count = check_cq_impl();
                goto retry_post;
            }
            profiler.end(profile::POST_PREFETCH_REMOTE);
            return cache_item;
        }
    }

    void *fetch_with_miss_handler(far_obj_t obj,
                                  const DataMissHandler &on_data_miss) {
        auto cache_item = post_fetch(obj);
        if (cache_item->state != OBJ_LOCAL) [[unlikely]] {
            fetch_ddl_t ddl = create_fetch_ddl();
            ASSERT(cache_item->state == OBJ_FETCHING);
            on_data_miss(cache_item, ddl);
            while (cache_item->state != OBJ_LOCAL) {
                check_cq();
            }
        }
        return cache_item->local_ptr;
    }

    // decrease ref count of the cache item
    // if ref count goes to 0,
    // 1. if is dirty, post an write request and remove the cache item after
    //    write is completed
    // 2. if is clean, remove the cache item immediately
    // local object will be freed after cache item is removed
    void release_cache(far_obj_t obj, bool dirty) {
        auto it = find_in_cache(obj);
        ASSERT(it != cache.end());
        CacheItem *cache_item = &(it->second);
        cache_item->is_dirty |= dirty;
        cache_item->ref_count--;
    }

    // check cq, call task when read is complete, remove cache item if write is
    // completed
    size_t check_cq() { return check_cq_impl(); }

    size_t get_remote_allocated() const {
        return remote_allocator.get_allocated();
    }
};

}  // namespace cache

}  // namespace Beehive