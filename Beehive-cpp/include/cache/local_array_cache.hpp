#pragma once

#include <x86intrin.h>

#include <cstdint>
#include <iostream>
#include <limits>
#include <queue>

#include "async/async.hpp"
#include "handler.hpp"
#include "local_allocator.hpp"
#include "object.hpp"
#include "rdma/client.hpp"
#include "remote_allocator.hpp"
#include "utils/debug.hpp"
#include "utils/stats.hpp"

namespace Beehive {
namespace cache {
// put it at the top of namespace cache for compatibility
enum FetchState {
    FETCH_PRESENT,
    FETCH_ENQUEUE,
    FETCH_QUEUE_FULL,
    FETCH_CACHE_FULL,
    FETCH_INVALID
};

static constexpr size_t KB = (1 << 10);
static constexpr size_t MB = (KB << 10);
static constexpr size_t GB = (MB << 10);

enum EntryState { FREE, LOCAL, REMOTE, FETCHING, EVICTING };

struct entry_t {
    struct {
        uint64_t dirty : 1;
        uint64_t state : 3;
        uint64_t hotness : 12;
        uint64_t local_addr : 48;
    };
    struct {
        uint64_t ref_cnt : 16;
        uint64_t remote_addr : 48;
    };

    static constexpr uint64_t HOTNESS_MAX = (1 << 12) - 1;

    void *get_local_addr() const {
        return reinterpret_cast<void *>(local_addr);
    }

    void set_local_addr(void *p) { local_addr = reinterpret_cast<uint64_t>(p); }
};

static_assert(sizeof(entry_t) == 16);

class FarObjectEntry : entry_t {
public:
    FarObjectEntry() { state = FREE; }

    FarObjectEntry(void *local_ptr, size_t remote_ptr) {
        dirty = 0;
        state = LOCAL;
        hotness = 0;
        set_local_addr(local_ptr);
        ref_cnt = 0;
        remote_addr = remote_ptr;
    }

    void inc_ref() { ref_cnt++; }

    void dec_ref() { ref_cnt--; }

    void inc_hotness() {
        if (hotness < HOTNESS_MAX) {
            hotness++;
        }
    }

    auto dec_hotness() {
        if (hotness > 0) {
            hotness--;
        }
        return hotness;
    }

    void update_dirty(bool dirty) {
        if (dirty) this->dirty = 1;
    }

    bool can_evict() { return ref_cnt == 0 && state == LOCAL; }

    void reset(void *local_ptr, size_t remote_ptr) {
        dirty = false;
        state = LOCAL;
        hotness = 1;
        set_local_addr(local_ptr);
        ref_cnt = 1;
        remote_addr = remote_ptr;
    }

    bool is_local() { return state == EntryState::LOCAL; }

    bool is_free() { return state == EntryState::FREE; }

    friend class ArrayCache;
};

class ArrayCache {
private:
    RemoteAllocator remote_allocator;
    LocalAllocator local_allocator;
    FarObjectEntry *cache;
    size_t evict_batch_size;
    size_t max_id;
    std::queue<uint64_t> free_queue;
    std::queue<far_obj_t> evict_queue;

    static std::unique_ptr<ArrayCache> default_instance;

    void *allocate_local(size_t size, bool retry = true) {
    retry_alloc:
        void *local_ptr = local_allocator.allocate(size);
        if (local_ptr == nullptr) {
            // check if size is too big
            ASSERT(size < local_allocator.get_buffer_size());
            // local is not enough
            // poll cq to deal complete request above to update cache
            auto complete_cnt = check_cq();
            if (!retry && complete_cnt == 0) {
                return nullptr;
            }
            if (complete_cnt == 0) {
                try_evict_for(size);
            }
            goto retry_alloc;
        }
        return local_ptr;
    }

    auto allocate_remote(size_t size) {
        return remote_allocator.allocate(size);
    }

    void do_evict(far_obj_t obj, FarObjectEntry &entry) {
        if (entry.dirty) {
            // dirty, need write back
            entry.state = EntryState::EVICTING;
            entry.dirty = false;
            uint64_t wr_id = obj.to_uint64_t();
        retry_post:
            bool post = rdma::Client::get_default()->post_write(
                entry.remote_addr, entry.get_local_addr(), obj.size, wr_id,
                obj.obj_id);
            if (!post) {
                // RDMA send fail, CQ full or send queue full
                // poll cq
                while (check_cq() == 0);
                goto retry_post;
            }
            // it will deallocate when poll cq
        } else {
            entry.state = EntryState::REMOTE;
            local_allocator.deallocate(entry.get_local_addr(), obj.size);
        }
    }

    size_t try_evict_one() {
        if (evict_queue.empty()) {
            WARN("can not evict, queue empty");
            std::cerr << "evict queue size: " << evict_queue.size()
                      << std::endl;
            return local_allocator.get_buffer_size();
        }
        far_obj_t obj = evict_queue.front();
        evict_queue.pop();
        assert(obj.obj_id < max_id);
        auto &entry = cache[obj.obj_id];
        if (entry.is_free()) [[unlikely]] {
            return 0;
        }
        if (entry.dec_hotness() == 0 && entry.can_evict()) {
            do_evict(obj, entry);
            return obj.size;
        } else {
            evict_queue.push(obj);
        }
        return 0;
    }

    void try_evict_for(size_t size) {
        size = std::min(std::max(evict_batch_size, size),
                        local_allocator.get_buffer_size());
        size_t evict_size = 0;
        while (evict_size < size) {
            evict_size += try_evict_one();
        }
    }

    obj_id_t alloc_id() {
        if (free_queue.empty()) {
            auto allocate_id = max_id;
            max_id++;
            return allocate_id;
        }
        // some id has free, reuse it
        auto allocate_id = free_queue.front();
        free_queue.pop();
        return allocate_id;
    }

public:
    static constexpr size_t MAX_OBJ_N = 256 * MB;
    ArrayCache(void *local_buf, size_t local_buf_size, size_t remote_buf_size,
               size_t evict_batch_size)
        : local_allocator(local_buf, local_buf_size),
          remote_allocator(remote_buf_size),
          evict_batch_size(evict_batch_size),
          max_id(0) {
        cache = new FarObjectEntry[MAX_OBJ_N];
        INFO("here is array cache");
    }

    ~ArrayCache() {
        while (!evict_queue.empty()) {
            auto obj = evict_queue.front();
            evict_queue.pop();
            auto &entry = cache[obj.obj_id];
            if (entry.is_local()) {
                deallocate(obj);
            }
        }
        delete[] cache;
    }

    static ArrayCache *init_default(void *local_buf, size_t local_buf_size,
                                    size_t remote_buf_size,
                                    size_t evict_batch_size) {
        default_instance.reset(new ArrayCache(
            local_buf, local_buf_size, remote_buf_size, evict_batch_size));
        return default_instance.get();
    }

    static ArrayCache *get_default() { return default_instance.get(); }

    static void destroy_default() { default_instance.reset(); }

    std::pair<far_obj_t, void *> allocate(size_t size) {
        void *local_ptr = allocate_local(size);
        auto remote_ptr = allocate_remote(size);
        obj_id_t id = alloc_id();
        cache[id].reset(local_ptr, remote_ptr);
        far_obj_t obj = {.size = size, .obj_id = id};
        evict_queue.push(obj);
        return {obj, local_ptr};
    }

    void deallocate(far_obj_t obj) {
        if (obj.obj_id < max_id) {
            // TODO deallocate it at remote
            auto &entry = cache[obj.obj_id];
            if (entry.is_free()) [[unlikely]] {
                ERROR("double deallocation");
            }
            if (entry.is_local()) {
                local_allocator.deallocate(entry.get_local_addr(), obj.size);
            }
            remote_allocator.deallocate(entry.remote_addr);
            free_queue.push(obj.obj_id);
            entry.state = EntryState::FREE;
        }
        // id >= max_id is meaningless
    }

    FetchState prefetch(far_obj_t obj, bool retry = true) {
        if (obj.obj_id >= max_id) {
            ERROR("prefetch an invalid id");
            return FETCH_INVALID;
        }
        auto &entry = cache[obj.obj_id];
        switch (entry.state) {
        case EntryState::FREE:
            WARN("prefetch an free id");
            return FETCH_INVALID;
        case EntryState::EVICTING:
            entry.state = EntryState::LOCAL;
            [[fallthrough]];
        case EntryState::LOCAL:
            profile::record_cache_access_state(profile::PREFETCH_PRESENT);
            entry.inc_hotness();
            return FETCH_PRESENT;
        case EntryState::FETCHING:
            profile::record_cache_access_state(profile::PREFETCH_FETCHING);
            entry.inc_hotness();
            return FETCH_ENQUEUE;
        default:
            break;
        }
        // object is remote, anyway we need to fetch it
        profile::record_cache_access_state(profile::PREFETCH_REMOTE);
        void *local_ptr = allocate_local(obj.size, retry);
        if (!retry && local_ptr == nullptr) {
            return FETCH_CACHE_FULL;
        }
        evict_queue.push(obj);
        uint64_t wr_id = obj.to_uint64_t();
        entry.state = EntryState::FETCHING;
    retry_post:
        bool post = rdma::Client::get_default()->post_read(
            entry.remote_addr, local_ptr, obj.size, wr_id, obj.obj_id);
        if (!post) {
            size_t complete_count = check_cq();
            if (retry || complete_count != 0) {
                goto retry_post;
            } else {
                // prefetch fail, deallocate local buffer
                local_allocator.deallocate(local_ptr, obj.size);
                return FETCH_QUEUE_FULL;
            }
        }
        // prefetch post success, set entry
        entry.set_local_addr(local_ptr);
        return FETCH_ENQUEUE;
    }

    void *sync_fetch(far_obj_t obj) {
        ON_MISS_BEGIN
        ON_MISS_END
        return fetch_with_miss_handler(obj, __on_miss__);
    }

    // low-level API, corresponding to hashmap cache
    // of course, this API increase ref too
    FarObjectEntry *post_fetch(far_obj_t obj) {
        if (obj.obj_id >= max_id) [[unlikely]] {
            ERROR("post fetch: fetch an invalid id");
            return nullptr;
        }
        FarObjectEntry &entry = cache[obj.obj_id];
        switch (entry.state) {
        case EntryState::FREE:
            WARN("post fetch: fetch a free id");
            break;
        case EntryState::EVICTING:
            entry.state = EntryState::LOCAL;
            [[fallthrough]];
        case EntryState::LOCAL:
            profile::record_cache_access_state(profile::FETCH_PRESENT);
            entry.inc_ref();
            entry.inc_hotness();
            break;
        case EntryState::FETCHING:
            profile::record_cache_access_state(profile::FETCH_FETCHING);
            entry.inc_ref();
            entry.inc_hotness();
            break;
        case EntryState::REMOTE: {
            profile::record_cache_access_state(profile::FETCH_REMOTE);
            void *local_ptr = allocate_local(obj.size, true);
            entry.set_local_addr(local_ptr);
            entry.state = EntryState::FETCHING;
            entry.inc_ref();
            entry.inc_hotness();
            evict_queue.push(obj);
            uint64_t wr_id = obj.to_uint64_t();
        retry_post:
            bool post = rdma::Client::get_default()->post_read(
                entry.remote_addr, local_ptr, obj.size, wr_id, obj.obj_id);
            if (!post) {
                check_cq();
                goto retry_post;
            }
            break;
        }
        default:
            ERROR("post fetch: invalid state");
        }
        return &entry;
    }

    bool check_fetch(FarObjectEntry *entry, fetch_ddl_t &ddl) {
        fetch_ddl_t now = __rdtsc();
        if (now >= ddl) [[unlikely]] {
            check_cq();
            if (entry->is_local()) return true;
            ddl = delay_fetch_ddl(ddl);
        }
        return false;
    }

    void *fetch_with_miss_handler(far_obj_t obj,
                                  const DataMissHandler &handler) {
        auto entry = post_fetch(obj);
        fetch_ddl_t ddl = create_fetch_ddl();
        if (entry->state != EntryState::LOCAL) [[unlikely]] {
            ASSERT(entry->state == EntryState::FETCHING);
            while (async::out_of_order_task) [[unlikely]] {
                async::out_of_order_task->blocked_entry = entry;
                async::out_of_order_task->yield(async::FETCH);
                if (check_fetch(entry, ddl)) goto back;
            }
            handler(entry, ddl);
            while (entry->state != EntryState::LOCAL) {
                check_cq();
            }
        }
    back:
        return entry->get_local_addr();
    }

    void release_cache(far_obj_t obj, bool dirty) {
        if (obj.obj_id >= max_id) {
            ERROR("release cache: invalid id");
            return;
        }
        auto &entry = cache[obj.obj_id];
        if (entry.is_free()) {
            ERROR("release cache: try to release a free id");
            return;
        }
        entry.dec_ref();
        entry.update_dirty(dirty);
    }

    // here wr_id = object id
    size_t check_cq() {
        auto client = rdma::Client::get_default();
        ibv_wc wc[rdma::CHECK_CQ_BATCH_SIZE];
        size_t cnt = client->check_cq(wc, rdma::CHECK_CQ_BATCH_SIZE);
        for (size_t i = 0; i < cnt; i++) {
            ASSERT(wc[i].status == IBV_WC_SUCCESS);
            far_obj_t obj = far_obj_t::from_uint64_t(wc[i].wr_id);
            ASSERT(obj.obj_id < max_id);
            auto &entry = cache[obj.obj_id];
            if (entry.is_free()) {
                WARN("check cq: check a free id");
                continue;
            }
            switch (wc[i].opcode) {
            case IBV_WC_RDMA_WRITE: {
                switch (entry.state) {
                case EntryState::EVICTING: {
                    entry.state = EntryState::REMOTE;
                    local_allocator.deallocate(entry.get_local_addr(),
                                               obj.size);
                    break;
                }
                case EntryState::LOCAL: {
                    break;
                }
                default:
                    ERROR("check_cq: invalid state when evict");
                }
                break;
            }
            case IBV_WC_RDMA_READ: {
                entry.state = EntryState::LOCAL;
                break;
            }
            }
        }
        return cnt;
    }
};

}  // namespace cache

}  // namespace Beehive
