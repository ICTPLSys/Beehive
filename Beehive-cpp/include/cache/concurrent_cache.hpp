#pragma once
#include <x86intrin.h>

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "cache/entry.hpp"
#include "cache/handler.hpp"
#include "cache/object.hpp"
#include "cache/scope.hpp"
#include "rdma/client.hpp"
#include "region_based_allocator.hpp"
#include "remote_allocator.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/fork_join.hpp"
#include "utils/signal.hpp"
#include "utils/stats.hpp"
#include "utils/uthreads.hpp"

namespace FarLib {
namespace cache {
class ConcurrentArrayCache {
    friend class DereferenceScope;

private:
    RemoteAllocator remote_allocator;
    std::atomic_bool working;
    std::atomic_bool mutator_can_not_allocate;
    uthread::Mutex eviction_mutex;
    uthread::Condition eviction_cond;
    uthread::Condition mutator_cond;
    std::unique_ptr<UThread> master_evacuation_thread;
    size_t evacuate_thread_cnt;
    std::atomic_flag flag;

    static std::unique_ptr<ConcurrentArrayCache> default_instance;

    enum MutatorState { OutOfScope, InScopeV0, InScopeV1, MutatorStateCount };
    struct alignas(64) {
        std::atomic<MutatorState> global_state = InScopeV0;
        std::atomic_size_t count[MutatorStateCount];
    } mutator_states;

    template <typename T>
    friend class UniqueFarPtr;

private:
    void *allocate_local(size_t size, far_obj_t obj, DereferenceScope &scope) {
        allocator::BlockHead *block =
            allocator::thread_local_allocate(size, obj, &scope);
        if (block == nullptr) [[unlikely]] {
                // can not allocate, evict
#ifdef ASSERT_ALL_LOCAL
                ERROR("should not evict in allocate local");
#endif
            bool suspended = profile::suspend_work();
            scope.begin_eviction();
            size_t retry_count = 0;
            do {
                on_demand_invoke_eviction();
                block = allocator::thread_local_allocate(size, obj, nullptr);
                if (++retry_count > 1024) {
                    ERROR("can not allocate!");
                }
            } while (block == nullptr);
            scope.end_eviction();
            profile::resume_work(suspended);
        }
        assert(obj == block->obj_meta_data);
        return static_cast<void *>(block + 1);
    }

    uint64_t allocate_remote(size_t size) {
        return remote_allocator.allocate(size);
    }

    static constexpr size_t EvictBatchSize = 64;
    struct EvictBuffer {
        struct EvictRequest {
            void *local_addr;
            uint64_t remote_addr : 48;
            uint64_t size : 16;
        };
        EvictRequest requests[EvictBatchSize];
        size_t count = 0;
    };

    // wr_id = object pointer
    template <bool IsReadRequest>
    void post_rdma_request(far_obj_t obj) {
        auto &entry = get_entry_of(obj);
        uint64_t remote_addr = entry.remote_addr();
        void *local_addr = entry.local_addr();
        auto block = static_cast<allocator::BlockHead *>(local_addr) - 1;
        assert(block->obj_meta_data == obj);
        uint64_t wr_id = reinterpret_cast<uint64_t>(local_addr);
    retry:
        profile::add_post_retry_count();
        bool posted;
        if constexpr (IsReadRequest) {
            assert(entry.load_state().state == FETCHING);
            profile::trace_post_rdma_read(wr_id, remote_addr);
            posted = rdma::Client::get_default()->post_read(
                remote_addr, local_addr, obj.size, wr_id, obj.obj_id);
        } else {
            posted = rdma::Client::get_default()->post_write(
                remote_addr, local_addr, obj.size, wr_id, obj.obj_id);
        }
        if (!posted) {
            check_cq();
            goto retry;
        }
    }

    void post_read_request(far_obj_t obj) {
        profile::start_post_fetch();
        post_rdma_request<true>(obj);
        profile::end_post_fetch();
    }

    void post_write_requests(EvictBuffer &buffer) {
        if (buffer.count == 0) return;
        assert(buffer.count <= EvictBatchSize);
        auto client = rdma::Client::get_default();

        ibv_sge write_sge[EvictBatchSize];
        ibv_send_wr write_wr[EvictBatchSize];
        for (size_t i = 0; i < buffer.count; i++) {
            auto &req = buffer.requests[i];
            client->build_send_wr(write_wr[i], write_sge[i], req.remote_addr,
                                  req.local_addr, req.size,
                                  reinterpret_cast<uint64_t>(req.local_addr),
                                  true, IBV_WR_RDMA_WRITE);
            if (i > 0) {
                write_wr[i - 1].next = &(write_wr[i]);
            }
        }
        ibv_send_wr *bad_wr = write_wr;
        bool posted_all = client->post_writes(bad_wr, &bad_wr);
        while (!posted_all) [[unlikely]] {
            check_cq();
            posted_all = client->post_writes(bad_wr, &bad_wr);
        }

        buffer.count = 0;
    }

    void add_write_request(EvictBuffer &buffer, void *local_addr,
                           uint64_t remote_addr, uint64_t size) {
        if (buffer.count == EvictBatchSize) [[unlikely]] {
            post_write_requests(buffer);
        }
        assert(buffer.count < EvictBatchSize);
        buffer.requests[buffer.count] = {local_addr, remote_addr, size};
        buffer.count++;
    }

    EntryState try_mark(allocator::BlockHead *block) {
    retry:
        far_obj_t obj = block->obj_meta_data.load();
        auto entry = &get_entry_of(obj);
        if (entry == nullptr) {
            // this object is deallocated by user
            // mark it as garbage
            return FREE;
        }
        auto old_state = entry->load_state();
        if (old_state.state == FREE ||
            entry->local_addr() != block->get_object_ptr()) [[unlikely]] {
            goto retry;
        }
        auto new_state = old_state;
        new_state.dec_hotness();
        if (new_state.can_evict()) {
            // mark this object
            new_state.state = MARKED;
            if (!entry->cas_state_weak(old_state, new_state)) [[unlikely]] {
                goto retry;
            }
            profile::count_mark();
            return MARKED;
        } else {
            // not evictable, but its hotness is decreased
            if (!entry->cas_state_weak(old_state, new_state)) [[unlikely]] {
                goto retry;
            }
            profile::count_not_mark();
            return new_state.state;
        }
    }

    EntryState try_evict(allocator::BlockHead *block,
                         EvictBuffer &evict_buffer) {
    retry:
        far_obj_t obj = block->obj_meta_data.load();
        auto entry = &get_entry_of(obj);
        if (entry == nullptr) {
            // this object is deallocated by user
            return FREE;
        }
        auto old_state = entry->load_state();
        if (old_state.state == FREE ||
            entry->local_addr() != block->get_object_ptr()) [[unlikely]]
            goto retry;
        auto new_state = old_state;
        switch (new_state.state) {
        case MARKED: {
            if (old_state.dirty) {
                new_state.dirty = 0;
                new_state.state = EVICTING;
                new_state.inc_ref_cnt();  // dec on rdma work completed
                uint64_t remote_addr = entry->remote_addr();
                if (!entry->cas_state_weak(old_state, new_state)) [[unlikely]] {
                    goto retry;
                }
                add_write_request(evict_buffer, block->get_object_ptr(),
                                  remote_addr, obj.size);
                return EVICTING;
            } else {
                assert(new_state.ref_cnt == 0);
                new_state.state = REMOTE;
                if (!entry->cas_state_weak(old_state, new_state)) [[unlikely]] {
                    goto retry;
                }
                block->obj_meta_data.store(far_obj_t::null(),
                                           std::memory_order::relaxed);
                return FREE;
            }
            return new_state.state;
        }
        default:
            // this object is touched after marked
            // should not evict
            return new_state.state;
        }
    }

    void evacuate_work() {
        uint32_t timestamp = 0;
        std::function<void(size_t)> fn_mark = [this, &timestamp](size_t) {
            mark_phase(timestamp);
        };
        std::function<void(size_t)> fn_evacuate = [this, &timestamp](size_t) {
            evacuate_phase(timestamp);
        };
        std::function<void(size_t)> fn_gc = [this, &timestamp](size_t) {
            gc_phase(timestamp);
        };
        while (true) {
            if (!working.load()) return;
            if (!memory_low()) {
                uthread::lock(&eviction_mutex);
                uthread::notify_all_locked(&mutator_cond);
                uthread::wait_locked(&eviction_cond, &eviction_mutex);
                continue;
            }
            profile::count_evacuation();
            timestamp++;
            uthread::fork_join<true>(evacuate_thread_cnt, fn_mark);
            flip_scope_state();
            timestamp++;
            uthread::fork_join<true>(evacuate_thread_cnt, fn_evacuate);
            timestamp++;
            uthread::fork_join<true>(evacuate_thread_cnt, fn_gc);
            mutator_can_not_allocate.store(false);
        }
    }

    void mark_phase(uint32_t timestamp) {
        auto mark = [this](allocator::BlockHead *b) {
            return this->try_mark(b);
        };
        int64_t mark_start = profile::start_mark();
        allocator::global_heap.mark(mark, timestamp);
        profile::end_mark(mark_start);
    }
    void evacuate_phase(uint32_t timestamp) {
        EvictBuffer buffer;
        auto evict = [this, &buffer](allocator::BlockHead *b) {
            return this->try_evict(b, buffer);
        };
        int64_t evict_start = profile::start_evict();
        allocator::global_heap.evict(evict, timestamp);
        post_write_requests(buffer);
        while (check_cq());
        profile::end_evict(evict_start);
    }
    void gc_phase(uint32_t timestamp) {
        auto gc = [this](allocator::BlockHead *b) {
            retry:
                auto obj = b->obj_meta_data.load();
                if (obj.is_null()) return FREE;
                auto entry = obj.get_entry_ptr();
                auto state = entry->load_state().state;
                if (state == FREE || entry->local_addr() != b->get_object_ptr())
                    [[unlikely]]
                    goto retry;
                return entry->load_state().state;
        };
        int64_t evict_start = profile::start_evict();
        allocator::global_heap.evict(gc, timestamp);
        profile::end_evict(evict_start);
    }

    void flip_scope_state() {
        auto old_state =
            mutator_states.global_state.load(std::memory_order::relaxed);
        auto new_state = old_state == InScopeV0 ? InScopeV1 : InScopeV0;
        // avoid ABA
        while (mutator_states.count[new_state] != 0) uthread::yield();
        mutator_states.global_state.store(new_state);
        while (mutator_states.count[old_state] != 0) {
            uthread::yield();
        }
    }

    void on_demand_invoke_eviction() {
        assert(uthread::get_tls()->scope_state == OutOfScope);
        uthread::lock(&eviction_mutex);
        uthread::set_high_priority();
        mutator_can_not_allocate.store(true);
        uthread::notify_all_locked(&eviction_cond);
        uthread::wait_locked(&mutator_cond, &eviction_mutex);
        uthread::set_default_priority();
    }

    void deallocate_local(void *ptr, FarObjectEntry &entry) {
        auto block = static_cast<allocator::BlockHead *>(ptr) - 1;
        assert(block->get_object_ptr() == entry.local_addr());
        assert(block->obj_meta_data.load().get_entry_ptr() == &entry);
        block->obj_meta_data = far_obj_t::null();
    }

    // return true if already at local
    template <bool IncreaseRefCount>
    bool post_fetch(far_obj_t obj, DereferenceScope &scope) {
        auto &entry = get_entry_of(obj);
    retry:
        auto old_state = entry.load_state();
        auto new_state = old_state;
        if (IncreaseRefCount) {
            new_state.inc_ref_cnt();
        }
        new_state.inc_hotness();
        switch (new_state.state) {
        case LOCAL:
            if (!entry.cas_state_weak(old_state, new_state)) goto retry;
            return true;
        case FETCHING:
            if (!entry.cas_state_weak(old_state, new_state)) goto retry;
            // TODO: this thread should check if memory is low
            // otherwise, since this thread is blocked
            // it may be busy polling and never exit its scope
            return false;
        case MARKED:
        case EVICTING:
            new_state.state = LOCAL;
            if (!entry.cas_state_weak(old_state, new_state)) goto retry;
            return true;
        case REMOTE: {
            // 1st: allocate local memory, trigger evacuation on memory low
            signal::disable_signal();
            void *local_ptr = allocate_local(obj.size, obj, scope);
            // 2nd: modify state to prevent object to be evacuated
            new_state.state = FETCHING;
            if (!entry.cas_state_weak(old_state, new_state)) {
                deallocate_local(local_ptr, entry);
                signal::enable_signal();
                goto retry;
            }
            assert(local_ptr);
            entry.set_local_addr(local_ptr);
            assert(entry.local_addr());
            // 3rd: post read request
            post_read_request(obj);
            signal::enable_signal();
            return false;
        }
        case BUSY:
            goto retry;
        case FREE:
            return true;
        default:
            ERROR("invalid entry state when fetching");
        }
    }

    template <bool Mut>
    static bool fetch_lite_fast_path(EntryStateBits state) {
        return state.is_deref_fast_path<Mut>();
    }

    template <bool Mut>
    bool post_fetch_lite(far_obj_t obj, DereferenceScope &scope) {
        auto &entry = get_entry_of(obj);
        auto old_state = entry.load_state(std::memory_order::relaxed);
        if (fetch_lite_fast_path<Mut>(old_state)) [[likely]] {
            assert(entry.local_addr() != nullptr);
            return true;
        }
        return post_fetch_lite_slow_path<Mut>(entry, obj, scope);
    }
    
    template <bool Mut>
    bool post_fetch_lite_slow_path(FarObjectEntry &entry, far_obj_t obj,
                                   DereferenceScope &scope) {
        auto old_state = entry.load_state(std::memory_order::relaxed);
    retry:
        auto new_state = old_state;
        if constexpr (Mut) {
            new_state.dirty = true;
        }
        new_state.inc_hotness();
        switch (new_state.state) {
        case LOCAL:
            if (!entry.cas_state_weak(old_state, new_state)) goto retry;
            return true;
        case FETCHING:
            if (!entry.cas_state_weak(old_state, new_state)) goto retry;
            // TODO: this thread should check if memory is low
            // otherwise, since this thread is blocked
            // it may be busy polling and never exit its scope
            return false;
        case MARKED:
        case EVICTING:
            new_state.state = LOCAL;
            if (!entry.cas_state_weak(old_state, new_state)) goto retry;
            assert(entry.local_addr());
            return true;
        case REMOTE: {
            signal::disable_signal();

            void *local_ptr = allocate_local(obj.size, obj, scope);
            new_state.state = FETCHING;
            if (!entry.cas_state_weak(old_state, new_state)) {
                deallocate_local(local_ptr, entry);
                signal::enable_signal();
                goto retry;
            }
            assert(local_ptr != nullptr);
            entry.set_local_addr(local_ptr);
            assert(entry.local_addr());
            post_read_request(obj);
            signal::enable_signal();
            return false;
        }
        case BUSY:
            goto retry;
        case FREE:
            return true;
        default:
            ERROR("invalid entry state when fetching");
        }
    }

    template <bool DeallocateEntry, bool OldAccessor = false>
    void deallocate(FarObjectEntry &entry, size_t size) {
    retry_load:
        auto old_state = entry.load_state();
        if (old_state.ref_cnt != 0) {
            check_cq();
            uthread::yield();
            check_cq();
            goto retry_load;
        }
    retry:
        auto new_state = old_state;
        if constexpr (OldAccessor) {
            new_state.ref_cnt--;
            if (new_state.ref_cnt != 0) {
                // someone still hold obj's accessor
                // cas and return
                if (entry.cas_state_weak(old_state, new_state)) {
                    return;
                }
                goto retry_load;
            }
        }
        switch (old_state.state) {
        case FREE:
            ERROR("should not deallocate FREE object");
        case FETCHING:
        case EVICTING:
            check_cq();
            // TODO: can we deallocate concurrenctly?
        case BUSY:
            goto retry_load;
        case MARKED:
        case LOCAL: {
            // check if package about this object
            // are on the fly
            if (new_state.ref_cnt != 0) {
                check_cq();
                goto retry_load;
            }
            new_state.state = FREE;
            if (!entry.cas_state_weak(old_state, new_state)) [[unlikely]] {
                goto retry;
            }
            assert(new_state.ref_cnt == 0);
            deallocate_local(entry.local_addr(), entry);
            remote_allocator.deallocate(entry.remote_addr());
            if constexpr (DeallocateEntry) {
                deallocate_entry(&entry);
            }
            break;
        }
        case REMOTE:
            new_state.state = FREE;
            if (!entry.cas_state_weak(old_state, new_state)) [[unlikely]] {
                goto retry;
            }
            remote_allocator.deallocate(entry.remote_addr());
            if constexpr (DeallocateEntry) {
                deallocate_entry(&entry);
            }
            break;
        };
    }

public:
    ConcurrentArrayCache(void *local_buf, size_t local_buf_size,
                         size_t remote_buf_size, size_t evict_batch_size)
        : remote_allocator(remote_buf_size),
          working(true),
          mutator_can_not_allocate(false) {
        INFO("here is concurrent array cache");
        allocator::global_heap.register_heap(local_buf, local_buf_size);
        void (*evict_fn)(ConcurrentArrayCache *) =
            [](ConcurrentArrayCache *cache) { cache->evacuate_work(); };
        ASSERT(get_config().evacuate_thread_cnt > 0);
        evacuate_thread_cnt = get_config().evacuate_thread_cnt;
        master_evacuation_thread = uthread::create<true>(
            evict_fn, this, std::string("evacuation master"));
        allocator::global_heap.set_on_memory_low(
            [this] { uthread::notify_all(&eviction_cond, &eviction_mutex); });
    }

    ~ConcurrentArrayCache() {
        working.store(false);
        uthread::notify_all(&eviction_cond, &eviction_mutex);
        uthread::join(std::move(master_evacuation_thread));
        allocator::global_heap.destroy();
    }

    static FarObjectEntry &get_entry_of(far_obj_t obj) {
        return *reinterpret_cast<FarObjectEntry *>(obj.obj_id);
    }

    static ConcurrentArrayCache *init_default(void *local_buf,
                                              size_t local_buf_size,
                                              size_t remote_buf_size,
                                              size_t evict_batch_size) {
        default_instance.reset(new ConcurrentArrayCache(
            local_buf, local_buf_size, remote_buf_size, evict_batch_size));
        return default_instance.get();
    }

    static ConcurrentArrayCache *get_default() {
        return default_instance.get();
    }

    static void destroy_default() { default_instance.reset(); }

    template <bool Lite = false>
    std::pair<far_obj_t, void *> allocate(FarObjectEntry *entry, size_t size,
                                          bool dirty, DereferenceScope &scope) {
        far_obj_t obj = {.size = size,
                         .obj_id = reinterpret_cast<uint64_t>(entry)};
        void *local_ptr = allocate_local(size, obj, scope);
        auto remote_ptr = allocate_remote(size);
        entry->reset<Lite>(local_ptr, remote_ptr, size, dirty);
        return {obj, local_ptr};
    }

    template <bool Lite = false>
    std::pair<far_obj_t, void *> allocate(size_t size, bool dirty,
                                          DereferenceScope &scope) {
        FarObjectEntry *entry = allocate_entry(scope);
        return allocate<Lite>(entry, size, dirty, scope);
    }

    FarObjectEntry *allocate_entry(DereferenceScope &scope) {
        return new FarObjectEntry;
    }

    void deallocate_entry(FarObjectEntry *entry) { delete entry; }

    template <bool OldAccessor = false>
    void deallocate(far_obj_t obj) {
        deallocate<true, OldAccessor>(get_entry_of(obj), obj.size);
    }

    void deallocate_unique(FarObjectEntry &entry, size_t size) {
        deallocate<false>(entry, size);
    }

    bool at_local(far_obj_t obj) {
        return obj.is_null() || get_entry_of(obj).is_local();
    }

    std::pair<bool, void *> async_fetch(far_obj_t obj,
                                        DereferenceScope &scope) {
        bool at_local = post_fetch<true>(obj, scope);
        return {at_local, get_entry_of(obj).local_addr()};
    }

    bool prefetch(far_obj_t obj, DereferenceScope &scope) {
        profile::start_prefetch();
        bool at_local = post_fetch<false>(obj, scope);
        profile::end_prefetch();
        return at_local;
    }

    void *sync_fetch(far_obj_t obj, DereferenceScope &scope) {
        ON_MISS_BEGIN
        ON_MISS_END
        return fetch_with_miss_handler(obj, __on_miss__, scope);
    }

    bool check_fetch(FarObjectEntry *entry, fetch_ddl_t &ddl) {
        fetch_ddl_t now = __rdtsc();
        if (now >= ddl) [[unlikely]] {
            do {
                if (entry->is_local()) return true;
            } while (check_cq());
            ddl = delay_fetch_ddl(ddl);
        }
        return false;
    }

    void *fetch_with_miss_handler(far_obj_t obj, const DataMissHandler &handler,
                                  DereferenceScope &scope) {
        bool work_suspended = profile::suspend_work();
        bool at_local = post_fetch<true>(obj, scope);
        auto entry = &get_entry_of(obj);
        if (!at_local) [[unlikely]] {
            fetch_ddl_t ddl = create_fetch_ddl();
            profile::resume_work(work_suspended);
            handler(entry, ddl);
            // work_suspended = profile::suspend_work();
            profile::start_poll();
            while (!entry->is_local()) {
                check_cq();
            }
            profile::end_poll();
        }
    back:
        profile::resume_work(work_suspended);
        return entry->local_addr();
    }

    // do not pin object, to reduce CAS
    template <bool Mut>
    void *fetch_lite(far_obj_t obj, const DataMissHandler &handler,
                     DereferenceScope &scope) {
        auto entry = &get_entry_of(obj);
        auto state = entry->load_state(std::memory_order::relaxed);
        if (fetch_lite_fast_path<Mut>(state)) [[likely]] {
            return entry->local_addr();
        }
        return fetch_lite_slow_path<Mut>(obj, handler, scope);
    }

    template <bool Mut>
    void *fetch_lite_slow_path(far_obj_t obj, const DataMissHandler &handler,
                               DereferenceScope &scope) {
        bool work_suspended = profile::suspend_work();
        auto entry = &get_entry_of(obj);
        bool at_local = post_fetch_lite_slow_path<Mut>(*entry, obj, scope);
        if (!at_local) [[unlikely]] {
            fetch_ddl_t ddl = create_fetch_ddl();
            profile::resume_work(work_suspended);
            handler(entry, ddl);
            // work_suspended = profile::suspend_work();
            profile::start_poll();
            while (!entry->is_local()) {
                check_cq();
            }
            profile::end_poll();
        }
    back:
        profile::resume_work(work_suspended);
        return entry->local_addr();
    }

    template <bool Mut>
    std::pair<bool, void *> async_fetch_lite(far_obj_t obj,
                                             DereferenceScope &scope) {
        bool at_local = post_fetch_lite<Mut>(obj, scope);
        return {at_local, get_entry_of(obj).local_addr()};
    }

    static void pin(far_obj_t obj) {
        if (obj.is_null()) return;
        get_entry_of(obj).pin();
    }

    static void unpin(far_obj_t obj) {
        if (obj.is_null()) return;
        get_entry_of(obj).unpin();
    }

    void mark_dirty(far_obj_t obj) {
        FarObjectEntry &entry = get_entry_of(obj);
        EntryStateBits old_state = entry.load_state();
        if (old_state.dirty) return;
        EntryStateBits new_state;
        do {
            new_state = old_state;
            new_state.dirty = 1;
        } while (!entry.cas_state_weak(old_state, new_state));
    }

    void release_cache(FarObjectEntry *entry, bool dirty) {
        EntryStateBits old_state = entry->load_state();
        EntryStateBits new_state;
        do {
            new_state = old_state;
            if (new_state.state == FREE) [[unlikely]] {
                ERROR("release cache: try to release a free entry");
            }
            new_state.dec_ref_cnt();
            if (dirty) new_state.dirty = 1;
        } while (!entry->cas_state_weak(old_state, new_state));
    }

    void release_cache(far_obj_t obj, bool dirty) {
        release_cache(&get_entry_of(obj), dirty);
    }

    void enter_scope() {
        assert(uthread::get_tls()->scope_state == OutOfScope);
        auto state =
            mutator_states.global_state.load(std::memory_order::relaxed);
        uthread::get_tls()->scope_state = state;
        mutator_states.count[state].fetch_add(1, std::memory_order::relaxed);
    }
    void exit_scope() {
        auto prev_state = uthread::get_tls()->scope_state;
        assert(prev_state != OutOfScope);
        uthread::get_tls()->scope_state = OutOfScope;
        mutator_states.count[prev_state].fetch_sub(1,
                                                   std::memory_order::relaxed);
    }
    void update_scope(DereferenceScope &scope) {
        auto &state = uthread::get_tls()->scope_state;
        assert(state != OutOfScope);
        auto new_state =
            mutator_states.global_state.load(std::memory_order::relaxed);
        if (new_state != state) {
            mutator_states.count[new_state].fetch_add(
                1, std::memory_order::relaxed);
            auto old_state = state;
            state = new_state;
            scope.recursive_unmark();
            mutator_states.count[old_state].fetch_sub(
                1, std::memory_order::relaxed);
        }
    }

private:
    void handle_rdma_write_complete(const ibv_wc &wc) {
        void *local_ptr = reinterpret_cast<void *>(wc.wr_id);
        allocator::BlockHead *block =
            static_cast<allocator::BlockHead *>(local_ptr) - 1;
    retry:
        auto obj = block->obj_meta_data.load();
        auto &entry = get_entry_of(obj);
        auto old_state = entry.load_state();
        if (old_state.state == FREE || entry.local_addr() != local_ptr)
            [[unlikely]]
            goto retry;
        auto new_state = old_state;
        // each RDMA write will increase the reference count
        // to avoid deallocating the local buffer while RDMA write is not done
        new_state.dec_ref_cnt();
        switch (new_state.state) {
        case EntryState::EVICTING:
            if (new_state.ref_cnt == 0) {
                new_state.state = REMOTE;
                if (!entry.cas_state_weak(old_state, new_state)) goto retry;
                block->obj_meta_data = far_obj_t::null();
            } else {
                // maybe another rdma request (write back) in the progress
                // do not mark it as remote to prevent reallocating memory to
                // other object
                if (!entry.cas_state_weak(old_state, new_state)) goto retry;
            }
            break;
        case EntryState::LOCAL:
            // another access interrupted the eviction
            // just save the ref count and return
            if (!entry.cas_state_weak(old_state, new_state)) goto retry;
            break;
        case EntryState::BUSY:
            goto retry;
        default:
            ERROR("check_cq: invalid state when evict");
        }
    }

    void handle_rdma_read_complete(const ibv_wc &wc) {
        profile::trace_rdma_read_complete(wc.wr_id);
        void *local_ptr = reinterpret_cast<void *>(wc.wr_id);
        allocator::BlockHead *block =
            static_cast<allocator::BlockHead *>(local_ptr) - 1;
    retry:
        auto obj = block->obj_meta_data.load();
        auto &entry = get_entry_of(obj);
        auto old_state = entry.load_state();
        if (old_state.state == FREE || entry.local_addr() != local_ptr)
            [[unlikely]]
            goto retry;
        if (old_state.state == EntryState::BUSY) [[unlikely]] {
            goto retry;
        }
        assert(old_state.state == EntryState::FETCHING);
        auto new_state = old_state;
        new_state.state = LOCAL;
        if (!entry.cas_state_weak(old_state, new_state)) goto retry;
    }

    void handle_work_complete(const ibv_wc &wc) {
        assert(wc.status == IBV_WC_SUCCESS);
        switch (wc.opcode) {
        case IBV_WC_RDMA_WRITE: {
            handle_rdma_write_complete(wc);
            break;
        }
        case IBV_WC_RDMA_READ: {
            handle_rdma_read_complete(wc);
            break;
        }
        default:
            WARN("unexpected rdma wc opcode");
            break;
        }
    }

public:
    bool memory_low() {
        return mutator_can_not_allocate.load() ||
               allocator::global_heap.memory_low();
    }

public:
    size_t check_cq() {
        static std::atomic_flag ttas_lock = 0;
        if (ttas_lock.test() || ttas_lock.test_and_set()) {
            return 0;
        }
        auto client = rdma::Client::get_default();
        ibv_wc wc[rdma::CHECK_CQ_BATCH_SIZE];
        profile::start_check_cq();
        size_t cnt = client->check_cq(wc, rdma::CHECK_CQ_BATCH_SIZE);
        profile::end_check_cq();
        for (size_t i = 0; i < cnt; i++) {
            handle_work_complete(wc[i]);
        }
        ttas_lock.clear();
        return cnt;
    }

    void check_memory_low(auto &&scope) {
        if (memory_low()) {
            scope.begin_eviction();
            on_demand_invoke_eviction();
            scope.end_eviction();
        }
    }

    void invoke_eviction() { on_demand_invoke_eviction(); }

    void debug() {
        WARN("debug triggered");
        return;
    }

    template <typename T, bool Mut>
    friend class LiteAccessor;
};

}  // namespace cache

}  // namespace FarLib
