#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>

namespace FarLib {

namespace cache {

enum EntryState {
    FREE,      // this entry is not used
    LOCAL,     // the object is at local and not evicting
    MARKED,    // the object is marked to evict
    EVICTING,  // the object is evicting to remote
    REMOTE,    // the object is at remote, no local buffer used
    FETCHING,  // the object is being fetched
    BUSY,      // the entry is modifying, e.g. moving
};

struct EntryStateBits {
    uint32_t invalid : 1;
    uint32_t dirty : 1;
    EntryState state : 3;
    uint32_t hotness : 3;
    uint32_t ref_cnt : 8;
    uint32_t size : 16;

    static constexpr uint32_t HOTNESS_MAX = 3;
    static constexpr uint32_t COLD = 0;
    static_assert(COLD < HOTNESS_MAX);

    void inc_hotness() { hotness = HOTNESS_MAX; }

    void dec_hotness() {
        if (hotness != 0) hotness--;
    }

    void inc_ref_cnt() { ref_cnt++; }

    void dec_ref_cnt() { ref_cnt--; }

    __attribute__((always_inline)) bool can_evict() const {
        return hotness == 0 && ref_cnt == 0 &&
               state < EVICTING /* FREE, LOCAL or MARKED_CLEAN */;
    }

    template <bool Mut>
    __attribute__((always_inline)) bool is_deref_fast_path() const {
#ifdef ASSERT_ALL_LOCAL
        return true;
#else
        return (state == LOCAL) && (hotness > EntryStateBits::COLD) &&
               ((!Mut) || dirty);
#endif
    }
};

static_assert(sizeof(EntryStateBits) == sizeof(uint32_t));
static_assert(std::atomic<EntryStateBits>::is_always_lock_free);

class FarObjectEntry {
private:
    std::atomic<EntryStateBits> state;
    uint32_t local_addr_low;
    uint32_t remote_addr_low;
    uint16_t local_addr_high;
    uint16_t remote_addr_high;

    static constexpr uint64_t MASK_HIGH = ((1L << 16) - 1) << 32;
    static constexpr uint64_t MASK_LOW = (1L << 32) - 1;
    static constexpr uint32_t OFFS_LOW = 32;

public:
    inline void *local_addr() const {
        return reinterpret_cast<void *>(((uint64_t)(local_addr_high) << 32) |
                                        local_addr_low);
    }

    void set_local_addr(void *p) {
        uint64_t local_addr = reinterpret_cast<uint64_t>(p);
        local_addr_low = local_addr & MASK_LOW;
        local_addr_high = local_addr >> OFFS_LOW;
    }

    inline size_t remote_addr() const {
        return (((uint64_t)remote_addr_high) << 32) | remote_addr_low;
    }

    void set_remote_addr(size_t remote_addr) {
        remote_addr_low = remote_addr & MASK_LOW;
        remote_addr_high = remote_addr >> OFFS_LOW;
    }

public:
    EntryStateBits load_state(
        std::memory_order order = std::memory_order::seq_cst) const {
        return state.load(order);
    }

    void set_state(EntryStateBits bits) { state.store(bits); }

    bool cas_state_weak(EntryStateBits &expected, EntryStateBits desired) {
        return state.compare_exchange_weak(expected, desired);
    }

    bool cas_state_strong(EntryStateBits &expected, EntryStateBits desired) {
        return state.compare_exchange_strong(expected, desired);
    }

    bool is_local() const { return state.load().state <= EVICTING; }

    template <bool Lite>
    void reset(void *local_ptr, size_t remote_ptr, size_t size,
               bool dirty = false) {
        EntryStateBits reset_state = {
            .invalid = 0,
            .dirty = dirty ? 1u : 0u,
            .state = LOCAL,
            .hotness = 1,
            .ref_cnt = Lite ? 0 : 1,
            .size = static_cast<uint32_t>(size),
        };
        state.store(reset_state, std::memory_order::relaxed);
        set_local_addr(local_ptr);
        set_remote_addr(remote_ptr);
    }

    void set_free() {
        EntryStateBits free_state = {
            .invalid = 0,
            .dirty = 0,
            .state = FREE,
            .hotness = 0,
            .ref_cnt = 0,
            .size = 0,
        };
        state.store(free_state);
        set_local_addr(nullptr);
        set_remote_addr(0);
    }

    void pin() {
        auto old_state = load_state();
        assert(old_state.state != MARKED);
        assert(old_state.state != EVICTING);
        assert(old_state.state != REMOTE);
    retry:
        auto new_state = old_state;
        new_state.inc_ref_cnt();
        if (new_state.state == MARKED || new_state.state == EVICTING) {
            new_state.state = LOCAL;
        }
        if (!cas_state_weak(old_state, new_state)) goto retry;
    }

    void unpin() {
        auto old_state = load_state();
        assert(old_state.state != MARKED);
        assert(old_state.state != EVICTING);
        assert(old_state.state != REMOTE);
    retry:
        auto new_state = old_state;
        new_state.dec_ref_cnt();
        if (!cas_state_weak(old_state, new_state)) goto retry;
    }

    void mark_dirty() {
        EntryStateBits old_state = load_state();
        if (old_state.dirty) return;
        EntryStateBits new_state;
        do {
            new_state = old_state;
            new_state.dirty = 1;
        } while (!cas_state_weak(old_state, new_state));
    }

    friend class ConcurrentArrayCache;
};

static_assert(sizeof(FarObjectEntry) == 16);

class DereferenceScope;

}  // namespace cache

}  // namespace FarLib