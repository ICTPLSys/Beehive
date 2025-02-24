#pragma once
#include "cache/region_based_allocator.hpp"
#include "scope.hpp"
#include "selection.hpp"

#ifdef ARRAY_TYPE_CACHE
#include "local_array_cache.hpp"

namespace Beehive {
using Cache = cache::ArrayCache;
}

#elif defined(HASHMAP_TYPE_CACHE)
#include "local_cache.hpp"

namespace Beehive {
using Cache = cache::LocalCache;
}
#elif defined(CONCURRENT_ARRAY_TYPE_CACHE)
#include "concurrent_array_cache.hpp"

namespace Beehive {
using Cache = cache::ConcurrentArrayCache;
}
#else
#error "Invalid cache type"
#endif

namespace Beehive {

namespace cache {

inline bool check_fetch(FarObjectEntry *entry, fetch_ddl_t &ddl) {
    return Cache::get_default()->check_fetch(entry, ddl);
}

inline size_t check_cq() { return Cache::get_default()->check_cq(); }

inline bool at_local(far_obj_t obj) {
    return Cache::get_default()->at_local(obj);
}

template <typename T>
class Accessor;

template <typename T, bool Mut = false>
class LiteAccessor;

template <typename T, typename Impl>
struct UniqueFarPtrBase {
private:
    Impl *get_impl() { return reinterpret_cast<Impl *>(this); }
    const Impl *get_impl() const {
        return reinterpret_cast<const Impl *>(this);
    }

public:
    FarObjectEntry entry;

    FarObjectEntry &get_entry() { return entry; }
    const FarObjectEntry &get_entry() const { return entry; }

    UniqueFarPtrBase() { entry.set_free(); }
    UniqueFarPtrBase(const UniqueFarPtrBase &) = delete;
    UniqueFarPtrBase(UniqueFarPtrBase &&other) noexcept { move(&other.entry); }
    ~UniqueFarPtrBase() { reset(); }
    UniqueFarPtrBase &operator=(const UniqueFarPtrBase &) = delete;
    UniqueFarPtrBase &operator=(UniqueFarPtrBase &&other) {
        reset();
        move(&other.entry);
        return *this;
    }
    void reset() {
        if (!is_null()) {
            Cache::get_default()->deallocate_unique(entry, get_impl()->size());
        }
    }
    far_obj_t obj() const {
        return {.size = get_impl()->size(),
                .obj_id = reinterpret_cast<uint64_t>(&entry)};
    }

    bool is_null() const { return entry.load_state().state == FREE; }

    template <bool Mut = false, typename Scope>
    __attribute__((always_inline)) LiteAccessor<T, Mut> access(
        Scope &&scope) const {
        bool fast = entry.load_state().is_deref_fast_path<Mut>();
        if (fast) [[likely]] {
            void *p = entry.local_addr();
            auto block = static_cast<allocator::BlockHead *>(p) - 1;
            return LiteAccessor<T, Mut>(block, p);
        } else {
            return LiteAccessor<T, Mut>(*this, std::forward<Scope>(scope));
        }
    }

    template <bool Mut = false, typename Scope>
    __attribute__((always_inline)) LiteAccessor<T, Mut> access(
        __DMH__, Scope &&scope) const {
        bool fast = entry.load_state().is_deref_fast_path<Mut>();
        if (fast) [[likely]] {
            void *p = entry.local_addr();
            auto block = static_cast<allocator::BlockHead *>(p) - 1;
            return LiteAccessor<T, Mut>(block, p);
        } else {
            return LiteAccessor<T, Mut>(*this, __on_miss__,
                                        std::forward<Scope>(scope));
        }
    }

    // !!! this is NOT thread safe
    // the src and dst entry should not be accessed concurrently
    //
    // move another entry to this, will rewrite the object header
    // dereferencing unique ptr when moving is UB for mutators
    // only eviction may be concurrent with this
    void move(FarObjectEntry *other) {
        // 1. check the state
        EntryStateBits state = other->load_state();
    retry:
        switch (state.state) {
        case FREE:
            entry.set_free();
            return;
        case REMOTE:
            // no local buffer used
            entry.set_state(state);
            entry.set_local_addr(nullptr);
            entry.set_remote_addr(other->remote_addr());
            // no data race since no other mutator dereferencing this ptr
            break;
        case LOCAL:
        case FETCHING:
        case MARKED:
        case EVICTING: {
            // there is a local buffer, reset header
            // 2. pin the buffer at local
            EntryStateBits pin_state = state;
            if (state.state != FETCHING) {
                // for MARKED_CLEAN / MARKED_SYNCING
                // interrupt the eviction to avoid bugs
                // TODO: this may be optimized
                pin_state.state = LOCAL;
            }
            pin_state.ref_cnt++;
            if (!other->cas_state_weak(state, pin_state)) [[unlikely]] {
                goto retry;
            }
            // since the buffer is pinned, no other thread will write on the
            // entry
            // 3. copy metadata & addrs
            void *local_addr = other->local_addr();
            EntryStateBits unpin_state = pin_state;
            unpin_state.ref_cnt--;
            // once this entry is in use, the object is unpinned
            entry.set_state(unpin_state);
            entry.set_local_addr(local_addr);
            entry.set_remote_addr(other->remote_addr());
            // 4. rewrite the object header
            auto block = static_cast<allocator::BlockHead *>(local_addr) - 1;
            far_obj_t obj =
                block->obj_meta_data.load(std::memory_order::relaxed);
            obj.obj_id = reinterpret_cast<uint64_t>(this);
            block->obj_meta_data.store(obj, std::memory_order::relaxed);
            break;
        }
        case BUSY:
        default:
            ERROR("invalid state");
        }
        other->set_free();
    }
    operator bool() const { return !is_null(); }

    bool cas_null_to_busy() {
        EntryStateBits prev_state =
            entry.load_state(std::memory_order::relaxed);
        if (prev_state.state != FREE) return false;
        EntryStateBits busy_state = {
            .dirty = 0,
            .state = BUSY,
            .hotness = 0,
            .ref_cnt = 0,
        };
        return entry.cas_state_strong(prev_state, busy_state);
    }

    // move this to `to`
    // set this as busy & null
    void atomic_move_to_and_set_busy(UniqueFarPtrBase<T, Impl> &to) {
        assert(to.entry.load_state(std::memory_order::relaxed).state == BUSY);
    retry:
        EntryStateBits prev_state =
            entry.load_state(std::memory_order::relaxed);
        EntryStateBits busy_state = {
            .dirty = 0,
            .state = BUSY,
            .hotness = 0,
            .ref_cnt = 0,
        };
        if (!entry.cas_state_weak(prev_state, busy_state)) goto retry;

        switch (prev_state.state) {
        case FREE:
            to.entry.set_free();
            return;
        case REMOTE:
            // no local buffer used
            to.entry.set_state(prev_state);
            to.entry.set_local_addr(nullptr);
            to.entry.set_remote_addr(entry.remote_addr());
            break;
        case LOCAL:
        case FETCHING:
        case MARKED:
        case EVICTING: {
            // there is a local buffer, reset header
            void *local_addr = entry.local_addr();
            to.entry.set_state(prev_state);
            to.entry.set_local_addr(local_addr);
            to.entry.set_remote_addr(entry.remote_addr());
            // rewrite the object header
            auto block = static_cast<allocator::BlockHead *>(local_addr) - 1;
            far_obj_t obj =
                block->obj_meta_data.load(std::memory_order::relaxed);
            obj.obj_id = reinterpret_cast<uint64_t>(&to);
            block->obj_meta_data.store(obj, std::memory_order::relaxed);
            break;
        }
        case BUSY:
        default:
            ERROR("invalid state");
        }
        entry.set_local_addr(nullptr);
        entry.set_remote_addr(0);
    }
};

template <typename T>
struct UniqueFarPtr : public UniqueFarPtrBase<T, UniqueFarPtr<T>> {
private:
    using Base = UniqueFarPtrBase<T, UniqueFarPtr<T>>;
    using Base::entry;

public:
    static size_t size() { return sizeof(T); }

    template <typename... Args>
    Accessor<T> allocate(Args &&...args) {
        this->reset();
        auto cache = Cache::get_default();
        constexpr bool dirty = !(std::is_trivially_constructible<T>::value &&
                                 sizeof...(Args) == 0);
        cache->allocate(&entry, sizeof(T), dirty);
        new (entry.local_addr()) T(std::forward<Args>(args)...);
        Accessor<T> accessor;
        accessor.obj = this->obj();
        accessor.local_ptr = entry.local_addr();
        return accessor;
    }
    Accessor<T> allocate_uninitialized() {
        this->reset();
        auto cache = Cache::get_default();
        cache->allocate(&entry, sizeof(T), false);
        Accessor<T> accessor;
        accessor.obj = this->obj();
        accessor.local_ptr = entry.local_addr();
        return accessor;
    }

    template <typename Evictor>
    Accessor<T> allocate_uninitialized(Evictor &&evictor) {
        this->reset();
        auto cache = Cache::get_default();
        cache->allocate(&entry, sizeof(T), false, evictor);
        Accessor<T> accessor;
        accessor.obj = this->obj();
        accessor.local_ptr = entry.local_addr();
        return accessor;
    }

    template <typename Evictor, typename... Args>
    LiteAccessor<T, true> allocate_lite(Evictor &&evictor, Args &&...args) {
        this->reset();
        auto cache = Cache::get_default();
        cache->template allocate<true>(&entry, sizeof(T), true,
                                       std::forward<Evictor>(evictor));
        T *p = static_cast<T *>(entry.local_addr());
        std::construct_at(p, std::forward<Args>(args)...);
        LiteAccessor<T, true> accessor;
        accessor.block = reinterpret_cast<allocator::BlockHead *>(p) - 1;
        accessor.local_ptr = p;
        return accessor;
    }

    template <typename Evictor, typename... Args>
    LiteAccessor<T, false> allocate_lite_uninitialized(Evictor &&evictor) {
        this->reset();
        auto cache = Cache::get_default();
        cache->template allocate<true>(&entry, sizeof(T), false,
                                       std::forward<Evictor>(evictor));
        T *p = static_cast<T *>(entry.local_addr());
        LiteAccessor<T, false> accessor;
        accessor.block = reinterpret_cast<allocator::BlockHead *>(p) - 1;
        accessor.local_ptr = p;
        return accessor;
    }

    template <typename Evictor, typename... Args>
    LiteAccessor<T, true> allocate_lite_from_busy(Evictor &&evictor,
                                                  Args &&...args) {
        assert(entry.load_state().state == BUSY);
        auto cache = Cache::get_default();
        cache->template allocate<true>(&entry, sizeof(T), true,
                                       std::forward<Evictor>(evictor));
        T *p = static_cast<T *>(entry.local_addr());
        std::construct_at(p, std::forward<Args>(args)...);
        LiteAccessor<T, true> accessor;
        accessor.block = reinterpret_cast<allocator::BlockHead *>(p) - 1;
        accessor.local_ptr = p;
        return accessor;
    }
};

template <typename T>
struct UniqueFarPtr<T[]> : public UniqueFarPtrBase<T[], UniqueFarPtr<T[]>> {
private:
    using Base = UniqueFarPtrBase<T[], UniqueFarPtr<T[]>>;
    using Base::entry;

public:
    size_t size() const { return entry.load_state().size; }
    size_t element_count() const { return size() / sizeof(T); }

    Accessor<T[]> allocate(size_t n) {
        this->reset();
        auto cache = Cache::get_default();
        constexpr bool dirty = !std::is_trivially_constructible<T>::value;
        cache->allocate<false>(&entry, n * sizeof(T), dirty);
        if constexpr (dirty) {
            T *base_addr = entry.local_addr();
            for (size_t i = 0; i < n; i++) {
                new (base_addr + i) T;
            }
        }
        Accessor<T> accessor;
        accessor.obj = this->obj();
        accessor.local_ptr = entry.local_addr();
        return accessor;
    }

    template <bool Mut = false, typename Evictor>
    LiteAccessor<T[], Mut> allocate_lite(size_t n, Evictor &&evictor) {
        this->reset();
        auto cache = Cache::get_default();
        constexpr bool dirty =
            !std::is_trivially_constructible<T>::value || Mut;
        cache->allocate<true>(&entry, n * sizeof(T), dirty,
                              std::forward<Evictor>(evictor));
        if constexpr (dirty) {
            T *base_addr = entry.local_addr();
            for (size_t i = 0; i < n; i++) {
                new (base_addr + i) T;
            }
        }
        LiteAccessor<T, Mut> accessor;
        void *p = entry.local_addr();
        accessor.block = reinterpret_cast<allocator::BlockHead *>(p) - 1;
        accessor.local_ptr = p;
        return accessor;
    }
};

template <>
struct UniqueFarPtr<void> : public UniqueFarPtrBase<void, UniqueFarPtr<void>> {
private:
    using Base = UniqueFarPtrBase<void, UniqueFarPtr<void>>;
    using Base::entry;

public:
    size_t size() const { return entry.load_state().size; }

    Accessor<void> allocate(size_t nbytes);

    template <bool Mut = false, typename Evictor>
    LiteAccessor<void, Mut> allocate_lite(size_t nbytes, Evictor &&evictor);
};

template <typename T>
class Accessor {
private:
    far_obj_t obj;
    void *local_ptr;
    bool dirty;

private:
    template <typename U>
    friend class UniqueFarPtr;

    template <typename U>
    friend class Accessor;

    template <typename U>
    friend class ConstAccessor;

public:
    Accessor() : obj(far_obj_t::null()), local_ptr(nullptr), dirty(false) {}

    Accessor(far_obj_t obj, size_t offset = 0) : obj(obj), dirty(false) {
        void *p = Cache::get_default()->sync_fetch(obj);
        local_ptr = static_cast<char *>(p) + offset;
    }

    Accessor(far_obj_t obj, const DataMissHandler &on_data_miss,
             size_t offset = 0)
        : obj(obj), dirty(false) {
        void *ptr =
            Cache::get_default()->fetch_with_miss_handler(obj, on_data_miss);
        local_ptr = static_cast<char *>(ptr) + offset;
    }

    Accessor(const UniqueFarPtr<T> &uptr) : obj(uptr.obj()), dirty(false) {
        local_ptr = Cache::get_default()->sync_fetch(obj);
    }

    Accessor(const UniqueFarPtr<T> &uptr, const DataMissHandler &on_data_miss)
        : obj(uptr.obj()), dirty(false) {
        local_ptr =
            Cache::get_default()->fetch_with_miss_handler(obj, on_data_miss);
    }

    Accessor(const Accessor<T> &) = delete;

    Accessor(Accessor<T> &&other)
        : obj(other.obj), local_ptr(other.local_ptr), dirty(other.dirty) {
        other.obj = far_obj_t::null();
    }

    Accessor<T> &operator=(Accessor<T> &&other) noexcept {
        if (!obj.is_null()) {
            Cache::get_default()->release_cache(obj, dirty);
        }
        obj = other.obj;
        local_ptr = other.local_ptr;
        dirty = other.dirty;
        other.obj = far_obj_t::null();
        return *this;
    }

    template <typename U>
    Accessor(Accessor<U> &&other, T *local_ptr)
        : obj(other.obj), local_ptr(local_ptr), dirty(other.dirty) {
        other.obj = far_obj_t::null();
    }

    ~Accessor() {
        if (!obj.is_null()) {
            Cache::get_default()->release_cache(obj, dirty);
        }
    }

    bool is_null() const { return obj.is_null(); }

    template <typename... Args>
    static Accessor allocate(Args &&...args) {
        auto accessor = Accessor<T>();
        auto cache = Cache::get_default();
        auto [o, p] = cache->allocate(sizeof(T), true);
        accessor.obj = o;
        accessor.local_ptr = p;
        accessor.dirty = false;
        if constexpr (!(std::is_trivially_constructible<T>::value &&
                        sizeof...(Args) == 0)) {
            accessor.dirty = true;
        }
        new (accessor.local_ptr) T(std::forward<Args>(args)...);
        return accessor;
    }

    void deallocate() {
        Cache::get_default()->deallocate<true>(obj);
        obj = far_obj_t::null();
    }

    far_obj_t get_obj() const { return obj; }

    T *as_ptr() {
        dirty = true;
        return static_cast<T *>(local_ptr);
    }

    template <typename U>
    U *cast() {
        return static_cast<U *>(as_ptr());
    }

    const T *as_const_ptr() const { return static_cast<T *>(local_ptr); }

    template <typename U>
    const U *constcast() {
        return static_cast<const U *>(as_const_ptr());
    }

    T *operator->() { return as_ptr(); }

    T &operator*() { return *as_ptr(); }
};

template <>
class Accessor<void> {
private:
    far_obj_t obj;
    void *local_ptr;
    bool dirty;

private:
    template <typename U>
    friend class Accessor;

    friend class UniqueFarPtr<void>;

public:
    Accessor() : obj(far_obj_t::null()), local_ptr(nullptr), dirty(false) {}

    Accessor(far_obj_t obj, size_t offset = 0) : obj(obj), dirty(false) {
        void *p = Cache::get_default()->sync_fetch(obj);
        local_ptr = static_cast<char *>(p) + offset;
    }

    Accessor(const Accessor<void> &) = delete;

    template <typename T>
    Accessor(Accessor<T> &&other)
        : obj(other.obj), local_ptr(other.local_ptr), dirty(other.dirty) {
        other.obj = far_obj_t::null();
    }

    template <typename T>
    Accessor<void> &operator=(Accessor<T> &&other) noexcept {
        if (!obj.is_null()) {
            Cache::get_default()->release_cache(obj, dirty);
        }
        obj = other.obj;
        local_ptr = other.local_ptr;
        dirty = other.dirty;
        other.obj = far_obj_t::null();
        return *this;
    }

    template <typename T>
    Accessor(Accessor<T> &&other, void *local_ptr)
        : obj(other.obj), local_ptr(local_ptr), dirty(other.dirty) {
        other.obj = far_obj_t::null();
    }

    ~Accessor() {
        if (!obj.is_null()) {
            Cache::get_default()->release_cache(obj, dirty);
        }
    }

    static Accessor<void> allocate(size_t size) {
        auto accessor = Accessor<void>();
        auto cache = Cache::get_default();
        auto [o, p] = cache->allocate(size, true);
        accessor.obj = o;
        accessor.local_ptr = p;
        accessor.dirty = false;
        return accessor;
    }

    void deallocate() {
        Cache::get_default()->deallocate<true>(obj);
        obj = far_obj_t::null();
    }

    bool is_null() const { return obj.is_null(); }

    far_obj_t get_obj() const { return obj; }

    void *as_ptr() {
        dirty = true;
        return local_ptr;
    }

    template <typename T>
    T *cast() {
        return static_cast<T *>(as_ptr());
    }

    const void *as_const_ptr() const { return local_ptr; }

    template <typename T>
    const T *constcast() {
        return static_cast<const T *>(as_const_ptr());
    }
};

template <typename T>
class ConstAccessor {
private:
    far_obj_t obj;
    const void *local_ptr;

private:
    template <typename U>
    friend class ConstAccessor;

public:
    ConstAccessor() : obj(far_obj_t::null()), local_ptr(nullptr) {}

    ConstAccessor(far_obj_t obj, size_t offset = 0) : obj(obj) {
        void *p = Cache::get_default()->sync_fetch(obj);
        local_ptr = static_cast<char *>(p) + offset;
    }

    ConstAccessor(far_obj_t obj, const DataMissHandler &on_data_miss,
                  size_t offset = 0)
        : obj(obj) {
        void *ptr =
            Cache::get_default()->fetch_with_miss_handler(obj, on_data_miss);
        local_ptr = static_cast<const char *>(ptr) + offset;
    }

    ConstAccessor(const UniqueFarPtr<T> &uptr) : obj(uptr.obj()) {
        local_ptr = Cache::get_default()->sync_fetch(obj);
    }

    ConstAccessor(const UniqueFarPtr<T> &uptr,
                  const DataMissHandler &on_data_miss)
        : obj(uptr.obj()) {
        local_ptr =
            Cache::get_default()->fetch_with_miss_handler(obj, on_data_miss);
    }

    ConstAccessor(const ConstAccessor<T> &) = delete;

    ConstAccessor(ConstAccessor<T> &&other)
        : obj(other.obj), local_ptr(other.local_ptr) {
        other.obj = far_obj_t::null();
    }

    template <typename U>
    ConstAccessor(ConstAccessor<U> &&other, const T *local_ptr)
        : obj(other.obj), local_ptr(local_ptr) {
        other.obj = far_obj_t::null();
    }

    ~ConstAccessor() {
        if (!obj.is_null()) {
            Cache::get_default()->release_cache(obj, false);
        }
    }

    ConstAccessor<T> &operator=(ConstAccessor<T> &&other) {
        if (!obj.is_null()) {
            Cache::get_default()->release_cache(obj, false);
        }
        obj = other.obj;
        local_ptr = other.local_ptr;
        other.obj = far_obj_t::null();
        return *this;
    }

    bool is_null() const { return obj.is_null(); }

    far_obj_t get_obj() const { return obj; }

    const T *as_ptr() const { return static_cast<const T *>(local_ptr); }

    template <typename U>
    const U *cast() {
        return static_cast<const U *>(as_ptr());
    }

    const T *operator->() const { return as_ptr(); }

    const T &operator*() const { return *as_ptr(); }

    Accessor<T> as_mut() {
        Accessor<T> accessor;
        accessor.obj = obj;
        accessor.local_ptr = const_cast<void *>(local_ptr);
        accessor.dirty = false;
        obj = far_obj_t::null();
        local_ptr = nullptr;
        return accessor;
    }

    bool async_fetch(far_obj_t obj) {
        if (!this->obj.is_null()) {
            Cache::get_default()->release_cache(obj, false);
        }
        this->obj = obj;
        auto [at_local, local_addr] = Cache::get_default()->async_fetch(obj);
        this->local_ptr = local_addr;
        return at_local;
    }

    void deallocate() {
        Cache::get_default()->deallocate<true>(obj);
        obj = far_obj_t::null();
    }

    void release_self() {
        if (!obj.is_null()) [[likely]] {
            Cache::get_default()->release_cache(obj, false);
            obj = far_obj_t::null();
        }
    }
};

// LiteAccessor mut be used in a dereference scope
template <typename T, bool Mut>
class LiteAccessor {
    using Pointer = std::conditional_t<Mut, T *, const T *>;
    using Reference = std::conditional_t<Mut, T &, const T &>;

    allocator::BlockHead *block;
    Pointer local_ptr;

    template <typename U, bool M>
    friend class LiteAccessor;

    template <typename U>
    friend class UniqueFarPtr;

public:
    void check() const {
        if (is_null()) [[unlikely]] {
            return;
        }
        check(block, local_ptr);
    }
    void check(allocator::BlockHead *block) const {
        assert((block->obj_meta_data.load().get_entry_ptr()->local_addr()) ==
               block + 1);
    }

    void check(allocator::BlockHead *block, Pointer local_addr) const {
        check(block);
        assert(block->get_object_ptr() == local_addr);
    }

public:
    LiteAccessor() : block(nullptr), local_ptr(nullptr) {}

    // unsafe!
    LiteAccessor(allocator::BlockHead *block, void *local_ptr)
        : block(block), local_ptr(static_cast<Pointer>(local_ptr)) {}

    template <typename Evictor>
    LiteAccessor(far_obj_t obj, Evictor &&evictor) {
        ON_MISS_BEGIN
        ON_MISS_END
        void *ptr = Cache::get_default()->template fetch_lite<Mut>(
            obj, __on_miss__, std::forward<Evictor>(evictor));
        block = static_cast<allocator::BlockHead *>(ptr) - 1;
        local_ptr = static_cast<T *>(ptr);
        check(block, local_ptr);
    }

    template <typename Evictor>
    LiteAccessor(far_obj_t obj, __DMH__, Evictor &&evictor) {
        void *ptr = Cache::get_default()->template fetch_lite<Mut>(
            obj, __on_miss__, std::forward<Evictor>(evictor));
        block = static_cast<allocator::BlockHead *>(ptr) - 1;
        local_ptr = static_cast<T *>(ptr);
        check(block, local_ptr);
    }

    template <typename Impl, typename Evictor>
    LiteAccessor(const UniqueFarPtrBase<T, Impl> &uptr, Evictor &&evictor) {
        ON_MISS_BEGIN
        ON_MISS_END
        void *ptr = Cache::get_default()->template fetch_lite<Mut>(
            uptr.obj(), __on_miss__, std::forward<Evictor>(evictor));
        block = static_cast<allocator::BlockHead *>(ptr) - 1;
        local_ptr = static_cast<T *>(ptr);
        check(block, local_ptr);
    }

    template <typename Impl, typename Evictor>
    LiteAccessor(const UniqueFarPtrBase<T, Impl> &uptr, __DMH__,
                 Evictor &&evictor) {
        void *ptr = Cache::get_default()->template fetch_lite<Mut>(
            uptr.obj(), __on_miss__, std::forward<Evictor>(evictor));
        block = static_cast<allocator::BlockHead *>(ptr) - 1;
        local_ptr = static_cast<T *>(ptr);
        check(block, local_ptr);
    }

    template <typename Evictor>
    LiteAccessor(const UniqueFarPtr<void> &uptr, Evictor &&evictor) {
        ON_MISS_BEGIN
        ON_MISS_END
        void *ptr = Cache::get_default()->template fetch_lite<Mut>(
            uptr.obj(), __on_miss__, std::forward<Evictor>(evictor));
        block = static_cast<allocator::BlockHead *>(ptr) - 1;
        local_ptr = static_cast<T *>(ptr);
        check(block, local_ptr);
    }

    template <typename Evictor>
    LiteAccessor(const UniqueFarPtr<void> &uptr, __DMH__, Evictor &&evictor) {
        void *ptr = Cache::get_default()->template fetch_lite<Mut>(
            uptr.obj(), __on_miss__, std::forward<Evictor>(evictor));
        block = static_cast<allocator::BlockHead *>(ptr) - 1;
        local_ptr = static_cast<T *>(ptr);
        check(block, local_ptr);
    }

    LiteAccessor(const LiteAccessor<T, Mut> &) = default;
    LiteAccessor(LiteAccessor<T, Mut> &&) = default;
    LiteAccessor<T, Mut> &operator=(const LiteAccessor<T, Mut> &) = default;
    LiteAccessor<T, Mut> &operator=(LiteAccessor<T, Mut> &&) = default;

    template <typename U>
    LiteAccessor(LiteAccessor<U, Mut> other, Pointer local_ptr)
        : block(other.block), local_ptr(local_ptr) {
        check(block, local_ptr);
    }

    ~LiteAccessor() = default;

    LiteAccessor<T, true> as_mut() const {
        check(block, local_ptr);
        if constexpr (!Mut) {
            Cache::get_default()->mark_dirty(get_obj());
        }
        LiteAccessor<T, true> mut_accessor;
        mut_accessor.block = block;
        mut_accessor.local_ptr = const_cast<T *>(local_ptr);
        return mut_accessor;
    }

    bool is_null() const {
        check(block, local_ptr);
        return block == nullptr;
    }

    far_obj_t get_obj() const {
        check(block, local_ptr);
        return block->obj_meta_data;
    }

    Pointer as_ptr() const {
        check(block, local_ptr);
        return local_ptr;
    }

    Pointer operator->() const {
        check(block, local_ptr);
        return as_ptr();
    }

    Reference operator*() const {
        check(block, local_ptr);
        return *as_ptr();
    }

    template <typename Evictor>
    bool async_fetch_slow_path(far_obj_t obj, Evictor &&evictor) {
        auto [at_local, local_addr] =
            Cache::get_default()->template async_fetch_lite<Mut>(
                obj, std::forward<Evictor>(evictor));
        this->local_ptr = static_cast<Pointer>(local_addr);
        this->block = static_cast<allocator::BlockHead *>(local_addr) - 1;
        check(block, local_ptr);
        return at_local;
    }

    template <typename Evictor>
    __attribute__((always_inline)) bool async_fetch(far_obj_t obj,
                                                    Evictor &&evictor) {
        auto entry = obj.get_entry_ptr();
        bool fast = entry->load_state().template is_deref_fast_path<Mut>();
        if (fast) [[likely]] {
            void *local_addr = entry->local_addr();
            this->local_ptr = static_cast<Pointer>(local_addr);
            this->block = static_cast<allocator::BlockHead *>(local_addr) - 1;
            check(block, local_ptr);
            return true;
        }
        bool res = async_fetch_slow_path(obj, std::forward<Evictor>(evictor));
        check(block, local_ptr);
        return res;
    }

    template <typename Evictor>
    __attribute__((always_inline)) bool async_fetch(const UniqueFarPtr<T> &uptr,
                                                    Evictor &&evictor) {
        bool fast =
            uptr.get_entry().load_state().template is_deref_fast_path<Mut>();
        if (fast) [[likely]] {
            void *local_addr = uptr.get_entry().local_addr();
            this->local_ptr = static_cast<Pointer>(local_addr);
            this->block = static_cast<allocator::BlockHead *>(local_addr) - 1;
            check(block, local_ptr);
            return true;
        }
        bool res =
            async_fetch_slow_path(uptr.obj(), std::forward<Evictor>(evictor));
        check(block, local_ptr);
        return res;
    }

    void sync() {
        this->local_ptr =
            static_cast<Pointer>(Cache::get_entry_of(get_obj()).local_addr());
        check(block, local_ptr);
    }

    // when a new eviction phase starts, LiteAccessors should be pinned
    void pin() const {
        assert(local_ptr == nullptr || block != nullptr);
        if (block != nullptr) Cache::get_default()->pin(get_obj());
        check(block, local_ptr);
    }

    void unpin() const {
        assert(local_ptr == nullptr || block != nullptr);
        if (block != nullptr) Cache::get_default()->unpin(get_obj());
        check(block, local_ptr);
    }
};

template <bool Mut>
class LiteAccessor<void, Mut> {
    using Pointer = std::conditional_t<Mut, void *, const void *>;

    allocator::BlockHead *block;
    Pointer local_ptr;

    template <typename U, bool M>
    friend class LiteAccessor;

    template <typename U>
    friend class UniqueFarPtr;

public:
    void check() const { check(block, local_ptr); }
    void check(allocator::BlockHead *block) const {
        assert((block->obj_meta_data.load().get_entry_ptr()->local_addr()) ==
               block + 1);
    }

    void check(allocator::BlockHead *block, Pointer local_addr) const {
        check(block);
        assert(block->get_object_ptr() == local_addr);
    }

public:
    LiteAccessor() : block(nullptr), local_ptr(nullptr) {}
    LiteAccessor(const LiteAccessor<void, Mut> &) = default;
    LiteAccessor(LiteAccessor<void, Mut> &&) = default;
    LiteAccessor<void, Mut> &operator=(const LiteAccessor<void, Mut> &) =
        default;
    LiteAccessor<void, Mut> &operator=(LiteAccessor<void, Mut> &&) = default;
    template <typename U>
    LiteAccessor(LiteAccessor<U, Mut> other, Pointer local_ptr)
        : block(other.block), local_ptr(local_ptr) {
        check(block, local_ptr);
    }
    // unsafe!
    LiteAccessor(allocator::BlockHead *block, void *local_ptr)
        : block(block), local_ptr(static_cast<Pointer>(local_ptr)) {
        check(block, local_ptr);
    }
    template <typename Impl, typename Evictor>
    LiteAccessor(const UniqueFarPtrBase<void, Impl> &uptr, Evictor &&evictor) {
        ON_MISS_BEGIN
        ON_MISS_END
        void *ptr = Cache::get_default()->template fetch_lite<Mut>(
            uptr.obj(), __on_miss__, std::forward<Evictor>(evictor));
        block = static_cast<allocator::BlockHead *>(ptr) - 1;
        local_ptr = ptr;
        check(block, local_ptr);
    }
    ~LiteAccessor() = default;
    bool is_null() const {
        check(block, local_ptr);
        return block == nullptr;
    }
    far_obj_t get_obj() const {
        check(block, local_ptr);
        return block->obj_meta_data;
    }
    Pointer as_ptr() {
        check(block, local_ptr);
        return local_ptr;
    }
    Pointer operator->() {
        check(block, local_ptr);
        return as_ptr();
    }
    template <typename T>
    LiteAccessor<T, Mut> as() {
        check(block, local_ptr);
        using Dest = LiteAccessor<T, Mut>;
        return Dest(std::move(*this),
                    static_cast<typename Dest::Pointer>(local_ptr));
    }

    template <typename Evictor>
    static LiteAccessor<void, Mut> allocate(size_t size, Evictor &&evictor) {
        auto accessor = LiteAccessor<void, Mut>();
        auto cache = Cache::get_default();
        auto [o, p] =
            cache->allocate<true>(size, Mut, std::forward<Evictor>(evictor));
        accessor.block = static_cast<allocator::BlockHead *>(p) - 1;
        accessor.local_ptr = p;
        return accessor;
    }

    void pin() const {
        assert(local_ptr == nullptr || block != nullptr);
        if (block != nullptr) Cache::get_default()->pin(get_obj());
        check(block, local_ptr);
    }

    void unpin() const {
        assert(local_ptr == nullptr || block != nullptr);
        if (block != nullptr) Cache::get_default()->unpin(get_obj());
        check(block, local_ptr);
    }

    LiteAccessor<void, true> as_mut() const {
        check(block, local_ptr);
        if constexpr (!Mut) {
            Cache::get_default()->mark_dirty(get_obj());
        }
        LiteAccessor<void, true> mut_accessor;
        mut_accessor.block = block;
        mut_accessor.local_ptr = const_cast<void *>(local_ptr);
        return mut_accessor;
    }
    template <typename Evictor>
    bool async_fetch_slow_path(far_obj_t obj, Evictor &&evictor) {
        auto [at_local, local_addr] =
            Cache::get_default()->template async_fetch_lite<Mut>(
                obj, std::forward<Evictor>(evictor));
        this->local_ptr = static_cast<Pointer>(local_addr);
        this->block = static_cast<allocator::BlockHead *>(local_addr) - 1;
        check(block, local_ptr);
        return at_local;
    }

    template <typename Evictor>
    __attribute__((always_inline)) bool async_fetch(far_obj_t obj,
                                                    Evictor &&evictor) {
        profile::count_deref();
        auto entry = obj.get_entry_ptr();
        bool fast = entry->load_state().template is_deref_fast_path<Mut>();
        if (fast) [[likely]] {
            void *local_addr = entry->local_addr();
            this->local_ptr = static_cast<Pointer>(local_addr);
            this->block = static_cast<allocator::BlockHead *>(local_addr) - 1;
            check(block, local_ptr);
            return true;
        }
        bool res = async_fetch_slow_path(obj, std::forward<Evictor>(evictor));
        check(block, local_ptr);
        return res;
    }

    template <typename Evictor>
    __attribute__((always_inline)) bool async_fetch(
        const UniqueFarPtr<void> &uptr, Evictor &&evictor) {
        profile::count_deref();
        bool fast =
            uptr.get_entry().load_state().template is_deref_fast_path<Mut>();
        if (fast) [[likely]] {
            void *local_addr = uptr.get_entry().local_addr();
            this->local_ptr = static_cast<Pointer>(local_addr);
            this->block = static_cast<allocator::BlockHead *>(local_addr) - 1;
            check(block, local_ptr);
            return true;
        }
        bool res =
            async_fetch_slow_path(uptr.obj(), std::forward<Evictor>(evictor));
        check(block, local_ptr);
        return res;
    }
};

inline Accessor<void> UniqueFarPtr<void>::allocate(size_t nbytes) {
    this->reset();
    auto cache = Cache::get_default();
    cache->allocate<false>(&entry, nbytes, false);
    Accessor<void> accessor;
    accessor.obj = this->obj();
    accessor.local_ptr = entry.local_addr();
    return accessor;
}

template <bool Mut, typename Evictor>
inline LiteAccessor<void, Mut> UniqueFarPtr<void>::allocate_lite(
    size_t nbytes, Evictor &&evictor) {
    this->reset();
    auto cache = Cache::get_default();
    cache->allocate<true>(&entry, nbytes, Mut, std::forward<Evictor>(evictor));
    LiteAccessor<void, Mut> accessor;
    void *p = entry.local_addr();
    accessor.block = static_cast<allocator::BlockHead *>(p) - 1;
    accessor.local_ptr = p;
    return accessor;
}

template <typename T>
inline bool at_local(const Accessor<T> &accessor) {
    return at_local(accessor.get_obj());
}

template <typename T>
inline bool at_local(const ConstAccessor<T> &accessor) {
    return at_local(accessor.get_obj());
}

template <typename T, bool Mut>
inline bool at_local(const LiteAccessor<T, Mut> &accessor) {
    return at_local(accessor.get_obj());
}

template <typename T>
inline bool async_fetch(far_obj_t obj, ConstAccessor<T> &accessor) {
    return accessor.async_fetch(obj);
}

inline void check_memory_low(DereferenceScope &scope) {
    auto cache = Cache::get_default();
    cache->check_memory_low();
    cache->update_scope(scope);
}

inline void DereferenceScope::enter() { Cache::get_default()->enter_scope(); }

inline void DereferenceScope::exit() { Cache::get_default()->exit_scope(); }

}  // namespace cache

using cache::Accessor;
using cache::ConstAccessor;
using cache::DereferenceScope;
using cache::far_obj_t;
using cache::LiteAccessor;
using cache::RootDereferenceScope;
using cache::UniqueFarPtr;

static_assert(sizeof(far_obj_t) == sizeof(uint64_t));

template <typename T, typename... Args>
inline Accessor<T> alloc_obj(Args &&...args) {
    return Accessor<T>::allocate(std::forward<Args>(args)...);
}

inline Accessor<void> alloc_uninitialized(size_t size) {
    return Accessor<void>::allocate(size);
}

inline LiteAccessor<void, true> alloc_uninitialized(size_t size,
                                                    DereferenceScope &scope) {
    return LiteAccessor<void, true>::allocate(size, scope);
}

}  // namespace Beehive