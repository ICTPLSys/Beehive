#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <type_traits>
#include <utility>

#include "cache/handler.hpp"
#include "utils/debug.hpp"

namespace FarLib {

namespace allocator {
constexpr size_t BlockHeadSize = 16;

#ifdef USE_BUMP_ALLOCATOR
constexpr size_t HeapSize = 10ul * 1024 * 1024 * 1024;
extern std::byte *heap;
// TODO: multi-threading support
extern size_t bump_pointer_offset;

inline void *allocate(size_t size) {
    size = (size + 64 - 1) & (~(64 - 1));
    ASSERT(bump_pointer_offset + size <= HeapSize);
    void *ptr = heap + bump_pointer_offset;
    bump_pointer_offset += size;
    return ptr;
}

inline void deallocate(void *ptr) {}
#else
inline void *allocate(size_t size) { return malloc(size); }

inline void deallocate(void *ptr) { free(ptr); }
#endif

}  // namespace allocator

namespace cache {

struct far_obj_t {
    void *ptr;
};

class Cache {
public:
    static void init_default(void *, size_t, size_t, size_t) {}
    static void destroy_default() {}
    static Cache *get_default() { return nullptr; }
    void deallocate(far_obj_t obj) { allocator::deallocate(obj.ptr); }
    void prefetch(far_obj_t obj) {}
    void prefetch(far_obj_t obj, auto &&) {}
};

struct FarObjectEntry {
    void *ptr;
};

inline bool check_fetch(auto &&, auto &&) { return true; }

inline size_t check_cq() { return 0; }

inline bool at_local(far_obj_t obj) { return true; }

template <typename T>
class Accessor;

#define ON_MISS_BEGIN_X                   \
    auto &__last_on_miss__ = __on_miss__; \
    auto __on_miss__ = FarLib::cache::DataMissHandlerImpl([&](auto __entry__, auto __ddl__) {
#define ON_MISS_END_X                                           \
    if (FarLib::cache::check_fetch(__entry__, __ddl__)) return; \
    __last_on_miss__(__entry__, __ddl__);                       \
    });

#define ON_MISS_BEGIN \
    auto __on_miss__ = FarLib::cache::DataMissHandlerImpl([&](auto __entry__, auto __ddl__) {
#define ON_MISS_END \
    });

template <typename T>
struct UniqueFarPtr {
    FarObjectEntry entry;

    UniqueFarPtr() { entry.ptr = nullptr; }
    UniqueFarPtr(const UniqueFarPtr &) = delete;
    UniqueFarPtr(UniqueFarPtr &&other) { move(&other.entry); }
    ~UniqueFarPtr() { reset(); }
    UniqueFarPtr &operator=(UniqueFarPtr &&other) {
        reset();
        move(&other.entry);
        return *this;
    }
    void reset() {
        if (entry.ptr != nullptr) allocator::deallocate(entry.ptr);
    }
    far_obj_t obj() const { return {entry.ptr}; }

    template <typename... Args>
    Accessor<T> allocate(Args &&...args) {
        entry.ptr = allocator::allocate(sizeof(T));
        new (entry.ptr) T(std::forward<Args>(args)...);
        Accessor<T> accessor;
        accessor.local_ptr = entry.ptr;
        return accessor;
    }
    Accessor<T> allocate_uninitialized() {
        entry.ptr = allocator::allocate(sizeof(T));
        Accessor<T> accessor;
        accessor.local_ptr = entry.ptr;
        return accessor;
    }

    template <typename Evictor>
    Accessor<T> allocate_uninitialized(Evictor &&evictor) {
        entry.ptr = allocator::allocate(sizeof(T));
        return {entry.ptr};
    }

    bool is_null() const { return entry.ptr == nullptr; }

    // move another entry to this, will rewrite the object header
    // dereferencing unique ptr when moving is UB for mutators
    // only eviction may be concurrent with this
    void move(FarObjectEntry *other) {
        entry.ptr = other->ptr;
        other->ptr = nullptr;
    }
    operator bool() const { return !is_null(); }
};

template <typename T>
class Accessor {
private:
    void *local_ptr;

private:
    template <typename U>
    friend class UniqueFarPtr;

    template <typename U>
    friend class Accessor;

    template <typename U>
    friend class ConstAccessor;

public:
    Accessor() : local_ptr(nullptr) {}

    Accessor(far_obj_t obj, size_t offset = 0) {
        void *p = obj.ptr;
        local_ptr = static_cast<char *>(p) + offset;
    }

    Accessor(far_obj_t obj, const DataMissHandler &on_data_miss,
             size_t offset = 0) {
        void *p = obj.ptr;
        local_ptr = static_cast<char *>(p) + offset;
    }

    Accessor(const UniqueFarPtr<T> &uptr) { local_ptr = uptr.entry.ptr; }

    Accessor(const UniqueFarPtr<T> &uptr, const DataMissHandler &on_data_miss) {
        local_ptr = uptr.entry.ptr;
    }

    Accessor(const Accessor<T> &) = delete;

    Accessor(Accessor<T> &&other) = default;

    Accessor<T> &operator=(Accessor<T> &&other) noexcept {
        local_ptr = other.local_ptr;
        return *this;
    }

    template <typename U>
    Accessor(Accessor<U> &&other, T *local_ptr) : local_ptr(local_ptr) {}

    ~Accessor() {}

    bool is_null() const { return local_ptr == nullptr; }

    template <typename... Args>
    static Accessor allocate(Args &&...args) {
        return Accessor<T>::allocate(std::forward<Args>(args)...);
    }

    void deallocate() { allocator::deallocate(local_ptr); }

    far_obj_t get_obj() const { return {local_ptr}; }

    T *as_ptr() { return static_cast<T *>(local_ptr); }

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
};

template <typename T>
class ConstAccessor {
private:
    const void *local_ptr;

public:
    ConstAccessor() : local_ptr(nullptr) {}

    ConstAccessor(far_obj_t obj, size_t offset = 0) {
        void *p = obj.ptr;
        local_ptr = static_cast<char *>(p) + offset;
    }

    ConstAccessor(far_obj_t obj, const DataMissHandler &on_data_miss,
                  size_t offset = 0) {
        void *p = obj.ptr;
        local_ptr = static_cast<char *>(p) + offset;
    }

    ConstAccessor(const UniqueFarPtr<T> &uptr) { local_ptr = uptr.entry.ptr; }

    ConstAccessor(const UniqueFarPtr<T> &uptr,
                  const DataMissHandler &on_data_miss) {
        local_ptr = uptr.entry.ptr;
    }

    ConstAccessor(const Accessor<T> &) = delete;

    ConstAccessor(Accessor<T> &&other) { local_ptr = other.local_ptr; };

    ConstAccessor<T> &operator=(Accessor<T> &&other) noexcept {
        local_ptr = other.local_ptr;
        return *this;
    }

    template <typename U>
    ConstAccessor(ConstAccessor<U> &&other, T *local_ptr)
        : local_ptr(local_ptr) {}

    ~ConstAccessor() {}

    bool is_null() const { return local_ptr == nullptr; }

    far_obj_t get_obj() const { return {const_cast<void *>(local_ptr)}; }

    T *as_ptr() { return static_cast<T *>(local_ptr); }

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

// LiteAccessor mut be used in a dereference scope
template <typename T, bool Mut = false>
class LiteAccessor {
    using Pointer = std::conditional_t<Mut, T *, const T *>;
    using Reference = std::conditional_t<Mut, T &, const T &>;

    Pointer local_ptr;

    template <typename U, bool M>
    friend class LiteAccessor;

public:
    LiteAccessor() : local_ptr(nullptr) {}

    template <typename Evictor>
    LiteAccessor(far_obj_t obj, Evictor &&evictor) {
        local_ptr = static_cast<Pointer>(obj.ptr);
    }

    template <typename Evictor>
    LiteAccessor(far_obj_t obj, __DMH__, Evictor &&evictor) {
        local_ptr = static_cast<Pointer>(obj.ptr);
    }

    template <typename Evictor>
    LiteAccessor(const UniqueFarPtr<T> &uptr, Evictor &&evictor) {
        local_ptr = static_cast<Pointer>(uptr.entry.ptr);
    }

    template <typename Evictor>
    LiteAccessor(const UniqueFarPtr<T> &uptr, __DMH__, Evictor &&evictor) {
        local_ptr = static_cast<Pointer>(uptr.entry.ptr);
    }

    LiteAccessor(const LiteAccessor<T, Mut> &) = default;
    LiteAccessor(LiteAccessor<T, Mut> &&) = default;
    LiteAccessor<T, Mut> &operator=(const LiteAccessor<T, Mut> &) = default;
    LiteAccessor<T, Mut> &operator=(LiteAccessor<T, Mut> &&) = default;

    template <typename U>
    LiteAccessor(LiteAccessor<U, Mut> other, Pointer local_ptr)
        : local_ptr(local_ptr) {}

    ~LiteAccessor() = default;

    LiteAccessor<T, true> as_mut() const {
        LiteAccessor<T, true> mut_accessor;
        mut_accessor.local_ptr = const_cast<T *>(local_ptr);
        return mut_accessor;
    }

    bool is_null() const { return local_ptr == nullptr; }

    Pointer as_ptr() const { return local_ptr; }

    Pointer operator->() const { return as_ptr(); }

    Reference operator*() const { return *as_ptr(); }

    template <typename Evictor>
    bool async_fetch(far_obj_t obj, Evictor &&evictor) {
        local_ptr = static_cast<Pointer>(obj.ptr);
        return true;
    }

    template <typename Evictor>
    bool async_fetch(const UniqueFarPtr<T> &uptr, Evictor &&evictor) {
        local_ptr = static_cast<Pointer>(uptr.entry.ptr);
        return true;
    }

    void pin() const {}

    void unpin() const {}
};

template <bool Mut>
class LiteAccessor<void, Mut> {
    using Pointer = std::conditional_t<Mut, void *, const void *>;

    Pointer local_ptr;

    template <typename U, bool M>
    friend class LiteAccessor;

public:
    LiteAccessor() : local_ptr(nullptr) {}
    LiteAccessor(const LiteAccessor<void, Mut> &) = default;
    LiteAccessor(LiteAccessor<void, Mut> &&) = default;
    LiteAccessor<void, Mut> &operator=(const LiteAccessor<void, Mut> &) =
        default;
    LiteAccessor<void, Mut> &operator=(LiteAccessor<void, Mut> &&) = default;
    template <typename U>
    LiteAccessor(LiteAccessor<U, Mut> other, Pointer local_ptr)
        : local_ptr(local_ptr) {}
    ~LiteAccessor() = default;
    bool is_null() const { return local_ptr == nullptr; }
    Pointer as_ptr() { return local_ptr; }
    Pointer operator->() { return as_ptr(); }

    template <typename Evictor>
    static LiteAccessor<void, Mut> allocate(size_t size, Evictor &&evictor) {
        auto accessor = LiteAccessor<void, Mut>();
        accessor.local_ptr = allocator::allocate(size);
        return accessor;
    }
};

class DereferenceScope {
protected:
    virtual void pin() const {}
    virtual void unpin() const {}
    void enter() const {}
    void exit() const {}

    void recursive_pin() {}

    void recursive_unpin() {}

public:
    DereferenceScope(DereferenceScope *parent) {}

    void begin_eviction() {}
    void end_eviction() {}
};

class RootDereferenceScope : public DereferenceScope {
public:
    RootDereferenceScope() : DereferenceScope(nullptr) { enter(); }
    ~RootDereferenceScope() { exit(); }
};

struct OnMissScope : public DereferenceScope {
    OnMissScope(FarObjectEntry *entry, DereferenceScope *parent)
        : DereferenceScope(nullptr) {}
};

template <typename T>
inline bool at_local(const Accessor<T> &accessor) {
    return true;
}

template <typename T>
inline bool at_local(const ConstAccessor<T> &accessor) {
    return true;
}

template <typename T, bool Mut>
inline bool at_local(const LiteAccessor<T, Mut> &accessor) {
    return true;
}

template <typename T>
inline bool async_fetch(far_obj_t obj, ConstAccessor<T> &accessor) {
    return true;
}

}  // namespace cache

using cache::Accessor;
using cache::Cache;
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

}  // namespace FarLib