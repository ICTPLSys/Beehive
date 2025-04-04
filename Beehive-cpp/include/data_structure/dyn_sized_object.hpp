#pragma once
#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>

#include "cache/cache.hpp"

namespace FarLib {

template <size_t ChunkSize = 4096 - allocator::BlockHeadSize>
class DynObjUniqueRef {
public:
    DynObjUniqueRef();

    DynObjUniqueRef(const DynObjUniqueRef &) = delete;

    DynObjUniqueRef(DynObjUniqueRef &&);

    ~DynObjUniqueRef();

    DynObjUniqueRef &operator=(DynObjUniqueRef &&);

    void allocate(size_t size, DereferenceScope &scope);

    // allocate and initialize the object / the first chunk
    template <std::invocable<void *> Fn>
    void allocate(size_t size, Fn &&init, DereferenceScope &scope);

    void reset();

    // data will be copied to new address
    void reallocate(size_t old_size, size_t new_size, DereferenceScope &scope);

    bool is_chunked() const;

    // assert: !is_chunked()
    UniqueFarPtr<void> *as_inlined();

    // assert: is_chunked()
    size_t n_chunks() const;

    // assert: is_chunked()
    UniqueFarPtr<void> &chunk(size_t n);

    void reallocate_inline(size_t old_size, size_t new_size,
                           DereferenceScope &scope);
    void reallocate_inline_to_chunked(size_t old_size, size_t new_size,
                                      DereferenceScope &scope);
    void reallocate_chunked(size_t new_size, DereferenceScope &scope);

private:
    std::byte m_[16];

    struct ChunkedRef {
        uint32_t is_chunked : 1;
        uint32_t entries_cap : 31;
        uint32_t n_chunks;
        UniqueFarPtr<void> *chunks;
    };

    static constexpr uint32_t MinChunkEntriesCapacity = 4;

    // assert: is_chunked()
    ChunkedRef *as_chunked();
    const ChunkedRef *as_chunked() const;
};

template <size_t ChunkSize>
DynObjUniqueRef<ChunkSize>::DynObjUniqueRef() {
    auto ptr = reinterpret_cast<UniqueFarPtr<void> *>(this);
    std::construct_at<UniqueFarPtr<void>>(ptr);
}

template <size_t ChunkSize>
DynObjUniqueRef<ChunkSize>::DynObjUniqueRef(DynObjUniqueRef &&other) {
    if (other.is_chunked()) {
        auto ptr = reinterpret_cast<ChunkedRef *>(this);
        *ptr = *other.as_chunked();
        std::construct_at(&other);
    } else {
        auto ptr = reinterpret_cast<UniqueFarPtr<void> *>(this);
        std::construct_at(ptr, std::move(*other.as_inlined()));
    }
}

template <size_t ChunkSize>
DynObjUniqueRef<ChunkSize>::~DynObjUniqueRef() {
    if (!is_chunked()) [[likely]] {
        std::destroy_at(as_inlined());
    } else {
        delete[] as_chunked()->chunks;
    }
}

template <size_t ChunkSize>
DynObjUniqueRef<ChunkSize> &DynObjUniqueRef<ChunkSize>::operator=(
    DynObjUniqueRef &&other) {
    reset();
    if (other.is_chunked()) {
        auto ptr = reinterpret_cast<ChunkedRef *>(this);
        *ptr = *other.as_chunked();
        std::construct_at(&other);
    } else {
        *as_inlined() = std::move(*other.as_inlined());
    }
    return *this;
}

template <size_t ChunkSize>
void DynObjUniqueRef<ChunkSize>::allocate(size_t size,
                                          DereferenceScope &scope) {
    reset();
    if (size <= ChunkSize) {
        as_inlined()->allocate_lite(size, scope);
    } else {
        uint32_t n_chunks = (size + ChunkSize - 1) / ChunkSize;
        uint32_t entries_cap =
            std::bit_ceil(std::max(n_chunks, MinChunkEntriesCapacity));
        ChunkedRef *r = as_chunked();
        r->is_chunked = true;
        r->entries_cap = entries_cap;
        r->n_chunks = n_chunks;
        r->chunks = new UniqueFarPtr<void>[entries_cap];
        for (uint32_t i = 0; i < n_chunks; i++) {
            r->chunks[i].allocate_lite(ChunkSize, scope);
        }
    }
}

template <size_t ChunkSize>
template <std::invocable<void *> Fn>
void DynObjUniqueRef<ChunkSize>::allocate(size_t size, Fn &&init,
                                          DereferenceScope &scope) {
    reset();
    if (size <= ChunkSize) {
        auto accessor = as_inlined()->template allocate_lite<true>(size, scope);
        init(accessor.as_ptr());
    } else {
        uint32_t n_chunks = (size + ChunkSize - 1) / ChunkSize;
        uint32_t entries_cap =
            std::bit_ceil(std::max(n_chunks, MinChunkEntriesCapacity));
        ChunkedRef *r = as_chunked();
        r->is_chunked = true;
        r->entries_cap = entries_cap;
        r->n_chunks = n_chunks;
        r->chunks = new UniqueFarPtr<void>[entries_cap];
        auto chunk0 =
            r->chunks[0].template allocate_lite<true>(ChunkSize, scope);
        init(chunk0.as_ptr());
        for (uint32_t i = 1; i < n_chunks; i++) {
            r->chunks[i].allocate_lite(ChunkSize, scope);
        }
    }
}

template <size_t ChunkSize>
void DynObjUniqueRef<ChunkSize>::reset() {
    if (!is_chunked()) [[likely]] {
        as_inlined()->reset();
    } else {
        delete[] as_chunked()->chunks;
        auto ptr = reinterpret_cast<UniqueFarPtr<void> *>(this);
        std::construct_at<UniqueFarPtr<void>>(ptr);
    }
}

template <size_t ChunkSize>
void DynObjUniqueRef<ChunkSize>::reallocate(size_t old_size, size_t new_size,
                                            DereferenceScope &scope) {
    if (new_size <= old_size) [[unlikely]] {
        return;
    }
    if (!is_chunked()) {
        if (new_size <= ChunkSize) {
            reallocate_inline(old_size, new_size, scope);
        } else {
            reallocate_inline_to_chunked(old_size, new_size, scope);
        }
    } else {
        reallocate_chunked(new_size, scope);
    }
}

template <size_t ChunkSize>
bool DynObjUniqueRef<ChunkSize>::is_chunked() const {
    return reinterpret_cast<const ChunkedRef *>(this)->is_chunked;
}

template <size_t ChunkSize>
UniqueFarPtr<void> *DynObjUniqueRef<ChunkSize>::as_inlined() {
    assert(!is_chunked());
    return reinterpret_cast<UniqueFarPtr<void> *>(this);
}

template <size_t ChunkSize>
DynObjUniqueRef<ChunkSize>::ChunkedRef *
DynObjUniqueRef<ChunkSize>::as_chunked() {
    assert(is_chunked());
    return reinterpret_cast<ChunkedRef *>(this);
}

template <size_t ChunkSize>
const DynObjUniqueRef<ChunkSize>::ChunkedRef *
DynObjUniqueRef<ChunkSize>::as_chunked() const {
    assert(is_chunked());
    return reinterpret_cast<const ChunkedRef *>(this);
}

template <size_t ChunkSize>
size_t DynObjUniqueRef<ChunkSize>::n_chunks() const {
    return as_chunked()->n_chunks;
}

template <size_t ChunkSize>
UniqueFarPtr<void> &DynObjUniqueRef<ChunkSize>::chunk(size_t n) {
    return as_chunked()->chunks[n];
}

template <size_t ChunkSize>
void DynObjUniqueRef<ChunkSize>::reallocate_inline(
    size_t old_size, size_t new_size, DereferenceScope &parent_scope) {
    struct Scope : public DereferenceScope {
        Scope(DereferenceScope *parent) : DereferenceScope(parent) {}
        void pin() const override { old_obj.pin(); }
        void unpin() const override { old_obj.unpin(); }
        LiteAccessor<void, false> old_obj;
    } scope(&parent_scope);
    UniqueFarPtr<void> new_ptr;
    scope.old_obj = as_inlined()->access(scope);
    auto new_obj = new_ptr.allocate_lite<true>(new_size, scope);
    std::memcpy(new_obj.as_ptr(), scope.old_obj.as_ptr(), old_size);
    *as_inlined() = std::move(new_ptr);
}

template <size_t ChunkSize>
void DynObjUniqueRef<ChunkSize>::reallocate_inline_to_chunked(
    size_t old_size, size_t new_size, DereferenceScope &scope) {
    uint32_t n_chunks = (new_size + ChunkSize - 1) / ChunkSize;
    uint32_t entries_cap =
        std::bit_ceil(std::max(n_chunks, MinChunkEntriesCapacity));
    auto *chunks = new UniqueFarPtr<void>[entries_cap];
    if (as_inlined()->size() == ChunkSize) [[likely]] {
        chunks[0] = std::move(*as_inlined());
    } else {
        TODO("reallocation");
    }
    for (uint32_t i = 1; i < n_chunks; i++) {
        chunks[i].allocate_lite(ChunkSize, scope);
    }
    ChunkedRef *r = reinterpret_cast<ChunkedRef *>(this);
    r->is_chunked = true;
    r->entries_cap = entries_cap;
    r->n_chunks = n_chunks;
    r->chunks = chunks;
}

template <size_t ChunkSize>
void DynObjUniqueRef<ChunkSize>::reallocate_chunked(size_t new_size,
                                                    DereferenceScope &scope) {
    ChunkedRef *r = as_chunked();
    uint32_t n_chunks = (new_size + ChunkSize - 1) / ChunkSize;
    if (r->n_chunks >= n_chunks) return;
    if (r->entries_cap < n_chunks) {
        uint32_t entries_cap =
            std::bit_ceil(std::max(n_chunks, MinChunkEntriesCapacity));
        auto chunks = new UniqueFarPtr<void>[entries_cap];
        for (uint32_t i = 0; i < r->n_chunks; i++) {
            chunks[i] = std::move(r->chunks[i]);
        }
        delete[] r->chunks;
        r->entries_cap = entries_cap;
        r->chunks = chunks;
    }
    for (uint32_t i = r->n_chunks; i < n_chunks; i++) {
        r->chunks[i].allocate_lite(ChunkSize, scope);
    }
    r->n_chunks = n_chunks;
}

}  // namespace FarLib
