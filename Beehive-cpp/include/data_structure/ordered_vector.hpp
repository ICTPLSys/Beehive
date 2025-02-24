/*
    A small vector acts like an vector.
    When the vector length is small, all elements are in a same object.
    When the vector length is large, elements are stored in difference chunks.
*/

#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "async/context.hpp"
#include "dyn_sized_object.hpp"

namespace Beehive {

template <typename T, bool Ordered = false,
          size_t ChunkSizeInByte = 4096 - allocator::BlockHeadSize>
class FarSmallVector {
public:
    struct Head {
        uint32_t size;
        uint32_t capacity;
        T data[0];
    };

    static constexpr size_t inline_obj_size(size_t size) {
        size_t min_block_size =
            allocator::BlockHeadSize + sizeof(Head) + (size) * sizeof(T);
        size_t new_block_size = std::bit_ceil(min_block_size);
        return new_block_size - allocator::BlockHeadSize;
    }

    static constexpr size_t inline_capacity(size_t size) {
        return (inline_obj_size(size) - sizeof(Head)) / sizeof(T);
    }

    static constexpr size_t InitialCapacity = inline_capacity(4);

    static constexpr size_t MaxInlineCapacity =
        (ChunkSizeInByte - sizeof(Head)) / sizeof(T);

    static_assert(InitialCapacity <= MaxInlineCapacity);

    static constexpr size_t ElementPerChunk = ChunkSizeInByte / sizeof(T);

    // the first chunk: Head + MaxInlineCapacity * elements
    // the other chunks: ElementPerChunk * elements

    static constexpr size_t get_chunk(size_t idx) {
        return (idx + ElementPerChunk - MaxInlineCapacity) / ElementPerChunk;
    }

    static constexpr size_t get_offset_in_byte_in_chunk(size_t idx) {
        if (idx < MaxInlineCapacity) {
            return sizeof(Head) + idx * sizeof(T);
        } else {
            return (idx - MaxInlineCapacity) % ElementPerChunk * sizeof(T);
        }
    }

public:
    FarSmallVector(FarSmallVector &&) = default;
    FarSmallVector &operator=(FarSmallVector &&) = default;
    bool is_null() { return !ref.is_chunked() && ref.as_inlined()->is_null(); }
    DynObjUniqueRef<ChunkSizeInByte> ref;
    FarSmallVector(DereferenceScope &scope) {
        ref.allocate(
            sizeof(Head) + InitialCapacity * sizeof(T),
            [](void *p) {
                Head *head = static_cast<Head *>(p);
                head->size = 0;
                head->capacity = InitialCapacity;
            },
            scope);
    }

    bool insert(T value, DereferenceScope &scope) {
        if (!ref.is_chunked()) [[likely]] {
            // inline
            LiteAccessor<void, true> acc =
                ref.as_inlined()->template access<true>(scope);
            Head *head = static_cast<Head *>(acc.as_ptr());
            struct Scope : public DereferenceScope {
                Scope(DereferenceScope *parent) : DereferenceScope(parent) {}
                void pin() const override { head_acc->pin(); }
                void unpin() const override { head_acc->unpin(); }
                LiteAccessor<void, true> *head_acc;
            } scope_head_pre(&scope);
            scope_head_pre.head_acc = &acc;
            if (head->size < head->capacity) {
                // no reallocation
                head->data[head->size] = value;
                head->size++;
                if constexpr (Ordered) {
                    int i = head->size - 1;
                    while (i > 0 && head->data[i - 1] > value) {
                        std::swap(head->data[i], head->data[i - 1]);
                        --i;
                    }
                }
            } else {
                // need reallocation
                size_t old_size = head->size;
                size_t old_cap = head->capacity;
                size_t old_obj_size = sizeof(Head) + sizeof(T) * old_cap;
                size_t inline_cap = inline_capacity(old_cap + 1);
                if (inline_cap <= MaxInlineCapacity) {
                    // inline after reallocation
                    size_t new_obj_size = inline_obj_size(old_cap + 1);
                    ref.reallocate(old_obj_size, new_obj_size, scope_head_pre);
                    assert(!ref.is_chunked());
                    acc = ref.as_inlined()->template access<true>(scope);
                    head = static_cast<Head *>(acc.as_ptr());
                    head->data[head->size] = value;
                    head->size++;
                    head->capacity = inline_cap;
                    if constexpr (Ordered) {
                        int i = head->size - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                } else {
                    // chunked after reallocation
                    size_t new_obj_size = ChunkSizeInByte * 2;
                    size_t new_cap =
                        MaxInlineCapacity + ChunkSizeInByte / sizeof(T);
                    head->size++;
                    head->capacity = new_cap;
                    ref.reallocate(old_obj_size, new_obj_size, scope_head_pre);
                    assert(ref.is_chunked());
                    acc = ref.chunk(0).template access<true>(scope);
                    head = static_cast<Head *>(acc.as_ptr());
                    struct Scope : public DereferenceScope {
                        Scope(DereferenceScope *parent)
                            : DereferenceScope(parent) {}
                        void pin() const override { acc_head->pin(); }
                        void unpin() const override { acc_head->unpin(); }
                        LiteAccessor<void, true> *acc_head;
                    } scope_head(&scope);
                    scope_head.acc_head = &acc;
                    auto p = ref.chunk(1).template access<true>(scope_head);
                    T *chunk = reinterpret_cast<T *>(p.as_ptr());
                    chunk[0] = value;
                    if (head->data[MaxInlineCapacity - 1] > value) {
                        std::swap(head->data[MaxInlineCapacity - 1], chunk[0]);
                        if constexpr (Ordered) {
                            int i = MaxInlineCapacity - 1;
                            while (i > 0 && head->data[i - 1] > value) {
                                std::swap(head->data[i], head->data[i - 1]);
                                --i;
                            }
                        }
                    }
                }
            }
            acc.check();
        } else {
            // chunked
            auto head_acc = ref.chunk(0).template access<true>(scope);
            Head *head = reinterpret_cast<Head *>(head_acc.as_ptr());
            if (head->size < MaxInlineCapacity) {
                // no reallocation
                head->data[head->size] = value;
                head->size++;
                if constexpr (Ordered) {
                    int i = head->size - 1;
                    while (i > 0 && head->data[i - 1] > value) {
                        std::swap(head->data[i], head->data[i - 1]);
                        --i;
                    }
                }
                head_acc.check();
                return true;
            }
            size_t idx = head->size;
            size_t chunk_idx = get_chunk(idx);
            size_t offset = get_offset_in_byte_in_chunk(idx);
            struct Scope : public DereferenceScope {
                Scope(DereferenceScope *parent) : DereferenceScope(parent) {}
                void pin() const override { head_acc->pin(); }
                void unpin() const override { head_acc->unpin(); }
                LiteAccessor<void, true> *head_acc;
            } scope_head(&scope);
            scope_head.head_acc = &head_acc;
            if (head->size < head->capacity) {
                // no reallocation
                head->size++;
                auto chunk_acc =
                    ref.chunk(chunk_idx).template access<true>(scope_head);
                T *tbase = reinterpret_cast<T *>(chunk_acc.as_ptr());
                auto element_ptr =
                    reinterpret_cast<std::byte *>(chunk_acc.as_ptr()) + offset;
                *reinterpret_cast<T *>(element_ptr) = value;
                if constexpr (Ordered) {
                    T *tpoint = reinterpret_cast<T *>(element_ptr);
                    while (tpoint > tbase) {
                        if (*(tpoint - 1) < value) {
                            chunk_acc.check();
                            return true;
                        }
                        std::swap(*(tpoint - 1), *tpoint);
                        tpoint--;
                    }
                    if (chunk_idx > 1) {
                        struct Scope : public DereferenceScope {
                            Scope(DereferenceScope *parent)
                                : DereferenceScope(parent) {}
                            void pin() const override { chunk_acc->pin(); }
                            void unpin() const override { chunk_acc->unpin(); }
                            LiteAccessor<void, true> *chunk_acc;
                        } scope_chunk(&scope_head);
                        scope_chunk.chunk_acc = &chunk_acc;
                        auto last_chunk_acc =
                            ref.chunk(chunk_idx - 1)
                                .template access<true>(scope_chunk);
                        auto last_element_ptr =
                            reinterpret_cast<T *>(last_chunk_acc.as_ptr());
                        if (last_element_ptr[ElementPerChunk - 1] < value) {
                            scope_chunk.chunk_acc->check();
                            last_chunk_acc.check();
                            return true;
                        }
                        std::swap(last_element_ptr[ElementPerChunk - 1],
                                  *tbase);
                        for (size_t chunk_idx_temp = chunk_idx - 1;
                             chunk_idx_temp > 0; chunk_idx_temp--) {
                            chunk_acc = ref.chunk(chunk_idx_temp)
                                            .template access<true>(scope_chunk);
                            auto element_ptr =
                                reinterpret_cast<T *>(chunk_acc.as_ptr());
                            int i = ElementPerChunk - 1;
                            while (i > 0) {
                                if (element_ptr[i - 1] < value) {
                                    chunk_acc.check();
                                    return true;
                                }
                                std::swap(element_ptr[i], element_ptr[i - 1]);
                                --i;
                            }
                            if (chunk_idx_temp != 1) {
                                auto last_chunk_acc =
                                    ref.chunk(chunk_idx_temp - 1)
                                        .template access<true>(scope_chunk);
                                auto last_element_ptr = reinterpret_cast<T *>(
                                    last_chunk_acc.as_ptr());
                                if (last_element_ptr[ElementPerChunk - 1] <
                                    value) {
                                    last_chunk_acc.check();
                                    return true;
                                }
                                std::swap(last_element_ptr[ElementPerChunk - 1],
                                          element_ptr[0]);
                            }
                        }
                    }
                    chunk_acc = ref.chunk(1).template access<true>(scope_head);
                    auto element_0_ptr =
                        reinterpret_cast<T *>(chunk_acc.as_ptr());
                    if (head->data[MaxInlineCapacity - 1] < value) {
                        chunk_acc.check();
                        return true;
                    }
                    std::swap(head->data[MaxInlineCapacity - 1],
                              element_0_ptr[0]);
                    int i = MaxInlineCapacity - 1;
                    while (i > 0 && head->data[i - 1] > value) {
                        std::swap(head->data[i], head->data[i - 1]);
                        --i;
                    }
                }
            } else {
                // need reallocation
                size_t new_chunk_count = chunk_idx * 2;
                size_t new_obj_size = new_chunk_count * ChunkSizeInByte;
                size_t new_cap =
                    MaxInlineCapacity + (new_chunk_count - 1) * ElementPerChunk;
                head->capacity = new_cap;
                head->size++;
                ref.reallocate_chunked(new_obj_size, scope_head);
                auto chunk_acc =
                    ref.chunk(chunk_idx).template access<true>(scope_head);
                auto element_ptr =
                    reinterpret_cast<std::byte *>(chunk_acc.as_ptr()) + offset;
                *reinterpret_cast<T *>(element_ptr) = value;
                if constexpr (Ordered) {
                    auto element_t_ptr = reinterpret_cast<T *>(element_ptr);
                    if (chunk_idx > 1) {
                        struct Scope : public DereferenceScope {
                            Scope(DereferenceScope *parent)
                                : DereferenceScope(parent) {}
                            void pin() const override { chunk_acc->pin(); }
                            void unpin() const override { chunk_acc->unpin(); }
                            LiteAccessor<void, true> *chunk_acc;
                        } scope_chunk(&scope_head);
                        scope_chunk.chunk_acc = &chunk_acc;
                        auto last_chunk_acc =
                            ref.chunk(chunk_idx - 1)
                                .template access<true>(scope_chunk);
                        auto last_element_ptr =
                            reinterpret_cast<T *>(last_chunk_acc.as_ptr());
                        if (last_element_ptr[ElementPerChunk - 1] < value) {
                            scope_chunk.chunk_acc->check();
                            last_chunk_acc.check();
                            return true;
                        }
                        if constexpr (Ordered) {
                            std::swap(last_element_ptr[ElementPerChunk - 1],
                                      *element_t_ptr);
                        }
                        for (size_t chunk_idx_temp = chunk_idx - 1;
                             chunk_idx_temp > 0; chunk_idx_temp--) {
                            chunk_acc = ref.chunk(chunk_idx_temp)
                                            .template access<true>(scope_chunk);
                            auto element_ptr =
                                reinterpret_cast<T *>(chunk_acc.as_ptr());
                            if constexpr (Ordered) {
                                int i = ElementPerChunk - 1;
                                while (i > 0) {
                                    if (element_ptr[i - 1] < value) {
                                        chunk_acc.check();
                                        return true;
                                    }
                                    std::swap(element_ptr[i],
                                              element_ptr[i - 1]);
                                    --i;
                                }
                            }
                            if (chunk_idx_temp != 1) {
                                last_chunk_acc =
                                    ref.chunk(chunk_idx_temp - 1)
                                        .template access<true>(scope_chunk);
                                last_element_ptr = reinterpret_cast<T *>(
                                    last_chunk_acc.as_ptr());
                                if (last_element_ptr[ElementPerChunk - 1] <
                                    value) {
                                    last_chunk_acc.check();
                                    return true;
                                }
                                if constexpr (Ordered) {
                                    std::swap(
                                        last_element_ptr[ElementPerChunk - 1],
                                        element_ptr[0]);
                                }
                            }
                        }
                    }
                    chunk_acc = ref.chunk(1).template access<true>(scope_head);
                    auto element_0_ptr =
                        reinterpret_cast<T *>(chunk_acc.as_ptr());
                    if (head->data[MaxInlineCapacity - 1] < value) {
                        chunk_acc.check();
                        return true;
                    }
                    if constexpr (Ordered) {
                        std::swap(head->data[MaxInlineCapacity - 1],
                                  element_0_ptr[0]);
                        int i = MaxInlineCapacity - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                }
                head_acc.check();
            }
        }
        return true;
    }

    void erase(const T value, DereferenceScope &scope) {
        if (!ref.is_chunked()) [[likely]] {
            auto head_acc = ref.as_inlined()->template access<true>(scope);
            Head *head = static_cast<Head *>(head_acc.as_ptr());
            size_t size = head->size;
            for (size_t i = 0; i < size; i++) {
                if (head->data[i] == value) {
                    for (int j = i + 1; j < size; j++) {
                        head->data[i - 1] = head->data[i];
                    }
                    head->size--;
                    return;
                }
            }
        } else {
            auto head_acc = ref.chunk(0).template access<true>(scope);
            Head *head = reinterpret_cast<Head *>(head_acc.as_ptr());
            size_t size = head->size;
            size_t idx;
            bool flag = false;
            for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                if (flag) {
                    head->data[idx - 1] = head->data[idx];
                } else if (head->data[idx] == value) {
                    flag = true;
                }
            }
            struct Scope : public DereferenceScope {
                Scope(DereferenceScope *parent) : DereferenceScope(parent) {}
                void pin() const override { head_acc->pin(); }
                void unpin() const override { head_acc->unpin(); }
                LiteAccessor<void, true> *head_acc;
            } scope_head(&scope);
            scope_head.head_acc = &head_acc;
            if (flag) {
                auto next_chunk_acc =
                    ref.chunk(1).template access<true>(scope_head);
                auto next_element_ptr =
                    reinterpret_cast<T *>(next_chunk_acc.as_ptr());
                head->data[MaxInlineCapacity - 1] = next_element_ptr[0];
            }
            for (size_t chunk_idx = 1; idx < size; chunk_idx++) {
                auto chunk_acc =
                    ref.chunk(chunk_idx).template access<true>(scope_head);
                auto element_ptr = reinterpret_cast<T *>(chunk_acc.as_ptr());
                struct Scope : public DereferenceScope {
                    Scope(DereferenceScope *parent)
                        : DereferenceScope(parent) {}
                    void pin() const override { chunk_acc->pin(); }
                    void unpin() const override { chunk_acc->unpin(); }
                    LiteAccessor<void, true> *chunk_acc;
                } scope_chunk(&scope_head);
                scope_chunk.chunk_acc = &chunk_acc;
                for (size_t i = 0; i < ElementPerChunk && idx < size;
                     i++, idx++) {
                    if (flag && i > 0) {
                        element_ptr[i - 1] = element_ptr[i];
                    } else if (element_ptr[i] == value) {
                        flag = true;
                    }
                }
                if (flag && idx < size) {
                    auto next_chunk_acc =
                        ref.chunk(chunk_idx + 1)
                            .template access<true>(scope_chunk);
                    auto next_element_ptr =
                        reinterpret_cast<T *>(next_chunk_acc.as_ptr());
                    element_ptr[ElementPerChunk - 1] = next_element_ptr[0];
                }
            }
            if (flag) {
                head->size--;
            }
        }
    }

    // TODO
    std::vector<T> get_range_elements(T start, T stop,
                                      DereferenceScope &scope) {
        std::vector<T> rangeElements;
        if (!ref.is_chunked()) [[likely]] {
            LiteAccessor<void> acc = ref.as_inlined()->template access(scope);
            const Head *head = static_cast<const Head *>(acc.as_ptr());
            size_t size = head->size;
            for (size_t i = 0; i < size; i++) {
                if (head->data[i] > start) {
                    if (head->data[i] > stop) {
                        return rangeElements;
                    }
                    rangeElements.push_back(head->data[i]);
                }
            }
        } else {
            auto head_acc = ref.chunk(0).access(scope);
            const Head *head =
                reinterpret_cast<const Head *>(head_acc.as_ptr());
            size_t size = head->size;
            size_t idx;
            for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                if (head->data[idx] > start) {
                    if (head->data[idx] > stop) {
                        return rangeElements;
                    }
                    rangeElements.push_back(head->data[idx]);
                }
            }
            for (size_t chunk_idx = 1; idx < size; chunk_idx++) {
                auto chunk_acc = ref.chunk(chunk_idx).access(scope);
                auto element_ptr =
                    reinterpret_cast<const T *>(chunk_acc.as_ptr());
                for (size_t i = 0; i < ElementPerChunk && idx < size;
                     i++, idx++) {
                    if (element_ptr[i] > start) {
                        if (element_ptr[i] > stop) {
                            return rangeElements;
                        }
                        rangeElements.push_back(element_ptr[i]);
                    }
                }
            }
        }
        return rangeElements;
    }

    std::vector<T> get_all_elements(DereferenceScope &scope) {
        std::vector<T> rangeElements;
        if (!ref.is_chunked()) [[likely]] {
            LiteAccessor<void> acc = ref.as_inlined()->template access(scope);
            const Head *head = static_cast<const Head *>(acc.as_ptr());
            size_t size = head->size;
            for (size_t i = 0; i < size; i++) {
                rangeElements.push_back(head->data[i]);
            }
        } else {
            auto head_acc = ref.chunk(0).access(scope);
            const Head *head =
                reinterpret_cast<const Head *>(head_acc.as_ptr());
            size_t size = head->size;
            size_t idx;
            for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                rangeElements.push_back(head->data[idx]);
            }
            for (size_t chunk_idx = 1; idx < size; chunk_idx++) {
                auto chunk_acc = ref.chunk(chunk_idx).access(scope);
                auto element_ptr =
                    reinterpret_cast<const T *>(chunk_acc.as_ptr());
                for (size_t i = 0; i < ElementPerChunk && idx < size;
                     i++, idx++) {
                    rangeElements.push_back(element_ptr[i]);
                }
            }
        }
        return rangeElements;
    }

    template <std::invocable<size_t, const T &> Fn>
    void for_each(Fn &&fn, DereferenceScope &scope) {
        if (!ref.is_chunked()) [[likely]] {
            LiteAccessor<void> acc = ref.as_inlined()->template access(scope);
            const Head *head = static_cast<const Head *>(acc.as_ptr());
            size_t size = head->size;
            for (size_t i = 0; i < size; i++) {
                fn(i, head->data[i]);
            }
        } else {
            auto head_acc = ref.chunk(0).access(scope);
            const Head *head =
                reinterpret_cast<const Head *>(head_acc.as_ptr());
            size_t size = head->size;
            size_t idx;
            for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                fn(idx, head->data[idx]);
            }
            for (size_t chunk_idx = 1; idx < size; chunk_idx++) {
                auto chunk_acc = ref.chunk(chunk_idx).access(scope);
                auto element_ptr =
                    reinterpret_cast<const T *>(chunk_acc.as_ptr());
                for (size_t i = 0; i < ElementPerChunk && idx < size;
                     i++, idx++) {
                    fn(idx, element_ptr[i]);
                }
            }
        }
    }

    struct InsertFrame {
        LiteAccessor<void, true> head_accessor;
        LiteAccessor<void, true> head_accessor_new;
        LiteAccessor<void, true> element_accessor;
        LiteAccessor<void, true> element_accessor_new;

        FarSmallVector *order;
        T value;
        Head *head;
        size_t old_size, old_cap, old_obj_size, inline_cap, new_obj_size,
            new_cap, new_chunk_count;
        size_t idx, chunk_idx, offset;
        T *chunk;
        T *tbase, *tpoint;
        std::byte *element_ptr_first;
        T *element_t_ptr;
        T *last_element_ptr;
        T *element_0_ptr;
        T *element_ptr;
        size_t chunk_idx_temp;
        int i;
        bool result;

        bool fetched() {
            switch (status) {
            case NO_CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case NO_CHUNK_SECOND:
                return cache::at_local(head_accessor_new);
            case NO_CHUNK_THIRD:
                return cache::at_local(head_accessor_new);
            case NO_CHUNK_FOURTH:
                return cache::at_local(element_accessor);
            case CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case CHUNK_SECOND:
                return cache::at_local(element_accessor);
            case CHUNK_THIRD:
                return cache::at_local(element_accessor_new);
            case CHUNK_FOURTH:
                return cache::at_local(element_accessor);
            case CHUNK_FIFTH:
                return cache::at_local(element_accessor_new);
            case CHUNK_SIXTH:
                return cache::at_local(element_accessor_new);
            case ELSE_CHUNK_SECOND:
                return cache::at_local(element_accessor);
            case ELSE_CHUNK_THIRD:
                return cache::at_local(element_accessor_new);
            case ELSE_CHUNK_FOURTH:
                return cache::at_local(element_accessor);
            case ELSE_CHUNK_FIFTH:
                return cache::at_local(element_accessor_new);
            case ELSE_CHUNK_SIXTH:
                return cache::at_local(element_accessor_new);
            }
            return true;
        }
        void pin() {
            head_accessor.pin();
            head_accessor_new.pin();
            element_accessor.pin();
            element_accessor_new.pin();
        }
        void unpin() {
            head_accessor.unpin();
            head_accessor_new.unpin();
            element_accessor.unpin();
            element_accessor_new.unpin();
        }

        enum {
            INIT,
            NO_CHUNK_FIRST,
            NO_CHUNK_SECOND,
            NO_CHUNK_THIRD,
            NO_CHUNK_FOURTH,
            CHUNK_FIRST,
            CHUNK_SECOND,
            CHUNK_THIRD,
            CHUNK_FOURTH,
            CHUNK_FIFTH,
            CHUNK_SIXTH,
            ELSE_CHUNK_SECOND,
            ELSE_CHUNK_THIRD,
            ELSE_CHUNK_FOURTH,
            ELSE_CHUNK_FIFTH,
            ELSE_CHUNK_SIXTH,
        } status = INIT;

        bool run(DereferenceScope &scope) {
            switch (status) {
            case NO_CHUNK_FIRST:
                goto NO_CHUNK_FIRST;
            case NO_CHUNK_SECOND:
                goto NO_CHUNK_SECOND;
            case NO_CHUNK_THIRD:
                goto NO_CHUNK_THIRD;
            case NO_CHUNK_FOURTH:
                goto NO_CHUNK_FOURTH;
            case CHUNK_FIRST:
                goto CHUNK_FIRST;
            case CHUNK_SECOND:
                goto CHUNK_SECOND;
            case CHUNK_THIRD:
                goto CHUNK_THIRD;
            case CHUNK_FOURTH:
                goto CHUNK_FOURTH;
            case CHUNK_FIFTH:
                goto CHUNK_FIFTH;
            case CHUNK_SIXTH:
                goto CHUNK_SIXTH;
            case ELSE_CHUNK_SECOND:
                goto ELSE_CHUNK_SECOND;
            case ELSE_CHUNK_THIRD:
                goto ELSE_CHUNK_THIRD;
            case ELSE_CHUNK_FOURTH:
                goto ELSE_CHUNK_FOURTH;
            case ELSE_CHUNK_FIFTH:
                goto ELSE_CHUNK_FIFTH;
            case ELSE_CHUNK_SIXTH:
                goto ELSE_CHUNK_SIXTH;
            }
            // if not chunk
            if (!order->ref.is_chunked()) [[likely]] {
                // inline
                // LiteAccessor<void, true> acc =
                //     ref.as_inlined()->template access<true>(scope);
                if (!head_accessor.async_fetch(*order->ref.as_inlined(),
                                               scope)) {
                    status = NO_CHUNK_FIRST;
                    return false;
                }
            NO_CHUNK_FIRST:
                head = static_cast<Head *>(head_accessor.as_ptr());
                if (head->size < head->capacity) {
                    // no reallocation
                    head->data[head->size] = value;
                    head->size++;
                    if constexpr (Ordered) {
                        int i = head->size - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                } else {
                    // need reallocation
                    old_size = head->size;
                    old_cap = head->capacity;
                    old_obj_size = sizeof(Head) + sizeof(T) * old_cap;
                    inline_cap = inline_capacity(old_cap + 1);
                    if (inline_cap <= MaxInlineCapacity) {
                        // inline after reallocation
                        new_obj_size = inline_obj_size(old_cap + 1);
                        order->ref.reallocate(old_obj_size, new_obj_size,
                                              scope);
                        assert(!order->ref.is_chunked());
                        if (!head_accessor_new.async_fetch(
                                *order->ref.as_inlined(), scope)) {
                            status = NO_CHUNK_SECOND;
                            return false;
                        }
                    // acc = ref.as_inlined()->template access<true>(scope);
                    NO_CHUNK_SECOND:
                        head = static_cast<Head *>(head_accessor_new.as_ptr());
                        head->data[head->size] = value;
                        head->size++;
                        head->capacity = inline_cap;
                        if constexpr (Ordered) {
                            int i = head->size - 1;
                            while (i > 0 && head->data[i - 1] > value) {
                                std::swap(head->data[i], head->data[i - 1]);
                                --i;
                            }
                        }
                    } else {
                        // chunked after reallocation
                        new_obj_size = ChunkSizeInByte * 2;
                        new_cap =
                            MaxInlineCapacity + ChunkSizeInByte / sizeof(T);
                        head->size++;
                        head->capacity = new_cap;
                        order->ref.reallocate(old_obj_size, new_obj_size,
                                              scope);
                        assert(order->ref.is_chunked());
                        if (!head_accessor_new.async_fetch(order->ref.chunk(0),
                                                           scope)) {
                            status = NO_CHUNK_THIRD;
                            return false;
                        }
                    NO_CHUNK_THIRD:
                        // acc = ref.chunk(0).template access<true>(scope);
                        head = static_cast<Head *>(head_accessor_new.as_ptr());
                        if (!element_accessor.async_fetch(order->ref.chunk(1),
                                                          scope)) {
                            status = NO_CHUNK_FOURTH;
                            return false;
                        }
                    NO_CHUNK_FOURTH:
                        // auto p = ref.chunk(1).template
                        // access<true>(scope_head);
                        chunk =
                            reinterpret_cast<T *>(element_accessor.as_ptr());
                        chunk[0] = value;
                        if constexpr (Ordered) {
                            if (head->data[MaxInlineCapacity - 1] > value) {
                                std::swap(head->data[MaxInlineCapacity - 1],
                                          chunk[0]);
                                int i = MaxInlineCapacity - 1;
                                while (i > 0 && head->data[i - 1] > value) {
                                    std::swap(head->data[i], head->data[i - 1]);
                                    --i;
                                }
                            }
                        }
                    }
                }
            } else {
                // chunked
                if (!head_accessor.async_fetch(order->ref.chunk(0), scope)) {
                    status = CHUNK_FIRST;
                    return false;
                }
            CHUNK_FIRST:
                // auto head_acc = ref.chunk(0).template access<true>(scope);
                head = reinterpret_cast<Head *>(head_accessor.as_ptr());
                idx = head->size;
                if (head->size < MaxInlineCapacity) {
                    // no reallocation
                    head->data[head->size] = value;
                    head->size++;
                    int i = head->size - 1;
                    if constexpr (Ordered) {
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                    result = true;
                    head_accessor.check();
                    return true;
                }
                chunk_idx = get_chunk(idx);
                offset = get_offset_in_byte_in_chunk(idx);
                if (head->size < head->capacity) {
                    // no reallocation
                    head->size++;
                    // auto chunk_acc =
                    //     ref.chunk(chunk_idx).template
                    //     access<true>(scope_head);
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = CHUNK_SECOND;
                        return false;
                    }
                CHUNK_SECOND:
                    // swap the final trunk
                    tbase = reinterpret_cast<T *>(element_accessor.as_ptr());
                    element_ptr_first = reinterpret_cast<std::byte *>(
                                            element_accessor.as_ptr()) +
                                        offset;
                    *reinterpret_cast<T *>(element_ptr_first) = value;
                    if constexpr (Ordered) {
                        tpoint = reinterpret_cast<T *>(element_ptr_first);
                        while (tpoint > tbase) {
                            if (*(tpoint - 1) < value) {
                                element_accessor.check();
                                return true;
                            }
                            std::swap(*(tpoint - 1), *tpoint);
                            tpoint--;
                        }
                    }
                    // swap all trunk  except the first trunk
                    if (chunk_idx > 1) {
                        // auto last_chunk_acc =
                        //     ref.chunk(chunk_idx - 1).template
                        //     access<true>(scope_chunk);
                        if (!element_accessor_new.async_fetch(
                                order->ref.chunk(chunk_idx - 1), scope)) {
                            status = CHUNK_THIRD;
                            return false;
                        }
                    CHUNK_THIRD:
                        last_element_ptr = reinterpret_cast<T *>(
                            element_accessor_new.as_ptr());
                        if (last_element_ptr[ElementPerChunk - 1] < value) {
                            element_accessor_new.check();
                            return true;
                        }
                        if constexpr (Ordered) {
                            std::swap(last_element_ptr[ElementPerChunk - 1],
                                      *tbase);
                        }
                        for (chunk_idx_temp = chunk_idx - 1; chunk_idx_temp > 0;
                             chunk_idx_temp--) {
                            if (!element_accessor.async_fetch(
                                    order->ref.chunk(chunk_idx_temp), scope)) {
                                status = CHUNK_FOURTH;
                                return false;
                            }
                        CHUNK_FOURTH:
                            // chunk_acc = ref.chunk(chunk_idx_temp)
                            //                 .template
                            //                 access<true>(scope_chunk);
                            element_ptr = reinterpret_cast<T *>(
                                element_accessor.as_ptr());
                            i = ElementPerChunk - 1;
                            if constexpr (Ordered) {
                                while (i > 0) {
                                    if (element_ptr[i - 1] < value) {
                                        element_accessor.check();
                                        return true;
                                    }
                                    std::swap(element_ptr[i],
                                              element_ptr[i - 1]);
                                    --i;
                                }
                            }
                            if (chunk_idx_temp != 1) {
                                // auto last_chunk_acc =
                                //     ref.chunk(chunk_idx_temp - 1)
                                //         .template access<true>(scope_chunk);
                                if (!element_accessor_new.async_fetch(
                                        order->ref.chunk(chunk_idx_temp - 1),
                                        scope)) {
                                    status = CHUNK_FIFTH;
                                    return false;
                                }
                            CHUNK_FIFTH:
                                last_element_ptr = reinterpret_cast<T *>(
                                    element_accessor_new.as_ptr());
                                if (last_element_ptr[ElementPerChunk - 1] <
                                    value) {
                                    result = true;
                                    element_accessor_new.check();
                                    return true;
                                }
                                if constexpr (Ordered) {
                                    std::swap(
                                        last_element_ptr[ElementPerChunk - 1],
                                        element_ptr[0]);
                                }
                            }
                        }
                    }
                    // chunk_acc = ref.chunk(1).template
                    // access<true>(scope_head);
                    if (!element_accessor_new.async_fetch(order->ref.chunk(1),
                                                          scope)) {
                        status = CHUNK_SIXTH;
                        return false;
                    }
                CHUNK_SIXTH:
                    // swap the first trunk
                    element_0_ptr =
                        reinterpret_cast<T *>(element_accessor_new.as_ptr());
                    if (head->data[MaxInlineCapacity - 1] < value) {
                        result = true;
                        element_accessor_new.check();
                        return true;
                    }
                    if constexpr (Ordered) {
                        std::swap(head->data[MaxInlineCapacity - 1],
                                  element_0_ptr[0]);
                        int i = MaxInlineCapacity - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                } else {
                    // need reallocation
                    new_chunk_count = chunk_idx * 2;
                    new_obj_size = new_chunk_count * ChunkSizeInByte;
                    new_cap = MaxInlineCapacity +
                              (new_chunk_count - 1) * ElementPerChunk;
                    head->capacity = new_cap;
                    head->size++;
                    order->ref.reallocate_chunked(new_obj_size, scope);
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = ELSE_CHUNK_SECOND;
                        return false;
                    }
                    // swap the final trunk
                ELSE_CHUNK_SECOND:
                    // auto chunk_acc =
                    //     ref.chunk(chunk_idx).template
                    //     access<true>(scope_head);
                    element_ptr_first = reinterpret_cast<std::byte *>(
                                            element_accessor.as_ptr()) +
                                        offset;
                    *reinterpret_cast<T *>(element_ptr_first) = value;
                    element_t_ptr = reinterpret_cast<T *>(element_ptr_first);
                    // swap all trunk  except the first trunk
                    if (chunk_idx > 1) {
                        if (!element_accessor_new.async_fetch(
                                order->ref.chunk(chunk_idx - 1), scope)) {
                            status = ELSE_CHUNK_THIRD;
                            return false;
                        }
                    ELSE_CHUNK_THIRD:
                        // auto last_chunk_acc =
                        //     ref.chunk(chunk_idx - 1)
                        //         .template access<true>(scope_chunk);
                        last_element_ptr = reinterpret_cast<T *>(
                            element_accessor_new.as_ptr());
                        if (last_element_ptr[ElementPerChunk - 1] < value) {
                            result = true;
                            element_accessor_new.check();
                            return true;
                        }
                        if constexpr (Ordered) {
                            std::swap(last_element_ptr[ElementPerChunk - 1],
                                      *element_t_ptr);
                        }
                        for (chunk_idx_temp = chunk_idx - 1; chunk_idx_temp > 0;
                             chunk_idx_temp--) {
                            if (!element_accessor.async_fetch(
                                    order->ref.chunk(chunk_idx_temp), scope)) {
                                status = ELSE_CHUNK_FOURTH;
                                return false;
                            }
                        ELSE_CHUNK_FOURTH:
                            // chunk_acc = ref.chunk(chunk_idx_temp)
                            //                 .template
                            //                 access<true>(scope_chunk);
                            element_ptr = reinterpret_cast<T *>(
                                element_accessor.as_ptr());
                            if constexpr (Ordered) {
                                i = ElementPerChunk - 1;
                                while (i > 0) {
                                    if (element_ptr[i - 1] < value) {
                                        result = true;
                                        element_accessor.check();
                                        return true;
                                    }
                                    std::swap(element_ptr[i],
                                              element_ptr[i - 1]);
                                    --i;
                                }
                            }
                            if (chunk_idx_temp != 1) {
                                if (!element_accessor_new.async_fetch(
                                        order->ref.chunk(chunk_idx_temp - 1),
                                        scope)) {
                                    status = ELSE_CHUNK_FIFTH;
                                    return false;
                                }
                            ELSE_CHUNK_FIFTH:
                                // last_chunk_acc =
                                //     ref.chunk(chunk_idx_temp - 1)
                                //         .template access<true>(scope_chunk);
                                last_element_ptr = reinterpret_cast<T *>(
                                    element_accessor_new.as_ptr());
                                if (last_element_ptr[ElementPerChunk - 1] <
                                    value) {
                                    result = true;
                                    element_accessor_new.check();
                                    return true;
                                }
                                if constexpr (Ordered) {
                                    std::swap(
                                        last_element_ptr[ElementPerChunk - 1],
                                        element_ptr[0]);
                                }
                            }
                        }
                    }
                    if (!element_accessor_new.async_fetch(order->ref.chunk(1),
                                                          scope)) {
                        status = ELSE_CHUNK_SIXTH;
                        return false;
                    }
                    // swap the first trunk
                ELSE_CHUNK_SIXTH:
                    // chunk_acc = ref.chunk(1).template
                    // access<true>(scope_head);
                    element_0_ptr =
                        reinterpret_cast<T *>(element_accessor_new.as_ptr());
                    if (head->data[MaxInlineCapacity - 1] < value) {
                        result = true;
                        element_accessor_new.check();
                        return true;
                    }
                    if constexpr (Ordered) {
                        std::swap(head->data[MaxInlineCapacity - 1],
                                  element_0_ptr[0]);
                        int i = MaxInlineCapacity - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                }
            }
            result = true;
            return true;
        }
    };

    struct DefaultInsertCont {
        void operator()(bool result) {}
        void operator()(bool result, const DereferenceScope &scope) {}
    };

    template <bool Mut = true, typename Cont = DefaultInsertCont>
    struct InsertContext : public async::GenericContext {
        LiteAccessor<void, Mut> head_accessor;
        LiteAccessor<void, Mut> head_accessor_new;
        LiteAccessor<void, Mut> element_accessor;
        LiteAccessor<void, Mut> element_accessor_new;

        FarSmallVector *order;
        T value;
        Head *head;
        size_t old_size, old_cap, old_obj_size, inline_cap, new_obj_size,
            new_cap, new_chunk_count;
        size_t idx, chunk_idx, offset;
        T *chunk;
        T *tbase, *tpoint;
        std::byte *element_ptr_first;
        T *element_t_ptr;
        T *last_element_ptr;
        T *element_0_ptr;
        T *element_ptr;
        size_t chunk_idx_temp;
        int i;
        Cont continuation;

        virtual bool fetched() const override {
            switch (status) {
            case NO_CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case NO_CHUNK_SECOND:
                return cache::at_local(head_accessor_new);
            case NO_CHUNK_THIRD:
                return cache::at_local(head_accessor_new);
            case NO_CHUNK_FOURTH:
                return cache::at_local(element_accessor);
            case CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case CHUNK_SECOND:
                return cache::at_local(element_accessor);
            case CHUNK_THIRD:
                return cache::at_local(element_accessor_new);
            case CHUNK_FOURTH:
                return cache::at_local(element_accessor);
            case CHUNK_FIFTH:
                return cache::at_local(element_accessor_new);
            case CHUNK_SIXTH:
                return cache::at_local(element_accessor_new);
            case ELSE_CHUNK_SECOND:
                return cache::at_local(element_accessor);
            case ELSE_CHUNK_THIRD:
                return cache::at_local(element_accessor_new);
            case ELSE_CHUNK_FOURTH:
                return cache::at_local(element_accessor);
            case ELSE_CHUNK_FIFTH:
                return cache::at_local(element_accessor_new);
            case ELSE_CHUNK_SIXTH:
                return cache::at_local(element_accessor_new);
            }
            return true;
        }
        virtual void pin() const override {
            head_accessor.pin();
            head_accessor_new.pin();
            element_accessor.pin();
            element_accessor_new.pin();
        }
        virtual void unpin() const override {
            head_accessor.unpin();
            head_accessor_new.unpin();
            element_accessor.unpin();
            element_accessor_new.unpin();
        }

        virtual void destruct() override {
            std::destroy_at(&head_accessor);
            std::destroy_at(&head_accessor_new);
            std::destroy_at(&element_accessor);
            std::destroy_at(&element_accessor_new);
        }

        enum {
            INIT,
            NO_CHUNK_FIRST,
            NO_CHUNK_SECOND,
            NO_CHUNK_THIRD,
            NO_CHUNK_FOURTH,
            CHUNK_FIRST,
            CHUNK_SECOND,
            CHUNK_THIRD,
            CHUNK_FOURTH,
            CHUNK_FIFTH,
            CHUNK_SIXTH,
            ELSE_CHUNK_SECOND,
            ELSE_CHUNK_THIRD,
            ELSE_CHUNK_FOURTH,
            ELSE_CHUNK_FIFTH,
            ELSE_CHUNK_SIXTH,
        } status = INIT;

        InsertContext(FarSmallVector *ordered_vector, T value, Cont &&cont = {})
            : order(ordered_vector),
              value(value),
              continuation(std::move(cont)) {
            status = INIT;
        }

        virtual bool run(DereferenceScope &scope) override {
            switch (status) {
            case NO_CHUNK_FIRST:
                goto NO_CHUNK_FIRST;
            case NO_CHUNK_SECOND:
                goto NO_CHUNK_SECOND;
            case NO_CHUNK_THIRD:
                goto NO_CHUNK_THIRD;
            case NO_CHUNK_FOURTH:
                goto NO_CHUNK_FOURTH;
            case CHUNK_FIRST:
                goto CHUNK_FIRST;
            case CHUNK_SECOND:
                goto CHUNK_SECOND;
            case CHUNK_THIRD:
                goto CHUNK_THIRD;
            case CHUNK_FOURTH:
                goto CHUNK_FOURTH;
            case CHUNK_FIFTH:
                goto CHUNK_FIFTH;
            case CHUNK_SIXTH:
                goto CHUNK_SIXTH;
            case ELSE_CHUNK_SECOND:
                goto ELSE_CHUNK_SECOND;
            case ELSE_CHUNK_THIRD:
                goto ELSE_CHUNK_THIRD;
            case ELSE_CHUNK_FOURTH:
                goto ELSE_CHUNK_FOURTH;
            case ELSE_CHUNK_FIFTH:
                goto ELSE_CHUNK_FIFTH;
            case ELSE_CHUNK_SIXTH:
                goto ELSE_CHUNK_SIXTH;
            }
            // if not chunk
            if (!order->ref.is_chunked()) [[likely]] {
                // inline
                if (!head_accessor.async_fetch(*order->ref.as_inlined(),
                                               scope)) {
                    status = NO_CHUNK_FIRST;
                    return false;
                }
            NO_CHUNK_FIRST:
                head = static_cast<Head *>(head_accessor.as_ptr());
                if (head->size < head->capacity) {
                    // no reallocation
                    head->data[head->size] = value;
                    head->size++;
                    if constexpr (Ordered) {
                        int i = head->size - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                } else {
                    // need reallocation
                    old_size = head->size;
                    old_cap = head->capacity;
                    old_obj_size = sizeof(Head) + sizeof(T) * old_cap;
                    inline_cap = inline_capacity(old_cap + 1);
                    if (inline_cap <= MaxInlineCapacity) {
                        // inline after reallocation
                        new_obj_size = inline_obj_size(old_cap + 1);
                        order->ref.reallocate(old_obj_size, new_obj_size,
                                              scope);
                        assert(!order->ref.is_chunked());
                        if (!head_accessor_new.async_fetch(
                                *order->ref.as_inlined(), scope)) {
                            status = NO_CHUNK_SECOND;
                            return false;
                        }
                    // acc = ref.as_inlined()->template access<true>(scope);
                    NO_CHUNK_SECOND:
                        head = static_cast<Head *>(head_accessor_new.as_ptr());
                        head->data[head->size] = value;
                        head->size++;
                        head->capacity = inline_cap;
                        if constexpr (Ordered) {
                            int i = head->size - 1;
                            while (i > 0 && head->data[i - 1] > value) {
                                std::swap(head->data[i], head->data[i - 1]);
                                --i;
                            }
                        }
                    } else {
                        // chunked after reallocation
                        new_obj_size = ChunkSizeInByte * 2;
                        new_cap =
                            MaxInlineCapacity + ChunkSizeInByte / sizeof(T);
                        head->size++;
                        head->capacity = new_cap;
                        order->ref.reallocate(old_obj_size, new_obj_size,
                                              scope);
                        assert(order->ref.is_chunked());
                        if (!head_accessor_new.async_fetch(order->ref.chunk(0),
                                                           scope)) {
                            status = NO_CHUNK_THIRD;
                            return false;
                        }
                    NO_CHUNK_THIRD:
                        // acc = ref.chunk(0).template access<true>(scope);
                        head = static_cast<Head *>(head_accessor_new.as_ptr());
                        if (!element_accessor.async_fetch(order->ref.chunk(1),
                                                          scope)) {
                            status = NO_CHUNK_FOURTH;
                            return false;
                        }
                    NO_CHUNK_FOURTH:
                        // auto p = ref.chunk(1).template
                        // access<true>(scope_head);
                        chunk =
                            reinterpret_cast<T *>(element_accessor.as_ptr());
                        chunk[0] = value;
                        if (head->data[MaxInlineCapacity - 1] > value) {
                            if constexpr (Ordered) {
                                std::swap(head->data[MaxInlineCapacity - 1],
                                          chunk[0]);
                                int i = MaxInlineCapacity - 1;
                                while (i > 0 && head->data[i - 1] > value) {
                                    std::swap(head->data[i], head->data[i - 1]);
                                    --i;
                                }
                            }
                        }
                    }
                }
            } else {
                // chunked
                if (!head_accessor.async_fetch(order->ref.chunk(0), scope)) {
                    status = CHUNK_FIRST;
                    return false;
                }
            CHUNK_FIRST:
                // auto head_acc = ref.chunk(0).template access<true>(scope);
                head = reinterpret_cast<Head *>(head_accessor.as_ptr());
                idx = head->size;
                if (head->size < MaxInlineCapacity) {
                    // no reallocation
                    head->data[head->size] = value;
                    head->size++;
                    if constexpr (Ordered) {
                        int i = head->size - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                    head_accessor.check();
                    continuation(true, scope);
                    return true;
                }
                chunk_idx = get_chunk(idx);
                offset = get_offset_in_byte_in_chunk(idx);
                if (head->size < head->capacity) {
                    // no reallocation
                    head->size++;
                    // auto chunk_acc =
                    //     ref.chunk(chunk_idx).template
                    //     access<true>(scope_head);
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = CHUNK_SECOND;
                        return false;
                    }
                CHUNK_SECOND:
                    // swap the final trunk
                    tbase = reinterpret_cast<T *>(element_accessor.as_ptr());
                    element_ptr_first = reinterpret_cast<std::byte *>(
                                            element_accessor.as_ptr()) +
                                        offset;
                    *reinterpret_cast<T *>(element_ptr_first) = value;
                    if constexpr (Ordered) {
                        tpoint = reinterpret_cast<T *>(element_ptr_first);
                        while (tpoint > tbase) {
                            if (*(tpoint - 1) < value) {
                                element_accessor.check();
                                return true;
                            }
                            std::swap(*(tpoint - 1), *tpoint);
                            tpoint--;
                        }
                    }
                    // swap all trunk  except the first trunk
                    if (chunk_idx > 1) {
                        // auto last_chunk_acc =
                        //     ref.chunk(chunk_idx - 1).template
                        //     access<true>(scope_chunk);
                        if (!element_accessor_new.async_fetch(
                                order->ref.chunk(chunk_idx - 1), scope)) {
                            status = CHUNK_THIRD;
                            return false;
                        }
                    CHUNK_THIRD:
                        last_element_ptr = reinterpret_cast<T *>(
                            element_accessor_new.as_ptr());
                        if (last_element_ptr[ElementPerChunk - 1] < value) {
                            element_accessor_new.check();
                            continuation(false, scope);
                            return true;
                        }
                        if constexpr (Ordered) {
                            std::swap(last_element_ptr[ElementPerChunk - 1],
                                      *tbase);
                        }
                        for (chunk_idx_temp = chunk_idx - 1; chunk_idx_temp > 0;
                             chunk_idx_temp--) {
                            if (!element_accessor.async_fetch(
                                    order->ref.chunk(chunk_idx_temp), scope)) {
                                status = CHUNK_FOURTH;
                                return false;
                            }
                        }
                    CHUNK_FOURTH:
                        // chunk_acc = ref.chunk(chunk_idx_temp)
                        //                 .template
                        //                 access<true>(scope_chunk);
                        element_ptr =
                            reinterpret_cast<T *>(element_accessor.as_ptr());
                        if constexpr (Ordered) {
                            i = ElementPerChunk - 1;
                            while (i > 0) {
                                if (element_ptr[i - 1] < value) {
                                    element_accessor.check();
                                    continuation(false, scope);
                                    return true;
                                }
                                std::swap(element_ptr[i], element_ptr[i - 1]);
                                --i;
                            }
                        }
                        if (chunk_idx_temp != 1) {
                            // auto last_chunk_acc =
                            //     ref.chunk(chunk_idx_temp - 1)
                            //         .template access<true>(scope_chunk);
                            if (!element_accessor_new.async_fetch(
                                    order->ref.chunk(chunk_idx_temp - 1),
                                    scope)) {
                                status = CHUNK_FIFTH;
                                return false;
                            }
                        CHUNK_FIFTH:
                            last_element_ptr = reinterpret_cast<T *>(
                                element_accessor_new.as_ptr());
                            if (last_element_ptr[ElementPerChunk - 1] < value) {
                                element_accessor_new.check();
                                continuation(true, scope);
                                return true;
                            }
                            if constexpr (Ordered) {
                                std::swap(last_element_ptr[ElementPerChunk - 1],
                                          element_ptr[0]);
                            }
                        }
                    }

                    // chunk_acc = ref.chunk(1).template
                    // access<true>(scope_head);
                    if (!element_accessor_new.async_fetch(order->ref.chunk(1),
                                                          scope)) {
                        status = CHUNK_SIXTH;
                        return false;
                    }
                CHUNK_SIXTH:
                    // swap the first trunk
                    element_0_ptr =
                        reinterpret_cast<T *>(element_accessor_new.as_ptr());
                    if (head->data[MaxInlineCapacity - 1] < value) {
                        element_accessor_new.check();
                        continuation(true, scope);
                        return true;
                    }
                    if constexpr (Ordered) {
                        std::swap(head->data[MaxInlineCapacity - 1],
                                  element_0_ptr[0]);
                        int i = MaxInlineCapacity - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                } else {
                    // need reallocation
                    new_chunk_count = chunk_idx * 2;
                    new_obj_size = new_chunk_count * ChunkSizeInByte;
                    new_cap = MaxInlineCapacity +
                              (new_chunk_count - 1) * ElementPerChunk;
                    head->capacity = new_cap;
                    head->size++;
                    order->ref.reallocate_chunked(new_obj_size, scope);
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = ELSE_CHUNK_SECOND;
                        return false;
                    }
                    // swap the final trunk
                ELSE_CHUNK_SECOND:
                    // auto chunk_acc =
                    //     ref.chunk(chunk_idx).template
                    //     access<true>(scope_head);
                    element_ptr_first = reinterpret_cast<std::byte *>(
                                            element_accessor.as_ptr()) +
                                        offset;
                    *reinterpret_cast<T *>(element_ptr_first) = value;
                    element_t_ptr = reinterpret_cast<T *>(element_ptr_first);
                    // swap all trunk  except the first trunk
                    if (chunk_idx > 1) {
                        if (!element_accessor_new.async_fetch(
                                order->ref.chunk(chunk_idx - 1), scope)) {
                            status = ELSE_CHUNK_THIRD;
                            return false;
                        }
                    ELSE_CHUNK_THIRD:
                        // auto last_chunk_acc =
                        //     ref.chunk(chunk_idx - 1)
                        //         .template access<true>(scope_chunk);
                        last_element_ptr = reinterpret_cast<T *>(
                            element_accessor_new.as_ptr());
                        if (last_element_ptr[ElementPerChunk - 1] < value) {
                            element_accessor_new.check();
                            continuation(true, scope);
                            return true;
                        }
                        if constexpr (Ordered) {
                            std::swap(last_element_ptr[ElementPerChunk - 1],
                                      *element_t_ptr);
                        }
                        for (chunk_idx_temp = chunk_idx - 1; chunk_idx_temp > 0;
                             chunk_idx_temp--) {
                            if (!element_accessor.async_fetch(
                                    order->ref.chunk(chunk_idx_temp), scope)) {
                                status = ELSE_CHUNK_FOURTH;
                                return false;
                            }
                        ELSE_CHUNK_FOURTH:
                            // chunk_acc = ref.chunk(chunk_idx_temp)
                            //                 .template
                            //                 access<true>(scope_chunk);
                            element_ptr = reinterpret_cast<T *>(
                                element_accessor.as_ptr());
                            if constexpr (Ordered) {
                                i = ElementPerChunk - 1;
                                while (i > 0) {
                                    if (element_ptr[i - 1] < value) {
                                        element_accessor.check();
                                        continuation(true, scope);
                                        return true;
                                    }
                                    std::swap(element_ptr[i],
                                              element_ptr[i - 1]);
                                    --i;
                                }
                            }
                            if (chunk_idx_temp != 1) {
                                if (!element_accessor_new.async_fetch(
                                        order->ref.chunk(chunk_idx_temp - 1),
                                        scope)) {
                                    status = ELSE_CHUNK_FIFTH;
                                    return false;
                                }
                            ELSE_CHUNK_FIFTH:
                                // last_chunk_acc =
                                //     ref.chunk(chunk_idx_temp - 1)
                                //         .template access<true>(scope_chunk);
                                last_element_ptr = reinterpret_cast<T *>(
                                    element_accessor_new.as_ptr());
                                if (last_element_ptr[ElementPerChunk - 1] <
                                    value) {
                                    element_accessor_new.check();
                                    continuation(true, scope);
                                    return true;
                                }
                                if constexpr (Ordered) {
                                    std::swap(
                                        last_element_ptr[ElementPerChunk - 1],
                                        element_ptr[0]);
                                }
                            }
                        }
                    }
                    if (!element_accessor_new.async_fetch(order->ref.chunk(1),
                                                          scope)) {
                        status = ELSE_CHUNK_SIXTH;
                        return false;
                    }
                    // swap the first trunk
                ELSE_CHUNK_SIXTH:
                    // chunk_acc = ref.chunk(1).template
                    // access<true>(scope_head);
                    element_0_ptr =
                        reinterpret_cast<T *>(element_accessor_new.as_ptr());
                    if (head->data[MaxInlineCapacity - 1] < value) {
                        element_accessor_new.check();
                        continuation(true, scope);
                        return true;
                    }
                    if constexpr (Ordered) {
                        std::swap(head->data[MaxInlineCapacity - 1],
                                  element_0_ptr[0]);
                        int i = MaxInlineCapacity - 1;
                        while (i > 0 && head->data[i - 1] > value) {
                            std::swap(head->data[i], head->data[i - 1]);
                            --i;
                        }
                    }
                }
            }
            continuation(true, scope);
            return true;
        }
    };

    struct EraseFrame {
        LiteAccessor<void, true> head_accessor;
        LiteAccessor<void, true> element_accessor;
        LiteAccessor<void, true> element_accessor_new;
        FarSmallVector *order;
        Head *head;
        size_t size;
        size_t idx;
        bool flag;
        size_t chunk_idx;
        T *next_element_ptr;
        T *element_ptr;
        T value;

        enum {
            INIT,
            NO_CHUNK_FIRST,
            CHUNK_FIRST,
            CHUNK_SECOND,
            CHUNK_THIRD,
            CHUNK_FOURTH,
        } status = INIT;

        bool fetched() {
            switch (status) {
            case NO_CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case CHUNK_SECOND:
                return cache::at_local(element_accessor);
            case CHUNK_THIRD:
                return cache::at_local(element_accessor);
            case CHUNK_FOURTH:
                return cache::at_local(element_accessor_new);
            }
            return true;
        }
        void pin() {
            head_accessor.pin();
            element_accessor.pin();
            element_accessor_new.pin();
        }
        void unpin() {
            head_accessor.unpin();
            element_accessor.unpin();
            element_accessor_new.unpin();
        }

        bool run(DereferenceScope &scope) {
            switch (status) {
            case NO_CHUNK_FIRST:
                goto NO_CHUNK_FIRST;
            case CHUNK_FIRST:
                goto CHUNK_FIRST;
            case CHUNK_SECOND:
                goto CHUNK_SECOND;
            case CHUNK_THIRD:
                goto CHUNK_THIRD;
            case CHUNK_FOURTH:
                goto CHUNK_FOURTH;
            }
            if (!order->ref.is_chunked()) [[likely]] {
                if (!head_accessor.async_fetch(*order->ref.as_inlined(),
                                               scope)) {
                    status = NO_CHUNK_FIRST;
                    return false;
                }
            NO_CHUNK_FIRST:
                head = static_cast<Head *>(head_accessor.as_ptr());
                size = head->size;
                for (size_t i = 0; i < size; i++) {
                    if (head->data[i] == value) {
                        for (int j = i + 1; j < size; j++) {
                            head->data[i - 1] = head->data[i];
                        }
                        head->size--;
                        head_accessor.check();
                        return true;
                    }
                }
            } else {
                if (!head_accessor.async_fetch(order->ref.chunk(0), scope)) {
                    status = CHUNK_FIRST;
                    return false;
                }
            CHUNK_FIRST:
                head = reinterpret_cast<Head *>(head_accessor.as_ptr());
                size = head->size;
                idx;
                flag = false;
                for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                    if (flag) {
                        head->data[idx - 1] = head->data[idx];
                    } else if (head->data[idx] == value) {
                        flag = true;
                    }
                }
                if (flag) {
                    // auto next_chunk_acc =
                    //     ref.chunk(1).template access<true>(scope);
                    if (!element_accessor.async_fetch(order->ref.chunk(1),
                                                      scope)) {
                        status = CHUNK_SECOND;
                        return false;
                    }
                CHUNK_SECOND:
                    next_element_ptr =
                        reinterpret_cast<T *>(element_accessor.as_ptr());
                    head->data[MaxInlineCapacity - 1] = next_element_ptr[0];
                }
                for (chunk_idx = 1; idx < size; chunk_idx++) {
                    // auto chunk_acc =
                    //     ref.chunk(chunk_idx).template
                    //     access<true>(scope_head);
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = CHUNK_THIRD;
                        return false;
                    }
                CHUNK_THIRD:
                    element_ptr =
                        reinterpret_cast<T *>(element_accessor.as_ptr());
                    for (size_t i = 0; i < ElementPerChunk && idx < size;
                         i++, idx++) {
                        if (flag && i > 0) {
                            element_ptr[i - 1] = element_ptr[i];
                        } else if (element_ptr[i] == value) {
                            flag = true;
                        }
                    }
                    if (flag && idx < size) {
                        if (!element_accessor_new.async_fetch(
                                order->ref.chunk(chunk_idx + 1), scope)) {
                            status = CHUNK_FOURTH;
                            return false;
                        }
                    CHUNK_FOURTH:
                        // auto next_chunk_acc =
                        //     ref.chunk(chunk_idx + 1)
                        //         .template access<true>(scope_chunk);
                        next_element_ptr = reinterpret_cast<T *>(
                            element_accessor_new.as_ptr());
                        element_ptr[ElementPerChunk - 1] = next_element_ptr[0];
                    }
                }
                if (flag) {
                    head->size--;
                }
            }
            return true;
        }
    };

    bool insert_f(T value, DereferenceScope &scope, int j) {
        InsertFrame frame{.order = this, .value = value};
        while (!frame.run(scope)) {
            do {
                cache::check_cq();
            } while (!frame.fetched());
        }
        return frame.result;
    }

    void erase_f(T value, DereferenceScope &scope) {
        EraseFrame frame{.order = this, .value = value};
        while (!frame.run(scope)) {
            do {
                cache::check_cq();
            } while (!frame.fetched());
        }
    }

    struct GetFrame {
        LiteAccessor<void> head_accessor;
        LiteAccessor<void> element_accessor;
        std::vector<T> rangeElements;
        FarSmallVector *order;
        T start;
        T stop;
        const Head *head;
        size_t size;
        size_t idx;
        size_t chunk_idx;
        const T *element_ptr;
        enum {
            INIT,
            NO_CHUNK,
            CHUNK_FIRST,
            CHUNK_SECOND,
        } status = INIT;
        bool fetched() {
            switch (status) {
            case NO_CHUNK:
                return cache::at_local(head_accessor);
            case CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case CHUNK_SECOND:
                return cache::at_local(element_accessor);
            }
            return true;
        }
        void pin() {
            head_accessor.pin();
            element_accessor.pin();
        }
        void unpin() {
            head_accessor.unpin();
            element_accessor.unpin();
        }
        bool run(DereferenceScope &scope) {
            switch (status) {
            case NO_CHUNK:
                goto NO_CHUNK;
            case CHUNK_FIRST:
                goto CHUNK_FIRST;
            case CHUNK_SECOND:
                goto CHUNK_SECOND;
            }
            if (!order->ref.is_chunked()) [[likely]] {
                if (!head_accessor.async_fetch(*order->ref.as_inlined(),
                                               scope)) {
                    status = NO_CHUNK;
                    return false;
                }
            NO_CHUNK:
                head = static_cast<const Head *>(head_accessor.as_ptr());
                size = head->size;
                for (size_t i = 0; i < size; i++) {
                    if (head->data[i] > start) {
                        if (head->data[i] > stop) {
                            head_accessor.check();
                            return true;
                        }
                        rangeElements.push_back(head->data[i]);
                    }
                }
            } else {
                if (!head_accessor.async_fetch(order->ref.chunk(0), scope)) {
                    status = CHUNK_FIRST;
                    return false;
                }
            CHUNK_FIRST:
                head = reinterpret_cast<const Head *>(head_accessor.as_ptr());
                size = head->size;
                for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                    if (head->data[idx] > start) {
                        if (head->data[idx] > stop) {
                            head_accessor.check();
                            return true;
                        }
                        rangeElements.push_back(head->data[idx]);
                    }
                }
                for (chunk_idx = 1; idx < size; chunk_idx++) {
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = CHUNK_SECOND;
                        return false;
                    }
                CHUNK_SECOND:
                    element_ptr =
                        reinterpret_cast<const T *>(element_accessor.as_ptr());
                    for (size_t i = 0; i < ElementPerChunk && idx < size;
                         i++, idx++) {
                        if (element_ptr[i] > start) {
                            if (element_ptr[i] > stop) {
                                element_accessor.check();
                                return true;
                            }
                            rangeElements.push_back(element_ptr[i]);
                        }
                    }
                }
            }
            return true;
        }
    };

    std::vector<T> get_f(T start, T stop, DereferenceScope &scope) {
        GetFrame frame{.order = this, .start = start, .stop = stop};
        while (!frame.run(scope)) {
            do {
                cache::check_cq();
            } while (!frame.fetched());
        }
        return frame.rangeElements;
    }

    template <bool Mut>
    struct DefaultGetCont {
        void operator()(std::vector<T> &&result) {}
    };

    template <bool Mut = false, typename Cont = DefaultGetCont<Mut>>
    struct GetContext : public async::GenericContext {
        LiteAccessor<void, Mut> head_accessor;
        LiteAccessor<void, Mut> element_accessor;
        std::vector<T> rangeElements;
        FarSmallVector *order;
        T start;
        T stop;
        const Head *head;
        size_t size;
        size_t idx;
        size_t chunk_idx;
        const T *element_ptr;
        Cont continuation;

        enum {
            INIT,
            NO_CHUNK,
            CHUNK_FIRST,
            CHUNK_SECOND,
        } status = INIT;

        virtual bool fetched() const override {
            switch (status) {
            case NO_CHUNK:
                return cache::at_local(head_accessor);
            case CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case CHUNK_SECOND:
                return cache::at_local(element_accessor);
            }
            return true;
        }

        virtual void pin() const override {
            head_accessor.pin();
            element_accessor.pin();
        }

        virtual void unpin() const override {
            head_accessor.unpin();
            element_accessor.unpin();
        }

        virtual void destruct() override {
            std::destroy_at(&head_accessor);
            std::destroy_at(&element_accessor);
        }

        GetContext(FarSmallVector *ordered_vector, T start, T stop,
                   Cont &&cont = {})
            : order(ordered_vector),
              start(start),
              stop(stop),
              continuation(std::move(cont)) {
            status = INIT;
        }

        virtual bool run(DereferenceScope &scope) override {
            switch (status) {
            case NO_CHUNK:
                goto NO_CHUNK;
            case CHUNK_FIRST:
                goto CHUNK_FIRST;
            case CHUNK_SECOND:
                goto CHUNK_SECOND;
            }
            if (!order->ref.is_chunked()) [[likely]] {
                if (!head_accessor.async_fetch(*order->ref.as_inlined(),
                                               scope)) {
                    status = NO_CHUNK;
                    return false;
                }
            NO_CHUNK:
                head = static_cast<const Head *>(head_accessor.as_ptr());
                size = head->size;
                for (size_t i = 0; i < size; i++) {
                    if (head->data[i] > start) {
                        if (head->data[i] > stop) {
                            head_accessor.check();
                            continuation(std::move(rangeElements), scope);
                            return true;
                        }
                        rangeElements.push_back(head->data[i]);
                    }
                }
            } else {
                if (!head_accessor.async_fetch(order->ref.chunk(0), scope)) {
                    status = CHUNK_FIRST;
                    return false;
                }
            CHUNK_FIRST:
                head = reinterpret_cast<const Head *>(head_accessor.as_ptr());
                size = head->size;
                for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                    if (head->data[idx] > start) {
                        if (head->data[idx] > stop) {
                            head_accessor.check();
                            continuation(std::move(rangeElements), scope);
                            return true;
                        }
                        rangeElements.push_back(head->data[idx]);
                    }
                }
                for (chunk_idx = 1; idx < size; chunk_idx++) {
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = CHUNK_SECOND;
                        return false;
                    }
                CHUNK_SECOND:
                    element_ptr =
                        reinterpret_cast<const T *>(element_accessor.as_ptr());
                    for (size_t i = 0; i < ElementPerChunk && idx < size;
                         i++, idx++) {
                        if (element_ptr[i] > start) {
                            if (element_ptr[i] > stop) {
                                element_accessor.check();
                                continuation(std::move(rangeElements), scope);
                                return true;
                            }
                            rangeElements.push_back(element_ptr[i]);
                        }
                    }
                }
            }
            continuation(std::move(rangeElements), scope);
            return true;
        }
    };

    struct GetAllFrame {
        LiteAccessor<void> head_accessor;
        LiteAccessor<void> element_accessor;
        std::vector<T> rangeElements;
        FarSmallVector *order;
        const Head *head;
        size_t size;
        size_t idx;
        size_t chunk_idx;
        const T *element_ptr;
        enum {
            INIT,
            NO_CHUNK,
            CHUNK_FIRST,
            CHUNK_SECOND,
        } status = INIT;
        bool fetched() {
            switch (status) {
            case NO_CHUNK:
                return cache::at_local(head_accessor);
            case CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case CHUNK_SECOND:
                return cache::at_local(element_accessor);
            }
            return true;
        }
        void pin() {
            head_accessor.pin();
            element_accessor.pin();
        }
        void unpin() {
            head_accessor.unpin();
            element_accessor.unpin();
        }
        bool run(DereferenceScope &scope) {
            switch (status) {
            case NO_CHUNK:
                goto NO_CHUNK;
            case CHUNK_FIRST:
                goto CHUNK_FIRST;
            case CHUNK_SECOND:
                goto CHUNK_SECOND;
            }
            if (!order->ref.is_chunked()) [[likely]] {
                if (!head_accessor.async_fetch(*order->ref.as_inlined(),
                                               scope)) {
                    status = NO_CHUNK;
                    return false;
                }
            NO_CHUNK:
                head = static_cast<const Head *>(head_accessor.as_ptr());
                size = head->size;
                for (size_t i = 0; i < size; i++) {
                    rangeElements.push_back(head->data[i]);
                }
            } else {
                if (!head_accessor.async_fetch(order->ref.chunk(0), scope)) {
                    status = CHUNK_FIRST;
                    return false;
                }
            CHUNK_FIRST:
                head = reinterpret_cast<const Head *>(head_accessor.as_ptr());
                size = head->size;
                for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                    rangeElements.push_back(head->data[idx]);
                }
                for (chunk_idx = 1; idx < size; chunk_idx++) {
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = CHUNK_SECOND;
                        return false;
                    }
                CHUNK_SECOND:
                    element_ptr =
                        reinterpret_cast<const T *>(element_accessor.as_ptr());
                    for (size_t i = 0; i < ElementPerChunk && idx < size;
                         i++, idx++) {
                        rangeElements.push_back(element_ptr[i]);
                    }
                }
            }
            return true;
        }
    };

    std::vector<T> get_all_f(T start, T stop, DereferenceScope &scope) {
        GetAllFrame frame{.order = this};
        while (!frame.run(scope)) {
            do {
                cache::check_cq();
            } while (!frame.fetched());
        }
        return frame.rangeElements;
    }

    template <bool Mut = false, typename Cont = DefaultGetCont<Mut>>
    struct GetAllContext : public async::GenericContext {
        LiteAccessor<void> head_accessor;
        LiteAccessor<void> element_accessor;
        std::vector<T> rangeElements;
        FarSmallVector *order;
        const Head *head;
        size_t size;
        size_t idx;
        size_t chunk_idx;
        const T *element_ptr;
        Cont continuation;

        enum {
            INIT,
            NO_CHUNK,
            CHUNK_FIRST,
            CHUNK_SECOND,
        } status = INIT;

        virtual bool fetched() const override {
            switch (status) {
            case NO_CHUNK:
                return cache::at_local(head_accessor);
            case CHUNK_FIRST:
                return cache::at_local(head_accessor);
            case CHUNK_SECOND:
                return cache::at_local(element_accessor);
            }
            return true;
        }

        virtual void pin() const override {
            head_accessor.pin();
            element_accessor.pin();
        }

        virtual void unpin() const override {
            head_accessor.unpin();
            element_accessor.unpin();
        }

        virtual void destruct() override {
            std::destroy_at(&head_accessor);
            std::destroy_at(&element_accessor);
        }

        GetAllContext(FarSmallVector *ordered_vector, Cont &&cont = {})
            : order(ordered_vector), continuation(cont) {
            status = INIT;
        }

        virtual bool run(DereferenceScope &scope) override {
            switch (status) {
            case NO_CHUNK:
                goto NO_CHUNK;
            case CHUNK_FIRST:
                goto CHUNK_FIRST;
            case CHUNK_SECOND:
                goto CHUNK_SECOND;
            }
            if (!order->ref.is_chunked()) [[likely]] {
                if (!head_accessor.async_fetch(*order->ref.as_inlined(),
                                               scope)) {
                    status = NO_CHUNK;
                    return false;
                }
            NO_CHUNK:
                head = static_cast<const Head *>(head_accessor.as_ptr());
                size = head->size;
                for (size_t i = 0; i < size; i++) {
                    rangeElements.push_back(head->data[i]);
                }
            } else {
                if (!head_accessor.async_fetch(order->ref.chunk(0), scope)) {
                    status = CHUNK_FIRST;
                    return false;
                }
            CHUNK_FIRST:
                head = reinterpret_cast<const Head *>(head_accessor.as_ptr());
                size = head->size;
                for (idx = 0; idx < std::min(MaxInlineCapacity, size); idx++) {
                    rangeElements.push_back(head->data[idx]);
                }
                for (chunk_idx = 1; idx < size; chunk_idx++) {
                    if (!element_accessor.async_fetch(
                            order->ref.chunk(chunk_idx), scope)) {
                        status = CHUNK_SECOND;
                        return false;
                    }
                CHUNK_SECOND:
                    element_ptr =
                        reinterpret_cast<const T *>(element_accessor.as_ptr());
                    for (size_t i = 0; i < ElementPerChunk && idx < size;
                         i++, idx++) {
                        rangeElements.push_back(element_ptr[i]);
                    }
                }
            }
            continuation(std::move(rangeElements));
            return true;
        }
    };
};
template <typename T>
using OrderedVector = FarSmallVector<T, true>;
}  // namespace Beehive