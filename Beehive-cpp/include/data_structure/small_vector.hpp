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

#include "dyn_sized_object.hpp"

namespace Beehive {

template <typename T, size_t ChunkSizeInByte = 4096 - allocator::BlockHeadSize>
class SmallVector {
private:
    static_assert(std::is_trivial_v<T>);

    DynObjUniqueRef<ChunkSizeInByte> ref;

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
    SmallVector(DereferenceScope &scope) {
        ref.allocate(
            sizeof(Head) + InitialCapacity * sizeof(T),
            [](void *p) {
                Head *head = static_cast<Head *>(p);
                head->size = 0;
                head->capacity = InitialCapacity;
            },
            scope);
    }

    void push_back(T value, DereferenceScope &scope) {
        if (!ref.is_chunked()) [[likely]] {
            // inline
            LiteAccessor<void, true> acc =
                ref.as_inlined()->template access<true>(scope);
            Head *head = static_cast<Head *>(acc.as_ptr());
            if (head->size < head->capacity) {
                // no reallocation
                head->data[head->size] = value;
                head->size++;
            } else {
                // need reallocation
                size_t old_size = head->size;
                size_t old_cap = head->capacity;
                size_t old_obj_size = sizeof(Head) + sizeof(T) * old_cap;
                size_t inline_cap = inline_capacity(old_cap + 1);
                if (inline_cap <= MaxInlineCapacity) {
                    // inline after reallocation
                    size_t new_obj_size = inline_obj_size(old_cap + 1);
                    ref.reallocate(old_obj_size, new_obj_size, scope);
                    assert(!ref.is_chunked());
                    acc = ref.as_inlined()->template access<true>(scope);
                    head = static_cast<Head *>(acc.as_ptr());
                    head->data[head->size] = value;
                    head->size++;
                    head->capacity = inline_cap;
                } else {
                    // chunked after reallocation
                    size_t new_obj_size = ChunkSizeInByte * 2;
                    size_t new_cap =
                        MaxInlineCapacity + ChunkSizeInByte / sizeof(T);
                    head->size++;
                    head->capacity = new_cap;
                    ref.reallocate(old_obj_size, new_obj_size, scope);
                    assert(ref.is_chunked());
                    auto p = ref.chunk(1).template access<true>(scope);
                    T *chunk = reinterpret_cast<T *>(p.as_ptr());
                    chunk[0] = value;
                }
            }
        } else {
            // chunked
            auto head_acc = ref.chunk(0).template access<true>(scope);
            Head *head = reinterpret_cast<Head *>(head_acc.as_ptr());
            size_t idx = head->size;
            size_t chunk_idx = get_chunk(idx);
            size_t offset = get_offset_in_byte_in_chunk(idx);
            if (head->size < head->capacity) {
                // no reallocation
                head->size++;
                auto chunk_acc =
                    ref.chunk(chunk_idx).template access<true>(scope);
                auto element_ptr =
                    reinterpret_cast<std::byte *>(chunk_acc.as_ptr()) + offset;
                *reinterpret_cast<T *>(element_ptr) = value;
            } else {
                // need reallocation
                size_t new_chunk_count = chunk_idx * 2;
                size_t new_obj_size = new_chunk_count * ChunkSizeInByte;
                size_t new_cap =
                    MaxInlineCapacity + (new_chunk_count - 1) * ElementPerChunk;
                head->capacity = new_cap;
                head->size++;
                ref.reallocate_chunked(new_obj_size, scope);
                auto chunk_acc =
                    ref.chunk(chunk_idx).template access<true>(scope);
                auto element_ptr =
                    reinterpret_cast<std::byte *>(chunk_acc.as_ptr()) + offset;
                *reinterpret_cast<T *>(element_ptr) = value;
            }
        }
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
};

}  // namespace Beehive