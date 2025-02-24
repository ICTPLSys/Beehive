#pragma once
#include "data_structure/far_vector.hpp"

namespace Beehive {
template <VecElementType T, size_t GroupSize>
class FarVector<T, GroupSize>::Pointer {
public:
    using Element = T;

private:
    FarVector<T, GroupSize> &parent;
    UniquePtr *ptr;
    size_t idx;
    template <bool fast = false>
    void sync() {
        if (idx > GroupSize) {
            if constexpr (fast) {
                ptr++;
                idx = 0;
            } else {
                ptr += idx / GroupSize;
                idx %= GroupSize;
            }
        }
    }

public:
    Pointer(FarVector<T, GroupSize> &parent, UniquePtr *ptr, size_t idx = 0)
        : parent(parent), ptr(ptr), idx(idx) {
        sync();
    }
    Pointer(const Pointer &pointer, size_t n = 0)
        : parent(pointer.parent), ptr(pointer.ptr), idx(pointer.idx + n) {
        sync();
    }
    Pointer operator+(size_t n) const { return Pointer(*this, n); }
    Pointer &operator+=(size_t n) {
        idx += n;
        sync();
        return *this;
    }
    Pointer &operator++() {
        idx++;
        sync<true>();
        return *this;
    }

    template <bool Mut>
    Iterator<Mut> get_iterator() {
        return Iterator<Mut>(
            ptr, idx, parent.groups_.data(),
            parent.groups_.data() + (parent.size_ + GroupSize - 1) / GroupSize);
    }

    template <bool Mut>
    LiteIterator<Mut> get_iterator(DereferenceScope &scope) {
        return LiteIterator<Mut>(
            ptr, idx, parent.groups_.data(),
            parent.groups_.data() + (parent.size_ + GroupSize - 1) / GroupSize,
            scope);
    }
};
}  // namespace Beehive