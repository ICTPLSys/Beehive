#pragma once

#include <array>
#include <vector>

#include "cache/accessor.hpp"
#include "cache/cache.hpp"
#include "cache/scope.hpp"

// #define USE_YIELD

namespace FarLib {

template <typename T,
          size_t GroupSize = (4096 - allocator::BlockHeadSize) / sizeof(T)>
class DenseVector {
private:
    using Group = std::array<T, GroupSize>;
    using UniquePtr = UniqueFarPtr<Group>;

    std::vector<UniquePtr> groups_;
    size_t size_;

public:
    DenseVector() : size_(0) {}

    void clear() {
        groups_.clear();
        size_ = 0;
    }

    void resize(size_t n, T value, DereferenceScope &scope) {
        size_ = n;
        size_t n_groups = (n + GroupSize - 1) / GroupSize;
        groups_.resize(n_groups);
        for (size_t i = 0; i < n_groups; i++) {
            auto acc = groups_[i].template allocate_lite(scope);
            for (size_t j = 0; j < std::min(n - j * n_groups, GroupSize); j++) {
                (*acc)[j] = value;
            }
        }
    }

    void push_back(const T &value, DereferenceScope &scope) {
        size_t inner_idx = size_ % GroupSize;
        if (inner_idx == 0) {
            UniquePtr &group = groups_.emplace_back();
            LiteAccessor<Group, true> accessor =
                group.template allocate_lite_uninitialized<true>(scope);
            accessor->at(0) = value;
        } else {
            LiteAccessor<Group, true> group_accessor(groups_[size_ / GroupSize],
                                                     scope);
            group_accessor->at(inner_idx) = value;
        }
        size_++;
    }

    template <typename... Args>
    void emplace_back(DereferenceScope &scope, Args &&...args) {
        size_t inner_idx = size_ % GroupSize;
        if (inner_idx == 0) {
            UniquePtr &group = groups_.emplace_back();
            LiteAccessor<Group, true> accessor =
                group.template allocate_lite_uninitialized<true>(scope);
            T *ptr = &(accessor->at(0));
            new (ptr) T(std::forward<Args>(args)...);
        } else {
            LiteAccessor<Group, true> group_accessor(groups_[size_ / GroupSize],
                                                     scope);
            T *ptr = &(group_accessor->at(inner_idx));
            new (ptr) T(std::forward<Args>(args)...);
        }
        size_++;
    }

    void prefetch(size_t i, DereferenceScope &scope) const {
        Cache::get_default()->prefetch(groups_[i / GroupSize].obj(), scope);
    }

    template <bool Mut = false>
    LiteAccessor<T, Mut> get_lite(size_t i, DereferenceScope &scope) const {
        auto group_accessor =
            LiteAccessor<Group, Mut>(groups_[i / GroupSize], scope);
        return LiteAccessor<T, Mut>(group_accessor,
                                    &(group_accessor->at(i % GroupSize)));
    }

    template <bool Mut = false>
    LiteAccessor<T, Mut> get_lite(size_t i, __DMH__,
                                  DereferenceScope &scope) const {
        auto group_accessor = LiteAccessor<Group, Mut>(groups_[i / GroupSize],
                                                       __on_miss__, scope);
        return LiteAccessor<T, Mut>(group_accessor,
                                    &(group_accessor->at(i % GroupSize)));
    }

    template <bool Mut = false>
    bool async_get_lite(size_t i, LiteAccessor<T, Mut> &accessor,
                        DereferenceScope &scope) const {
        LiteAccessor<Group, Mut> group_accessor;
        bool at_local =
            group_accessor.async_fetch(groups_[i / GroupSize], scope);
        accessor = LiteAccessor<T, Mut>(group_accessor,
                                        &(group_accessor->at(i % GroupSize)));
        return at_local;
    }

    size_t size() const { return size_; }

    template <bool Mut = false, typename Fn>
        requires requires(Fn &&fn, T &value) { fn(value); }
    void for_each(Fn &&fn, DereferenceScope &scope, size_t start = 0,
                  size_t end = std::numeric_limits<size_t>::max()) {
        end = std::min(end, size_);
        size_t i;
        size_t prefetch_idx = 0;
        ON_MISS_BEGIN
            auto cache = Cache::get_default();
            __define_oms__(scope);
            for (prefetch_idx = std::max(i + 1, prefetch_idx);
                 prefetch_idx * GroupSize < end; prefetch_idx++) {
                cache->prefetch(groups_[prefetch_idx].obj(), oms);
                if (cache::check_fetch(__entry__, __ddl__)) return;
            }
        ON_MISS_END
        for (i = start / GroupSize; i * GroupSize < end; i++) {
            auto group =
                LiteAccessor<Group, Mut>(groups_[i], __on_miss__, scope);
            for (size_t j = 0; j < GroupSize; j++) {
                if (i * GroupSize + j < start) continue;
                if (i * GroupSize + j >= end) return;
                fn(group->at(j));
            }
        }
    }
};

}  // namespace FarLib
