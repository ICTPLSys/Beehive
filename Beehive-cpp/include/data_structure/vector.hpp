#pragma once

#include <array>
#include <vector>

#include "async/loop.hpp"
#include "cache/cache.hpp"
#include "cache/scope.hpp"

// #define USE_YIELD

namespace Beehive {

template <typename T>
class Vector {
private:
    std::vector<far_obj_t> elements;

public:
    ~Vector() {
        auto cache = Cache::get_default();
        for (far_obj_t obj : elements) {
            cache->deallocate(obj);
        }
    }

    void push_back(const T &value) {
        auto accessor = alloc_obj<T>(value);
        elements.push_back(accessor.get_obj());
    }

    template <typename... Args>
    void emplace_back(Args &&...args) {
        auto accessor = alloc_obj<T>(std::forward<Args>(args)...);
        elements.push_back(accessor.get_obj());
    }

    Accessor<T> get_mut(size_t i) const { return elements[i]; }

    ConstAccessor<T> get(size_t i) const { return elements[i]; }

    Accessor<T> get_mut(size_t i, cache::DataMissHandler &handler) const {
        return Accessor<T>(elements[i], handler);
    }

    ConstAccessor<T> get(size_t i, cache::DataMissHandler &handler) const {
        return ConstAccessor<T>(elements[i], handler);
    }

    size_t size() const { return elements.size(); }
};

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

    void push_back(const T &value) {
        size_t inner_idx = size_ % GroupSize;
        if (inner_idx == 0) {
            UniquePtr &group = groups_.emplace_back();
            Accessor<Group> accessor = group.allocate_uninitialized();
            accessor->at(0) = value;
        } else {
            Accessor<Group> group_accessor = groups_[size_ / GroupSize];
            group_accessor->at(inner_idx) = value;
        }
        size_++;
    }

    template <typename... Args>
    void emplace_back(Args &&...args) {
        size_t inner_idx = size_ % GroupSize;
        if (inner_idx == 0) {
            UniquePtr &group = groups_.emplace_back();
            Accessor<Group> accessor = group.allocate_uninitialized();
            T *ptr = &(accessor->at(0));
            new (ptr) T(std::forward<Args>(args)...);
        } else {
            Accessor<Group> group_accessor = groups_[size_ / GroupSize];
            T *ptr = &(group_accessor->at(inner_idx));
            new (ptr) T(std::forward<Args>(args)...);
        }
        size_++;
    }

    void prefetch(size_t i) const {
        // TODO
        Cache::get_default()->prefetch(groups_[i / GroupSize].obj());
    }

    void prefetch(size_t i, DereferenceScope &scope) const {
        // TODO
        Cache::get_default()->prefetch(groups_[i / GroupSize].obj(), scope);
    }

    Accessor<T> get_mut(size_t i) const {
        Accessor<Group> group_accessor = groups_[i / GroupSize];
        return Accessor<T>(std::move(group_accessor),
                           &(group_accessor->at(i % GroupSize)));
    }

    ConstAccessor<T> get(size_t i) const {
        ConstAccessor<Group> group_accessor = groups_[i / GroupSize];
        return ConstAccessor<T>(std::move(group_accessor),
                                &(group_accessor->at(i % GroupSize)));
    }

    Accessor<T> get_mut(size_t i, cache::DataMissHandler &handler) const {
        auto group_accessor = Accessor<Group>(groups_[i / GroupSize], handler);
        return Accessor<T>(std::move(group_accessor),
                           &(group_accessor->at(i % GroupSize)));
    }

    ConstAccessor<T> get(size_t i, cache::DataMissHandler &handler) const {
        auto group_accessor =
            ConstAccessor<Group>(groups_[i / GroupSize], handler);
        return ConstAccessor<T>(std::move(group_accessor),
                                &(group_accessor->at(i % GroupSize)));
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

    template <typename Fn>
        requires requires(Fn &&fn, T &value) { fn(value); }
    void for_each_mut(Fn &&fn, size_t start = 0,
                      size_t end = std::numeric_limits<size_t>::max()) {
        end = std::min(end, size_);
        size_t i;
        size_t prefetch_idx = 0;
        ON_MISS_BEGIN
            auto cache = Cache::get_default();
            for (prefetch_idx = std::max(i + 1, prefetch_idx);
                 prefetch_idx * GroupSize < end; prefetch_idx++) {
                cache->prefetch(groups_[prefetch_idx].obj());
                if (cache::check_fetch(__entry__, __ddl__)) return;
            }
        ON_MISS_END
        for (i = start / GroupSize; i * GroupSize < end; i++) {
            auto group = Accessor<Group>(groups_[i], __on_miss__);
            for (size_t j = 0; j < GroupSize; j++) {
                if (i * GroupSize + j < start) continue;
                if (i * GroupSize + j >= end) return;
                fn(group->at(j));
            }
        }
    }

    template <typename Fn>
        requires requires(Fn &&fn, const T &value) { fn(value); }
    void for_each_const(Fn &&fn, size_t start = 0,
                        size_t end = std::numeric_limits<size_t>::max()) const {
        end = std::min(end, size_);
        size_t i;
        size_t prefetch_idx = 0;

        ON_MISS_BEGIN
            auto cache = Cache::get_default();
            for (prefetch_idx = std::max(i + 1, prefetch_idx);
                 prefetch_idx * GroupSize < end; prefetch_idx++) {
                cache->prefetch(groups_[prefetch_idx].obj());
                if (cache::check_fetch(__entry__, __ddl__)) return;
            }
        ON_MISS_END
        for (i = start / GroupSize; i * GroupSize < end; i++) {
            auto group = ConstAccessor<Group>(groups_[i], __on_miss__);
            for (size_t j = 0; j < GroupSize; j++) {
                if (i * GroupSize + j < start) continue;
                if (i * GroupSize + j >= end) return;
                fn(group->at(j));
            }
        }
    }

    template <typename Fn>
        requires requires(Fn &&fn, T &value) { fn(value); }
    void for_each_mut_no_prefetch(Fn &&fn, size_t start, size_t end, __DMH__) {
        end = std::min(end, size_);
        for (size_t i = start / GroupSize; i * GroupSize < end; i++) {
            auto group = Accessor<Group>(groups_[i], __on_miss__);
            for (size_t j = 0; j < GroupSize; j++) {
                if (i * GroupSize + j < start) continue;
                if (i * GroupSize + j >= end) return;
                fn(group->at(j));
            }
        }
    }

    template <typename Fn>
        requires requires(Fn &&fn, const T &value) { fn(value); }
    void for_each_const_no_prefetch(Fn &&fn, size_t start, size_t end,
                                    __DMH__) const {
        end = std::min(end, size_);
        for (size_t i = start / GroupSize; i * GroupSize < end; i++) {
            auto group = ConstAccessor<Group>(groups_[i], __on_miss__);
            for (size_t j = 0; j < GroupSize; j++) {
                if (i * GroupSize + j < start) continue;
                if (i * GroupSize + j >= end) return;
                fn(group->at(j));
            }
        }
    }

    template <typename Fn>
        requires requires(Fn &&fn, T &value) { fn(value); }
    void for_each_mut_no_prefetch(
        Fn &&fn, size_t start = 0,
        size_t end = std::numeric_limits<size_t>::max()) {
        ON_MISS_BEGIN
        ON_MISS_END
        for_each_mut_no_prefetch(std::forward<Fn>(fn), start, end, __on_miss__);
    }

    template <typename Fn>
        requires requires(Fn &&fn, const T &value) { fn(value); }
    void for_each_const_no_prefetch(
        Fn &&fn, size_t start = 0,
        size_t end = std::numeric_limits<size_t>::max()) const {
        ON_MISS_BEGIN
        ON_MISS_END
        for_each_const_no_prefetch(std::forward<Fn>(fn), start, end,
                                   __on_miss__);
    }

    template <typename Fn>
        requires requires(Fn &&fn, T &value) { fn(value); }
    void for_each_mut_run_ahead(
        Fn &&fn, size_t start = 0,
        size_t end = std::numeric_limits<size_t>::max()) {
        end = std::min(end, size_);
        ASYNC_FOR(size_t, i, start / GroupSize, i * GroupSize < end, i++)
            auto group = Accessor<Group>(groups_[i], __on_miss__);
            for (size_t j = 0; j < GroupSize; j++) {
                if (i * GroupSize + j < start) continue;
                if (i * GroupSize + j >= end) return;
                fn(group->at(j));
            }
        ASYNC_FOR_END
    }

    template <typename Fn>
        requires requires(Fn &&fn, const T &value) { fn(value); }
    void for_each_const_run_ahead(
        Fn &&fn, size_t start = 0,
        size_t end = std::numeric_limits<size_t>::max()) const {
        end = std::min(end, size_);
        ASYNC_FOR(size_t, i, start / GroupSize, i * GroupSize < end, i++)
            auto group = ConstAccessor<Group>(groups_[i], __on_miss__);
            for (size_t j = 0; j < GroupSize; j++) {
                if (i * GroupSize + j < start) continue;
                if (i * GroupSize + j >= end) return;
                fn(group->at(j));
            }
        ASYNC_FOR_END
    }

    template <bool Mut = false>
    class VecIterator {
        using Group = std::array<T, GroupSize>;
        using ElementType = std::conditional_t<Mut, T, const T>;

    private:
        DenseVector<T, GroupSize> *vector;
        size_t group_idx;
        size_t inner_idx;
        size_t prefetch_idx = 0;
        size_t idx_limit;  // max idx this iterator could be
        // LiteAccessor<Group, Mut> accessor;
        LiteAccessor<Group, Mut> accessor;

    public:
        VecIterator(DenseVector<T, GroupSize> &vec, int idx,
                    DereferenceScope &scope) {
            vector = &vec;
            // accessor = vector->groups_[idx / GroupSize];
            // accessor = LiteAccessor<Group, Mut>(
            //     vector->groups_[idx / GroupSize], __on_miss__, scope);
            ON_MISS_BEGIN
                // profile::count_on_miss();
            ON_MISS_END
            accessor = vector->groups_[idx / GroupSize].template access<Mut>(
                __on_miss__, scope);
            // profile::count_access();
            group_idx = idx / GroupSize;
            inner_idx = idx % GroupSize;
            // idx_limit = std::min(idx + 4096, 512 * 512 * 512);
            idx_limit = std::min(idx + 8192, 1024 * 1024 * 1024);
        }
        void pin() { accessor.pin(); }
        void unpin() { accessor.unpin(); }
        ElementType &get() { return (*accessor)[inner_idx]; }
        void set(T value) {
            get() = value;
            // cause compile error when const iterator call set()
        }
        void next(DereferenceScope &scope) {
            inner_idx++;
            if (inner_idx >= GroupSize) [[unlikely]] {
                inner_idx = 0;
                group_idx++;

                ON_MISS_BEGIN
                    // profile::count_on_miss();
#ifdef USE_YIELD
                    uthread::yield();
#else
                    cache::OnMissScope oms(__entry__, &scope);
                    auto cache = Cache::get_default();
                    for (prefetch_idx = std::max(group_idx, prefetch_idx);
                         prefetch_idx * GroupSize < idx_limit &&
                         prefetch_idx < group_idx + 256;
                         prefetch_idx++) {
                        cache->prefetch(vector->groups_[prefetch_idx].obj(),
                                        oms);
                        // profile::count_fetch();

                        if (prefetch_idx % 8 == 0 &&
                            cache::check_fetch(__entry__, __ddl__))
                            return;
                    }
#endif
                ON_MISS_END
                accessor = vector->groups_[group_idx].template access<Mut>(
                    __on_miss__, scope);
                // profile::count_access();
            }
        }
    };

    using VecMutIterator = VecIterator<true>;
};

}  // namespace Beehive
