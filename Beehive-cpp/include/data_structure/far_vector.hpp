#pragma once
#include <array>
#include <concepts>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <unordered_set>
#include <vector>

#include "async/scoped_inline_task.hpp"
#include "cache/cache.hpp"
#include "option.hpp"
#include "utils/parallel.hpp"
namespace Beehive {
// TODO VecElementType can be non-trivially destructible
template <typename T>
concept VecElementType =
    std::copy_constructible<T> && std::is_trivially_destructible_v<T> &&
    std::is_copy_assignable_v<T>;
enum SortAlgorithm { LITE_SORT, OOO_LITE_SORT, UTHREAD_LITE_SORT };

template <VecElementType T,
          size_t GroupSize = (4096 - allocator::BlockHeadSize) / sizeof(T)>
class FarVector {
public:
    using size_type = size_t;
    using index_type = int64_t;
    using value_type = T;
    static constexpr size_t GROUP_SIZE = GroupSize;
    static constexpr size_t UTHREAD_FACTOR = 1;

private:
    using Group = std::array<T, GroupSize>;
    using UniquePtr = UniqueFarPtr<Group>;
    using in_iterator_t = UniquePtr;
    using Self = FarVector<T, GroupSize>;
    static_assert(GroupSize > 0);
    std::vector<UniquePtr> groups_;
    size_type size_;

    template <bool Mut = true>
    class GenericIterator;

    inline void resize_shrink(size_type count) {
        size_type group_cnt = (count + GroupSize - 1) / GroupSize;
        groups_.resize(group_cnt);
    }

    template <bool MultiThread>
    inline void resize_enlarge(size_type count) {
        if constexpr (MultiThread) {
            size_type alloc_num =
                (count + GroupSize - 1) / GroupSize - groups_.size();
            size_type start_idx = groups_.size();
            groups_.resize((count + GroupSize - 1) / GroupSize);
            uthread::parallel_for_with_scope<1>(
                uthread::get_thread_count(), alloc_num,
                [&](size_t i, DereferenceScope &scope) {
                    assert(i + start_idx < groups_.size());
                    groups_[i + start_idx].allocate_uninitialized(scope);
                });
            // TODO use pararoutine
        } else {
            RootDereferenceScope scope;
            size_type alloc_num =
                (count + GroupSize - 1) / GroupSize - groups_.size();
            while (alloc_num--) {
                UniquePtr &uptr = groups_.emplace_back();
                uptr.allocate_uninitialized(scope);
            }
        }
    }

    inline void resize_enlarge(size_type count, DereferenceScope &scope) {
        size_type alloc_num =
            (count + GroupSize - 1) / GroupSize - groups_.size();
        while (alloc_num--) {
            UniquePtr &uptr = groups_.emplace_back();
            uptr.allocate_uninitialized(scope);
        }
    }

    inline void resize_enlarge(size_type count, const T &init_val) {
        size_type init_idx_for_last_group = size_ % GroupSize;
        if (init_idx_for_last_group > 0) {
            size_type init_lim_for_last_group =
                count > GroupSize * groups_.size() ? GroupSize
                                                   : count % GroupSize;
            Accessor<Group> group_acc(groups_.back());
            for (; init_idx_for_last_group < init_lim_for_last_group;
                 init_idx_for_last_group++) {
                new (&((*group_acc)[init_idx_for_last_group])) T(init_val);
            }
        }

        size_type alloc_num =
            (count + GroupSize - 1) / GroupSize - groups_.size();
        if (alloc_num > 0) {
            RootDereferenceScope scope;
            while (alloc_num > 1) {
                UniquePtr &uptr = groups_.emplace_back();
                uptr.allocate_uninitialized(scope);
                LiteAccessor<Group, true> acc(uptr, scope);
                std::fill(acc->begin(), acc->end(), init_val);
                alloc_num--;
            }
            UniquePtr &uptr = groups_.emplace_back();
            uptr.allocate_uninitialized(scope);
            LiteAccessor<Group, true> acc(uptr, scope);
            std::fill(acc->begin(), acc->begin() + count % GroupSize, init_val);
        }
    }

    inline void check_index(size_type index) const { assert(index < size_); }

public:
    template <bool Mut = true>
    class Iterator;

    template <bool Mut = true>
    class ReverseIterator;

    template <bool Mut = true>
    class LiteIterator;

    using ConstIterator = Iterator<false>;
    using ConstReverseIterator = ReverseIterator<false>;
    using ConstLiteIterator = LiteIterator<false>;
    using iterator = Iterator<true>;
    using const_iterator = ConstIterator;
    using reverse_iterator = ReverseIterator<true>;
    using const_reverse_iterator = ConstReverseIterator;
    using lite_iterator = LiteIterator<true>;
    using const_lite_iterator = LiteIterator<false>;

public:
    FarVector() : size_(0) {};

    template <bool multithread = false>
    FarVector(size_type size) : FarVector() {
        resize<multithread>(size);
    }

    FarVector(size_type size, DereferenceScope &scope) : FarVector() {
        resize(size, scope);
    }

    FarVector(const FarVector &that) : size_(that.size()) {
        assert(that.capacity() > that.size());
        groups_.resize(that.groups_count());
        std::memset(groups_.data(), 0, sizeof(UniquePtr) * groups_.size());
        const size_t thread_cnt = uthread::get_thread_count();
        const size_t block = (groups_.size() + thread_cnt - 1) / thread_cnt;
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope &scope) {
                struct Scope : public DereferenceScope {
                    LiteAccessor<Group, true> group_acc;
                    LiteAccessor<Group> that_group_acc;

                    void pin() const override {
                        group_acc.pin();
                        that_group_acc.pin();
                    }

                    void unpin() const override {
                        group_acc.unpin();
                        that_group_acc.unpin();
                    }
                    Scope(DereferenceScope *scope) : DereferenceScope(scope) {}
                } scp(&scope);
                const size_t idx_begin = i * block;
                const size_t idx_end =
                    std::min(idx_begin + block, groups_.size());
                if (idx_begin >= idx_end) {
                    return;
                }
                for (size_t i = idx_begin; i < idx_end; i++) {
                    auto &group_uptr = groups_[i];
                    auto &that_group_uptr = that.groups_[i];
                    group_uptr.allocate_uninitialized(scp);
                    ON_MISS_BEGIN
                        cache::OnMissScope oms(__entry__, &scope);
                        constexpr size_t PREFETCH_LIM = 16UL;
                        auto lim = std::min(i + PREFETCH_LIM, groups_.size());
                        for (size_t prefetch_i = i + 1; prefetch_i < lim;
                             prefetch_i++) {
                            Cache::get_default()->prefetch(
                                that.groups_[prefetch_i].obj(), oms);
                            if (cache::check_fetch(__entry__, __ddl__)) {
                                return;
                            }
                        }
                    ON_MISS_END
                    new (&(scp.that_group_acc))
                        LiteAccessor<Group>(that_group_uptr, scp);
                    new (&(scp.group_acc))
                        LiteAccessor<Group, true>(group_uptr, scp);
                    const size_t group_real_size =
                        i < groups_.size() - 1
                            ? GroupSize
                            : (this->size() - 1) % GroupSize + 1;
                    if constexpr (std::is_trivially_copy_assignable_v<T>) {
                        std::memcpy(scp.group_acc.as_ptr(),
                                    scp.that_group_acc.as_ptr(),
                                    group_real_size * sizeof(T));
                    } else {
                        for (size_t i = 0; i < group_real_size; i++) {
                            // TODO opt by iterator
                            (*(scp.group_acc))[i] = (*(scp.that_group_acc))[i];
                        }
                    }
                }
            });
        // struct Scope : public cache::RootDereferenceScope {
        //     LiteAccessor<Group> that_group_acc;
        //     LiteAccessor<Group, true> group_acc;

        //     void pin() const override {
        //         that_group_acc.pin();
        //         group_acc.pin();
        //     }

        //     void unpin() const override {
        //         that_group_acc.unpin();
        //         group_acc.unpin();
        //     }
        // } scope;
        // for (const UniquePtr &that_uptr : that.groups_) {
        //     new (&(scope.that_group_acc)) LiteAccessor<Group>(that_uptr,
        //     scope); UniquePtr &group_uptr = groups_.emplace_back();
        //     group_uptr.allocate_uninitialized(scope);
        //     new (&(scope.group_acc))
        //         LiteAccessor<Group, true>(group_uptr, scope);
        //     if constexpr (std::is_trivially_copy_assignable_v<T>) {
        //         std::memcpy(scope.group_acc.as_ptr(),
        //                     scope.that_group_acc.as_ptr(), sizeof(Group));
        //     } else {
        //         for (size_t i = 0; i < GroupSize; i++) {
        //             // TODO opt by iterator
        //             (*(scope.group_acc))[i] = (*(scope.that_group_acc))[i];
        //         }
        //     }
        // }
    }

    FarVector(FarVector &&that) = default;

    size_type size() const { return size_; }

    bool empty() const { return size() == 0; }

    ~FarVector() {
        uthread::parallel_for<1>(uthread::get_thread_count(), groups_.size(),
                                 [this](size_t i) { groups_[i].reset(); });
    }

    void clear() {
        // TODO T can be non-trivially destructible
        uthread::parallel_for<1>(uthread::get_thread_count(), groups_.size(),
                                 [this](size_t i) { groups_[i].reset(); });
        std::memset(groups_.data(), 0, sizeof(UniquePtr) * groups_.size());
        groups_.clear();
        size_ = 0;
    }

    size_type capacity() const { return groups_.size() * GroupSize; }

    size_type groups_count() const { return groups_.size(); }

    void push_back_slow(T &&value) {
        size_type inner_idx = size_ % GroupSize;
        if (inner_idx == 0) {
            // need a new group
            UniquePtr &group_uptr = groups_.emplace_back();
            auto group_accessor = group_uptr.allocate_uninitialized();
            new (&((*group_accessor)[0])) T(std::move(value));
        } else {
            // store at the last group
            Accessor<Group> group_accessor(groups_[size_ / GroupSize]);
            new (&((*group_accessor)[inner_idx])) T(std::move(value));
        }
        size_++;
    }

    void push_back_slow(const T &value) {
        size_type inner_idx = size_ % GroupSize;
        if (inner_idx == 0) {
            // need a new group
            UniquePtr &group_uptr = groups_.emplace_back();
            auto group_accessor = group_uptr.allocate_uninitialized();
            new (&((*group_accessor)[0])) T(value);
        } else {
            // store at the last group
            Accessor<Group> group_accessor(groups_[size_ / GroupSize]);
            new (&((*group_accessor)[inner_idx])) T(value);
        }
        size_++;
    }

    void push_back(T &&value, DereferenceScope &scope) {
        assert(capacity() >= size());
        size_type inner_idx = size_ % GroupSize;
        if (inner_idx == 0) {
            // need a new group
            UniquePtr &group_uptr = groups_.emplace_back();
            group_uptr.allocate_uninitialized(scope);
            LiteAccessor<Group, true> group_accessor(group_uptr, scope);
            new (&((*group_accessor)[0])) T(std::move(value));
        } else {
            // store at the last group
            LiteAccessor<Group, true> group_accessor(groups_[size_ / GroupSize],
                                                     scope);
            new (&((*group_accessor)[inner_idx])) T(std::move(value));
        }
        size_++;
    }

    void push_back(const T &value, DereferenceScope &scope) {
        assert(capacity() >= size());
        size_type inner_idx = size_ % GroupSize;
        if (inner_idx == 0) {
            // need a new group
            UniquePtr &group_uptr = groups_.emplace_back();
            group_uptr.allocate_uninitialized(scope);
            LiteAccessor<Group, true> group_accessor(group_uptr, scope);
            new (&((*group_accessor)[0])) T(value);
        } else {
            // store at the last group
            LiteAccessor<Group, true> group_accessor(groups_[size_ / GroupSize],
                                                     scope);
            new (&((*group_accessor)[inner_idx])) T(value);
        }
        size_++;
    }

    template <typename... Args>
    void emplace_back_slow(Args &&...args) {
        push_back_slow(T(std::forward<Args>(args)...));
    }

    void pop_back() {
        if (size_ == 0) {
            // pop back an empty vector is an UB
            // do nothing and silent return here
            return;
        }
        if constexpr (!std::is_trivially_destructible_v<T>) {
            std::destroy_at<T>(back_mut().as_ptr());
        }
        // idx that will be poped
        size_type inner_idx = (size_ - 1) % GroupSize;
        if (inner_idx == 0) {
            groups_.back().reset();
            groups_.pop_back();
        }
        size_--;
    }

    void reserve(size_type count) {
        groups_.reserve((count + GroupSize - 1) / GroupSize);
    }

    template <bool MultiThread = false>
    void resize(size_type count) {
        assert(capacity() >= size());
        if (count < size_) {
            resize_shrink(count);
        } else if (count > size_) {
            resize_enlarge<MultiThread>(count);
        }
        size_ = count;
    }

    void resize(size_type count, DereferenceScope &scope) {
        assert(capacity() >= size());
        if (count < size_) {
            resize_shrink(count);
        } else if (count > size_) {
            resize_enlarge(count, scope);
        }
        size_ = count;
    }

    void resize(size_type count, const T &init_val) {
        assert(capacity() >= size());
        if (count < size_) {
            resize_shrink(count);
        } else if (count > size_) {
            resize_enlarge(count, init_val);
        }
        size_ = count;
    }

    Accessor<T> front_mut() { return at_mut(0); }

    ConstAccessor<T> front() { return at(0); }

    Accessor<T> back_mut() { return at_mut(size_ - 1); }

    ConstAccessor<T> back() { return at(size_ - 1); }

    Accessor<T> at_mut(size_type index) {
        check_index(index);
        size_type inner_idx = index % GroupSize;
        Accessor<Group> group_accessor = groups_[index / GroupSize];
        return Accessor<T>(std::move(group_accessor),
                           &((*group_accessor)[inner_idx]));
    }

    ConstAccessor<T> at(size_type index) const {
        check_index(index);
        size_type inner_idx = index % GroupSize;
        ConstAccessor<Group> const_group_accessor = groups_[index / GroupSize];
        return ConstAccessor<T>(std::move(const_group_accessor),
                                &((*const_group_accessor)[inner_idx]));
    }

    LiteAccessor<T> at(size_type index, DereferenceScope &scope,
                       __DMH__) const {
        check_index(index);
        {
            ON_MISS_BEGIN_X
            ON_MISS_END_X
            size_type inner_index = index % GroupSize;
            LiteAccessor<Group> group_lite_acc(groups_[index / GroupSize],
                                               __on_miss__, scope);
            return LiteAccessor<T>(group_lite_acc,
                                   &((*group_lite_acc)[inner_index]));
        }
    }

    LiteAccessor<T, true> at_mut(size_type index, DereferenceScope &scope,
                                 __DMH__) {
        check_index(index);
        {
            ON_MISS_BEGIN_X
            ON_MISS_END_X
            size_type inner_index = index % GroupSize;
            LiteAccessor<Group, true> group_lite_acc(groups_[index / GroupSize],
                                                     __on_miss__, scope);
            return LiteAccessor<T, true>(group_lite_acc,
                                         &((*group_lite_acc)[inner_index]));
        }
    }

    Accessor<T> operator[](size_type index) { return at_mut(index); }

    ConstAccessor<T> operator[](size_type index) const { return at(index); }

    Self &operator=(std::vector<T> &&data) {
        // TODO can be faster without clear and reallocate
        clear();
        resize<true>(data.size());
        RootDereferenceScope scope;
        auto it = lbegin(scope);
        for (auto &d : data) {
            *it = d;
            it.next(scope);
        }
        return *this;
    }

    Self &operator=(const std::vector<T> &data) {
        // TODO can be faster without clear and reallocate
        clear();
        resize<true>(data.size());
        RootDereferenceScope scope;
        auto it = lbegin(scope);
        for (auto &d : data) {
            *it = d;
            it.next(scope);
        }
        return *this;
    }

    Self &operator=(const FarVector &that) {
        clear();
        new (this) FarVector(that);
        return *this;
    }

    Self &operator=(FarVector &&that) = default;

    iterator begin() {
        return iterator(
            this->groups_.data(), 0, this->groups_.data(),
            this->groups_.data() + (size_ + GroupSize - 1) / GroupSize);
    }

    iterator end() { return iterator(begin() + size_); }

    reverse_iterator rbegin() { return reverse_iterator(end() - 1); }

    reverse_iterator rend() { return reverse_iterator(begin() - 1); }

    const_iterator cbegin() const {
        return const_iterator(
            this->groups_.data(), 0, this->groups_.data(),
            this->groups_.data() + (size_ + GroupSize - 1) / GroupSize);
    }

    const_iterator cend() const { return const_iterator(cbegin() + size_); }

    const_reverse_iterator crbegin() const {
        return const_reverse_iterator(cend() - 1);
    }

    const_reverse_iterator crend() const {
        return const_reverse_iterator(cbegin() - 1);
    }

    const_iterator begin() const { return cbegin(); }

    const_iterator end() const { return cend(); }

    const_reverse_iterator rbegin() const { return crbegin(); }

    const_reverse_iterator rend() const { return crend(); }

    lite_iterator lbegin(DereferenceScope &scope) {
        return lite_iterator(
            this->groups_.data(), 0, this->groups_.data(),
            this->groups_.data() + (size_ + GroupSize - 1) / GroupSize, scope);
    }

    lite_iterator lbegin(DereferenceScope &scope, __DMH__) {
        return lite_iterator(
            this->groups_.data(), 0, this->groups_.data(),
            this->groups_.data() + (size_ + GroupSize - 1) / GroupSize, scope,
            __on_miss__);
    }

    lite_iterator lend(DereferenceScope &scope) {
        return lite_iterator(lbegin(), size_, scope);
    }

    // need fetch manually
    lite_iterator get_lite_iter(size_t index) {
        return lite_iterator(lbegin(), index);
    }

    lite_iterator get_lite_iter(size_t index, DereferenceScope &scope) {
        return lite_iterator(lbegin(), index, scope);
    }

    lite_iterator get_lite_iter(size_t index, DereferenceScope &scope,
                                __DMH__) {
        return lite_iterator(lbegin(), index, scope, __on_miss__);
    }

    lite_iterator get_lite_iter(size_t index, DereferenceScope &scope,
                                size_t start, size_t end) {
        return lite_iterator(
            lbegin(), index, scope, groups_.data() + start / GroupSize,
            groups_.data() + (end + GroupSize - 1) / GroupSize);
    }

    lite_iterator get_lite_iter(size_t index, DereferenceScope &scope, __DMH__,
                                size_t start, size_t end) {
        return lite_iterator(
            lbegin(), index, scope, __on_miss__,
            groups_.data() + start / GroupSize,
            groups_.data() + (end + GroupSize - 1) / GroupSize);
    }

    const_lite_iterator get_const_lite_iter(size_t index) const {
        return const_lite_iterator(clbegin(), index);
    }

    const_lite_iterator get_const_lite_iter(size_t index,
                                            DereferenceScope &scope) const {
        return const_lite_iterator(clbegin(), index, scope);
    }

    const_lite_iterator get_const_lite_iter(size_t index,
                                            DereferenceScope &scope,
                                            size_t start, size_t end) const {
        return const_lite_iterator(
            clbegin(), index, scope, groups_.data() + start / GroupSize,
            groups_.data() + (end + GroupSize - 1) / GroupSize);
    }

    const_lite_iterator get_const_lite_iter(size_t index,
                                            DereferenceScope &scope, __DMH__,
                                            size_t start, size_t end) const {
        return const_lite_iterator(
            clbegin(), index, scope, __on_miss__,
            groups_.data() + start / GroupSize,
            groups_.data() + (end + GroupSize - 1) / GroupSize);
    }

    // for boundary hint, will not access
    lite_iterator lbegin() {
        return lite_iterator(
            this->groups_.data(), 0, this->groups_.data(),
            this->groups_.data() + (size_ + GroupSize - 1) / GroupSize);
    }

    // for boundary hint, will not access
    lite_iterator lend() { return lite_iterator(lbegin(), size_); }

    const_lite_iterator clbegin(DereferenceScope &scope) const {
        return const_lite_iterator(
            this->groups_.data(), 0, this->groups_.data(),
            this->groups_.data() + (size_ + GroupSize - 1) / GroupSize, scope);
    }

    const_lite_iterator clbegin(DereferenceScope &scope, __DMH__) const {
        return const_lite_iterator(
            this->groups_.data(), 0, this->groups_.data(),
            this->groups_.data() + (size_ + GroupSize - 1) / GroupSize, scope,
            __on_miss__);
    }

    const_lite_iterator clend(DereferenceScope &scope) const {
        return const_lite_iterator(clbegin(), size_, scope);
    }

    // for boundary hint, will not access
    const_lite_iterator clbegin() const {
        return const_lite_iterator(
            this->groups_.data(), 0, this->groups_.data(),
            this->groups_.data() + (size_ + GroupSize - 1) / GroupSize);
    }

    // for boundary hint, will not access
    const_lite_iterator clend() const {
        return const_lite_iterator(clbegin(), size_);
    }

    const_lite_iterator lbegin(DereferenceScope &scope) const {
        return clbegin(scope);
    }

    const_lite_iterator lend(DereferenceScope &scope) const {
        return clend(scope);
    }

    // for boundary hint, will not access
    const_lite_iterator lbegin() const { return clbegin(); }

    // for boundary hint, will not access
    const_lite_iterator lend() const { return clend(); }

    Self get_unique_values(DereferenceScope *scope = nullptr) const {
        std::unordered_set<T> unique_set;
        auto visit_func = [&unique_set](const T &elem) {
            unique_set.insert(elem);
        };
        if (scope) {
            visit(visit_func, *scope);
        } else {
            visit(visit_func);
        }
        Self vec(unique_set.size());
        auto it = unique_set.begin();
        vec.visit([&it](T &elem) {
            elem = *it;
            it++;
        });
        return std::move(vec);
    }

    void prefetch(const size_t idx, DereferenceScope &scope) const {
        Cache::get_default()->prefetch(groups_[idx / GroupSize].obj(), scope);
    }

    template <Algorithm alg, typename Fn>
        requires requires(Fn &&fn, T &val, DereferenceScope &scope) {
            fn(val, scope);
        }
    void for_each(Fn &&fn, size_t start, size_t end,
                  DereferenceScope &scope) const {
        assert(start <= end);
        assert(end <= size_);
        using it_t = decltype(clbegin());
        using func_t = std::remove_reference_t<decltype(fn)>;
        const size_t iter_count = (end - start + GroupSize - 1) / GroupSize;
        if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
            struct Scope : public DereferenceScope {
                it_t it;

                void pin() const override { it.pin(); }

                void unpin() const override { it.unpin(); }

                Scope(DereferenceScope *scope) : DereferenceScope(scope) {}
            } scp(&scope);
            // without on miss arg in iter, iter will exec prefetch
            scp.it = get_const_lite_iter(start, scp, start, end);
            for (size_t idx = start; idx < end; idx++, scp.it.next(scp)) {
                fn(*(scp.it), scp);
            }
        } else if constexpr (alg == PARAROUTINE) {
            size_t init_start = start;
            size_t init_end =
                std::min((start / GroupSize + 1) * GroupSize, end);
            struct Context {
                size_t idx_start;
                size_t idx_end;
                func_t *f;
                const UniquePtr *ptr;
                LiteAccessor<Group> lite_acc;
                bool fetch_end;
                void pin() { lite_acc.pin(); }

                void unpin() { lite_acc.unpin(); }

                bool fetched() { return lite_acc.at_local(); }

                bool run(DereferenceScope &scope) {
                    if (fetch_end) {
                        goto next;
                    }
                    fetch_end = true;
                    if (!lite_acc.async_fetch(*ptr, scope)) {
                        return false;
                    }
                next:
                    for (size_t i = idx_start; i < idx_end; i++) {
                        (*f)(lite_acc->at(i), scope);
                    }
                    return true;
                }
            };
            ON_MISS_BEGIN
            ON_MISS_END
            SCOPED_INLINE_ASYNC_FOR(
                Context, size_t, i, 0, i < iter_count,
                {
                    i++;
                    init_start = init_end;
                    init_end = std::min(init_start + GroupSize, end);
                },
                scope)
                return Context{
                    .idx_start = init_start % GroupSize,
                    .idx_end = (init_end - 1) % GroupSize + 1,
                    .f = &fn,
                    .ptr = &(groups_[init_start / GroupSize]),
                    .fetch_end = false,
                };
            SCOPED_INLINE_ASYNC_FOR_END
        } else {
            struct Scope : public DereferenceScope {
                it_t it;

                void pin() const override { it.pin(); }

                void unpin() const override { it.unpin(); }

                Scope(DereferenceScope *scope) : DereferenceScope(scope) {}

            } scp(&scope);
            ON_MISS_BEGIN
                if constexpr (alg == UTHREAD) {
                    uthread::yield();
                }
            ON_MISS_END
            scp.it = get_const_lite_iter(start, scp, __on_miss__, start, end);
            for (size_t i = start; i < end;
                 i++, scp.it.next(scp, __on_miss__)) {
                fn(*(scp.it), scp);
            }
        }
    }

    template <Algorithm alg, typename Fn>
        requires requires(Fn &&fn, T &val, DereferenceScope &scope) {
            fn(val, scope);
        }
    void for_each_aligned_group(Fn &&fn, size_t start, size_t end,
                                DereferenceScope &scope) const {
        assert(start % GroupSize == 0);
        using it_t = decltype(clbegin());
        using func_t = std::remove_reference_t<decltype(fn)>;
        const size_t iter_count = (end - start + GroupSize - 1) / GroupSize;
        if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
            struct Scope : public DereferenceScope {
                it_t it;

                void pin() const override { it.pin(); }

                void unpin() const override { it.unpin(); }

                Scope(DereferenceScope *scope) : DereferenceScope(scope) {}
            } scp(&scope);
            // without on miss arg in iter, iter will exec prefetch
            scp.it = get_const_lite_iter(start, scp, start, end);
            for (size_t idx = start; idx < end;
                 idx += GroupSize, scp.it.next_group(scp)) {
                auto &group = *scp.it.get_group_accessor();
                for (size_t ii = 0; ii < std::min(GroupSize, end - idx); ii++) {
                    fn(group[ii], scp);
                }
            }
        } else if constexpr (alg == PARAROUTINE) {
            size_t init_start = start;
            size_t init_end =
                std::min((start / GroupSize + 1) * GroupSize, end);
            struct Context {
                size_t idx_start;
                size_t idx_end;
                func_t *f;
                const UniquePtr *ptr;
                LiteAccessor<Group> lite_acc;
                bool fetch_end;
                void pin() { lite_acc.pin(); }

                void unpin() { lite_acc.unpin(); }

                bool fetched() { return lite_acc.at_local(); }

                bool run(DereferenceScope &scope) {
                    if (fetch_end) {
                        goto next;
                    }
                    fetch_end = true;
                    if (!lite_acc.async_fetch(*ptr, scope)) {
                        return false;
                    }
                next:
                    for (size_t i = idx_start; i < idx_end; i++) {
                        (*f)(lite_acc->at(i), scope);
                    }
                    return true;
                }
            };
            ON_MISS_BEGIN
            ON_MISS_END
            SCOPED_INLINE_ASYNC_FOR(
                Context, size_t, i, 0, i < iter_count,
                {
                    i++;
                    init_start = init_end;
                    init_end = std::min(init_start + GroupSize, end);
                },
                scope)
                return Context{
                    .idx_start = init_start % GroupSize,
                    .idx_end = (init_end - 1) % GroupSize + 1,
                    .f = &fn,
                    .ptr = &(groups_[init_start / GroupSize]),
                    .fetch_end = false,
                };
            SCOPED_INLINE_ASYNC_FOR_END
        } else {
            struct Scope : public DereferenceScope {
                it_t it;

                void pin() const override { it.pin(); }

                void unpin() const override { it.unpin(); }

                Scope(DereferenceScope *scope) : DereferenceScope(scope) {}

            } scp(&scope);
            ON_MISS_BEGIN
                if constexpr (alg == UTHREAD) {
                    uthread::yield();
                }
            ON_MISS_END
            scp.it = get_const_lite_iter(start, scp, __on_miss__, start, end);
            for (size_t i = start; i < end;
                 i++, scp.it.next(scp, __on_miss__)) {
                fn(*(scp.it), scp);
            }
        }
    }

    void assign_all(const T *ptr, size_t ssize) {
        resize<true>(ssize);
        assert(ssize == size_);
        assert(capacity() >= size());
        const size_t thread_cnt = uthread::get_thread_count() * UTHREAD_FACTOR;
        const size_t block =
            (groups_count() + thread_cnt - 1) / thread_cnt * GroupSize;
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope &scope) {
                const size_t idx_start = i * block;
                const size_t idx_end = std::min(idx_start + block, ssize);
                if (idx_start >= idx_end) {
                    return;
                }
                assert(idx_start % GroupSize == 0);
                using it_t = decltype(lbegin());
                struct Scope : public DereferenceScope {
                    it_t it;

                    void pin() const override { it.pin(); }

                    void unpin() const override { it.unpin(); }

                    Scope(DereferenceScope *scope) : DereferenceScope(scope) {}
                } scp(&scope);
                scp.it = get_lite_iter(idx_start, scp, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end;
                     idx += GroupSize, scp.it.next_group(scp)) {
                    auto &group = *(scp.it.get_group_accessor());
                    for (size_t j = 0; j < std::min(GroupSize, idx_end - idx);
                         j++) {
                        group[j] = *(ptr + idx + j);
                    }
                }
            });
    }

    void copy_to_local(T *ptr, size_t sstart, size_t ssize) {
        assert(sstart + ssize <= size_);
        const size_t thread_cnt = uthread::get_thread_count() * UTHREAD_FACTOR;
        const size_t block = (ssize + thread_cnt - 1) / thread_cnt;
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope &scope) {
                const size_t idx_start = sstart + i * block;
                const size_t idx_end =
                    std::min(idx_start + block, sstart + ssize);
                const size_t ii_start = i * block;
                const size_t ii_end = std::min(ii_start + block, ssize);
                if (idx_start >= idx_end) [[unlikely]] {
                    return;
                }
                auto it =
                    get_const_lite_iter(idx_start, scope, idx_start, idx_end);
                for (size_t ii = ii_start; ii < ii_end; ii++, it.next(scope)) {
                    *(ptr + ii) = *(it);
                }
            });
    }

private:
    /* ---------------------- lite sort ---------------------- */
    template <SortAlgorithm Alg>
    static inline void inc_group_idx(in_iterator_t *&group, in_iterator_t *lim,
                                     LiteAccessor<Group, true> &lite_acc,
                                     size_type &idx, DereferenceScope *scope);

    template <SortAlgorithm Alg, typename Comp>
    static inline void merge(in_iterator_t *begin_group, size_type begin_idx,
                             in_iterator_t *latter_begin_group,
                             in_iterator_t *end_group, size_type end_idx,
                             Comp cmp, FarVector<T, GroupSize> &aux,
                             size_type offs, DereferenceScope *scope);

    template <SortAlgorithm Alg, typename Comp>
    static void sort(in_iterator_t *begin_group, size_type begin_idx,
                     in_iterator_t *end_group, size_type end_idx, Comp cmp);

    template <SortAlgorithm Alg, typename Comp>
    static void merge_sort_inrecursive(in_iterator_t *begin_group,
                                       size_type begin_idx,
                                       in_iterator_t *end_group,
                                       size_type end_idx, Comp cmp);

    template <SortAlgorithm Alg, typename Comp>
    static void merge_sort_recursive(
        in_iterator_t *begin, in_iterator_t *end, in_iterator_t *begin_group,
        size_type begin_idx, in_iterator_t *end_group, size_type end_idx,
        Comp cmp, FarVector<T, GroupSize> &aux, DereferenceScope *scope);

    template <SortAlgorithm Alg, typename Comp>
    static void sort_merge(in_iterator_t *begin_group, size_type begin_idx,
                           in_iterator_t *end_group, size_type end_idx,
                           Comp cmp);

    template <SortAlgorithm Alg, typename Comp>
    static inline void merge_gap(in_iterator_t *begin_group,
                                 size_type begin_idx, in_iterator_t *end_group,
                                 size_type end_idx, Comp cmp,
                                 FarVector<T, GroupSize> &aux, size_type i,
                                 size_type gap, DereferenceScope *scope);

    template <SortAlgorithm Alg, size_type UthreadCount, typename Comp>
    static void merge_sort_recursive_uth(
        in_iterator_t *begin, in_iterator_t *end, in_iterator_t *begin_group,
        size_type begin_idx, in_iterator_t *end_group, size_type end_idx,
        Comp cmp, FarVector<T, GroupSize> &aux, DereferenceScope *scope);

public:
    /* --------------- sort ------------------- */
    template <SortAlgorithm Alg = OOO_LITE_SORT>
    static void sort(LiteIterator<> begin, LiteIterator<> end);

    template <SortAlgorithm Alg, typename Comp>
    static void sort(LiteIterator<> begin, LiteIterator<> end, Comp cmp);

    template <typename Comp, SortAlgorithm Alg = OOO_LITE_SORT>
    static void sort(LiteIterator<> begin, LiteIterator<> end, Comp cmp);

    template <typename Comp>
    static void sort_uth(LiteIterator<> begin, LiteIterator<> end, Comp cmp);

    /* ------------------- visit ---------------------- */
    template <typename Visitor>
    void visit_impl(Visitor &&visitor, DereferenceScope &scope);

    template <typename Visitor>
    void visit_impl(Visitor &&visitor, DereferenceScope &scope) const;

public:
    /* --------------- visit ------------------- */
    template <typename Visitor>
    void visit(Visitor &&visitor);

    template <typename Visitor>
    void visit(Visitor &&visitor, DereferenceScope &scope);

    template <typename Visitor>
    void visit(Visitor &&visitor) const;

    template <typename Visitor>
    void visit(Visitor &&visitor, DereferenceScope &scope) const;

    /* ------------------------- fill ------------------------------------- */
private:
    static void iota_impl(LiteIterator<> begin, LiteIterator<> end, T value,
                          DereferenceScope &scope);

public:
    static void iota(LiteIterator<> begin, LiteIterator<> end, T value,
                     DereferenceScope &scope) {
        iota_impl(begin, end, value, scope);
    }

    static void iota(LiteIterator<> begin, LiteIterator<> end, T value) {
        RootDereferenceScope scope;
        iota_impl(begin, end, value, scope);
    }

    /* ----------------------------------- View
     * --------------------------------------------- */
private:
    class Pointer;
    template <bool Mut>
    class VectorView;

public:
    template <bool Mut>
    VectorView<Mut> get_view();

    template <Algorithm alg>
    Self copy_data_by_idx(FarVector<size_type> &idx_vec) const;

    T vecmul(T *ptr, size_t ssize) {
        RootDereferenceScope scope;
        return vecmul(ptr, ssize, scope);
    }

    T vecmul(T *ptr, size_t ssize, DereferenceScope &scope) {
        // only for all local
        size_t mul_size = std::min(ssize, size());
        size_t multed_size = 0;
        T res;
        for (auto &uptr : groups_) {
            auto lite_accessor = LiteAccessor<Group>(uptr, scope);
            auto &group = *lite_accessor;
            for (int i = 0; i < std::min(mul_size - multed_size, GroupSize);
                 i++) {
                res += ptr[i + multed_size] * group[i];
            }
            multed_size += GroupSize;
            if (multed_size >= mul_size) {
                return res;
            }
        }
        return res;
    }

    T vecmul(T *ptr, size_t ssize, size_t vstart, DereferenceScope &scope) {
        // use this to run all local matmul to avoid difference taken by
        // compiler
        int64_t group_idx = vstart / GroupSize;
        int64_t inner_idx = vstart % GroupSize;
        T val(0);
        if (inner_idx == 0) {
            return vecmul_start_align(ptr, ssize, vstart, scope);
        }
        auto it = get_const_lite_iter(vstart, scope);
        auto &group = *it.lite_accessor;
        for (int64_t i = 0; i < std::min(GroupSize - inner_idx, ssize); i++) {
            val += ptr[i] * group[inner_idx + i];
        }
        if (ssize < GroupSize - inner_idx) {
            return val;
        }
        return val + vecmul_start_align(ptr + GroupSize - inner_idx,
                                        ssize - (GroupSize - inner_idx),
                                        vstart + GroupSize - inner_idx, scope);
    }

private:
    T vecmul_start_align(T *ptr, size_t ssize, size_t vstart,
                         DereferenceScope &scope) {
        T val(0);
        auto it = get_const_lite_iter(vstart, scope);
        for (int64_t multed_size = 0, group_idx = vstart / GroupSize;
             multed_size < ssize; multed_size += GroupSize, group_idx++,
                     it.nextn(GroupSize, scope)) {
            auto &group = *it.lite_accessor;
            for (int64_t i = 0; i < std::min(GroupSize, ssize - multed_size);
                 i++) {
                val += ptr[i + multed_size] * group[i];
            }
        }
        return val;
    }
};
}  // namespace Beehive
/* clang-format off */
#include "data_structure/far_vector_iter.ipp"
#include "data_structure/far_vector_lite_iter.ipp"
#include "data_structure/far_vector_algorithm.ipp"
#include "data_structure/far_vector_pointer.ipp"
#include "data_structure/far_vector_view.ipp"
/* clang-format on */
