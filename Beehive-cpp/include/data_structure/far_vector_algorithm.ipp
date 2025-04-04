#include <x86intrin.h>

#include <thread>

#include "async/scoped_inline_task.hpp"
#include "data_structure/far_vector.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"
#include "utils/stats.hpp"
namespace FarLib {

/* lite sort */
template <VecElementType T, size_t GroupSize>
template <typename Comp>
void FarVector<T, GroupSize>::sort_uth(LiteIterator<> begin, LiteIterator<> end,
                                       Comp cmp) {
    sort<SortAlgorithm::UTHREAD_LITE_SORT>(begin.cursor, begin.idx, end.cursor,
                                           end.idx, cmp);
}

template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg>
void FarVector<T, GroupSize>::sort(LiteIterator<> begin, LiteIterator<> end) {
    // default sort: using <= as cmp
    sort<Alg>(begin, end, std::less<T>());
}

template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg, typename Comp>
void FarVector<T, GroupSize>::sort(LiteIterator<> begin, LiteIterator<> end,
                                   Comp cmp) {
    sort<Alg>(begin.cursor, begin.idx, end.cursor, end.idx, cmp);
}

template <VecElementType T, size_t GroupSize>
template <typename Comp, SortAlgorithm Alg>
void FarVector<T, GroupSize>::sort(LiteIterator<> begin, LiteIterator<> end,
                                   Comp cmp) {
    sort<Alg>(begin.cursor, begin.idx, end.cursor, end.idx, cmp);
}

template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg, typename Comp>
void FarVector<T, GroupSize>::sort(in_iterator_t *begin_group, size_t begin_idx,
                                   in_iterator_t *end_group, size_t end_idx,
                                   Comp cmp) {
    if (begin_group == end_group) {
        auto accessor = Accessor<Group>(*begin_group);
        std::sort(accessor->begin() + begin_idx, accessor->begin() + end_idx,
                  cmp);
        return;
    }
    // merge_sort
    merge_sort_inrecursive<Alg>(begin_group, begin_idx, end_group, end_idx,
                                cmp);
    // FarVector<T, GroupSize> aux((end_group - begin_group) * GroupSize +
    // end_idx - begin_idx); merge_sort_recursive<Alg>(begin_group, end_group,
    // begin_group, begin_idx, end_group, end_idx, cmp, aux, nullptr);
    // merge_sort_recursive_uth<Alg, UTHREAD_COUNT>(begin_group, end_group,
    // begin_group, begin_idx, end_group, end_idx, cmp, aux, nullptr);
}

/* merge sort inrecursive */
template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg, typename Comp>
void FarVector<T, GroupSize>::merge_sort_inrecursive(in_iterator_t *begin_group,
                                                     size_t begin_idx,
                                                     in_iterator_t *end_group,
                                                     size_t end_idx, Comp cmp) {
    {
        in_iterator_t *lim = end_idx > 0 ? end_group + 1 : end_group;
        size_t n = end_group - begin_group + (end_idx > 0 ? 1 : 0);
        FarLib::uthread::parallel_for_with_scope<1>(
            (Alg == UTHREAD_LITE_SORT
                 ? uthread::get_thread_count() * UTHREAD_FACTOR
                 : uthread::get_thread_count()),
            n, [&](size_t i, DereferenceScope &scope) {
                in_iterator_t *group_uptr = begin_group + i;
                ON_MISS_BEGIN
                    if constexpr (Alg == UTHREAD_LITE_SORT) {
                        FarLib::uthread::yield();
                    } else if constexpr (Alg == OOO_LITE_SORT) {
                        cache::OnMissScope oms(__entry__, &scope);
                        for (in_iterator_t *uptr = group_uptr + 1; uptr < lim;
                             uptr++) {
                            Cache::get_default()->prefetch(uptr->obj(), oms);
                            if (cache::check_fetch(__entry__, __ddl__)) {
                                return;
                            }
                        }
                    }
                ON_MISS_END
                LiteAccessor<Group, true> lite_accessor(*group_uptr,
                                                        __on_miss__, scope);
                if (group_uptr == begin_group) [[unlikely]] {
                    std::sort(lite_accessor->begin() + begin_idx,
                              lite_accessor->end(), cmp);
                } else if (group_uptr == end_group) [[unlikely]] {
                    std::sort(lite_accessor->begin(),
                              lite_accessor->begin() + end_idx, cmp);
                } else {
                    std::sort(lite_accessor->begin(), lite_accessor->end(),
                              cmp);
                }
            });
    }
    sort_merge<Alg>(begin_group, begin_idx, end_group, end_idx, cmp);
}

template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg, typename Comp>
void FarVector<T, GroupSize>::sort_merge(in_iterator_t *begin_group,
                                         size_t begin_idx,
                                         in_iterator_t *end_group,
                                         size_t end_idx, Comp cmp) {
    FarVector<T, GroupSize> aux;
    aux.template resize<true>((end_group - begin_group) * GroupSize + end_idx -
                              begin_idx);
    size_t n = end_group - begin_group + (end_idx > 0 ? 1 : 0);
    for (size_t gap = 1; gap < n; gap *= 2) {
        size_t uth_n = (n + 2 * gap - 1) / (2 * gap);
        FarLib::uthread::parallel_for_with_scope<1>(
            (Alg == UTHREAD_LITE_SORT
                 ? uthread::get_thread_count() * UTHREAD_FACTOR
                 : uthread::get_thread_count()),
            uth_n, [&](size_t i, DereferenceScope &scope) {
                merge_gap<Alg>(begin_group, begin_idx, end_group, end_idx, cmp,
                               aux, i, gap, &scope);
            });
    }
}

template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg, typename Comp>
void FarVector<T, GroupSize>::merge_gap(in_iterator_t *begin_group,
                                        size_t begin_idx,
                                        in_iterator_t *end_group,
                                        size_t end_idx, Comp cmp,
                                        FarVector<T, GroupSize> &aux, size_t i,
                                        size_t gap, DereferenceScope *scope) {
    in_iterator_t *begin = begin_group + i * 2 * gap;
    in_iterator_t *latter_begin = begin + gap;
    in_iterator_t *end = latter_begin + gap;
    if (latter_begin > end_group || latter_begin == end_group && end_idx == 0) {
        return;
    }
    size_t begin_sort_idx = begin == begin_group ? begin_idx : 0;
    size_t end_sort_idx;
    size_t offs = begin == begin_group
                      ? 0
                      : (begin - begin_group) * GroupSize - begin_idx;
    if (end > end_group) {
        end = end_group;
        end_sort_idx = end_idx;
    } else {
        end_sort_idx = 0;
    }
    merge<Alg>(begin, begin_sort_idx, latter_begin, end, end_sort_idx, cmp, aux,
               offs, scope);
}

template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg>
void FarVector<T, GroupSize>::inc_group_idx(in_iterator_t *&group,
                                            in_iterator_t *lim,
                                            LiteAccessor<Group, true> &lite_acc,
                                            size_t &idx,
                                            DereferenceScope *scope) {
    idx++;
    if (idx == GroupSize) [[unlikely]] {
        idx = 0;
        group++;
        ON_MISS_BEGIN
            // TODO a var which shows prefetch process
            if constexpr (Alg == SortAlgorithm::OOO_LITE_SORT) {
                cache::OnMissScope oms(__entry__, scope);
                for (in_iterator_t *uptr = group + 1; uptr < lim; uptr++) {
                    Cache::get_default()->prefetch(uptr->obj(), oms);
                    if (cache::check_fetch(__entry__, __ddl__)) {
                        return;
                    }
                }
            } else if constexpr (Alg == SortAlgorithm::UTHREAD_LITE_SORT) {
                FarLib::uthread::yield();
            }
        ON_MISS_END
        lite_acc = group < lim
                       ? LiteAccessor<Group, true>(*group, __on_miss__, *scope)
                       : LiteAccessor<Group, true>();
    }
}

template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg, typename Comp>
void FarVector<T, GroupSize>::merge(in_iterator_t *begin_group,
                                    size_t begin_idx,
                                    in_iterator_t *latter_begin_group,
                                    in_iterator_t *end_group, size_t end_idx,
                                    Comp cmp, FarVector<T, GroupSize> &aux,
                                    size_t offs, DereferenceScope *scope) {
    size_t aux_n = 0;
    in_iterator_t *aux_lim;
    in_iterator_t *end_lim;
    {
        struct Scope : public DereferenceScope {
            LiteAccessor<Group, true> lite_accessor[3];
            virtual void pin() const override {
                for (size_t i = 0; i < 3; i++) {
                    lite_accessor[i].pin();
                }
            }
            virtual void unpin() const override {
                for (size_t i = 0; i < 3; i++) {
                    lite_accessor[i].unpin();
                }
            }
            Scope(DereferenceScope *scope) : DereferenceScope(scope) {}
        } inner_scope(scope);
        inner_scope.lite_accessor[0] =
            LiteAccessor<Group, true>(*begin_group, inner_scope);
        inner_scope.lite_accessor[1] =
            LiteAccessor<Group, true>(*latter_begin_group, inner_scope);
        auto group = begin_group;
        auto latter_group = latter_begin_group;
        in_iterator_t *aux_group = (aux.lbegin().cursor) + offs / GroupSize;
        inner_scope.lite_accessor[2] =
            LiteAccessor<Group, true>(*aux_group, inner_scope);
        size_t group_idx = begin_idx;
        size_t latter_group_idx = 0;
        size_t aux_idx = offs % GroupSize;
        end_lim = end_idx > 0 ? end_group + 1 : end_group;
        aux_lim = aux_group + (end_lim - begin_group);
        auto &group_acc = inner_scope.lite_accessor[0];
        auto &latter_group_acc = inner_scope.lite_accessor[1];
        auto &aux_group_acc = inner_scope.lite_accessor[2];
        while (group < latter_begin_group &&
               (latter_group < end_group ||
                latter_group == end_group && latter_group_idx < end_idx)) {
            if (cmp(group_acc->at(group_idx),
                    latter_group_acc->at(latter_group_idx))) {
                aux_group_acc->at(aux_idx) = group_acc->at(group_idx);
                inc_group_idx<Alg>(group, latter_begin_group, group_acc,
                                   group_idx, &inner_scope);
            } else {
                aux_group_acc->at(aux_idx) =
                    latter_group_acc->at(latter_group_idx);
                inc_group_idx<Alg>(latter_group, end_lim, latter_group_acc,
                                   latter_group_idx, &inner_scope);
            }
            inc_group_idx<Alg>(aux_group, aux_lim, aux_group_acc, aux_idx,
                               &inner_scope);
            aux_n++;
        }
        latter_group_acc = LiteAccessor<Group, true>();
        while (group < latter_begin_group) {
            aux_group_acc->at(aux_idx) = group_acc->at(group_idx);
            inc_group_idx<Alg>(group, latter_begin_group, group_acc, group_idx,
                               &inner_scope);
            inc_group_idx<Alg>(aux_group, aux_lim, aux_group_acc, aux_idx,
                               &inner_scope);
            aux_n++;
        }
    }
    {
        struct Scope : public DereferenceScope {
            LiteAccessor<Group, true> lite_accessor[2];
            virtual void pin() const override {
                for (size_t i = 0; i < 2; i++) {
                    lite_accessor[i].pin();
                }
            }
            virtual void unpin() const override {
                for (size_t i = 0; i < 2; i++) {
                    lite_accessor[i].unpin();
                }
            }
            Scope(DereferenceScope *scope) : DereferenceScope(scope) {}
        } inner_scope(scope);
        in_iterator_t *group = begin_group;
        size_t group_idx = begin_idx;
        in_iterator_t *aux_group = (aux.lbegin().cursor) + offs / GroupSize;
        size_t aux_idx = offs % GroupSize;
        inner_scope.lite_accessor[0] =
            LiteAccessor<Group, true>(*begin_group, inner_scope);
        inner_scope.lite_accessor[1] =
            LiteAccessor<Group, true>(*aux_group, inner_scope);
        auto &group_acc = inner_scope.lite_accessor[0];
        auto &aux_group_acc = inner_scope.lite_accessor[1];
        for (size_t i = 0; i < aux_n; i++) {
            group_acc->at(group_idx) = aux_group_acc->at(aux_idx);
            inc_group_idx<Alg>(group, end_lim, group_acc, group_idx,
                               &inner_scope);
            inc_group_idx<Alg>(aux_group, aux_lim, aux_group_acc, aux_idx,
                               &inner_scope);
        }
    }
}

/* merge sort recursive
    reserved for future */
template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg, typename Comp>
void FarVector<T, GroupSize>::merge_sort_recursive(
    in_iterator_t *begin, in_iterator_t *end, in_iterator_t *begin_group,
    size_t begin_idx, in_iterator_t *end_group, size_t end_idx, Comp cmp,
    FarVector<T, GroupSize> &aux, DereferenceScope *scope) {
    ASSERT(Alg == OOO_LITE_SORT);
    in_iterator_t *latter_begin = begin + (end - begin) / 2;
    in_iterator_t *lim = end_idx > 0 ? end_group + 1 : end_group;
    if (begin == latter_begin) {
        RootDereferenceScope root_scope;
        ON_MISS_BEGIN
            if constexpr (Alg == OOO_LITE_SORT) {
                cache::OnMissScope oms(__entry__, &root_scope);
                for (in_iterator_t *uptr = begin + 1; uptr < lim; uptr++) {
                    Cache::get_default()->prefetch(uptr->obj(), oms);
                    if (cache::check_fetch(__entry__, __ddl__)) {
                        return;
                    }
                }
            } else if constexpr (Alg == UTHREAD_LITE_SORT) {
                FarLib::uthread::yield();
            }
        ON_MISS_END
        LiteAccessor<Group, true> group_acc(*begin, __on_miss__, root_scope);
        if (begin == begin_group) [[unlikely]] {
            std::sort(group_acc->begin() + begin_idx, group_acc->end(), cmp);
        } else if (begin == end_group) [[unlikely]] {
            std::sort(group_acc->begin(), group_acc->begin() + end_idx, cmp);
        } else {
            std::sort(group_acc->begin(), group_acc->end(), cmp);
        }
        return;
    } else {
        if constexpr (Alg == SortAlgorithm::OOO_LITE_SORT) {
            merge_sort_recursive<Alg>(begin, latter_begin, begin_group,
                                      begin_idx, end_group, end_idx, cmp, aux,
                                      scope);
            merge_sort_recursive<Alg>(latter_begin, end, begin_group, begin_idx,
                                      end_group, end_idx, cmp, aux, scope);
        }
        RootDereferenceScope root_scope;
        size_t begin_sort_idx = begin == begin_group ? begin_idx : 0;
        size_t end_sort_idx = end == end_group ? end_sort_idx : 0;
        size_t offs = begin == begin_group
                          ? 0
                          : (begin - begin_group) * GroupSize - begin_idx;
        merge<Alg>(begin, begin_sort_idx, latter_begin, end, end_sort_idx, cmp,
                   aux, offs, &root_scope);
    }
}

template <VecElementType T, size_t GroupSize>
template <SortAlgorithm Alg, size_t UthreadCount, typename Comp>
void FarVector<T, GroupSize>::merge_sort_recursive_uth(
    in_iterator_t *begin, in_iterator_t *end, in_iterator_t *begin_group,
    size_t begin_idx, in_iterator_t *end_group, size_t end_idx, Comp cmp,
    FarVector<T, GroupSize> &aux, DereferenceScope *scope) {
    ASSERT(Alg == UTHREAD_LITE_SORT);
    in_iterator_t *latter_begin = begin + (end - begin) / 2;
    in_iterator_t *lim = end_idx > 0 ? end_group + 1 : end_group;
    if (begin == latter_begin) {
        RootDereferenceScope root_scope;
        ON_MISS_BEGIN
            FarLib::uthread::yield();
        ON_MISS_END
        LiteAccessor<Group, true> group_acc(*begin, __on_miss__, root_scope);
        if (begin == begin_group) [[unlikely]] {
            std::sort(group_acc->begin() + begin_idx, group_acc->end(), cmp);
        } else if (begin == end_group) [[unlikely]] {
            std::sort(group_acc->begin(), group_acc->begin() + end_idx, cmp);
        } else {
            std::sort(group_acc->begin(), group_acc->end(), cmp);
        }
        return;
    } else {
        if constexpr (UthreadCount == 1) {
            merge_sort_recursive_uth<Alg, 1>(begin, latter_begin, begin_group,
                                             begin_idx, end_group, end_idx, cmp,
                                             aux, scope);
            merge_sort_recursive_uth<Alg, 1>(latter_begin, end, begin_group,
                                             begin_idx, end_group, end_idx, cmp,
                                             aux, scope);
        } else {
            auto fn = [&]() {
                merge_sort_recursive_uth<Alg, UthreadCount / 2>(
                    latter_begin, end, begin_group, begin_idx, end_group,
                    end_idx, cmp, aux, scope);
            };
            FarLib::uthread::spawn(fn);
            merge_sort_recursive_uth<Alg, UthreadCount / 2>(
                begin, latter_begin, begin_group, begin_idx, end_group, end_idx,
                cmp, aux, scope);
        }
        RootDereferenceScope root_scope;
        size_t begin_sort_idx = begin == begin_group ? begin_idx : 0;
        size_t end_sort_idx = end == end_group ? end_sort_idx : 0;
        size_t offs = begin == begin_group
                          ? 0
                          : (begin - begin_group) * GroupSize - begin_idx;
        merge<Alg>(begin, begin_sort_idx, latter_begin, end, end_sort_idx, cmp,
                   aux, offs, &root_scope);
    }
}
/* visit */
template <VecElementType T, size_t GroupSize>
template <typename Visitor>
void FarVector<T, GroupSize>::visit_impl(Visitor &&visitor,
                                         DereferenceScope &scope) {
    for (auto it = lbegin(scope); it < lend(); it.next(scope)) {
        visitor(*it);
    }
}

template <VecElementType T, size_t GroupSize>
template <typename Visitor>
void FarVector<T, GroupSize>::visit_impl(Visitor &&visitor,
                                         DereferenceScope &scope) const {
    for (auto it = clbegin(scope); it < clend(); it.next(scope)) {
        visitor(*it);
    }
}

template <VecElementType T, size_t GroupSize>
template <typename Visitor>
void FarVector<T, GroupSize>::visit(Visitor &&visitor,
                                    DereferenceScope &scope) {
    visit_impl(visitor, scope);
}

template <VecElementType T, size_t GroupSize>
template <typename Visitor>
void FarVector<T, GroupSize>::visit(Visitor &&visitor) {
    RootDereferenceScope scope;
    visit_impl(visitor, scope);
}

template <VecElementType T, size_t GroupSize>
template <typename Visitor>
void FarVector<T, GroupSize>::visit(Visitor &&visitor,
                                    DereferenceScope &scope) const {
    visit_impl(visitor, scope);
}

template <VecElementType T, size_t GroupSize>
template <typename Visitor>
void FarVector<T, GroupSize>::visit(Visitor &&visitor) const {
    RootDereferenceScope scope;
    visit_impl(visitor, scope);
}
/* iota */
template <VecElementType T, size_t GroupSize>
void FarVector<T, GroupSize>::iota_impl(LiteIterator<> begin,
                                        LiteIterator<> end, T value,
                                        DereferenceScope &scope) {
    for (LiteIterator<> it(begin, 0, scope); it < end; it.next(scope)) {
        *it = value;
        ++value;
    }
}

/* view */
template <VecElementType T, size_t GroupSize>
template <bool Mut>
FarVector<T, GroupSize>::VectorView<Mut> FarVector<T, GroupSize>::get_view() {
    return VectorView<Mut>(Pointer(*this, groups_.data(), 0),
                           Pointer(*this, groups_.data(), size_));
}

template <VecElementType T, size_t GroupSize>
template <Algorithm alg>
FarVector<T, GroupSize> FarVector<T, GroupSize>::copy_data_by_idx(
    FarVector<size_t> &idx_vec) const {
    // auto sstart = get_cycles();
    size_type vec_size = idx_vec.size();
    // profile::reset_all();
    // auto restart = get_cycles();
    Self vec(vec_size);
    profile::reset_all();
    // auto start = get_cycles();
    const size_t thread_cnt = alg == UTHREAD
                                  ? uthread::get_thread_count() * UTH_FACTOR
                                  : uthread::get_thread_count();
    // aligned to group
    const size_t block = (idx_vec.groups_count() + thread_cnt - 1) /
                         thread_cnt * idx_vec.GROUP_SIZE;
    // std::cout << "block size: " << block << ", thread count: " <<
    // thread_cnt
    //           << std::endl;
    // profile::reset_all();
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope &scope) {
            ON_MISS_BEGIN
            ON_MISS_END
            using idx_it_t = decltype(idx_vec.clbegin());
            using vec_it_t = decltype(vec.lbegin());
            using acc_t = decltype(at(0, scope, __on_miss__));
            struct Scope : public DereferenceScope {
                idx_it_t idx_it;
                vec_it_t vec_it;
                acc_t acc;
                void pin() const override {
                    idx_it.pin();
                    vec_it.pin();
                    acc.pin();
                }
                void unpin() const override {
                    idx_it.unpin();
                    vec_it.unpin();
                    acc.unpin();
                }
                void next(__DMH__) {
                    idx_it.next(*this, __on_miss__);
                    vec_it.next(*this, __on_miss__);
                }
                void next() {
                    idx_it.next(*this);
                    vec_it.next(*this);
                }
                void next_idx_group() { idx_it.next_group(*this); }
                void next_vec() { vec_it.next(*this); }
                Scope(DereferenceScope *scope) : DereferenceScope(scope) {}
            } scp(&scope);
            const size_t idx_start = i * block;
            const size_t idx_end = std::min(idx_start + block, vec_size);
            if (idx_start >= idx_end) [[unlikely]] {
                return;
            }
            // printf("[%lu, %lu)\n", idx_start, idx_end);
            assert(idx_start % idx_vec.GROUP_SIZE == 0);
            if constexpr (alg == UTHREAD) {
                ON_MISS_BEGIN
                    uthread::yield();
                ON_MISS_END
                scp.idx_it = idx_vec.get_const_lite_iter(
                    idx_start, scp, __on_miss__, idx_start, idx_end);
                scp.vec_it = vec.get_lite_iter(idx_start, scp, __on_miss__,
                                               idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end;
                     idx++, scp.next(__on_miss__)) {
                    scp.acc = at(*(scp.idx_it), scp, __on_miss__);
                    *(scp.vec_it) = *(scp.acc);
                }
            } else if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
                if constexpr (alg == PREFETCH) {
                    scp.idx_it = idx_vec.get_const_lite_iter(
                        idx_start, scp, idx_start, idx_end);
                    scp.vec_it = vec.get_lite_iter(idx_start, scp,
                                                   idx_start, idx_end);
                    ON_MISS_BEGIN
                    ON_MISS_END
                    for (size_t idx = idx_start; idx < idx_end;
                         idx += idx_vec.GROUP_SIZE, scp.next_idx_group()) {
                        auto &idx_group = *scp.idx_it.get_group_accessor();
                        for (size_t ii = 0;
                             ii <
                             std::min(idx_vec.GROUP_SIZE, idx_end - idx);
                             ii++, scp.next_vec()) {
                            scp.acc = at(idx_group[ii], scp, __on_miss__);
                            *(scp.vec_it) = *(scp.acc);
                        }
                    }
                } else {
                    // pararoutine
                    using it_t = decltype(clbegin());
                    struct Context {
                        idx_it_t idx_it;
                        vec_it_t vec_it;
                        it_t it;
                        const Self *self;
                        size_t idx_start;
                        size_t idx_end;
                        size_t ii;
                        bool has_start;
                        bool in_loop;
                        bool vec_at_local;
                        bool it_at_local;
                        void pin() {
                            idx_it.pin();
                            vec_it.pin();
                            it.pin();
                        }
                        void unpin() {
                            idx_it.unpin();
                            vec_it.unpin();
                            it.unpin();
                        }
                        bool fetched() {
                            if (in_loop) {
                                if (vec_it.at_local() && it.at_local()) {
                                    vec_it.sync_accessor();
                                    it.sync_accessor();
                                    return true;
                                }
                                return false;
                            } else {
                                if (idx_it.at_local()) {
                                    idx_it.sync_accessor();
                                    return true;
                                }
                                return false;
                            }
                        }
                        bool run(DereferenceScope &scope) {
                            if (in_loop) [[likely]] {
                                goto next;
                            }
                            if (has_start) [[likely]] {
                                goto loop;
                            }
                            has_start = true;
                            if (!idx_it.async_fetch(scope)) {
                                return false;
                            }
                        loop:
                            in_loop = true;
                            for (ii = this->idx_start; ii < this->idx_end;
                                 ii++, vec_it.next()) {
                                vec_at_local = vec_it.async_fetch(scope);
                                // std::cout
                                //     << (*idx_it.get_group_accessor())[ii]
                                //     << std::endl;
                                it = self->get_const_lite_iter(
                                    (*idx_it.get_group_accessor())[ii]);
                                it_at_local = it.async_fetch(scope);
                                if (!(vec_at_local && it_at_local)) {
                                    return false;
                                }
                            next:
                                *vec_it = *it;
                            }
                            return true;
                        }
                    };
                    auto idx_it = idx_vec.get_const_lite_iter(
                        idx_start, scope, __on_miss__, idx_start, idx_end);
                    auto glb_vec_it = vec.get_lite_iter(
                        idx_start, scope, __on_miss__, idx_start, idx_end);
                    SCOPED_INLINE_ASYNC_FOR(
                        Context, size_t, idx, idx_start, idx < idx_end,
                        {
                            // printf("%lu\n", idx_vec.GROUP_SIZE);
                            idx += idx_vec.GROUP_SIZE;
                            idx_it.next_group();
                            glb_vec_it.nextn(idx_vec.GROUP_SIZE);
                        },
                        scope)
                        // std::cout
                        //     << "[" << idx << ", "
                        //     << std::min(idx + idx_vec.GROUP_SIZE,
                        //     idx_end)
                        //     << std::endl;
                        return Context{
                            .idx_it = idx_it,
                            .vec_it = glb_vec_it,
                            .self = this,
                            // block has aligned, start must = 0
                            .idx_start = 0,
                            .idx_end =
                                std::min(idx_end - idx, idx_vec.GROUP_SIZE),
                            .has_start = false,
                            .in_loop = false,
                        };
                    SCOPED_INLINE_ASYNC_FOR_END
                }
            } else {
                ERROR("alg not exist");
            }
        });
    // profile::print_profile_data();
    // auto end = get_cycles();
    // std::cout << "parallel for: " << end - start << std::endl;
    // auto ssend = get_cycles();
    // std::cout << "copy data by idx: " << ssend - sstart << std::endl;
    return vec;
}
}  // namespace FarLib