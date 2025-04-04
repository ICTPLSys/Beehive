// Hossein Moein
// September 12, 2017
/*
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <DataFrame/DataFrame.h>
#include <DataFrame/GroupbyAggregators.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <functional>
#include <future>
#include <limits>
#include <random>

#include "async/scoped_inline_task.hpp"
#include "cache/accessor.hpp"
#include "option.hpp"
#include "utils/parallel.hpp"

// ----------------------------------------------------------------------------

namespace hmdf
{
template <typename I, typename H>
template <typename CF, typename... Ts>
void DataFrame<I, H>::sort_common_(DataFrame<I, H>& df, CF&& comp_func)
{
    // const size_type idx_s = df.indices_.size();
    // FarLib::FarVector<size_type> sorting_idxs(idx_s);
    // FarLib::FarVector<size_type>::iota(sorting_idxs.lbegin(), sorting_idxs.lend(), 0);
    // FarLib::FarVector<size_type>::sort(sorting_idxs.lbegin(), sorting_idxs.lend(), comp_func);

    // sort_functor_<Ts...> functor(sorting_idxs, idx_s);

    // for (auto& iter : df.data_) iter.change(functor);
    // _sort_by_sorted_index_(df.indices_, sorting_idxs, idx_s);
    ERROR("not implemented");
}

template <typename I, typename H>
template <Algorithm alg, bool Ascend, typename T, typename... Ts>
void DataFrame<I, H>::sort_common_(DataFrame<I, H>& df, FarLib::FarVector<T>& vec)
{
    using namespace FarLib;
    using namespace FarLib::cache;
    if (!(sizeof(T) <= 2 && std::is_integral<T>::value)) {
        ERROR("T is too big for count sort");
    }
    const size_type idx_s = df.indices_.size();
    FarLib::FarVector<size_type> sorting_idxs;
    {
        using namespace FarLib::cache;
        constexpr int64_t T_max     = std::numeric_limits<T>::max();
        constexpr int64_t T_min     = std::numeric_limits<T>::min();
        constexpr int64_t cnts_size = T_max - T_min + 1;
        std::vector<uint64_t> cnts(cnts_size, 0);
        // this alg is unstable
        const size_t thread_cnt =
            alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count();
        // aligned to group
        const size_t block1 = (vec.groups_count() + thread_cnt - 1) / thread_cnt * vec.GROUP_SIZE;
        std::vector<std::vector<uint64_t>> ucnts;
        for (size_t i = 0; i < thread_cnt; i++) {
            ucnts.emplace_back(cnts_size, 0);
        }
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                auto& ucnt             = ucnts[i];
                const size_t idx_start = i * block1;
                const size_t idx_end   = std::min(idx_start + block1, vec.size());
                if (idx_start >= idx_end) {
                    return;
                }
                vec.template for_each_aligned_group<alg>(
                    [&](const T& t, DereferenceScope& scope) { ucnt[t - T_min]++; }, idx_start,
                    idx_end, scope);
            });
        for (uint64_t i = 0; i < thread_cnt; i++) {
            for (uint64_t j = 0; j < cnts_size; j++) {
                cnts[j] += ucnts[i][j];
            }
        }
        // for (size_t i = 0; i < cnts_size; i++) {
        //     std::cout << i << ": " << cnts[i] << std::endl;
        // }

        for (uint64_t i = 1; i < cnts_size; i++) {
            cnts[i] += cnts[i - 1];
        }
        const size_t vec_size = vec.size();
        sorting_idxs.template resize<true>(vec.size());
        const size_t block2 = vec_size;
        uthread::parallel_for_with_scope<1>(1, 1, [&](size_t i, DereferenceScope& scope) {
            // count sort cannot asynchronous
            using it_t     = decltype(vec.clbegin());
            using idx_it_t = decltype(sorting_idxs.lbegin());
            struct Scope : public DereferenceScope {
                it_t it;
                idx_it_t idx_it;

                void pin() const override
                {
                    it.pin();
                    idx_it.pin();
                }

                void unpin() const override
                {
                    it.unpin();
                    idx_it.unpin();
                }

                void prev(__DMH__)
                {
                    it.prev(*this, __on_miss__);
                    idx_it.prev(*this, __on_miss__);
                }
                void prev()
                {
                    it.prev(*this);
                    idx_it.prev(*this);
                }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            const size_t idx_start = i * block2;
            const size_t idx_end   = std::min(idx_start + block2, vec_size);
            if (idx_start >= idx_end) {
                return;
            }
            if constexpr (alg == UTHREAD) {
                ON_MISS_BEGIN
                uthread::yield();
                ON_MISS_END
                scp.it = vec.get_const_lite_iter(idx_end - 1, scp, __on_miss__, idx_start, idx_end);
                scp.idx_it =
                    sorting_idxs.get_lite_iter(idx_end - 1, scp, __on_miss__, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end; idx++, scp.prev(__on_miss__)) {
                    auto new_idx = --cnts[*(scp.it) - T_min];
                    if constexpr (!Ascend) {
                        new_idx = vec_size - new_idx - 1;
                    }
                    *(scp.idx_it) = new_idx;
                }
            } else if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
                scp.it     = vec.get_const_lite_iter(idx_end - 1, scp, idx_start, idx_end);
                scp.idx_it = sorting_idxs.get_lite_iter(idx_end - 1, scp, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end; idx++, scp.prev()) {
                    auto new_idx = --cnts[*(scp.it) - T_min];
                    if constexpr (!Ascend) {
                        new_idx = vec_size - new_idx - 1;
                    }
                    *(scp.idx_it) = new_idx;
                }
            } else {
                ERROR("alg dont exist");
            }
        });
    }
    // std::cout << "[";
    // for (size_t i = 0; i < idx_s; i++) {
    //     std::cout << *(sorting_idxs[i]) << ", " << std::endl;
    // }
    // std::cout << "]";
    // abort();
    sort_copy_functor_<alg, Ts...> functor(sorting_idxs, idx_s);
    for (auto& iter : df.data_) {
        iter.change(functor);
    }
    _sort_by_sorted_index_copy_<alg>(df.indices_, sorting_idxs, idx_s);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <size_t N, typename... Ts>
void DataFrame<I, H>::shuffle(const std::array<const char*, N> col_names, bool also_shuffle_index)
{
    if (also_shuffle_index) {
        std::random_device rd;
        std::mt19937 g(rd());

        std::shuffle(indices_.begin(), indices_.end(), g);
    }

    shuffle_functor_<Ts...> functor;

    for (auto name_citer : col_names) {
        const auto citer = column_tb_.find(name_citer);

        if (citer == column_tb_.end()) {
            char buffer[512];

            sprintf(buffer, "DataFrame::shuffle(): ERROR: Cannot find column '%s'", name_citer);
            throw ColNotFound(buffer);
        }

        data_[citer->second].change(functor);
    }
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
inline constexpr T DataFrame<I, H>::_get_nan()
{
    if (std::numeric_limits<T>::has_quiet_NaN) return (std::numeric_limits<T>::quiet_NaN());
    return (T());
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
inline constexpr bool DataFrame<I, H>::_is_nan(const T& val)
{
    if (std::numeric_limits<T>::has_quiet_NaN) return (is_nan__(val));
    return (_get_nan<T>() == val);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
void DataFrame<I, H>::fill_missing_value_(ColumnVecType<T>& vec, const T& value, int limit,
                                          size_type col_num)
{
    const size_type vec_size = vec.size();
    int count                = 0;

    if (limit < 0) vec.reserve(col_num);
    for (size_type i = 0; i < col_num; ++i) {
        if (limit >= 0 && count >= limit) break;
        if (i >= vec_size) {
            vec.push_back(value);
            count += 1;
        } else if (_is_nan<T>(vec[i])) {
            vec[i] = value;
            count += 1;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
void DataFrame<I, H>::fill_missing_ffill_(ColumnVecType<T>& vec, int limit, size_type col_num)
{
    const size_type vec_size = vec.size();

    if (vec_size == 0) return;

    int count    = 0;
    T last_value = vec[0];

    for (size_type i = 1; i < col_num; ++i) {
        if (limit >= 0 && count >= limit) break;
        if (i >= vec_size) {
            if (!_is_nan(last_value)) {
                vec.reserve(col_num);
                vec.push_back(last_value);
                count += 1;
            } else
                break;
        } else {
            if (!_is_nan<T>(vec[i]))
                last_value = vec[i];
            else if (!_is_nan<T>(last_value)) {
                vec[i] = last_value;
                count += 1;
            }
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename std::enable_if<!std::is_arithmetic<T>::value ||
                                              !std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::fill_missing_midpoint_(ColumnVecType<T>& vec, int limit, size_type col_num)
{
    throw NotFeasible(
        "fill_missing_midpoint_(): ERROR: Mid-point filling is "
        "not feasible on non-arithmetic types");
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename std::enable_if<std::is_arithmetic<T>::value &&
                                              std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::fill_missing_midpoint_(ColumnVecType<T>& vec, int limit, size_type col_num)
{
    const size_type vec_size = vec.size();

    if (vec_size < 3) return;

    int count    = 0;
    T last_value = vec[0];

    for (size_type i = 1; i < vec_size - 1; ++i) {
        if (limit >= 0 && count >= limit) break;

        if (!_is_nan<T>(vec[i]))
            last_value = vec[i];
        else if (!_is_nan<T>(last_value)) {
            for (size_type j = i + 1; j < vec_size; ++j) {
                if (!_is_nan<T>(vec[j])) {
                    vec[i]     = (last_value + vec[j]) / T(2);
                    last_value = vec[i];
                    count += 1;
                    break;
                }
            }
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
void DataFrame<I, H>::fill_missing_bfill_(ColumnVecType<T>& vec, int limit)
{
    const long vec_size = static_cast<long>(vec.size());

    if (vec_size == 0) return;

    int count    = 0;
    T last_value = vec[vec_size - 1];

    for (long i = vec_size - 1; i >= 0; --i) {
        if (limit >= 0 && count >= limit) break;
        if (!_is_nan<T>(vec[i])) last_value = vec[i];
        if (_is_nan<T>(vec[i]) && !_is_nan<T>(last_value)) {
            vec[i] = last_value;
            count += 1;
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename std::enable_if<!std::is_arithmetic<T>::value ||
                                              !std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::fill_missing_linter_(ColumnVecType<T>&, const IndexVecType&, int)
{
    throw NotFeasible(
        "fill_missing_linter_(): ERROR: Interpolation is "
        "not feasible on non-arithmetic types");
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename std::enable_if<std::is_arithmetic<T>::value &&
                                              std::is_arithmetic<I>::value>::type*>
void DataFrame<I, H>::fill_missing_linter_(ColumnVecType<T>& vec, const IndexVecType& index,
                                           int limit)
{
    const long vec_size = static_cast<long>(vec.size());

    if (vec_size < 3) return;

    int count           = 0;
    T* y1               = &(vec[0]);
    T* y2               = &(vec[2]);
    const IndexType* x  = &(index[1]);
    const IndexType* x1 = &(index[0]);
    const IndexType* x2 = &(index[2]);

    for (long i = 1; i < vec_size - 1; ++i) {
        if (limit >= 0 && count >= limit) break;
        if (_is_nan<T>(vec[i])) {
            if (_is_nan<T>(*y2)) {
                bool found = false;

                for (long j = i + 1; j < vec_size; ++j) {
                    if (!_is_nan(vec[j])) {
                        y2    = &(vec[j]);
                        x2    = &(index[j]);
                        found = true;
                        break;
                    }
                }
                if (!found) break;
            }
            if (_is_nan<T>(*y1)) {
                for (long j = i - 1; j >= 0; --j) {
                    if (!_is_nan(vec[j])) {
                        y1 = &(vec[j]);
                        x1 = &(index[j]);
                        break;
                    }
                }
            }
            vec[i] = *y1 + (static_cast<T>(*x - *x1) / static_cast<T>(*x2 - *x1)) * (*y2 - *y1);
            count += 1;
        }
        if (i < (vec_size - 2)) {
            y1 = &(vec[i]);
            y2 = &(vec[i + 2]);
            x  = &(index[i + 1]);
            x1 = &(index[i]);
            x2 = &(index[i + 2]);
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, size_t N>
void DataFrame<I, H>::fill_missing(const std::array<const char*, N> col_names, fill_policy fp,
                                   const std::array<T, N> values, int limit)
{
    std::vector<std::future<void>> futures(get_thread_level());
    size_type thread_count = 0;

    for (size_type i = 0; i < N; ++i) {
        ColumnVecType<T>& vec = get_column<T>(col_names[i]);

        if (fp == fill_policy::value) {
            if (thread_count >= get_thread_level())
                fill_missing_value_(vec, values[i], limit, indices_.size());
            else {
                futures[thread_count] =
                    std::async(std::launch::async, &DataFrame::fill_missing_value_<T>,
                               std::ref(vec), std::cref(values[i]), limit, indices_.size());
                thread_count += 1;
            }
        } else if (fp == fill_policy::fill_forward) {
            if (thread_count >= get_thread_level())
                fill_missing_ffill_<T>(vec, limit, indices_.size());
            else {
                futures[thread_count] =
                    std::async(std::launch::async, &DataFrame::fill_missing_ffill_<T>,
                               std::ref(vec), limit, indices_.size());
                thread_count += 1;
            }
        } else if (fp == fill_policy::fill_backward) {
            if (thread_count >= get_thread_level())
                fill_missing_bfill_<T>(vec, limit);
            else {
                futures[thread_count] = std::async(
                    std::launch::async, &DataFrame::fill_missing_bfill_<T>, std::ref(vec), limit);
                thread_count += 1;
            }
        } else if (fp == fill_policy::linear_interpolate) {
            if (thread_count >= get_thread_level())
                fill_missing_linter_<T>(vec, indices_, limit);
            else {
                futures[thread_count] =
                    std::async(std::launch::async, &DataFrame::fill_missing_linter_<T>,
                               std::ref(vec), std::cref(indices_), limit);
                thread_count += 1;
            }
        } else if (fp == fill_policy::mid_point) {
            if (thread_count >= get_thread_level())
                fill_missing_midpoint_<T>(vec, limit, indices_.size());
            else {
                futures[thread_count] =
                    std::async(std::launch::async, &DataFrame::fill_missing_midpoint_<T>,
                               std::ref(vec), limit, indices_.size());
                thread_count += 1;
            }
        } else if (fp == fill_policy::linear_extrapolate) {
            char buffer[512];

            sprintf(buffer, "DataFrame::fill_missing(): fill_policy %d is not implemented",
                    static_cast<int>(fp));
            throw NotImplemented(buffer);
        }
    }

    for (size_type idx = 0; idx < thread_count; ++idx) futures[idx].get();
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
void DataFrame<I, H>::drop_missing_rows_(T& vec, const DropRowMap missing_row_map,
                                         drop_policy policy, size_type threshold, size_type col_num)
{
    size_type erase_count = 0;

    for (auto& iter : missing_row_map) {
        if (policy == drop_policy::all) {
            if (iter.second == col_num) {
                vec.erase(vec.begin() + (iter.first - erase_count));
                erase_count += 1;
            }
        } else if (policy == drop_policy::any) {
            if (iter.second > 0) {
                vec.erase(vec.begin() + (iter.first - erase_count));
                erase_count += 1;
            }
        } else if (policy == drop_policy::threshold) {
            if (iter.second > threshold) {
                vec.erase(vec.begin() + (iter.first - erase_count));
                erase_count += 1;
            }
        }
    }

    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
void DataFrame<I, H>::drop_missing(drop_policy policy, size_type threshold)
{
    DropRowMap missing_row_map;
    std::vector<std::future<void>> futures(get_thread_level());
    size_type thread_count    = 0;
    const size_type data_size = data_.size();

    map_missing_rows_functor_<Ts...> functor(indices_.size(), missing_row_map);

    for (size_type idx = 0; idx < data_size; ++idx) {
        if (thread_count >= get_thread_level())
            data_[idx].change(functor);
        else {
            auto to_be_called = static_cast<void (DataVec::*)(map_missing_rows_functor_<Ts...>&&)>(
                &DataVec::template change<map_missing_rows_functor_<Ts...>>);

            futures[thread_count] =
                std::async(std::launch::async, to_be_called, &(data_[idx]), std::move(functor));
            thread_count += 1;
        }
    }
    for (size_type idx = 0; idx < thread_count; ++idx) futures[idx].get();
    thread_count = 0;

    drop_missing_rows_(indices_, missing_row_map, policy, threshold, data_size);

    drop_missing_rows_functor_<Ts...> functor2(missing_row_map, policy, threshold, data_.size());

    for (size_type idx = 0; idx < data_size; ++idx) {
        if (thread_count >= get_thread_level())
            data_[idx].change(functor2);
        else {
            auto to_be_called = static_cast<void (DataVec::*)(drop_missing_rows_functor_<Ts...>&&)>(
                &DataVec::template change<drop_missing_rows_functor_<Ts...>>);

            futures[thread_count] =
                std::async(std::launch::async, to_be_called, &(data_[idx]), std::move(functor2));
            thread_count += 1;
        }
    }
    for (size_type idx = 0; idx < thread_count; ++idx) futures[idx].get();

    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, size_t N>
typename DataFrame<I, H>::size_type DataFrame<I, H>::replace(const char* col_name,
                                                             const std::array<T, N> old_values,
                                                             const std::array<T, N> new_values,
                                                             int limit)
{
    ColumnVecType<T>& vec = get_column<T>(col_name);
    size_type count       = 0;

    _replace_vector_vals_<ColumnVecType<T>, T, N>(vec, old_values, new_values, count, limit);

    return (count);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <size_t N>
typename DataFrame<I, H>::size_type DataFrame<I, H>::replace_index(
    const std::array<IndexType, N> old_values, const std::array<IndexType, N> new_values, int limit)
{
    size_type count = 0;

    _replace_vector_vals_<IndexVecType, IndexType, N>(indices_, old_values, new_values, count,
                                                      limit);

    return (count);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename F>
void DataFrame<I, H>::replace(const char* col_name, F& functor)
{
    ColumnVecType<T>& vec = get_column<T>(col_name);
    const size_type vec_s = vec.size();

    for (size_type i = 0; i < vec_s; ++i)
        if (!functor(indices_[i], vec[i])) break;

    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, size_t N>
std::future<typename DataFrame<I, H>::size_type> DataFrame<I, H>::replace_async(
    const char* col_name, const std::array<T, N> old_values, const std::array<T, N> new_values,
    int limit)
{
    return (std::async(std::launch::async, &DataFrame::replace<T, N>, this, col_name, old_values,
                       new_values, limit));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename F>
std::future<void> DataFrame<I, H>::replace_async(const char* col_name, F& functor)
{
    return (std::async(std::launch::async, &DataFrame::replace<T, F>, this, col_name,
                       std::ref(functor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
void DataFrame<I, H>::make_consistent()
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call make_consistent()");

    const size_type idx_s = indices_.size();
    consistent_functor_<Ts...> functor(idx_s);

    for (auto& iter : data_) iter.change(functor);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
void DataFrame<I, H>::shrink_to_fit()
{
    indices_.shrink_to_fit();

    shrink_to_fit_functor_<Ts...> functor;

    for (auto& iter : data_) iter.change(functor);
}

// ----------------------------------------------------------------------------
template <typename I, typename H>
template <Algorithm alg, typename T, typename... Ts>
void DataFrame<I, H>::sort(const char* name, sort_spec dir)
{
    make_consistent<Ts...>();
    if constexpr (alg == DEFAULT) {
        if (!::strcmp(name, DF_INDEX_COL_NAME)) {
            auto a = [](const T& t1, const T& t2) -> bool { return t1 < t2; };
            auto d = [](const T& t1, const T& t2) -> bool { return t1 > t2; };

            if (dir == sort_spec::ascen)
                sort_common_<decltype(a), Ts...>(*this, std::move(a));
            else
                sort_common_<decltype(d), Ts...>(*this, std::move(d));
        } else {
            const ColumnVecType<T>& idx_vec = get_column<T>(name);

            auto a = [](const T& t1, const T& t2) -> bool { return t1 < t2; };
            auto d = [](const T& t1, const T& t2) -> bool { return t1 < t2; };

            if (dir == sort_spec::ascen)
                sort_common_<decltype(a), Ts...>(*this, std::move(a));
            else
                sort_common_<decltype(d), Ts...>(*this, std::move(d));
        }
    } else {
        if (!::strcmp(name, DF_INDEX_COL_NAME)) {
            if (dir == sort_spec::ascen) {
                sort_common_<alg, true, typename IndexVecType::value_type, Ts...>(*this, indices_);
            } else {
                sort_common_<alg, false, typename IndexVecType::value_type, Ts...>(*this, indices_);
            }
        }
        auto& idx_vec = get_column<T>(name);
        if (dir == sort_spec::ascen) {
            sort_common_<alg, true, T, Ts...>(*this, idx_vec);
        } else {
            sort_common_<alg, false, T, Ts...>(*this, idx_vec);
        }
    }
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename... Ts>
void DataFrame<I, H>::sort(const char* name1, sort_spec dir1, const char* name2, sort_spec dir2)
{
    make_consistent<Ts...>();

    ColumnVecType<T1>* vec1{nullptr};
    ColumnVecType<T2>* vec2{nullptr};

    if (!::strcmp(name1, DF_INDEX_COL_NAME))
        vec1 = reinterpret_cast<ColumnVecType<T1>*>(&indices_);
    else
        vec1 = &(get_column<T1>(name1));

    if (!::strcmp(name2, DF_INDEX_COL_NAME))
        vec2 = reinterpret_cast<ColumnVecType<T2>*>(&indices_);
    else
        vec2 = &(get_column<T2>(name2));

    auto cf = [vec1, vec2, dir1, dir2](size_type i, size_type j) -> bool {
        if (dir1 == sort_spec::ascen) {
            if (vec1->at(i) < vec1->at(j))
                return (true);
            else if (vec1->at(i) > vec1->at(j))
                return (false);
        } else {
            if (vec1->at(i) > vec1->at(j))
                return (true);
            else if (vec1->at(i) < vec1->at(j))
                return (false);
        }
        if (dir2 == sort_spec::ascen)
            return (vec2->at(i) < vec2->at(j));
        else
            return (vec2->at(i) > vec2->at(j));
    };

    sort_common_<decltype(cf), Ts...>(*this, std::move(cf));
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename... Ts>
void DataFrame<I, H>::sort(const char* name1, sort_spec dir1, const char* name2, sort_spec dir2,
                           const char* name3, sort_spec dir3)
{
    make_consistent<Ts...>();

    ColumnVecType<T1>* vec1{nullptr};
    ColumnVecType<T2>* vec2{nullptr};
    ColumnVecType<T3>* vec3{nullptr};

    if (!::strcmp(name1, DF_INDEX_COL_NAME))
        vec1 = reinterpret_cast<ColumnVecType<T1>*>(&indices_);
    else
        vec1 = &(get_column<T1>(name1));

    if (!::strcmp(name2, DF_INDEX_COL_NAME))
        vec2 = reinterpret_cast<ColumnVecType<T2>*>(&indices_);
    else
        vec2 = &(get_column<T2>(name2));

    if (!::strcmp(name3, DF_INDEX_COL_NAME))
        vec3 = reinterpret_cast<ColumnVecType<T3>*>(&indices_);
    else
        vec3 = &(get_column<T3>(name3));

    auto cf = [vec1, vec2, vec3, dir1, dir2, dir3](size_type i, size_type j) -> bool {
        if (dir1 == sort_spec::ascen) {
            if (vec1->at(i) < vec1->at(j))
                return (true);
            else if (vec1->at(i) > vec1->at(j))
                return (false);
        } else {
            if (vec1->at(i) > vec1->at(j))
                return (true);
            else if (vec1->at(i) < vec1->at(j))
                return (false);
        }
        if (dir2 == sort_spec::ascen) {
            if (vec2->at(i) < vec2->at(j))
                return (true);
            else if (vec2->at(i) > vec2->at(j))
                return (false);
        } else {
            if (vec2->at(i) > vec2->at(j))
                return (true);
            else if (vec2->at(i) < vec2->at(j))
                return (false);
        }
        if (dir3 == sort_spec::ascen)
            return (vec3->at(i) < vec3->at(j));
        else
            return (vec3->at(i) > vec3->at(j));
    };

    sort_common_<decltype(cf), Ts...>(*this, std::move(cf));
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename... Ts>
void DataFrame<I, H>::sort(const char* name1, sort_spec dir1, const char* name2, sort_spec dir2,
                           const char* name3, sort_spec dir3, const char* name4, sort_spec dir4)
{
    make_consistent<Ts...>();

    ColumnVecType<T1>* vec1{nullptr};
    ColumnVecType<T2>* vec2{nullptr};
    ColumnVecType<T3>* vec3{nullptr};
    ColumnVecType<T4>* vec4{nullptr};

    if (!::strcmp(name1, DF_INDEX_COL_NAME))
        vec1 = reinterpret_cast<ColumnVecType<T1>*>(&indices_);
    else
        vec1 = &(get_column<T1>(name1));

    if (!::strcmp(name2, DF_INDEX_COL_NAME))
        vec2 = reinterpret_cast<ColumnVecType<T2>*>(&indices_);
    else
        vec2 = &(get_column<T2>(name2));

    if (!::strcmp(name3, DF_INDEX_COL_NAME))
        vec3 = reinterpret_cast<ColumnVecType<T3>*>(&indices_);
    else
        vec3 = &(get_column<T3>(name3));

    if (!::strcmp(name4, DF_INDEX_COL_NAME))
        vec4 = reinterpret_cast<ColumnVecType<T4>*>(&indices_);
    else
        vec4 = &(get_column<T4>(name4));

    auto cf = [vec1, vec2, vec3, vec4, dir1, dir2, dir3, dir4](size_type i, size_type j) -> bool {
        if (dir1 == sort_spec::ascen) {
            if (vec1->at(i) < vec1->at(j))
                return (true);
            else if (vec1->at(i) > vec1->at(j))
                return (false);
        } else {
            if (vec1->at(i) > vec1->at(j))
                return (true);
            else if (vec1->at(i) < vec1->at(j))
                return (false);
        }
        if (dir2 == sort_spec::ascen) {
            if (vec2->at(i) < vec2->at(j))
                return (true);
            else if (vec2->at(i) > vec2->at(j))
                return (false);
        } else {
            if (vec2->at(i) > vec2->at(j))
                return (true);
            else if (vec2->at(i) < vec2->at(j))
                return (false);
        }
        if (dir3 == sort_spec::ascen) {
            if (vec3->at(i) < vec3->at(j))
                return (true);
            else if (vec3->at(i) > vec3->at(j))
                return (false);
        } else {
            if (vec3->at(i) > vec3->at(j))
                return (true);
            else if (vec3->at(i) < vec3->at(j))
                return (false);
        }
        if (dir4 == sort_spec::ascen)
            return (vec4->at(i) < vec4->at(j));
        else
            return (vec4->at(i) > vec4->at(j));
    };

    sort_common_<decltype(cf), Ts...>(*this, std::move(cf));
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename... Ts>
void DataFrame<I, H>::sort(const char* name1, sort_spec dir1, const char* name2, sort_spec dir2,
                           const char* name3, sort_spec dir3, const char* name4, sort_spec dir4,
                           const char* name5, sort_spec dir5)
{
    make_consistent<Ts...>();

    ColumnVecType<T1>* vec1{nullptr};
    ColumnVecType<T2>* vec2{nullptr};
    ColumnVecType<T3>* vec3{nullptr};
    ColumnVecType<T4>* vec4{nullptr};
    ColumnVecType<T5>* vec5{nullptr};

    if (!::strcmp(name1, DF_INDEX_COL_NAME))
        vec1 = reinterpret_cast<ColumnVecType<T1>*>(&indices_);
    else
        vec1 = &(get_column<T1>(name1));

    if (!::strcmp(name2, DF_INDEX_COL_NAME))
        vec2 = reinterpret_cast<ColumnVecType<T2>*>(&indices_);
    else
        vec2 = &(get_column<T2>(name2));

    if (!::strcmp(name3, DF_INDEX_COL_NAME))
        vec3 = reinterpret_cast<ColumnVecType<T3>*>(&indices_);
    else
        vec3 = &(get_column<T3>(name3));

    if (!::strcmp(name4, DF_INDEX_COL_NAME))
        vec4 = reinterpret_cast<ColumnVecType<T4>*>(&indices_);
    else
        vec4 = &(get_column<T4>(name4));

    if (!::strcmp(name4, DF_INDEX_COL_NAME))
        vec5 = reinterpret_cast<ColumnVecType<T5>*>(&indices_);
    else
        vec5 = &(get_column<T5>(name5));

    auto cf = [vec1, vec2, vec3, vec4, vec5, dir1, dir2, dir3, dir4, dir5](size_type i,
                                                                           size_type j) -> bool {
        if (dir1 == sort_spec::ascen) {
            if (vec1->at(i) < vec1->at(j))
                return (true);
            else if (vec1->at(i) > vec1->at(j))
                return (false);
        } else {
            if (vec1->at(i) > vec1->at(j))
                return (true);
            else if (vec1->at(i) < vec1->at(j))
                return (false);
        }
        if (dir2 == sort_spec::ascen) {
            if (vec2->at(i) < vec2->at(j))
                return (true);
            else if (vec2->at(i) > vec2->at(j))
                return (false);
        } else {
            if (vec2->at(i) > vec2->at(j))
                return (true);
            else if (vec2->at(i) < vec2->at(j))
                return (false);
        }
        if (dir3 == sort_spec::ascen) {
            if (vec3->at(i) < vec3->at(j))
                return (true);
            else if (vec3->at(i) > vec3->at(j))
                return (false);
        } else {
            if (vec3->at(i) > vec3->at(j))
                return (true);
            else if (vec3->at(i) < vec3->at(j))
                return (false);
        }
        if (dir4 == sort_spec::ascen) {
            if (vec4->at(i) < vec4->at(j))
                return (true);
            else if (vec4->at(i) > vec4->at(j))
                return (false);
        } else {
            if (vec4->at(i) > vec4->at(j))
                return (true);
            else if (vec4->at(i) < vec4->at(j))
                return (false);
        }
        if (dir5 == sort_spec::ascen)
            return (vec5->at(i) < vec5->at(j));
        else
            return (vec5->at(i) > vec5->at(j));
    };

    sort_common_<decltype(cf), Ts...>(*this, std::move(cf));
    return;
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename... Ts>
std::future<void> DataFrame<I, H>::sort_async(const char* name, sort_spec dir)
{
    return (std::async(std::launch::async,
                       [name, dir, this]() -> void { this->sort<T, Ts...>(name, dir); }));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename... Ts>
std::future<void> DataFrame<I, H>::sort_async(const char* name1, sort_spec dir1, const char* name2,
                                              sort_spec dir2)
{
    return (std::async(std::launch::async, [name1, dir1, name2, dir2, this]() -> void {
        this->sort<T1, T2, Ts...>(name1, dir1, name2, dir2);
    }));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename... Ts>
std::future<void> DataFrame<I, H>::sort_async(const char* name1, sort_spec dir1, const char* name2,
                                              sort_spec dir2, const char* name3, sort_spec dir3)
{
    return (std::async(std::launch::async, [name1, dir1, name2, dir2, name3, dir3, this]() -> void {
        this->sort<T1, T2, T3, Ts...>(name1, dir1, name2, dir2, name3, dir3);
    }));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename... Ts>
std::future<void> DataFrame<I, H>::sort_async(const char* name1, sort_spec dir1, const char* name2,
                                              sort_spec dir2, const char* name3, sort_spec dir3,
                                              const char* name4, sort_spec dir4)
{
    return (std::async(
        std::launch::async, [name1, dir1, name2, dir2, name3, dir3, name4, dir4, this]() -> void {
            this->sort<T1, T2, T3, T4, Ts...>(name1, dir1, name2, dir2, name3, dir3, name4, dir4);
        }));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename... Ts>
std::future<void> DataFrame<I, H>::sort_async(const char* name1, sort_spec dir1, const char* name2,
                                              sort_spec dir2, const char* name3, sort_spec dir3,
                                              const char* name4, sort_spec dir4, const char* name5,
                                              sort_spec dir5)
{
    return (std::async(
        std::launch::async,
        [name1, dir1, name2, dir2, name3, dir3, name4, dir4, name5, dir5, this]() -> void {
            this->sort<T1, T2, T3, T4, T5, Ts...>(name1, dir1, name2, dir2, name3, dir3, name4,
                                                  dir4, name5, dir5);
        }));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <Algorithm alg, typename F, typename T, typename... Ts>
DataFrame<I, H> DataFrame<I, H>::groupby(F&& func, const char* gb_col_name,
                                         sort_state already_sorted) const
{
    DataFrame tmp_df = *this;
    auto& key_vec    = tmp_df.get_column<T>(gb_col_name);
    auto& d_vec      = tmp_df.get_column<uint64_t>("duration");

    if (already_sorted == sort_state::not_sorted)
        tmp_df.sort<alg, T, Ts...>(gb_col_name, sort_spec::ascen);
    // auto& index_vec    = tmp_df.get_index();
    // auto& duration_vec = tmp_df.get_column<uint64_t>("duration");

    // for (uint64_t i = 0; i < key_vec.size(); i++) {
    //     std::cout << *(index_vec[i]) << " " << static_cast<int>(*key_vec[i]) << " "
    //               << *duration_vec[i] << std::endl;
    // }
    // abort();

    DataFrame result;

    for (const auto& iter : tmp_df.column_tb_) {
        add_col_functor_<Ts...> functor(iter.first.c_str(), result);

        tmp_df.data_[iter.second].change(functor);
    }

    size_type marker = 0;

    if (!::strcmp(gb_col_name, DF_INDEX_COL_NAME)) {  // Index
        const size_type vec_size = tmp_df.indices_.size();
        for (size_type i = 0; i < vec_size; ++i) {
            bool res;
            {
                RootDereferenceScope scope;
                res = *tmp_df.indices_.get_const_lite_iter(i, scope) != *tmp_df.indices_.get_lite_iter(marker, scope);
            }
            if (res) {
                {
                    RootDereferenceScope scope;
                    result.append_index(*tmp_df.indices_.get_const_lite_iter(marker, scope), scope);
                }
                for (const auto& iter : tmp_df.column_tb_) {
                    groupby_functor_<F, Ts...> functor(iter.first.c_str(), marker, i,
                                                       tmp_df.indices_, func, result);

                    tmp_df.data_[iter.second].change(functor);
                }

                marker = i;
            }
        }
        if (marker < vec_size) {
            {
                RootDereferenceScope scope;

                result.append_index(*tmp_df.indices_.get_const_lite_iter(vec_size - 1, scope), scope);
            }
            for (const auto& iter : tmp_df.column_tb_) {
                groupby_functor_<F, Ts...> functor(iter.first.c_str(), marker, vec_size,
                                                   tmp_df.indices_, func, result);

                tmp_df.data_[iter.second].change(functor);
            }
        }
    } else {  // Non-index column
        const ColumnVecType<T>& gb_vec = tmp_df.get_column<T>(gb_col_name);

        const size_type vec_size = gb_vec.size();
        for (size_type i = 0; i < vec_size; ++i) {
            bool res;
            {
                RootDereferenceScope scope;
                res = *(gb_vec.get_const_lite_iter(i, scope)) != *(gb_vec.get_const_lite_iter(marker, scope));
            }
            if (res) {
                groupby_functor_<F, IndexType> ts_functor(DF_INDEX_COL_NAME, marker, i,
                                                          tmp_df.indices_, func, result);
                ts_functor(tmp_df.indices_);

                {
                    RootDereferenceScope scope;
                    result.append_column<T>(gb_col_name, *gb_vec.get_const_lite_iter(marker, scope), scope,
                                            nan_policy::dont_pad_with_nans);
                }

                for (const auto& iter : tmp_df.column_tb_) {
                    if (iter.first != gb_col_name) {
                        groupby_functor_<F, Ts...> functor(iter.first.c_str(), marker, i,
                                                           tmp_df.indices_, func, result);
                        tmp_df.data_[iter.second].change(functor);
                    }
                }

                marker = i;
            }
        }

        if (marker < vec_size) {
            groupby_functor_<F, IndexType> ts_functor(DF_INDEX_COL_NAME, marker, vec_size,
                                                      tmp_df.indices_, func, result);

            ts_functor(tmp_df.indices_);

            {
                RootDereferenceScope scope;
                result.append_column<T>(gb_col_name, *gb_vec.get_const_lite_iter(vec_size - 1, scope), scope,
                                        nan_policy::dont_pad_with_nans);
            }

            for (const auto& iter : tmp_df.column_tb_) {
                if (iter.first != gb_col_name) {
                    groupby_functor_<F, Ts...> functor(iter.first.c_str(), marker, vec_size,
                                                       tmp_df.indices_, func, result);

                    tmp_df.data_[iter.second].change(functor);
                }
            }
        }
    }

    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename F, typename T1, typename T2, typename... Ts>
DataFrame<I, H> DataFrame<I, H>::groupby(F&& func, const char* gb_col_name1,
                                         const char* gb_col_name2, sort_state already_sorted) const
{
    DataFrame tmp_df = *this;

    if (already_sorted == sort_state::not_sorted)
        tmp_df.sort<T1, T2, Ts...>(gb_col_name1, sort_spec::ascen, gb_col_name2, sort_spec::ascen);

    DataFrame result;

    for (const auto& iter : tmp_df.column_tb_) {
        add_col_functor_<Ts...> functor(iter.first.c_str(), result);

        tmp_df.data_[iter.second].change(functor);
    }

    const bool index_col_involved = !::strcmp(gb_col_name2, DF_INDEX_COL_NAME);

    const ColumnVecType<T1>* gb_vec1{nullptr};
    const ColumnVecType<T2>* gb_vec2{nullptr};

    if (!::strcmp(gb_col_name1, DF_INDEX_COL_NAME))
        gb_vec1 = reinterpret_cast<const ColumnVecType<T1>*>(&tmp_df.indices_);
    else
        gb_vec1 = &(tmp_df.get_column<T1>(gb_col_name1));

    if (!::strcmp(gb_col_name2, DF_INDEX_COL_NAME))
        gb_vec2 = reinterpret_cast<const ColumnVecType<T2>*>(&tmp_df.indices_);
    else
        gb_vec2 = &(tmp_df.get_column<T2>(gb_col_name2));

    size_type marker         = 0;
    const size_type vec_size = std::min(gb_vec1->size(), gb_vec2->size());

    if (index_col_involved) {  // Index is involved
        for (size_type i = 0; i < vec_size; ++i) {
            if (*tmp_df.indices_[i] != *tmp_df.indices_[marker]) {
                result.append_index(*tmp_df.indices_[marker]);
                for (const auto& iter : tmp_df.column_tb_) {
                    groupby_functor_<F, Ts...> functor(iter.first.c_str(), marker, i,
                                                       tmp_df.indices_, func, result);

                    tmp_df.data_[iter.second].change(functor);
                }

                marker = i;
            }
        }
        if (marker < vec_size) {
            result.append_index(*tmp_df.indices_[vec_size - 1]);
            for (const auto& iter : tmp_df.column_tb_) {
                groupby_functor_<F, Ts...> functor(iter.first.c_str(), marker, vec_size,
                                                   tmp_df.indices_, func, result);

                tmp_df.data_[iter.second].change(functor);
            }
        }
    } else {  // Index is not involved
        for (size_type i = 0; i < vec_size; ++i) {
            if (*(gb_vec2->at(i)) != *(gb_vec2->at(marker))) {
                groupby_functor_<F, IndexType> ts_functor(DF_INDEX_COL_NAME, marker, i,
                                                          tmp_df.indices_, func, result);

                ts_functor(tmp_df.indices_);
                result.append_column<T2>(gb_col_name2, *(gb_vec2->at(marker)),
                                         nan_policy::dont_pad_with_nans);

                for (const auto& iter : tmp_df.column_tb_) {
                    if (iter.first != gb_col_name2) {
                        groupby_functor_<F, Ts...> functor(iter.first.c_str(), marker, i,
                                                           tmp_df.indices_, func, result);

                        tmp_df.data_[iter.second].change(functor);
                    }
                }

                marker = i;
            }
        }

        if (marker < vec_size) {
            groupby_functor_<F, IndexType> ts_functor(DF_INDEX_COL_NAME, marker, vec_size,
                                                      tmp_df.indices_, func, result);

            ts_functor(tmp_df.indices_);
            result.append_column<T2>(gb_col_name2, *(gb_vec2->at(vec_size - 1)),
                                     nan_policy::dont_pad_with_nans);

            for (const auto& iter : tmp_df.column_tb_) {
                if (iter.first != gb_col_name2) {
                    groupby_functor_<F, Ts...> functor(iter.first.c_str(), marker, vec_size,
                                                       tmp_df.indices_, func, result);

                    tmp_df.data_[iter.second].change(functor);
                }
            }
        }
    }

    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename F, typename T, typename... Ts>
std::future<DataFrame<I, H>> DataFrame<I, H>::groupby_async(F&& func, const char* gb_col_name,
                                                            sort_state already_sorted) const
{
    return (std::async(
        std::launch::async,
        [func = std::forward<F>(func), gb_col_name, already_sorted, this]() mutable -> DataFrame {
            return (this->groupby<F, T, Ts...>(std::move(func), gb_col_name, already_sorted));
        }));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename F, typename T1, typename T2, typename... Ts>
std::future<DataFrame<I, H>> DataFrame<I, H>::groupby_async(F&& func, const char* gb_col_name1,
                                                            const char* gb_col_name2,
                                                            sort_state already_sorted) const
{
    return (std::async(std::launch::async,
                       [func = std::forward<decltype(func)>(func), gb_col_name1, gb_col_name2,
                        already_sorted, this]() mutable -> DataFrame {
                           return (this->groupby<F, T1, T2, Ts...>(std::move(func), gb_col_name1,
                                                                   gb_col_name2, already_sorted));
                       }));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
StdDataFrame<T> DataFrame<I, H>::value_counts(const char* col_name) const
{
    const ColumnVecType<T>& vec = get_column<T>(col_name);
    auto hash_func              = [](std::reference_wrapper<const T> v) -> std::size_t {
        return (std::hash<T>{}(v.get()));
    };
    auto equal_func = [](std::reference_wrapper<const T> lhs,
                         std::reference_wrapper<const T> rhs) -> bool {
        return (lhs.get() == rhs.get());
    };

    std::unordered_map<typename std::reference_wrapper<const T>::type, size_type,
                       decltype(hash_func), decltype(equal_func)>
        values_map(vec.size(), hash_func, equal_func);
    size_type nan_count = 0;

    // take care of nans
    for (auto citer : vec) {
        if (_is_nan<T>(citer)) {
            ++nan_count;
            continue;
        }

        auto insert_result = values_map.emplace(std::ref(citer), 1);

        if (insert_result.second == false) insert_result.first->second += 1;
    }

    std::vector<T> res_indices;
    std::vector<size_type> counts;

    counts.reserve(values_map.size());
    res_indices.reserve(values_map.size());

    for (const auto citer : values_map) {
        res_indices.push_back(citer.first);
        counts.emplace_back(citer.second);
    }
    if (nan_count > 0) {
        res_indices.push_back(_get_nan<T>());
        counts.emplace_back(nan_count);
    }

    StdDataFrame<T> result_df;

    result_df.load_index(std::move(res_indices));
    result_df.load_column("counts", std::move(counts));

    return (result_df);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename F, typename... Ts>
DataFrame<I, H> DataFrame<I, H>::bucketize(F&& func, const IndexType& bucket_interval) const
{
    DataFrame result;

    for (const auto& iter : column_tb_) {
        add_col_functor_<Ts...> functor(iter.first.c_str(), result);

        data_[iter.second].change(functor);
    }

    for (const auto& iter : column_tb_) {
        bucket_functor_<F, Ts...> functor(iter.first.c_str(), indices_, bucket_interval, func,
                                          result);

        data_[iter.second].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename F, typename... Ts>
std::future<DataFrame<I, H>> DataFrame<I, H>::bucketize_async(
    F&& func, const IndexType& bucket_interval) const
{
    return (std::async(std::launch::async, &DataFrame::bucketize<F, Ts...>, this, std::move(func),
                       std::cref(bucket_interval)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename V>
DataFrame<I, H> DataFrame<I, H>::transpose(IndexVecType&& indices, const V& current_col_order,
                                           const V& new_col_names) const
{
    const size_type num_cols = column_tb_.size();

    if (current_col_order.size() != num_cols)
        throw InconsistentData(
            "DataFrame::transpose(): ERROR: "
            "Length of current_col_order is not equal "
            "to number of columns");
    if (new_col_names.size() != indices_.size())
        throw InconsistentData(
            "DataFrame::transpose(): ERROR: "
            "Length of new_col_names is not equal "
            "to number of rows");

    std::vector<const std::vector<T>*> current_cols;

    current_cols.reserve(num_cols);
    for (const auto citer : current_col_order) current_cols.push_back(&(get_column<T>(citer)));

    std::vector<std::vector<T>> trans_cols(indices_.size());
    DataFrame df;
    size_type idx = 0;

    for (size_type i = 0; i < indices_.size(); ++i) {
        trans_cols[i].reserve(num_cols);
        for (size_type j = 0; j < num_cols; ++j) {
            if (current_cols[j]->size() > i)
                trans_cols[i].push_back((*(current_cols[j]))[i]);
            else
                trans_cols[i].push_back(_get_nan<T>());
        }
    }

    df.load_index(std::move(indices));
    for (size_type i = 0; i < new_col_names.size(); ++i)
        df.load_column(&(new_col_names[i][0]), std::move(trans_cols[i]));

    return (df);
}

}  // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
