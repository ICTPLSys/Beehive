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
#include <DataFrame/DataFrameStatsVisitors.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "async/scoped_inline_task.hpp"
#include "cache/accessor.hpp"
#include "option.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
// ----------------------------------------------------------------------------

namespace hmdf
{

template <typename I, typename H>
std::pair<typename DataFrame<I, H>::size_type, typename DataFrame<I, H>::size_type>
DataFrame<I, H>::shape() const
{
    return (std::make_pair(indices_.size(), column_tb_.size()));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
MemUsage DataFrame<I, H>::get_memory_usage(const char* col_name) const
{
    MemUsage result;

    result.index_type_size  = sizeof(IndexType);
    result.column_type_size = sizeof(T);
    _get_mem_numbers_(get_index(), result.index_used_memory, result.index_capacity_memory);
    _get_mem_numbers_(get_column<T>(col_name), result.column_used_memory,
                      result.column_capacity_memory);
    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
typename DataFrame<I, H>::template ColumnVecType<T>& DataFrame<I, H>::get_column(const char* name)
{
    auto iter = column_tb_.find(name);

    if (iter == column_tb_.end()) {
        char buffer[512];

        sprintf(buffer,
                "DataFrame::get_column(): ERROR: "
                "Cannot find column '%s'",
                name);
        throw ColNotFound(buffer);
    }

    DataVec& hv = data_[iter->second];
    const SpinGuard guard(lock_);

    return (hv.template get_vector<T>());
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
bool DataFrame<I, H>::has_column(const char* name) const
{
    auto iter = column_tb_.find(name);

    return (iter != column_tb_.end());
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
const typename DataFrame<I, H>::template ColumnVecType<T>& DataFrame<I, H>::get_column(
    const char* name) const
{
    return (const_cast<DataFrame*>(this)->get_column<T>(name));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <size_t N, typename... Ts>
HeteroVector DataFrame<I, H>::get_row(size_type row_num,
                                      const std::array<const char*, N> col_names) const
{
    if (row_num >= indices_.size()) {
        char buffer[512];

#ifdef _WIN32
        sprintf(buffer, "DataFrame::get_row(): ERROR: There aren't %zu rows",
#else
        sprintf(buffer, "DataFrame::get_row(): ERROR: There aren't %lu rows",
#endif  // _WIN32
                row_num);
        throw BadRange(buffer);
    }

    HeteroVector ret_vec;

    ret_vec.reserve<IndexType>(1);
    ret_vec.push_back(indices_[row_num]);

    get_row_functor_<Ts...> functor(ret_vec, row_num);

    for (auto name_citer : col_names) {
        const auto citer = column_tb_.find(name_citer);

        if (citer == column_tb_.end()) {
            char buffer[512];

            sprintf(buffer, "DataFrame::get_row(): ERROR: Cannot find column '%s'", name_citer);
            throw ColNotFound(buffer);
        }

        data_[citer->second].change(functor);
    }

    return (ret_vec);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <Algorithm alg, typename T>
FarLib::FarVector<T> DataFrame<I, H>::get_col_unique_values(const char* name) const
{
    using namespace FarLib;
    auto hash_func = [](std::reference_wrapper<const T> v) -> std::size_t {
        return (std::hash<T>{}(v.get()));
    };
    auto equal_func = [](std::reference_wrapper<const T> lhs,
                         std::reference_wrapper<const T> rhs) -> bool {
        return (lhs.get() == rhs.get());
    };
    return get_col_unique_values_impl<alg, T>(
        name, std::forward<decltype(hash_func)>(hash_func),
        std::forward<decltype(equal_func)>(equal_func),
        alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count());
}

template <typename I, typename H>
template <Algorithm alg, typename T, typename F>
FarLib::FarVector<T> DataFrame<I, H>::get_col_unique_values(const char* name, F&& func,
                                                            size_t uthread_cnt) const
{
    auto hash_func = [&func](std::reference_wrapper<const T> v) -> std::size_t {
        return (std::hash<size_t>{}(func(v.get())));
    };
    auto equal_func = [&func](std::reference_wrapper<const T> lhs,
                              std::reference_wrapper<const T> rhs) -> bool {
        return (func(lhs.get()) == func(rhs.get()));
    };
    return get_col_unique_values_impl<alg, T>(name, std::forward<decltype(hash_func)>(hash_func),
                                              std::forward<decltype(equal_func)>(equal_func),
                                              uthread_cnt);
}

template <typename I, typename H>
template <Algorithm alg, typename T, typename U, typename F>
std::unordered_map<U, size_t> DataFrame<I, H>::get_column_elem_count(const char* name, F&& func,
                                                                     size_t uthread_cnt) const
{
    using namespace FarLib::cache;
    using namespace FarLib;
    const ColumnVecType<T>& vec = get_column<T>(name);
    std::cout << "vector memory occupied: " << vec.size() * sizeof(T) << std::endl;
    using map_t = std::unordered_map<U, size_t>;
    map_t mp;
    profile::start_work();
    const size_t thread_cnt = uthread_cnt;
    const size_t block      = (vec.size() + thread_cnt - 1) / thread_cnt;
    std::vector<map_t> uthread_maps(thread_cnt);
    for (auto& m : uthread_maps) {
        m.reserve(10000);
    }
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            auto& u_map            = uthread_maps[i];
            const size_t idx_start = i * block;
            const size_t idx_end   = std::min(idx_start + block, vec.size());
            if (idx_start >= idx_end) {
                return;
            }
            vec.template for_each<alg>(
                [&](const T& elem, DereferenceScope& scope) { u_map[func(elem)]++; }, idx_start,
                idx_end, scope);
            profile::thread_end_work();
            // std::cout << "thread time : " << profile::tlpd.work_cycles << std::endl;
        });
    for (auto& m : uthread_maps) {
        // std::cout << "map size: " << m.size() << std::endl;
        for (auto& p : m) {
            mp[p.first] += p.second;
        }
    }
    return mp;
}

template <typename I, typename H>
template <Algorithm alg, typename T, typename HASH_F, typename EQUAL_F>
FarLib::FarVector<T> DataFrame<I, H>::get_col_unique_values_impl(const char* name,
                                                                 HASH_F&& hash_func,
                                                                 EQUAL_F&& equal_func,
                                                                 size_t uthread_cnt) const
{
    // auto sstart = get_cycles();
    using namespace FarLib::cache;
    using namespace FarLib;
    const ColumnVecType<T>& vec = get_column<T>(name);

    std::unordered_set<typename std::reference_wrapper<T>::type, HASH_F, EQUAL_F> table(
        0, hash_func, equal_func);
    bool counted_nan = false;
    FarLib::FarVector<T> result;

    result.reserve(vec.size());
    if constexpr (alg == DEFAULT) {
        RootDereferenceScope scope;
        ON_MISS_BEGIN
        ON_MISS_END
        for (size_t i = 0; i < vec.size(); i++) {
            auto citer = *(vec.at(i, scope, __on_miss__));
            if (_is_nan<T>(citer) && !counted_nan) {
                counted_nan = true;
                result.push_back(_get_nan<T>(), scope);
                continue;
            }

            const auto insert_ret = table.emplace(std::ref(citer));

            if (insert_ret.second) result.push_back(citer, scope);
        }
    } else {
        const size_t thread_cnt = uthread_cnt;
        const size_t block      = (vec.size() + thread_cnt - 1) / thread_cnt;
        std::vector<decltype(table)> uthread_tables;
        for (size_t i = 0; i < thread_cnt; i++) {
            uthread_tables.emplace_back(0, hash_func, equal_func);
        }
        // auto start = get_cycles();
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                auto& u_table          = uthread_tables[i];
                const size_t idx_start = i * block;
                const size_t idx_end   = std::min(idx_start + block, vec.size());
                if (idx_start >= idx_end) {
                    return;
                }
                counted_nan = false;
                vec.template for_each<alg>(
                    [&](const T& t, DereferenceScope& scope) {
                        if (_is_nan<T>(t) && !counted_nan) {
                            counted_nan = true;
                            u_table.emplace(_get_nan<T>());
                            return;
                        }
                        u_table.emplace(std::ref(t));
                    },
                    idx_start, idx_end, scope);
            });
        // auto end = get_cycles();
        // std::cout << "get set: " << end - start << std::endl;
        // start = get_cycles();
        for (auto& s : uthread_tables) {
            table.merge(s);
        }
        // end = get_cycles();
        // std::cout << "table merge: " << end - start << std::endl;
        std::vector<T> std_result(table.begin(), table.end());
        // start = get_cycles();
        result.template resize<true>(table.size());
        // end = get_cycles();
        // std::cout << "result resize: " << end - start << std::endl;
        // aligned to group
        // start = get_cycles();
        const size_t block_fill =
            (result.groups_count() + thread_cnt - 1) / thread_cnt * result.GROUP_SIZE;
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                using res_it_t = decltype(result.lbegin());

                struct Scope : public DereferenceScope {
                    res_it_t res_it;
                    void pin() const override
                    {
                        res_it.pin();
                    }

                    void unpin() const override
                    {
                        res_it.unpin();
                    }

                    void next(__DMH__)
                    {
                        res_it.next(*this, __on_miss__);
                    }

                    void next()
                    {
                        res_it.next(*this);
                    }

                    void next_group()
                    {
                        res_it.next_group(*this);
                    }

                    Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
                } scp(&scope);
                const size_t idx_start = i * block_fill;
                const size_t idx_end   = std::min(result.size(), idx_start + block_fill);
                if (idx_start >= idx_end) {
                    return;
                }
                if constexpr (alg == UTHREAD) {
                    ON_MISS_BEGIN
                    uthread::yield();
                    ON_MISS_END
                    auto std_res_it = std_result.cbegin() + idx_start;
                    scp.res_it =
                        result.get_lite_iter(idx_start, scp, __on_miss__, idx_start, idx_end);
                    for (size_t idx = idx_start; idx < idx_end;
                         idx++, scp.next(__on_miss__), std_res_it++) {
                        *(scp.res_it) = *std_res_it;
                    }
                } else if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
                    scp.res_it = result.get_lite_iter(idx_start, scp, idx_start, idx_end);
                    for (size_t idx = idx_start; idx < idx_end;
                         idx += result.GROUP_SIZE, scp.next_group()) {
                        auto& res_group = *scp.res_it.get_group_accessor();
                        for (size_t ii = 0; ii < std::min(result.GROUP_SIZE, idx_end - idx); ii++) {
                            res_group[ii] = std_result[idx + ii];
                        }
                    }
                } else {
                    ERROR("algorithm dont exist");
                }
            });
        // end = get_cycles();
        // std::cout << "get result: " << end - start << std::endl;
    }
    // auto ssend = get_cycles();
    // std::cout << "get col uniq val impl: " << ssend - sstart << std::endl;
    return (result);
}
// ----------------------------------------------------------------------------

template <typename I, typename H>
template <Algorithm alg, typename... Ts>
void DataFrame<I, H>::multi_visit(Ts... args)
{
    auto args_tuple = std::tuple<Ts...>(args...);
    auto fc         = [this](auto& pa) mutable -> void {
        auto& functor = *(pa.second);

        using T = typename std::remove_reference<decltype(functor)>::type::value_type;
        using V = typename std::remove_const<
                    typename std::remove_reference<decltype(functor)>::type>::type;

        this->visit<alg, T, V>(pa.first, functor);
    };

    for_each_in_tuple_(args_tuple, fc);
    return;
}

// ----------------------------------------------------------------------------
template <typename I, typename H>
template <Algorithm alg, typename T, typename V>
V& DataFrame<I, H>::visit(const char* name, V& visitor)
{
    using namespace FarLib;
    using namespace FarLib::cache;
    auto& vec             = get_column<T>(name);
    const size_type idx_s = indices_.size();
    const size_type min_s = std::min<size_type>(vec.size(), idx_s);
    size_type i           = 0;

    visitor.pre();
    if constexpr (alg == DEFAULT) {
        for (; i < min_s; ++i) visitor(*indices_[i], *vec[i]);
        for (; i < idx_s; ++i) {
            T nan_val = _get_nan<T>();

            visitor(*indices_[i], nan_val);
        }
    } else {
        const size_t thread_cnt = uthread::get_thread_count();
        const size_t block1     = (min_s + thread_cnt - 1) / thread_cnt;
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                using ind_it_t = decltype(indices_.clbegin());
                using vec_it_t = decltype(vec.clbegin());
                struct Scope : public DereferenceScope {
                    ind_it_t ind_it;
                    vec_it_t vec_it;

                    void pin() const override
                    {
                        ind_it.pin();
                        vec_it.pin();
                    }

                    void unpin() const override
                    {
                        ind_it.unpin();
                        vec_it.unpin();
                    }

                    void next(__DMH__)
                    {
                        ind_it.next(*this, __on_miss__);
                        vec_it.next(*this, __on_miss__);
                    }

                    void next()
                    {
                        ind_it.next(*this);
                        vec_it.next(*this);
                    }

                    Scope(DereferenceScope* scope) : DereferenceScope(scope) {}

                } scp(&scope);
                const size_t idx_start = i * block1;
                const size_t idx_end   = std::min(idx_start + block1, min_s);
                if (idx_start >= idx_end) {
                    return;
                }
                if constexpr (alg == UTHREAD) {
                    ON_MISS_BEGIN
                    uthread::yield();
                    ON_MISS_END
                    scp.ind_it = indices_.get_const_lite_iter(idx_start, scp, __on_miss__,
                                                              idx_start, idx_end);
                    scp.vec_it =
                        vec.get_const_lite_iter(idx_start, scp, __on_miss__, idx_start, idx_end);
                    for (size_t idx = idx_start; idx < idx_end; idx++, scp.next(__on_miss__)) {
                        visitor(*(scp.ind_it), *(scp.vec_it));
                    }
                } else if (alg == PARAROUTINE || alg == PREFETCH) {
                    scp.ind_it = indices_.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                    scp.vec_it = vec.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                    for (size_t idx = idx_start; idx < idx_end; idx++, scp.next()) {
                        visitor(*(scp.ind_it), *(scp.vec_it));
                    }
                } else {
                    ERROR("algorithm dont exist");
                }
            });
        const size_t block2 = (idx_s - min_s + thread_cnt - 1) / thread_cnt;
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                const size_t idx_start = i * block2;
                const size_t idx_end   = std::min(idx_start + block2, idx_s - min_s);
                if (idx_start >= idx_end) {
                    return;
                }
                indices_.template for_each<alg>(
                    [&](const I& idx, DereferenceScope& scope) {
                        T nan_val = _get_nan<T>();
                        visitor(idx, nan_val);
                    },
                    idx_start, idx_end, scope);
            });
    }

    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name, V& visitor)
{
    return (std::async(std::launch::async,
                       static_cast<V& (DataFrame::*)(const char*, V&)>(&DataFrame::visit<T, V>),
                       this, name, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name, V& visitor) const
{
    return (
        std::async(std::launch::async,
                   static_cast<V& (DataFrame::*)(const char*, V&) const>(&DataFrame::visit<T, V>),
                   this, name, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename V>
V& DataFrame<I, H>::visit(const char* name1, const char* name2, V& visitor)
{
    auto& vec1              = get_column<T1>(name1);
    auto& vec2              = get_column<T2>(name2);
    const size_type idx_s   = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type min_s   = std::min<size_type>({idx_s, data_s1, data_s2});
    size_type i             = 0;

    visitor.pre();
    for (; i < min_s; ++i) visitor(indices_[i], vec1[i], vec2[i]);
    for (; i < idx_s; ++i) {
        T1 nan_val1 = _get_nan<T1>();
        T2 nan_val2 = _get_nan<T2>();

        visitor(indices_[i], i < data_s1 ? vec1[i] : nan_val1, i < data_s2 ? vec2[i] : nan_val2);
    }
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name1, const char* name2, V& visitor)
{
    return (std::async(
        std::launch::async,
        static_cast<V& (DataFrame::*)(const char*, const char*, V&)>(&DataFrame::visit<T1, T2, V>),
        this, name1, name2, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name1, const char* name2, V& visitor) const
{
    return (std::async(std::launch::async,
                       static_cast<V& (DataFrame::*)(const char*, const char*, V&) const>(
                           &DataFrame::visit<T1, T2, V>),
                       this, name1, name2, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename V>
V& DataFrame<I, H>::visit(const char* name1, const char* name2, const char* name3, V& visitor)
{
    auto& vec1              = get_column<T1>(name1);
    auto& vec2              = get_column<T2>(name2);
    auto& vec3              = get_column<T3>(name3);
    const size_type idx_s   = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type min_s   = std::min<size_type>({idx_s, data_s1, data_s2, data_s3});
    size_type i             = 0;

    visitor.pre();
    for (; i < min_s; ++i) visitor(indices_[i], vec1[i], vec2[i], vec3[i]);
    for (; i < idx_s; ++i) {
        T1 nan_val1 = _get_nan<T1>();
        T2 nan_val2 = _get_nan<T2>();
        T3 nan_val3 = _get_nan<T3>();

        visitor(indices_[i], i < data_s1 ? vec1[i] : nan_val1, i < data_s2 ? vec2[i] : nan_val2,
                i < data_s3 ? vec3[i] : nan_val3);
    }
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name1, const char* name2,
                                             const char* name3, V& visitor)
{
    return (std::async(std::launch::async,
                       static_cast<V& (DataFrame::*)(const char*, const char*, const char*, V&)>(
                           &DataFrame::visit<T1, T2, T3, V>),
                       this, name1, name2, name3, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name1, const char* name2,
                                             const char* name3, V& visitor) const
{
    return (
        std::async(std::launch::async,
                   static_cast<V& (DataFrame::*)(const char*, const char*, const char*, V&) const>(
                       &DataFrame::visit<T1, T2, T3, V>),
                   this, name1, name2, name3, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename V>
V& DataFrame<I, H>::visit(const char* name1, const char* name2, const char* name3,
                          const char* name4, V& visitor)
{
    auto& vec1              = get_column<T1>(name1);
    auto& vec2              = get_column<T2>(name2);
    auto& vec3              = get_column<T3>(name3);
    auto& vec4              = get_column<T4>(name4);
    const size_type idx_s   = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type data_s4 = vec4.size();
    const size_type min_s   = std::min<size_type>({idx_s, data_s1, data_s2, data_s3, data_s4});
    size_type i             = 0;

    visitor.pre();
    for (; i < min_s; ++i) visitor(indices_[i], vec1[i], vec2[i], vec3[i], vec4[i]);
    for (; i < idx_s; ++i) {
        T1 nan_val1 = _get_nan<T1>();
        T2 nan_val2 = _get_nan<T2>();
        T3 nan_val3 = _get_nan<T3>();
        T4 nan_val4 = _get_nan<T4>();

        visitor(indices_[i], i < data_s1 ? vec1[i] : nan_val1, i < data_s2 ? vec2[i] : nan_val2,
                i < data_s3 ? vec3[i] : nan_val3, i < data_s4 ? vec4[i] : nan_val4);
    }
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name1, const char* name2,
                                             const char* name3, const char* name4, V& visitor)
{
    return (std::async(
        std::launch::async,
        static_cast<V& (DataFrame::*)(const char*, const char*, const char*, const char*, V&)>(
            &DataFrame::visit<T1, T2, T3, T4, V>),
        this, name1, name2, name3, name4, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name1, const char* name2,
                                             const char* name3, const char* name4, V& visitor) const
{
    return (std::async(
        std::launch::async,
        static_cast<V& (DataFrame::*)(const char*, const char*, const char*, const char*, V&)
                        const>(&DataFrame::visit<T1, T2, T3, T4, V>),
        this, name1, name2, name3, name4, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename V>
V& DataFrame<I, H>::visit(const char* name1, const char* name2, const char* name3,
                          const char* name4, const char* name5, V& visitor)
{
    auto& vec1              = get_column<T1>(name1);
    auto& vec2              = get_column<T2>(name2);
    auto& vec3              = get_column<T3>(name3);
    auto& vec4              = get_column<T4>(name4);
    auto& vec5              = get_column<T5>(name5);
    const size_type idx_s   = indices_.size();
    const size_type data_s1 = vec1.size();
    const size_type data_s2 = vec2.size();
    const size_type data_s3 = vec3.size();
    const size_type data_s4 = vec4.size();
    const size_type data_s5 = vec5.size();
    const size_type min_s =
        std::min<size_type>({idx_s, data_s1, data_s2, data_s3, data_s4, data_s5});
    size_type i = 0;

    visitor.pre();
    for (; i < min_s; ++i) visitor(indices_[i], vec1[i], vec2[i], vec3[i], vec4[i], vec5[i]);
    for (; i < idx_s; ++i) {
        T1 nan_val1 = _get_nan<T1>();
        T2 nan_val2 = _get_nan<T2>();
        T3 nan_val3 = _get_nan<T3>();
        T4 nan_val4 = _get_nan<T4>();
        T5 nan_val5 = _get_nan<T5>();

        visitor(indices_[i], i < data_s1 ? vec1[i] : nan_val1, i < data_s2 ? vec2[i] : nan_val2,
                i < data_s3 ? vec3[i] : nan_val3, i < data_s4 ? vec4[i] : nan_val4,
                i < data_s5 ? vec5[i] : nan_val5);
    }
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name1, const char* name2,
                                             const char* name3, const char* name4,
                                             const char* name5, V& visitor)
{
    return (std::async(
        std::launch::async,
        static_cast<V& (DataFrame::*)(const char*, const char*, const char*, const char*,
                                      const char*, V&)>(&DataFrame::visit<T1, T2, T3, T4, T5, V>),
        this, name1, name2, name3, name4, name5, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename V>
std::future<V&> DataFrame<I, H>::visit_async(const char* name1, const char* name2,
                                             const char* name3, const char* name4,
                                             const char* name5, V& visitor) const
{
    return (std::async(std::launch::async,
                       static_cast<V& (DataFrame::*)(const char*, const char*, const char*,
                                                     const char*, const char*, V&) const>(
                           &DataFrame::visit<T1, T2, T3, T4, T5, V>),
                       this, name1, name2, name3, name4, name5, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename V>
V& DataFrame<I, H>::single_act_visit(const char* name, V& visitor)
{
    auto& vec = get_column<T>(name);

    visitor.pre();
    visitor(indices_.begin(), indices_.end(), vec.begin(), vec.end());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename V>
std::future<V&> DataFrame<I, H>::single_act_visit_async(const char* name, V& visitor)
{
    return (std::async(
        std::launch::async,
        static_cast<V& (DataFrame::*)(const char*, V&)>(&DataFrame::single_act_visit<T, V>), this,
        name, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename V>
std::future<V&> DataFrame<I, H>::single_act_visit_async(const char* name, V& visitor) const
{
    return (std::async(
        std::launch::async,
        static_cast<V& (DataFrame::*)(const char*, V&) const>(&DataFrame::single_act_visit<T, V>),
        this, name, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename V>
V& DataFrame<I, H>::single_act_visit(const char* name1, const char* name2, V& visitor)
{
    const ColumnVecType<T1>& vec1 = get_column<T1>(name1);
    const ColumnVecType<T2>& vec2 = get_column<T2>(name2);

    visitor.pre();
    visitor(indices_.begin(), indices_.end(), vec1.begin(), vec1.end(), vec2.begin(), vec2.end());
    visitor.post();

    return (visitor);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename V>
std::future<V&> DataFrame<I, H>::single_act_visit_async(const char* name1, const char* name2,
                                                        V& visitor)
{
    return (std::async(std::launch::async,
                       static_cast<V& (DataFrame::*)(const char*, const char*, V&)>(
                           &DataFrame::single_act_visit<T1, T2, V>),
                       this, name1, name2, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename V>
std::future<V&> DataFrame<I, H>::single_act_visit_async(const char* name1, const char* name2,
                                                        V& visitor) const
{
    return (std::async(std::launch::async,
                       static_cast<V& (DataFrame::*)(const char*, const char*, V&) const>(
                           &DataFrame::single_act_visit<T1, T2, V>),
                       this, name1, name2, std::ref(visitor)));
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFrame<I, H> DataFrame<I, H>::get_data_by_idx(Index2D<IndexType> range, DereferenceScope &scope) const
{
    const size_t lower = indices_.lower_bound(range.begin, scope);
    const size_t upper = indices_.upper_bound(range.end, scope);
    DataFrame df;

    if (lower != indices_.size()) {
        df.load_index(lower, upper, scope);

        const size_type b_dist = lower;
        const size_type e_dist =
            upper < indices_.size() ? upper : indices_.size();

        for (auto& iter : column_tb_) {
            load_functor_<DataFrame, Ts...> functor(iter.first.c_str(), b_dist, e_dist, df);

            data_[iter.second].change(functor);
        }
    }

    return (df);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFrame<I, H> DataFrame<I, H>::get_data_by_idx(const std::vector<IndexType>& values) const
{
    const std::unordered_set<IndexType> val_table(values.begin(), values.end());
    IndexVecType new_index;
    std::vector<size_type> locations;
    const size_type values_s = values.size();
    const size_type idx_s    = indices_.size();

    new_index.reserve(values_s);
    locations.reserve(values_s);
    for (size_type i = 0; i < idx_s; ++i)
        if (val_table.find(indices_[i]) != val_table.end()) {
            new_index.push_back(indices_[i]);
            locations.push_back(i);
        }

    DataFrame df;

    df.load_index(std::move(new_index));
    for (auto col_citer : column_tb_) {
        sel_load_functor_<size_type, Ts...> functor(col_citer.first.c_str(), locations, idx_s, df);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFrameView<I> DataFrame<I, H>::get_view_by_idx(Index2D<IndexType> range) const
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_idx()");

    auto* nc_this = const_cast<DataFrame<I, H>*>(this);
    auto lower = std::lower_bound(nc_this->indices_.begin(), nc_this->indices_.end(), range.begin);
    auto upper = std::upper_bound(nc_this->indices_.begin(), nc_this->indices_.end(), range.end);
    DataFrameView<IndexType> dfv;

    if (lower != indices_.end()) {
        dfv.indices_ = typename DataFrameView<IndexType>::IndexVecType(&*lower, &*upper);

        const size_type b_dist = std::distance(nc_this->indices_.begin(), lower);
        const size_type e_dist =
            std::distance(nc_this->indices_.begin(),
                          upper < nc_this->indices_.end() ? upper : nc_this->indices_.end());

        for (auto& iter : nc_this->column_tb_) {
            view_setup_functor_<DataFrameView<IndexType>, Ts...> functor(iter.first.c_str(), b_dist,
                                                                         e_dist, dfv);

            nc_this->data_[iter.second].change(functor);
        }
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFramePtrView<I> DataFrame<I, H>::get_view_by_idx(const std::vector<IndexType>& values) const
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_idx()");

    using TheView = DataFramePtrView<IndexType>;

    const std::unordered_set<IndexType> val_table(values.begin(), values.end());
    typename TheView::IndexVecType new_index;
    std::vector<size_type> locations;
    const size_type values_s = values.size();
    const size_type idx_s    = indices_.size();

    new_index.reserve(values_s);
    locations.reserve(values_s);

    auto* nc_this = const_cast<DataFrame<I, H>*>(this);

    for (size_type i = 0; i < idx_s; ++i)
        if (val_table.find(indices_[i]) != val_table.end()) {
            new_index.push_back(&(nc_this->indices_[i]));
            locations.push_back(i);
        }

    TheView dfv;

    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_tb_) {
        sel_load_view_functor_<size_type, Ts...> functor(col_citer.first.c_str(), locations, idx_s,
                                                         dfv);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFrame<I, H> DataFrame<I, H>::get_data_by_loc(Index2D<long> range) const
{
    if (range.begin < 0) range.begin = static_cast<long>(indices_.size()) + range.begin;
    if (range.end < 0) range.end = static_cast<long>(indices_.size()) + range.end;

    if (range.end <= static_cast<long>(indices_.size()) && range.begin <= range.end &&
        range.begin >= 0) {
        DataFrame df;

        df.load_index(indices_.begin() + static_cast<size_type>(range.begin),
                      indices_.begin() + static_cast<size_type>(range.end));

        for (auto& iter : column_tb_) {
            load_functor_<DataFrame, Ts...> functor(iter.first.c_str(),
                                                    static_cast<size_type>(range.begin),
                                                    static_cast<size_type>(range.end), df);

            data_[iter.second].change(functor);
        }

        return (df);
    }

    char buffer[512];

    sprintf(buffer,
            "DataFrame::get_data_by_loc(): ERROR: "
            "Bad begin, end range: %ld, %ld",
            range.begin, range.end);
    throw BadRange(buffer);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFrame<I, H> DataFrame<I, H>::get_data_by_loc(const std::vector<long>& locations) const
{
    const size_type idx_s = indices_.size();
    DataFrame df;
    IndexVecType new_index;

    new_index.reserve(locations.size());
    for (const auto citer : locations) {
        const size_type index = citer >= 0 ? (citer) : (citer) + static_cast<long>(idx_s);

        new_index.push_back(indices_[index]);
    }
    df.load_index(std::move(new_index));

    for (auto col_citer : column_tb_) {
        sel_load_functor_<long, Ts...> functor(col_citer.first.c_str(), locations, idx_s, df);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFrameView<I> DataFrame<I, H>::get_view_by_loc(Index2D<long> range) const
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_loc()");

    const long idx_s = static_cast<long>(indices_.size());

    if (range.begin < 0) range.begin = idx_s + range.begin;
    if (range.end < 0) range.end = idx_s + range.end;

    if (range.end <= idx_s && range.begin <= range.end && range.begin >= 0) {
        DataFrameView<IndexType> dfv;
        auto* nc_this = const_cast<DataFrame<I, H>*>(this);

        dfv.indices_ = typename DataFrameView<IndexType>::IndexVecType(
            &*(nc_this->indices_.begin() + range.begin), &*(nc_this->indices_.begin() + range.end));
        for (const auto& iter : nc_this->column_tb_) {
            view_setup_functor_<DataFrameView<IndexType>, Ts...> functor(
                iter.first.c_str(), static_cast<size_type>(range.begin),
                static_cast<size_type>(range.end), dfv);

            nc_this->data_[iter.second].change(functor);
        }

        return (dfv);
    }

    char buffer[512];

    sprintf(buffer,
            "DataFrame::get_view_by_loc(): ERROR: "
            "Bad begin, end range: %ld, %ld",
            range.begin, range.end);
    throw BadRange(buffer);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFramePtrView<I> DataFrame<I, H>::get_view_by_loc(const std::vector<long>& locations) const
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_loc()");

    using TheView = DataFramePtrView<IndexType>;

    TheView dfv;
    const size_type idx_s = indices_.size();

    typename TheView::IndexVecType new_index;

    auto* nc_this = const_cast<DataFrame<I, H>*>(this);

    new_index.reserve(locations.size());
    for (const auto citer : locations) {
        const size_type index = citer >= 0 ? (citer) : (citer) + static_cast<long>(idx_s);

        new_index.push_back(&(nc_this->indices_[index]));
    }
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_tb_) {
        sel_load_view_functor_<long, Ts...> functor(col_citer.first.c_str(), locations,
                                                    indices_.size(), dfv);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------
template <typename I, typename H>
template <Algorithm alg, bool trivial_opt, typename T, typename F, typename... Ts>
DataFrame<I, H> DataFrame<I, H>::get_data_by_sel(const char* name, F& sel_functor) const
{
    // auto sstart = get_cycles();
    using namespace FarLib;
    using namespace FarLib::cache;
    const ColumnVecType<T>& vec = get_column<T>(name);
    const size_type idx_s       = indices_.size();
    const size_type col_s       = vec.size();
    FarLib::FarVector<size_type> col_indices;
    // col_indices.reserve(idx_s / 2);
    // auto start = get_cycles();
    // profile::reset_all();
    // only available if index = [0...idx_s - 1]
    const size_t thread_cnt =
        alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count();
    // aligned to group
    const size_t block = ((idx_s + vec.GROUP_SIZE - 1) / vec.GROUP_SIZE + thread_cnt - 1) /
                         thread_cnt * vec.GROUP_SIZE;
    std::vector<FarVector<size_t>> uthread_indices(thread_cnt);
    for (auto& v : uthread_indices) {
        v.reserve(block);
    }
    // auto start = get_cycles();
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            auto& u_indices        = uthread_indices[i];
            const size_t idx_start = i * block;
            const size_t idx_end   = std::min(idx_start + block, idx_s);
            if (idx_start >= idx_end) {
                return;
            }
            ON_MISS_BEGIN
            uthread::yield();
            ON_MISS_END
            if constexpr (trivial_opt) {
                using vec_it_t = decltype(vec.clbegin());
                struct Scope : public DereferenceScope {
                    vec_it_t vec_it;
                    void pin() const override
                    {
                        vec_it.pin();
                    }
                    void unpin() const override
                    {
                        vec_it.unpin();
                    }
                    Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
                } scp(&scope);
                if constexpr (alg == UTHREAD) {
                    scp.vec_it = vec.get_const_lite_iter(idx_start, scp, __on_miss__, idx_start,
                                                         idx_end);
                    for (size_t idx = idx_start; idx < idx_end;
                         idx++, scp.vec_it.next(scp, __on_miss__)) {
                        if (sel_functor(idx, *(scp.vec_it))) {
                            u_indices.push_back(idx, scp);
                        }
                    }
                } else if constexpr (alg == PARAROUTINE || alg == PREFETCH) {
                    scp.vec_it = vec.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                    for (size_t idx = idx_start; idx < idx_end;
                         idx += vec.GROUP_SIZE, scp.vec_it.next_group(scp)) {
                        auto& vec_group = *scp.vec_it.get_group_accessor();
                        for (size_t ii = 0; ii < std::min(idx_end - idx, vec.GROUP_SIZE);
                             ii++) {
                            if (sel_functor(idx + ii, vec_group[ii])) {
                                u_indices.push_back(idx + ii, scp);
                            }
                        }
                    }
                } else {
                    ERROR("algorithm dont exist");
                }
            } else {
                using vec_it_t = decltype(vec.clbegin());
                using idx_it_t = decltype(indices_.clbegin());
                struct Scope : public DereferenceScope {
                    vec_it_t vec_it;
                    idx_it_t idx_it;
                    void pin() const override
                    {
                        vec_it.pin();
                        idx_it.pin();
                    }
                    void unpin() const override
                    {
                        vec_it.unpin();
                        idx_it.unpin();
                    }
                    void next(__DMH__)
                    {
                        vec_it.next(*this, __on_miss__);
                        idx_it.next(*this, __on_miss__);
                    }
                    void next()
                    {
                        vec_it.next(*this);
                        idx_it.next(*this);
                    }
                    Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
                } scp(&scope);
                if constexpr (alg == UTHREAD) {
                    scp.vec_it = vec.get_const_lite_iter(idx_start, scp, __on_miss__, idx_start,
                                                         idx_end);
                    scp.idx_it = indices_.get_const_lite_iter(idx_start, scp, __on_miss__,
                                                              idx_start, idx_end);
                    for (size_t idx = idx_start; idx < idx_end; idx++, scp.next(__on_miss__)) {
                        if (sel_functor(*(scp.idx_it), *(scp.vec_it))) {
                            u_indices.push_back(idx, scp);
                        }
                    }
                } else if constexpr (alg == PARAROUTINE || alg == PREFETCH) {
                    scp.vec_it = vec.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                    scp.idx_it =
                        indices_.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                    for (size_t idx = idx_start; idx < idx_end; idx++, scp.next()) {
                        if (sel_functor(*(scp.idx_it), *(scp.vec_it))) {
                            u_indices.push_back(idx, scp);
                        }
                    }
                }
            }
        });
    // auto end = get_cycles();
    // std::cout << "uindices get: " << end - start << std::endl;
    std::vector<size_t> sizes(thread_cnt + 1);
    sizes[0] = 0;
    for (size_t i = 1; i <= thread_cnt; i++) {
        sizes[i] = sizes[i - 1] + uthread_indices[i - 1].size();
    }
    // for (auto siz : sizes) {
    //     std::cout << siz << ", ";
    // }
    // std::cout << std::endl;
    size_t sum_size = sizes[thread_cnt];
    col_indices.template resize<true>(sum_size);
    // start = get_cycles();
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            auto& u_indices = uthread_indices[i];
            using u_it_t    = decltype(u_indices.clbegin());
            using it_t      = decltype(col_indices.lbegin());
            ON_MISS_BEGIN
            uthread::yield();
            ON_MISS_END
            struct Scope : public DereferenceScope {
                u_it_t u_it;
                it_t it;
                void pin() const override
                {
                    u_it.pin();
                    it.pin();
                }
                void unpin() const override
                {
                    u_it.unpin();
                    it.unpin();
                }
                void next(__DMH__)
                {
                    u_it.next(*this, __on_miss__);
                    it.next(*this, __on_miss__);
                }
                void next()
                {
                    u_it.next(*this);
                    it.next(*this);
                }
                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            const size_t idx_start = sizes[i];
            const size_t idx_end   = sizes[i + 1];
            if (idx_start >= idx_end) {
                return;
            }
            if constexpr (alg == UTHREAD) {
                scp.u_it = u_indices.clbegin(scp, __on_miss__);
                scp.it =
                    col_indices.get_lite_iter(idx_start, scp, __on_miss__, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end; idx++, scp.next(__on_miss__)) {
                    *(scp.it) = *(scp.u_it);
                }
            } else if constexpr (alg == PARAROUTINE || alg == PREFETCH) {
                // TODO pararoutine
                scp.u_it = u_indices.clbegin(scp);
                scp.it   = col_indices.get_lite_iter(idx_start, scp, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end; idx++, scp.next()) {
                    *(scp.it) = *(scp.u_it);
                }
            }
        });
    // end = get_cycles();
    // std::cout << "uindice assign back: " << end - start << std::endl;
    // auto end = get_cycles();
    // std::cout << "get col indices: " << end - start << std::endl;
    // profile::print_profile_data();
    // for (int i = 0; i < col_indices.size(); i++) {
    //     std::cout << *col_indices[i] << std::endl;
    // }
    // abort();
    DataFrame df;
    // profile::reset_all();
    // start                  = get_cycles();
    IndexVecType new_index = indices_.template copy_data_by_idx<alg>(col_indices);
    // end                    = get_cycles();
    // std::cout << "new index get: " << end - start << std::endl;
    // profile::print_profile_data();
    // std::cout << "-------------------------------------------------" << std::endl;
    // for (int i = 0; i < col_indices.size(); i++) {
    //     printf("%lu = %lu\n", *new_index[i], *col_indices[i]);
    //     ASSERT(*new_index[i] == *col_indices[i]);
    // }
    // profile::reset_all();
    // start = get_cycles();
    df.load_index(std::move(new_index));
    // end = get_cycles();
    // std::cout << "load index: " << end - start << std::endl;
    // profile::print_profile_data();
    size_t change_time = 0;
    for (auto col_citer : column_tb_) {
        alg_sel_load_functor_<alg, size_type, Ts...> functor(col_citer.first.c_str(), col_indices,
                                                             idx_s, df);
        // profile::reset_all();
        // auto start = get_cycles();
        data_[col_citer.second].change(functor);
        // auto end = get_cycles();
        // std::cout << "change: " << end - start << std::endl;
        // profile::print_profile_data();
        // change_time += end - start;
    }
    // std::cout << "change all: " << change_time << std::endl;
    // auto ssend = get_cycles();
    // std::cout << "get data by sel: " << ssend - sstart << std::endl;
    return (df);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename F, typename... Ts>
DataFramePtrView<I> DataFrame<I, H>::get_view_by_sel(const char* name, F& sel_functor) const
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const ColumnVecType<T>& vec = get_column<T>(name);
    const size_type idx_s       = indices_.size();
    const size_type col_s       = vec.size();
    std::vector<size_type> col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor(indices_[i], vec[i])) col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView dfv;
    typename TheView::IndexVecType new_index;

    auto* nc_this = const_cast<DataFrame<I, H>*>(this);

    new_index.reserve(col_indices.size());
    for (const auto citer : col_indices) new_index.push_back(&(nc_this->indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_tb_) {
        sel_load_view_functor_<size_type, Ts...> functor(col_citer.first.c_str(), col_indices,
                                                         idx_s, dfv);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename F, typename... Ts>
DataFrame<I, H> DataFrame<I, H>::get_data_by_sel(const char* name1, const char* name2,
                                                 F& sel_functor) const
{
    const size_type idx_s         = indices_.size();
    const ColumnVecType<T1>& vec1 = get_column<T1>(name1);
    const ColumnVecType<T2>& vec2 = get_column<T2>(name2);
    const size_type col_s1        = vec1.size();
    const size_type col_s2        = vec2.size();
    const size_type col_s         = std::max(col_s1, col_s2);
    std::vector<size_type> col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor(indices_[i], i < col_s1 ? vec1[i] : _get_nan<T1>(),
                        i < col_s2 ? vec2[i] : _get_nan<T2>()))
            col_indices.push_back(i);

    DataFrame df;
    IndexVecType new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer : col_indices) new_index.push_back(indices_[citer]);
    df.load_index(std::move(new_index));

    for (auto col_citer : column_tb_) {
        sel_load_functor_<size_type, Ts...> functor(col_citer.first.c_str(), col_indices, idx_s,
                                                    df);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename F, typename... Ts>
DataFramePtrView<I> DataFrame<I, H>::get_view_by_sel(const char* name1, const char* name2,
                                                     F& sel_functor) const
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const ColumnVecType<T1>& vec1 = get_column<T1>(name1);
    const ColumnVecType<T2>& vec2 = get_column<T2>(name2);
    const size_type idx_s         = indices_.size();
    const size_type col_s1        = vec1.size();
    const size_type col_s2        = vec2.size();
    const size_type col_s         = std::max(col_s1, col_s2);
    std::vector<size_type> col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor(indices_[i], i < col_s1 ? vec1[i] : _get_nan<T1>(),
                        i < col_s2 ? vec2[i] : _get_nan<T2>()))
            col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView dfv;
    typename TheView::IndexVecType new_index;

    auto* nc_this = const_cast<DataFrame<I, H>*>(this);

    new_index.reserve(col_indices.size());
    for (const auto citer : col_indices) new_index.push_back(&(nc_this->indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_tb_) {
        sel_load_view_functor_<size_type, Ts...> functor(col_citer.first.c_str(), col_indices,
                                                         idx_s, dfv);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename F, typename... Ts>
DataFrame<I, H> DataFrame<I, H>::get_data_by_sel(const char* name1, const char* name2,
                                                 const char* name3, F& sel_functor) const
{
    const size_type idx_s         = indices_.size();
    const ColumnVecType<T1>& vec1 = get_column<T1>(name1);
    const ColumnVecType<T2>& vec2 = get_column<T2>(name2);
    const ColumnVecType<T3>& vec3 = get_column<T3>(name3);
    const size_type col_s1        = vec1.size();
    const size_type col_s2        = vec2.size();
    const size_type col_s3        = vec3.size();
    const size_type col_s         = std::max(std::max(col_s1, col_s2), col_s3);
    std::vector<size_type> col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor(indices_[i], i < col_s1 ? vec1[i] : _get_nan<T1>(),
                        i < col_s2 ? vec2[i] : _get_nan<T2>(),
                        i < col_s3 ? vec3[i] : _get_nan<T3>()))
            col_indices.push_back(i);

    DataFrame df;
    IndexVecType new_index;

    new_index.reserve(col_indices.size());
    for (const auto citer : col_indices) new_index.push_back(indices_[citer]);
    df.load_index(std::move(new_index));

    for (auto col_citer : column_tb_) {
        sel_load_functor_<size_type, Ts...> functor(col_citer.first.c_str(), col_indices, idx_s,
                                                    df);

        data_[col_citer.second].change(functor);
    }

    return (df);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T1, typename T2, typename T3, typename F, typename... Ts>
DataFramePtrView<I> DataFrame<I, H>::get_view_by_sel(const char* name1, const char* name2,
                                                     const char* name3, F& sel_functor) const
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_view_by_sel()");

    const ColumnVecType<T1>& vec1 = get_column<T1>(name1);
    const ColumnVecType<T2>& vec2 = get_column<T2>(name2);
    const ColumnVecType<T3>& vec3 = get_column<T3>(name3);
    const size_type idx_s         = indices_.size();
    const size_type col_s1        = vec1.size();
    const size_type col_s2        = vec2.size();
    const size_type col_s3        = vec3.size();
    const size_type col_s         = std::max(std::max(col_s1, col_s2), col_s3);
    std::vector<size_type> col_indices;

    col_indices.reserve(idx_s / 2);
    for (size_type i = 0; i < col_s; ++i)
        if (sel_functor(indices_[i], i < col_s1 ? vec1[i] : _get_nan<T1>(),
                        i < col_s2 ? vec2[i] : _get_nan<T2>(),
                        i < col_s3 ? vec3[i] : _get_nan<T3>()))
            col_indices.push_back(i);

    using TheView = DataFramePtrView<IndexType>;

    TheView dfv;
    typename TheView::IndexVecType new_index;

    auto* nc_this = const_cast<DataFrame<I, H>*>(this);

    new_index.reserve(col_indices.size());
    for (const auto citer : col_indices) new_index.push_back(&(nc_this->indices_[citer]));
    dfv.indices_ = std::move(new_index);

    for (auto col_citer : nc_this->column_tb_) {
        sel_load_view_functor_<size_type, Ts...> functor(col_citer.first.c_str(), col_indices,
                                                         idx_s, dfv);

        nc_this->data_[col_citer.second].change(functor);
    }

    return (dfv);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFrame<I, H> DataFrame<I, H>::get_data_by_rand(random_policy spec, double n,
                                                  size_type seed) const
{
    bool use_seed           = false;
    size_type n_rows        = static_cast<size_type>(n);
    const size_type index_s = indices_.size();

    if (spec == random_policy::num_rows_with_seed) {
        use_seed = true;
    } else if (spec == random_policy::frac_rows_with_seed) {
        use_seed = true;
        n_rows   = static_cast<size_type>(n * double(index_s));
    } else if (spec == random_policy::frac_rows_no_seed) {
        n_rows = static_cast<size_type>(n * double(index_s));
    }

    if (index_s > 0 && n_rows < index_s - 1) {
        std::random_device rd;
        std::mt19937 gen(rd());

        if (use_seed) gen.seed(static_cast<unsigned int>(seed));

        std::uniform_int_distribution<size_type> dis(0, index_s - 1);
        std::vector<size_type> rand_indices(n_rows);

        for (size_type i = 0; i < n_rows; ++i) rand_indices[i] = dis(gen);
        std::sort(rand_indices.begin(), rand_indices.end());

        IndexVecType new_index;
        size_type prev_value;

        new_index.reserve(n_rows);
        for (size_type i = 0; i < n_rows; ++i) {
            if (i == 0 || rand_indices[i] != prev_value)
                new_index.push_back(indices_[rand_indices[i]]);
            prev_value = rand_indices[i];
        }

        DataFrame df;

        df.load_index(std::move(new_index));
        for (auto& iter : column_tb_) {
            random_load_data_functor_<Ts...> functor(iter.first.c_str(), rand_indices, df);

            data_[iter.second].change(functor);
        }

        return (df);
    }

    char buffer[512];

    sprintf(buffer,
            "DataFrame::get_data_by_rand(): ERROR: "
#ifdef _WIN32
            "Number of rows requested %zu is more than available rows %zu",
#else
            "Number of rows requested %lu is more than available rows %lu",
#endif  // _WIN32
            n_rows, index_s);
    throw BadRange(buffer);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
DataFramePtrView<I> DataFrame<I, H>::get_view_by_rand(random_policy spec, double n,
                                                      size_type seed) const
{
    bool use_seed           = false;
    size_type n_rows        = static_cast<size_type>(n);
    const size_type index_s = indices_.size();

    if (spec == random_policy::num_rows_with_seed) {
        use_seed = true;
    } else if (spec == random_policy::frac_rows_with_seed) {
        use_seed = true;
        n_rows   = static_cast<size_type>(n * double(index_s));
    } else if (spec == random_policy::frac_rows_no_seed) {
        n_rows = static_cast<size_type>(n * double(index_s));
    }

    if (index_s > 0 && n_rows < index_s - 1) {
        std::random_device rd;
        std::mt19937 gen(rd());

        if (use_seed) gen.seed(static_cast<unsigned int>(seed));

        std::uniform_int_distribution<size_type> dis(0, index_s - 1);
        std::vector<size_type> rand_indices(n_rows);

        for (size_type i = 0; i < n_rows; ++i) rand_indices[i] = dis(gen);
        std::sort(rand_indices.begin(), rand_indices.end());

        using TheView = DataFramePtrView<IndexType>;

        typename TheView::IndexVecType new_index;
        size_type prev_value;

        new_index.reserve(n_rows);
        for (size_type i = 0; i < n_rows; ++i) {
            if (i == 0 || rand_indices[i] != prev_value)
                new_index.push_back(const_cast<I*>(&(indices_[rand_indices[i]])));
            prev_value = rand_indices[i];
        }

        TheView dfv;

        dfv.indices_ = std::move(new_index);
        for (auto& iter : column_tb_) {
            random_load_view_functor_<Ts...> functor(iter.first.c_str(), rand_indices, dfv);

            data_[iter.second].change(functor);
        }

        return (dfv);
    }

    char buffer[512];

    sprintf(buffer,
            "DataFrame::get_view_by_rand(): ERROR: "
#ifdef _WIN32
            "Number of rows requested %zu is more than available rows %zu",
#else
            "Number of rows requested %lu is more than available rows %lu",
#endif  // _WIN32
            n_rows, index_s);
    throw BadRange(buffer);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename... Ts>
StdDataFrame<T> DataFrame<I, H>::get_reindexed(const char* col_to_be_index,
                                               const char* old_index_name) const
{
    StdDataFrame<T> result;
    const auto& new_idx       = get_column<T>(col_to_be_index);
    const size_type new_idx_s = result.load_index(new_idx.begin(), new_idx.end());

    if (old_index_name) {
        const auto& curr_idx  = get_index();
        const size_type col_s = curr_idx.size() >= new_idx_s ? new_idx_s : curr_idx.size();

        result.template load_column<IndexType>(old_index_name,
                                               {curr_idx.begin(), curr_idx.begin() + col_s});
    }

    for (auto citer : column_tb_) {
        if (citer.first == col_to_be_index) continue;

        load_functor_<StdDataFrame<T>, Ts...> functor(citer.first.c_str(), 0, new_idx_s, result,
                                                      nan_policy::dont_pad_with_nans);

        data_[citer.second].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename... Ts>
DataFrameView<T> DataFrame<I, H>::get_reindexed_view(const char* col_to_be_index,
                                                     const char* old_index_name) const
{
    static_assert(std::is_base_of<HeteroVector, H>::value,
                  "Only a StdDataFrame can call get_reindexed_view()");

    auto* nc_this = const_cast<DataFrame<I, H>*>(this);
    DataFrameView<T> result;
    auto& new_idx             = nc_this->template get_column<T>(col_to_be_index);
    const size_type new_idx_s = new_idx.size();

    result.indices_ = typename DataFrameView<T>::IndexVecType();
    result.indices_.set_begin_end_special(&*(new_idx.begin()), &(new_idx.back()));
    if (old_index_name) {
        auto& curr_idx        = nc_this->get_index();
        const size_type col_s = curr_idx.size() >= new_idx_s ? new_idx_s : curr_idx.size();

        result.template setup_view_column_<IndexType, typename IndexVecType::iterator>(
            old_index_name, {curr_idx.begin(), curr_idx.begin() + col_s});
    }

    for (auto citer : column_tb_) {
        if (citer.first == col_to_be_index) continue;

        view_setup_functor_<DataFrameView<T>, Ts...> functor(citer.first.c_str(), 0, new_idx_s,
                                                             result);

        nc_this->data_[citer.second].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename... Ts>
std::vector<std::tuple<typename DataFrame<I, H>::ColNameType, typename DataFrame<I, H>::size_type,
                       std::type_index>>
DataFrame<I, H>::get_columns_info() const
{
    std::vector<std::tuple<ColNameType, size_type, std::type_index>> result;

    result.reserve(column_tb_.size());
    for (auto& citer : column_tb_) {
        columns_info_functor_<Ts...> functor(result, citer.first.c_str());

        data_[citer.second].change(functor);
    }

    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename V>
bool DataFrame<I, H>::is_monotonic_increasing_(const V& column)
{
    const size_type col_s = column.size();

    for (size_type i = 1; i < col_s; ++i)
        if (column[i] < column[i - 1]) return (false);
    return (true);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename V>
bool DataFrame<I, H>::is_strictly_monotonic_increasing_(const V& column)
{
    const size_type col_s = column.size();

    for (size_type i = 1; i < col_s; ++i)
        if (column[i] <= column[i - 1]) return (false);
    return (true);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename V>
bool DataFrame<I, H>::is_monotonic_decreasing_(const V& column)
{
    const size_type col_s = column.size();

    for (size_type i = 1; i < col_s; ++i)
        if (column[i] > column[i - 1]) return (false);
    return (true);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename V>
bool DataFrame<I, H>::is_strictly_monotonic_decreasing_(const V& column)
{
    const size_type col_s = column.size();

    for (size_type i = 1; i < col_s; ++i)
        if (column[i] >= column[i - 1]) return (false);
    return (true);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename V>
bool DataFrame<I, H>::is_normal_(const V& column, double epsilon, bool check_for_standard)
{
    using value_type = typename V::value_type;

    const I dummy_idx{I()};
    StatsVisitor<value_type, I> svisit;

    svisit.pre();
    for (auto citer : column) svisit(dummy_idx, citer);
    svisit.post();

    const value_type mean        = static_cast<value_type>(svisit.get_mean());
    const value_type std         = static_cast<value_type>(svisit.get_std());
    const value_type high_band_1 = static_cast<value_type>(mean + std);
    const value_type low_band_1  = static_cast<value_type>(mean - std);
    double count_1               = 0.0;
    const value_type high_band_2 = static_cast<value_type>(mean + std * 2.0);
    const value_type low_band_2  = static_cast<value_type>(mean - std * 2.0);
    double count_2               = 0.0;
    const value_type high_band_3 = static_cast<value_type>(mean + std * 3.0);
    const value_type low_band_3  = static_cast<value_type>(mean - std * 3.0);
    double count_3               = 0.0;

    for (auto citer : column) {
        if (citer >= low_band_1 && citer < high_band_1) {
            count_3 += 1;
            count_2 += 1;
            count_1 += 1;
        } else if (citer >= low_band_2 && citer < high_band_2) {
            count_3 += 1;
            count_2 += 1;
        } else if (citer >= low_band_3 && citer < high_band_3) {
            count_3 += 1;
        }
    }

    const double col_s = static_cast<double>(column.size());

    if (std::fabs((count_1 / col_s) - 0.68) <= epsilon &&
        std::fabs((count_2 / col_s) - 0.95) <= epsilon &&
        std::fabs((count_3 / col_s) - 0.997) <= epsilon) {
        if (check_for_standard)
            return (std::fabs(mean - 0) <= epsilon && std::fabs(std - 1.0) <= epsilon);
        return (true);
    }
    return (false);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename V>
bool DataFrame<I, H>::is_lognormal_(const V& column, double epsilon)
{
    using value_type = typename V::value_type;

    const I dummy_idx{I()};
    StatsVisitor<value_type, I> svisit;
    StatsVisitor<value_type, I> log_visit;

    svisit.pre();
    for (auto citer : column) {
        svisit(dummy_idx, static_cast<value_type>(std::log(citer)));
        log_visit(dummy_idx, citer);
    }
    svisit.post();

    const value_type mean        = static_cast<value_type>(svisit.get_mean());
    const value_type std         = static_cast<value_type>(svisit.get_std());
    const value_type high_band_1 = static_cast<value_type>(mean + std);
    const value_type low_band_1  = static_cast<value_type>(mean - std);
    double count_1               = 0.0;
    const value_type high_band_2 = static_cast<value_type>(mean + std * 2.0);
    const value_type low_band_2  = static_cast<value_type>(mean - std * 2.0);
    double count_2               = 0.0;
    const value_type high_band_3 = static_cast<value_type>(mean + std * 3.0);
    const value_type low_band_3  = static_cast<value_type>(mean - std * 3.0);
    double count_3               = 0.0;

    for (auto citer : column) {
        const double log_val = std::log(citer);

        if (log_val >= low_band_1 && log_val < high_band_1) {
            count_3 += 1;
            count_2 += 1;
            count_1 += 1;
        } else if (log_val >= low_band_2 && log_val < high_band_2) {
            count_3 += 1;
            count_2 += 1;
        } else if (log_val >= low_band_3 && log_val < high_band_3) {
            count_3 += 1;
        }
    }

    const double col_s = static_cast<double>(column.size());

    if (std::fabs((count_1 / col_s) - 0.68) <= epsilon &&
        std::fabs((count_2 / col_s) - 0.95) <= epsilon &&
        std::fabs((count_3 / col_s) - 0.997) <= epsilon &&
        log_visit.get_skew() > 10.0 * svisit.get_skew() &&
        log_visit.get_kurtosis() > 10.0 * svisit.get_kurtosis())
        return (true);
    return (false);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T>
bool DataFrame<I, H>::pattern_match(const char* col_name, pattern_spec pattern,
                                    double epsilon) const
{
    const auto& col = get_column<T>(col_name);

    switch (pattern) {
        case pattern_spec::monotonic_increasing:
            return (is_monotonic_increasing_(col));
        case pattern_spec::strictly_monotonic_increasing:
            return (is_strictly_monotonic_increasing_(col));
        case pattern_spec::monotonic_decreasing:
            return (is_monotonic_decreasing_(col));
        case pattern_spec::strictly_monotonic_decreasing:
            return (is_strictly_monotonic_decreasing_(col));
        case pattern_spec::normally_distributed:
            return (is_normal_(col, epsilon, false));
        case pattern_spec::standard_normally_distributed:
            return (is_normal_(col, epsilon, true));
        case pattern_spec::lognormally_distributed:
            return (is_lognormal_(col, epsilon));
        default:
            throw NotImplemented(
                "pattern_match(): "
                "Requested pattern is not implemented");
    }
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename DF, typename F>
std::vector<T> DataFrame<I, H>::combine(const char* col_name, const DF& rhs, F& functor) const
{
    const auto& lhs_col   = get_column<T>(col_name);
    const auto& rhs_col   = rhs.template get_column<T>(col_name);
    const size_type col_s = std::min(lhs_col.size(), rhs_col.size());
    std::vector<T> result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i)
        result.push_back(std::move(functor(lhs_col[i], rhs_col[i])));

    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename DF1, typename DF2, typename F>
std::vector<T> DataFrame<I, H>::combine(const char* col_name, const DF1& df1, const DF2& df2,
                                        F& functor) const
{
    const auto& lhs_col   = get_column<T>(col_name);
    const auto& df1_col   = df1.template get_column<T>(col_name);
    const auto& df2_col   = df2.template get_column<T>(col_name);
    const size_type col_s = std::min<size_type>({lhs_col.size(), df1_col.size(), df2_col.size()});
    std::vector<T> result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i)
        result.push_back(std::move(functor(lhs_col[i], df1_col[i], df2_col[i])));

    return (result);
}

// ----------------------------------------------------------------------------

template <typename I, typename H>
template <typename T, typename DF1, typename DF2, typename DF3, typename F>
std::vector<T> DataFrame<I, H>::combine(const char* col_name, const DF1& df1, const DF2& df2,
                                        const DF3& df3, F& functor) const
{
    const auto& lhs_col = get_column<T>(col_name);
    const auto& df1_col = df1.template get_column<T>(col_name);
    const auto& df2_col = df2.template get_column<T>(col_name);
    const auto& df3_col = df3.template get_column<T>(col_name);
    const size_type col_s =
        std::min<size_type>({lhs_col.size(), df1_col.size(), df2_col.size(), df3_col.size()});
    std::vector<T> result;

    result.reserve(col_s);
    for (size_type i = 0; i < col_s; ++i)
        result.push_back(std::move(functor(lhs_col[i], df1_col[i], df2_col[i], df3_col[i])));

    return (result);
}

}  // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
