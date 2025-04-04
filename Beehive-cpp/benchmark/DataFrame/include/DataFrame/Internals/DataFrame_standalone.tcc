// Hossein Moein
// December 30, 2019
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

// ----------------------------------------------------------------------------

#include "async/scoped_inline_task.hpp"
#include "cache/accessor.hpp"
#include "option.hpp"
#include "utils/parallel.hpp"
namespace hmdf
{
template <typename T, size_t GroupSize>
static inline void _sort_by_sorted_index_(FarLib::FarVector<T, GroupSize>& to_be_sorted,
                                          FarLib::FarVector<size_t>& sorting_idxs, size_t idx_s)
{
    using namespace FarLib;
    using namespace FarLib::cache;
    if (idx_s > 0) {
        idx_s -= 1;
        struct Scope : public RootDereferenceScope {
            decltype(sorting_idxs.lbegin()) sorting_idxs_i_it;
            LiteAccessor<size_t, true> sorting_idxs_sorting_idxs_i_acc;
            LiteAccessor<T, true> to_be_sorted_j_acc;
            LiteAccessor<T, true> to_be_sorted_sorting_idxs_j_acc;

            void pin() const override
            {
                sorting_idxs_i_it.pin();
                sorting_idxs_sorting_idxs_i_acc.pin();
                to_be_sorted_j_acc.pin();
                to_be_sorted_sorting_idxs_j_acc.pin();
            }

            void unpin() const override
            {
                sorting_idxs_i_it.unpin();
                sorting_idxs_sorting_idxs_i_acc.unpin();
                to_be_sorted_j_acc.unpin();
                to_be_sorted_sorting_idxs_j_acc.unpin();
            }
        } scope;
        scope.sorting_idxs_i_it = sorting_idxs.lbegin(scope);
        for (size_t i = 0; i < idx_s; ++i, scope.sorting_idxs_i_it.next(scope)) {
            // while the element i is not yet in place
            ON_MISS_BEGIN
            ON_MISS_END
            while (true) {
                const size_t j                        = *(scope.sorting_idxs_i_it);
                scope.sorting_idxs_sorting_idxs_i_acc = sorting_idxs.at_mut(j, scope, __on_miss__);
                if (j == *(scope.sorting_idxs_sorting_idxs_i_acc)) {
                    break;
                }
                scope.to_be_sorted_j_acc              = to_be_sorted.at_mut(j, scope, __on_miss__);
                scope.to_be_sorted_sorting_idxs_j_acc = to_be_sorted.at_mut(
                    *(scope.sorting_idxs_sorting_idxs_i_acc), scope, __on_miss__);
                std::swap(*(scope.to_be_sorted_j_acc), *(scope.to_be_sorted_sorting_idxs_j_acc));
                std::swap(*(scope.sorting_idxs_i_it), *(scope.sorting_idxs_sorting_idxs_i_acc));
            }
            /* the meaning of code above
            while (*sorting_idxs[i] != *sorting_idxs[*sorting_idxs[i]]) {
                 // swap it with the element at its final place
                 const size_t j = *sorting_idxs[i];

                 std::swap(*to_be_sorted[j], *to_be_sorted[*sorting_idxs[j]]);
                 std::swap(*sorting_idxs[i], *sorting_idxs[j]);
            } */
        }
    }
}

template <Algorithm alg, typename T>
static inline void _sort_by_sorted_index_copy_(FarLib::FarVector<T>& to_be_sorted,
                                               FarLib::FarVector<size_t>& sorting_idxs,
                                               size_t idx_s)
{
    using namespace FarLib;
    using namespace FarLib::cache;
    FarLib::FarVector<T> result;
    result.template resize<true>(to_be_sorted.size());
    const size_t thread_cnt =
        alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count();
    // aligned to group
    const size_t block =
        (sorting_idxs.groups_count() + thread_cnt - 1) / thread_cnt * sorting_idxs.GROUP_SIZE;
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            ON_MISS_BEGIN
            uthread::yield();
            ON_MISS_END
            using idx_it_t        = decltype(sorting_idxs.clbegin());
            using to_be_sort_it_t = decltype(to_be_sorted.clbegin());
            using result_acc_t    = decltype(result.at_mut(0, scope, __on_miss__));
            struct Scope : public DereferenceScope {
                idx_it_t idx_it;
                to_be_sort_it_t to_be_sort_it;
                result_acc_t result_acc;

                void pin()
                {
                    idx_it.pin();
                    to_be_sort_it.pin();
                    result_acc.pin();
                }

                void unpin()
                {
                    idx_it.unpin();
                    to_be_sort_it.unpin();
                    result_acc.unpin();
                }

                void next(__DMH__)
                {
                    idx_it.next(*this, __on_miss__);
                    to_be_sort_it.next(*this, __on_miss__);
                }

                void next()
                {
                    idx_it.next(*this);
                    to_be_sort_it.next(*this);
                }

                void next_group()
                {
                    idx_it.next_group(*this);
                }

                void next_to_be_sorted()
                {
                    to_be_sort_it.next(*this);
                }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}

            } scp(&scope);
            const size_t idx_start = i * block;
            const size_t idx_end   = std::min(idx_start + block, to_be_sorted.size());
            if (idx_start >= idx_end) {
                return;
            }
            if constexpr (alg == UTHREAD) {
                scp.idx_it        = sorting_idxs.get_const_lite_iter(idx_start, scp, __on_miss__,
                                                                     idx_start, idx_end);
                scp.to_be_sort_it = to_be_sorted.get_const_lite_iter(idx_start, scp, __on_miss__,
                                                                     idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end; idx++, scp.next(__on_miss__)) {
                    scp.result_acc    = result.at_mut(*(scp.idx_it), scp, __on_miss__);
                    *(scp.result_acc) = *(scp.to_be_sort_it);
                }
            } else if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
                // TODO pararoutine
                ON_MISS_BEGIN
                ON_MISS_END
                scp.idx_it = sorting_idxs.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                assert(scp.idx_it.at_local());
                scp.to_be_sort_it =
                    to_be_sorted.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end;
                     idx += sorting_idxs.GROUP_SIZE, scp.next_group()) {
                    auto& idx_group = *scp.idx_it.get_group_accessor();
                    for (size_t ii = 0; ii < std::min(idx_end - idx, sorting_idxs.GROUP_SIZE);
                         ii++, scp.next_to_be_sorted()) {
                        scp.result_acc    = result.at_mut(idx_group[ii], scp, __on_miss__);
                        *(scp.result_acc) = *scp.to_be_sort_it;
                    }
                }
            } else {
                ERROR("algorithm dont exist");
            }
        });
    // to_be_sorted.clear();
    std::swap(result, to_be_sorted);
}

// ----------------------------------------------------------------------------

template <typename V, typename T, size_t N>
inline static void _replace_vector_vals_(V& data_vec, const std::array<T, N>& old_values,
                                         const std::array<T, N>& new_values, size_t& count,
                                         int limit)
{
    const size_t vec_s = data_vec.size();

    for (size_t i = 0; i < N; ++i) {
        for (size_t j = 0; j < vec_s; ++j) {
            if (limit >= 0 && count >= static_cast<size_t>(limit)) return;
            if (old_values[i] == data_vec[j]) {
                data_vec[j] = new_values[i];
                count += 1;
            }
        }
    }
}

// ----------------------------------------------------------------------------

template <typename S, typename T>
inline static S& _write_json_df_index_(S& o, const T& value)
{
    return (o << value);
}

// ----------------------------------------------------------------------------

template <typename S>
inline static S& _write_json_df_index_(S& o, const DateTime& value)
{
    return (o << value.time() << '.' << value.nanosec());
}

// ----------------------------------------------------------------------------

template <typename S>
inline static S& _write_json_df_index_(S& o, const std::string& value)
{
    return (o << '"' << value << '"');
}

// ----------------------------------------------------------------------------

inline static void _get_token_from_file_(std::ifstream& file, char delim, char* value,
                                         char alt_delim = '\0')
{
    char c;
    int count = 0;

    while (file.get(c))
        if (c == delim) {
            break;
        } else if (c == alt_delim) {
            file.unget();
            break;
        } else {
            value[count++] = c;
        }

    value[count] = 0;
}

// ----------------------------------------------------------------------------

template <typename T, typename V>
inline static void _col_vector_push_back_(V& vec, std::ifstream& file,
                                          T (*converter)(const char*, char**, int),
                                          io_format file_type = io_format::csv)
{
    char value[8192];
    char c = 0;

    while (file.get(c)) {
        if (file_type == io_format::csv && c == '\n')
            break;
        else if (file_type == io_format::json && c == ']')
            break;
        file.unget();
        _get_token_from_file_(file, ',', value, file_type == io_format::json ? ']' : '\0');
        vec.push_back(static_cast<T>(converter(value, nullptr, 0)));
    }
}

// ----------------------------------------------------------------------------

template <typename T, typename V>
inline static void _col_vector_push_back_(V& vec, std::ifstream& file,
                                          T (*converter)(const char*, char**),
                                          io_format file_type = io_format::csv)
{
    char value[8192];
    char c = 0;

    while (file.get(c)) {
        if (file_type == io_format::csv && c == '\n')
            break;
        else if (file_type == io_format::json && c == ']')
            break;
        file.unget();
        _get_token_from_file_(file, ',', value, file_type == io_format::json ? ']' : '\0');
        vec.push_back(static_cast<T>(converter(value, nullptr)));
    }
}

// ----------------------------------------------------------------------------

template <>
inline void _col_vector_push_back_<const char*, std::vector<std::string>>(
    std::vector<std::string>& vec, std::ifstream& file,
    const char* (*converter)(const char*, char**), io_format file_type)
{
    char value[8192];
    char c = 0;

    while (file.get(c)) {
        if (file_type == io_format::csv && c == '\n')
            break;
        else if (file_type == io_format::json && c == ']')
            break;
        file.unget();
        _get_token_from_file_(file, ',', value, file_type == io_format::json ? ']' : '\0');
        vec.push_back(value);
    }
}

// ----------------------------------------------------------------------------

inline void _json_str_col_vector_push_back_(std::vector<std::string>& vec, std::ifstream& file)
{
    char value[1024];
    char c = 0;

    while (file.get(c))
        if (c != ' ' && c != '\n' && c != '\t') {
            file.unget();
            break;
        }

    while (file.get(c)) {
        if (c == ']') break;
        file.unget();

        std::size_t count = 0;

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t') break;
        if (c != '"')
            throw DataFrameError("_json_str_col_vector_push_back_(): ERROR: Expected '\"' (0)");

        while (file.get(c))
            if (c == '"')
                break;
            else
                value[count++] = c;
        if (c != '"') throw DataFrameError("DataFrame::read_json_(): ERROR: Expected '\"' (1)");

        value[count] = 0;
        vec.push_back(value);

        while (file.get(c))
            if (c != ' ' && c != '\n' && c != '\t') break;
        if (c == ']')
            break;
        else if (c != ',')
            throw DataFrameError("_json_str_col_vector_push_back_(): ERROR: Expected ',' (2)");
    }
}

// ----------------------------------------------------------------------------

template <>
inline void _col_vector_push_back_<DateTime, std::vector<DateTime>>(
    std::vector<DateTime>& vec, std::ifstream& file, DateTime (*converter)(const char*, char**),
    io_format file_type)
{
    char value[1024];
    char c = 0;

    while (file.get(c)) {
        if (file_type == io_format::csv && c == '\n')
            break;
        else if (file_type == io_format::json && c == ']')
            break;
        file.unget();
        _get_token_from_file_(file, ',', value, file_type == io_format::json ? ']' : '\0');

        time_t t;
        int n;
        DateTime dt;

#ifdef _WIN32
        ::sscanf(value, "%lld.%d", &t, &n);
#else
        ::sscanf(value, "%ld.%d", &t, &n);
#endif  // _WIN32
        dt.set_time(t, n);
        vec.emplace_back(std::move(dt));
    }
}

// ----------------------------------------------------------------------------

template <typename T>
struct _IdxParserFunctor_ {
    void operator()(std::vector<T>&, std::ifstream& file, io_format file_type = io_format::csv) {}
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<float> {
    inline void operator()(std::vector<float>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtof, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<double> {
    inline void operator()(std::vector<double>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtod, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<long double> {
    inline void operator()(std::vector<long double>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtold, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<int> {
    inline void operator()(std::vector<int>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<long> {
    inline void operator()(std::vector<long>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<long long> {
    inline void operator()(std::vector<long long>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtoll, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<unsigned int> {
    inline void operator()(std::vector<unsigned int>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtoul, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<unsigned long> {
    inline void operator()(std::vector<unsigned long>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtoul, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<unsigned long long> {
    inline void operator()(std::vector<unsigned long long>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtoull, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<std::string> {
    inline void operator()(std::vector<std::string>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        auto converter = [](const char* s, char**) -> const char* { return s; };

        _col_vector_push_back_<const char*, std::vector<std::string>>(vec, file, converter,
                                                                      file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<DateTime> {
    inline void operator()(std::vector<DateTime>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        auto converter = [](const char*, char**) -> DateTime { return DateTime(); };

        _col_vector_push_back_<DateTime, std::vector<DateTime>>(vec, file, converter, file_type);
    }
};

// ----------------------------------------------------------------------------

template <>
struct _IdxParserFunctor_<bool> {
    inline void operator()(std::vector<bool>& vec, std::ifstream& file,
                           io_format file_type = io_format::csv)
    {
        _col_vector_push_back_(vec, file, &::strtol, file_type);
    }
};

// ----------------------------------------------------------------------------

template <typename T>
inline static void _generate_ts_index_(std::vector<T>& index_vec, DateTime& start_di,
                                       time_frequency t_freq, long increment)
{
    switch (t_freq) {
        case time_frequency::annual:
            index_vec.push_back(static_cast<T>(start_di.date()));
            start_di.add_years(increment);
            break;
        case time_frequency::monthly:
            index_vec.push_back(static_cast<T>(start_di.date()));
            start_di.add_months(increment);
            break;
        case time_frequency::weekly:
            index_vec.push_back(static_cast<T>(start_di.date()));
            start_di.add_days(increment * 7);
            break;
        case time_frequency::daily:
            index_vec.push_back(static_cast<T>(start_di.date()));
            start_di.add_days(increment);
            break;
        case time_frequency::hourly:
            index_vec.push_back(static_cast<T>(start_di.time()));
            start_di.add_seconds(increment * 60 * 60);
            break;
        case time_frequency::minutely:
            index_vec.push_back(static_cast<T>(start_di.time()));
            start_di.add_seconds(increment * 60);
            break;
        case time_frequency::secondly:
            index_vec.push_back(static_cast<T>(start_di.time()));
            start_di.add_seconds(increment);
            break;
        case time_frequency::millisecondly:
            index_vec.push_back(static_cast<T>(start_di.long_time()));
            start_di.add_nanoseconds(increment * 1000000);
            break;
        default:
            break;
    }
}

// ----------------------------------------------------------------------------

template <>
inline void _generate_ts_index_<DateTime>(std::vector<DateTime>& index_vec, DateTime& start_di,
                                          time_frequency t_freq, long increment)
{
    index_vec.push_back(start_di);
    switch (t_freq) {
        case time_frequency::annual:
            start_di.add_years(increment);
            break;
        case time_frequency::monthly:
            start_di.add_months(increment);
            break;
        case time_frequency::weekly:
            start_di.add_days(increment * 7);
            break;
        case time_frequency::daily:
            start_di.add_days(increment);
            break;
        case time_frequency::hourly:
            start_di.add_seconds(increment * 60 * 60);
            break;
        case time_frequency::minutely:
            start_di.add_seconds(increment * 60);
            break;
        case time_frequency::secondly:
            start_di.add_seconds(increment);
            break;
        case time_frequency::millisecondly:
            start_di.add_nanoseconds(increment * 1000000);
            break;
        default:
            break;
    }
}

// ----------------------------------------------------------------------------

template <typename S, typename T>
inline static S& _write_csv2_df_header_(S& o, const char* col_name, std::size_t col_size,
                                        char last_delimit)
{
    o << col_name << ':' << col_size << ':';

    if (typeid(T) == typeid(float))
        o << "<float>" << last_delimit;
    else if (typeid(T) == typeid(double))
        o << "<double>" << last_delimit;
    else if (typeid(T) == typeid(long double))
        o << "<longdouble>" << last_delimit;
    else if (typeid(T) == typeid(short int))
        o << "<short>" << last_delimit;
    else if (typeid(T) == typeid(unsigned short int))
        o << "<ushort>" << last_delimit;
    else if (typeid(T) == typeid(int))
        o << "<int>" << last_delimit;
    else if (typeid(T) == typeid(unsigned int))
        o << "<uint>" << last_delimit;
    else if (typeid(T) == typeid(long int))
        o << "<long>" << last_delimit;
    else if (typeid(T) == typeid(long long int))
        o << "<longlong>" << last_delimit;
    else if (typeid(T) == typeid(unsigned long int))
        o << "<ulong>" << last_delimit;
    else if (typeid(T) == typeid(unsigned long long int))
        o << "<ulonglong>" << last_delimit;
    else if (typeid(T) == typeid(std::string))
        o << "<string>" << last_delimit;
    else if (typeid(T) == typeid(bool))
        o << "<bool>" << last_delimit;
    else if (typeid(T) == typeid(DateTime))
        o << "<DateTime>" << last_delimit;
    else
        o << "<N/A>" << last_delimit;
    return (o);
}

// ----------------------------------------------------------------------------

template <typename S, typename T>
inline static S& _write_json_df_header_(S& o, const char* col_name, std::size_t col_size)
{
    o << '"' << col_name << "\":{\"N\":" << col_size << ',';

    if (typeid(T) == typeid(float))
        o << "\"T\":\"float\",";
    else if (typeid(T) == typeid(double))
        o << "\"T\":\"double\",";
    else if (typeid(T) == typeid(long double))
        o << "\"T\":\"longdouble\",";
    else if (typeid(T) == typeid(short int))
        o << "\"T\":\"short\",";
    else if (typeid(T) == typeid(unsigned short int))
        o << "\"T\":\"ushort\",";
    else if (typeid(T) == typeid(int))
        o << "\"T\":\"int\",";
    else if (typeid(T) == typeid(unsigned int))
        o << "\"T\":\"uint\",";
    else if (typeid(T) == typeid(long int))
        o << "\"T\":\"long\",";
    else if (typeid(T) == typeid(long long int))
        o << "\"T\":\"longlong\",";
    else if (typeid(T) == typeid(unsigned long int))
        o << "\"T\":\"ulong\",";
    else if (typeid(T) == typeid(unsigned long long int))
        o << "\"T\":\"ulonglong\",";
    else if (typeid(T) == typeid(std::string))
        o << "\"T\":\"string\",";
    else if (typeid(T) == typeid(bool))
        o << "\"T\":\"bool\",";
    else if (typeid(T) == typeid(DateTime))
        o << "\"T\":\"DateTime\",";
    else
        o << "\"T\":\"N/A\",";
    return (o);
}

// ----------------------------------------------------------------------------

template <typename S, typename T>
inline static S& _write_csv_df_index_(S& o, const T& value)
{
    return (o << value);
}

// ----------------------------------------------------------------------------

template <typename S>
inline static S& _write_csv_df_index_(S& o, const DateTime& value)
{
    return (o << value.time() << '.' << value.nanosec());
}

// ----------------------------------------------------------------------------

template <typename T>
inline static void _get_mem_numbers_(const VectorView<T>& container, size_t& used_mem,
                                     size_t& capacity_mem)
{
    used_mem     = sizeof(T*) * 2;
    capacity_mem = sizeof(T*) * 2;
}

// ----------------------------------------------------------------------------

template <typename T>
inline static void _get_mem_numbers_(const VectorPtrView<T>& container, size_t& used_mem,
                                     size_t& capacity_mem)
{
    used_mem     = container.size() * sizeof(T*);
    capacity_mem = container.capacity() * sizeof(T*);
}

// ----------------------------------------------------------------------------

template <typename T>
inline static void _get_mem_numbers_(const std::vector<T>& container, size_t& used_mem,
                                     size_t& capacity_mem)
{
    used_mem     = container.size() * sizeof(T);
    capacity_mem = container.capacity() * sizeof(T);
}

// ----------------------------------------------------------------------------

//
// Specializing std::hash for tuples
//

// Code from boost
// Reciprocal of the golden ratio helps spread entropy and handles duplicates.

template <typename T>
inline void _hash_combine_(std::size_t& seed, T const& v)
{
    seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

// Recursive template code derived from Matthieu M.
template <typename Tuple, size_t I = std::tuple_size<Tuple>::value - 1>
struct _hash_value_impl_ {
    static inline void apply(size_t& seed, Tuple const& in_tuple)
    {
        _hash_value_impl_<Tuple, I - 1>::apply(seed, in_tuple);
        _hash_combine_(seed, std::get<I>(in_tuple));
    }
};

template <typename Tuple>
struct _hash_value_impl_<Tuple, 0> {
    static inline void apply(size_t& seed, Tuple const& in_tuple)
    {
        _hash_combine_(seed, std::get<0>(in_tuple));
    }
};

}  // namespace hmdf

namespace std
{
template <typename... TT>
struct hash<std::tuple<TT...>> {
    inline size_t operator()(std::tuple<TT...> const& in_tuple) const
    {
        size_t seed = 0;

        hmdf::_hash_value_impl_<std::tuple<TT...>>::apply(seed, in_tuple);
        return (seed);
    }
};

// ----------------------------------------------------------------------------

// Specialized version of std::remove_copy
//

template <typename I, typename O, typename PRE>
inline static O _remove_copy_if_(I first, I last, O d_first, PRE predicate)
{
    for (I i = first; i != last; ++i)
        if (!predicate(std::distance(first, i))) *d_first++ = *i;

    return d_first;
}

}  // namespace std

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
