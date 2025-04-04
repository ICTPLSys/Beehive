#include <DataFrame/DataFrame.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <string>

#include "async/scoped_inline_task.hpp"
#include "cache/accessor.hpp"
#include "option.hpp"
#include "rdma/client.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
#include "utils/profile.hpp"
// #define STANDALONE
// simple: ~74M, full: ~16G
// #define SIMPLE_BENCH

#ifdef STANDALONE
#include "rdma/server.hpp"
#endif

using namespace FarLib;
using namespace FarLib::rdma;
using namespace FarLib::cache;
using namespace std::chrono_literals;
using namespace hmdf;

// Download dataset at https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
// The following code is implemented based on the format of 2016 datasets.
static double haversine(double lat1, double lon1, double lat2, double lon2)
{
    // Distance between latitudes and longitudes
    double dLat = (lat2 - lat1) * M_PI / 180.0;
    double dLon = (lon2 - lon1) * M_PI / 180.0;

    // Convert to radians.
    lat1 = lat1 * M_PI / 180.0;
    lat2 = lat2 * M_PI / 180.0;

    // Apply formulae.
    double a   = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1) * cos(lat2);
    double rad = 6371;
    double c   = 2 * asin(sqrt(a));
    return rad * c;
}

StdDataFrame<uint64_t> load_data(const char* file_path)
{
    return read_csv<-1, int, SimpleTime, SimpleTime, int, double, double, double, int, char, double,
                    double, int, double, double, double, double, double, double, double>(
        file_path, "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "pickup_longitude", "pickup_latitude", "RatecodeID", "store_and_fwd_flag",
        "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount");
}

void clear_data_hetero_vector()
{
    HeteroVector::clear_all_hvector<int>();
    HeteroVector::clear_all_hvector<SimpleTime>();
    HeteroVector::clear_all_hvector<double>();
    HeteroVector::clear_all_hvector<char>();
}

template <Algorithm alg = DEFAULT_ALG>
void print_number_vendor_ids_and_unique(StdDataFrame<uint64_t>& df)
{
    std::cout << "print_number_vendor_ids_and_unique()" << std::endl;
    std::cout << "Number of vendor_ids in the train dataset: "
              << df.get_column<int>("VendorID").size() << std::endl;
    // auto start = get_cycles();
    std::cout << "Number of unique vendor_ids in the train dataset:"
              << df.get_col_unique_values<alg, int>("VendorID").size() << std::endl;
    std::cout << std::endl;
    // auto end = get_cycles();
    // std::cout << "print_number_vendor_ids_and_unique: " << end - start << std::endl;
}

template <Algorithm alg = DEFAULT_ALG, bool trivial_opt = DEFAULT_TRIVIAL_OPT>
void print_passage_counts_by_vendor_id(StdDataFrame<uint64_t>& df, int vendor_id)
{
    std::cout << "print_passage_counts_by_vendor_id(vendor_id), vendor_id = " << vendor_id
              << std::endl;

    auto sel_vendor_functor = [&](const uint64_t&, const int& vid) -> bool {
        return vid == vendor_id;
    };
    std::map<int, int> passage_count_map;
    // auto start = get_cycles();
    // profile::reset_all();
    auto sel_df = df.get_data_by_sel<alg, trivial_opt, int, decltype(sel_vendor_functor), int,
                                     SimpleTime, double, char>("VendorID", sel_vendor_functor);
    // auto end    = get_cycles();
    // std::cout << "sel df get: " << end - start << std::endl;
    // profile::print_profile_data();
    auto& passage_count_vec = sel_df.template get_column<int>("passenger_count");
    // profile::reset_all();
    if constexpr (alg == DEFAULT) {
        for (auto passage_count : passage_count_vec) {
            passage_count_map[passage_count]++;
        }
    } else {
        const size_t thread_cnt =
            alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count();
        // aligned to group
        const size_t block = (passage_count_vec.groups_count() + thread_cnt - 1) / thread_cnt *
                             passage_count_vec.GROUP_SIZE;
        using map_t = decltype(passage_count_map);
        // auto start  = get_cycles();
        std::vector<map_t> maps(thread_cnt);
        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                map_t& pmap            = maps[i];
                const size_t idx_start = i * block;
                const size_t idx_end   = std::min(idx_start + block, passage_count_vec.size());
                if (idx_start >= idx_end) {
                    return;
                }
                passage_count_vec.template for_each_aligned_group<alg>(
                    [&](const int& count, DereferenceScope& scope) { pmap[count]++; }, idx_start,
                    idx_end, scope);
            });
        // auto end = get_cycles();
        // std::cout << "count passager: " << end - start << std::endl;
        for (auto& m : maps) {
            for (auto& p : m) {
                passage_count_map[p.first] += p.second;
            }
        }
    }
    // profile::print_profile_data();

    for (auto& [passage_count, cnt] : passage_count_map) {
        std::cout << "passage_count= " << passage_count << ", cnt = " << cnt << std::endl;
    }
    std::cout << std::endl;
}

template <Algorithm alg = DEFAULT_ALG>
void calculate_trip_duration(StdDataFrame<uint64_t>& df)
{
    std::cout << "calculate_trip_duration()" << std::endl;

    auto& pickup_time_vec  = df.get_column<SimpleTime>("tpep_pickup_datetime");
    auto& dropoff_time_vec = df.get_column<SimpleTime>("tpep_dropoff_datetime");
    assert(pickup_time_vec.size() == dropoff_time_vec.size());

    FarLib::FarVector<uint64_t> duration_vec(pickup_time_vec.size());
    const size_t thread_cnt =
        alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count();
    // aligned to group
    const size_t block = (pickup_time_vec.groups_count() + thread_cnt - 1) / thread_cnt *
                         pickup_time_vec.GROUP_SIZE;
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            using pt_it = decltype(pickup_time_vec.clbegin());
            using dt_it = decltype(dropoff_time_vec.clbegin());
            using d_it  = decltype(duration_vec.lbegin());
            struct Scope : public DereferenceScope {
                pt_it pickup_time_it;
                dt_it dropoff_time_it;
                d_it duration_it;
                void pin() const override
                {
                    pickup_time_it.pin();
                    dropoff_time_it.pin();
                    duration_it.pin();
                }
                void unpin() const override
                {
                    pickup_time_it.unpin();
                    dropoff_time_it.unpin();
                    duration_it.unpin();
                }
                void next(__DMH__)
                {
                    pickup_time_it.next(*this, __on_miss__);
                    dropoff_time_it.next(*this, __on_miss__);
                    duration_it.next(*this, __on_miss__);
                }
                void next()
                {
                    pickup_time_it.next(*this);
                    dropoff_time_it.next(*this);
                    duration_it.next(*this);
                }
                void next_group()
                {
                    pickup_time_it.next_group(*this);
                    dropoff_time_it.next_group(*this);
                }
                void next_duration()
                {
                    duration_it.next(*this);
                }
                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            const size_t idx_start = i * block;
            const size_t idx_end   = std::min(idx_start + block, pickup_time_vec.size());
            if (idx_start >= idx_end) {
                return;
            }
            if constexpr (alg == UTHREAD) {
                ON_MISS_BEGIN
                uthread::yield();
                ON_MISS_END
                scp.pickup_time_it = pickup_time_vec.get_const_lite_iter(
                    idx_start, scp, __on_miss__, idx_start, idx_end);
                scp.dropoff_time_it = dropoff_time_vec.get_const_lite_iter(
                    idx_start, scp, __on_miss__, idx_start, idx_end);
                scp.duration_it =
                    duration_vec.get_lite_iter(idx_start, scp, __on_miss__, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end; idx++, scp.next(__on_miss__)) {
                    *(scp.duration_it) =
                        scp.dropoff_time_it->to_second() - scp.pickup_time_it->to_second();
                }
            } else if constexpr (alg == PARAROUTINE || alg == PREFETCH) {
                // TODO pararoutine
                scp.pickup_time_it =
                    pickup_time_vec.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                scp.dropoff_time_it =
                    dropoff_time_vec.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                scp.duration_it =
                    duration_vec.get_lite_iter(idx_start, scp, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end;
                     idx += pickup_time_vec.GROUP_SIZE, scp.next_group()) {
                    auto& dropoff_time_group = *scp.dropoff_time_it.get_group_accessor();
                    auto& pickup_time_group  = *scp.pickup_time_it.get_group_accessor();
                    for (size_t ii = 0;
                         ii < std::min(pickup_time_vec.GROUP_SIZE, idx_end - idx);
                         ii++, scp.next_duration()) {
                        *(scp.duration_it) = dropoff_time_group[ii].to_second() -
                                             pickup_time_group[ii].to_second();
                    }
                }
            } else {
                ERROR("algorithm dont exist");
            }
        });

    df.load_column<alg>("duration", std::move(duration_vec), nan_policy::dont_pad_with_nans);
    auto& d_vec   = df.get_column<uint64_t>("duration");
    uint64_t mind = std::numeric_limits<uint64_t>::max(), maxd = 0, sumd = 0, cntd = 0;
    if constexpr (alg != DEFAULT) {
        const size_t thread_cnt =
            alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count();
        // aligned to group
        const size_t block = (pickup_time_vec.groups_count() + thread_cnt - 1) / thread_cnt *
                             pickup_time_vec.GROUP_SIZE;
        std::vector<uint64_t> uminds(thread_cnt, std::numeric_limits<uint64_t>::max()),
            umaxds(thread_cnt, 0), usumds(thread_cnt, 0), ucntds(thread_cnt, 0);

        uthread::parallel_for_with_scope<1>(
            thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
                const size_t idx_start = i * block;
                const size_t idx_end   = std::min(idx_start + block, d_vec.size());
                if (idx_start >= idx_end) {
                    return;
                }
                uint64_t& umind = uminds[i];
                uint64_t& umaxd = umaxds[i];
                uint64_t& usumd = usumds[i];
                uint64_t& ucntd = ucntds[i];
                d_vec.template for_each_aligned_group<alg>(
                    [&](const uint64_t& d, DereferenceScope& scope) {
                        if (is_nan__(d)) {
                            return;
                        }
                        umind = std::min(umind, d);
                        umaxd = std::max(umaxd, d);
                        usumd += d;
                        ucntd++;
                    },
                    idx_start, idx_end, scope);
            });
        mind = *std::min_element(uminds.begin(), uminds.end());
        maxd = *std::max_element(umaxds.begin(), umaxds.end());
        sumd = std::accumulate(usumds.begin(), usumds.end(), static_cast<uint64_t>(0));
        cntd = std::accumulate(ucntds.begin(), ucntds.end(), static_cast<uint64_t>(0));
    }
    std::cout << "Mean duration = " << sumd / cntd << " seconds" << std::endl;
    std::cout << "Min duration = " << mind << " seconds" << std::endl;
    std::cout << "Max duration = " << maxd << " seconds" << std::endl;
    std::cout << std::endl;
}

template <Algorithm alg = DEFAULT_ALG, bool trivial_opt = DEFAULT_TRIVIAL_OPT>
void calculate_distribution_store_and_fwd_flag(StdDataFrame<uint64_t>& df)
{
    std::cout << "calculate_distribution_store_and_fwd_flag()" << std::endl;

    auto sel_N_saff_functor = [&](const uint64_t&, const char& saff) -> bool {
        return saff == 'N';
    };

    auto start = get_cycles();
    auto N_df =
        df.get_data_by_sel<alg, trivial_opt, char, decltype(sel_N_saff_functor), int, SimpleTime,
                           double, char>("store_and_fwd_flag", sel_N_saff_functor);
    std::cout << static_cast<double>(N_df.get_index().size()) / df.get_index().size() << std::endl;
    auto end = get_cycles();
    // std::cout << "N-df get: " << end - start << std::endl;
    auto sel_Y_saff_functor = [&](const uint64_t&, const char& saff) -> bool {
        return saff == 'Y';
    };
    start = get_cycles();
    auto Y_df =
        df.get_data_by_sel<alg, trivial_opt, char, decltype(sel_Y_saff_functor), int, SimpleTime,
                           double, char>("store_and_fwd_flag", sel_Y_saff_functor);
    end = get_cycles();
    // std::cout << "Y-df get: " << end - start << std::endl;
    start                     = get_cycles();
    auto unique_vendor_id_vec = Y_df.template get_col_unique_values<alg, int>("VendorID");
    end                       = get_cycles();
    // std::cout << "get col unique value: " << end - start << std::endl;
    std::cout << '{';
    if constexpr (alg == DEFAULT) {
        for (auto& vector_id : unique_vendor_id_vec) {
            std::cout << vector_id << ", ";
        }
    } else if constexpr (alg == UTHREAD) {
        const size_t thread_cnt = uthread::get_thread_count() * UTH_FACTOR;
        const size_t block      = (unique_vendor_id_vec.size() + thread_cnt - 1) / thread_cnt;
        // output cannot asynchronous
        uthread::parallel_for_with_scope<1>(1, 1, [&](size_t i, DereferenceScope& scope) {
            ON_MISS_BEGIN
            uthread::yield();
            ON_MISS_END
            using it_t = decltype(unique_vendor_id_vec.clbegin());
            struct Scope : public DereferenceScope {
                it_t it;

                void pin() const override
                {
                    it.pin();
                }

                void unpin() const override
                {
                    it.unpin();
                }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            const size_t idx_start = i * block;
            const size_t idx_end   = std::min(idx_start + block, unique_vendor_id_vec.size());
            if (idx_start >= idx_end) {
                return;
            }
            scp.it = unique_vendor_id_vec.get_const_lite_iter(idx_start, scp, __on_miss__,
                                                              idx_start, idx_end);
            for (size_t idx = idx_start; idx < idx_end; idx++, scp.it.next(scope, __on_miss__)) {
                std::cout << *(scp.it) << ", ";
            }
        });
    } else if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
        RootDereferenceScope scope;
        auto it         = unique_vendor_id_vec.clbegin(scope);
        size_t vec_size = unique_vendor_id_vec.size();
        for (size_t i = 0; i < vec_size; i++, it.next(scope)) {
            std::cout << *it << ", ";
        }
    } else {
        ERROR("alg dont exist");
    }
    std::cout << '}' << std::endl;
    std::cout << std::endl;
}

template <Algorithm alg = DEFAULT_ALG, bool trivial_opt = DEFAULT_TRIVIAL_OPT>
void calculate_haversine_distance_column(StdDataFrame<uint64_t>& df)
{
    std::cout << "calculate_haversine_distance_column()" << std::endl;

    auto& pickup_longitude_vec  = df.get_column<double>("pickup_longitude");
    auto& pickup_latitude_vec   = df.get_column<double>("pickup_latitude");
    auto& dropoff_longitude_vec = df.get_column<double>("dropoff_longitude");
    auto& dropoff_latitude_vec  = df.get_column<double>("dropoff_latitude");
    assert(pickup_longitude_vec.size() == pickup_latitude_vec.size());
    assert(pickup_longitude_vec.size() == dropoff_longitude_vec.size());
    assert(pickup_longitude_vec.size() == dropoff_latitude_vec.size());
    FarLib::FarVector<double> haversine_distance_vec(pickup_longitude_vec.size());
    const size_t thread_cnt =
        alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count();
    const size_t block = (pickup_longitude_vec.groups_count() + thread_cnt - 1) / thread_cnt *
                         pickup_longitude_vec.GROUP_SIZE;
    // const size_t block = (pickup_longitude_vec.size() + thread_cnt - 1) / thread_cnt;
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            using p_la_it_t = decltype(pickup_latitude_vec.clbegin());
            using p_lo_it_t = decltype(pickup_latitude_vec.clbegin());
            using d_la_it_t = decltype(dropoff_latitude_vec.clbegin());
            using d_lo_it_t = decltype(dropoff_longitude_vec.clbegin());
            using h_it_t    = decltype(haversine_distance_vec.lbegin());
            struct Scope : public DereferenceScope {
                p_la_it_t pickup_latitude_it;
                p_lo_it_t pickup_longitude_it;
                d_la_it_t dropoff_latitude_it;
                d_lo_it_t dropoff_longitude_it;
                h_it_t haversine_it;
                void pin() const override
                {
                    pickup_latitude_it.pin();
                    pickup_longitude_it.pin();
                    dropoff_latitude_it.pin();
                    dropoff_longitude_it.pin();
                    haversine_it.pin();
                }
                void unpin() const override
                {
                    pickup_latitude_it.unpin();
                    pickup_longitude_it.unpin();
                    dropoff_latitude_it.unpin();
                    dropoff_longitude_it.unpin();
                    haversine_it.unpin();
                }
                void next(__DMH__)
                {
                    pickup_latitude_it.next(*this, __on_miss__);
                    pickup_longitude_it.next(*this, __on_miss__);
                    dropoff_latitude_it.next(*this, __on_miss__);
                    dropoff_longitude_it.next(*this, __on_miss__);
                    haversine_it.next(*this, __on_miss__);
                }
                void next()
                {
                    pickup_latitude_it.next(*this);
                    pickup_longitude_it.next(*this);
                    dropoff_latitude_it.next(*this);
                    dropoff_longitude_it.next(*this);
                    haversine_it.next(*this);
                }
                void next_group()
                {
                    pickup_latitude_it.next_group(*this);
                    pickup_longitude_it.next_group(*this);
                    dropoff_latitude_it.next_group(*this);
                    dropoff_longitude_it.next_group(*this);
                    haversine_it.next_group(*this);
                }
                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            const size_t idx_start = i * block;
            const size_t idx_end   = std::min(idx_start + block, pickup_longitude_vec.size());
            if (idx_start >= idx_end) {
                return;
            }
            constexpr size_t group_size = pickup_latitude_vec.GROUP_SIZE;
            if constexpr (alg == UTHREAD) {
                ON_MISS_BEGIN
                uthread::yield();
                ON_MISS_END
                scp.pickup_latitude_it = pickup_latitude_vec.get_const_lite_iter(
                    idx_start, scp, __on_miss__, idx_start, idx_end);
                scp.pickup_longitude_it = pickup_longitude_vec.get_const_lite_iter(
                    idx_start, scp, __on_miss__, idx_start, idx_end);
                scp.dropoff_latitude_it = dropoff_latitude_vec.get_const_lite_iter(
                    idx_start, scp, __on_miss__, idx_start, idx_end);
                scp.dropoff_longitude_it = dropoff_longitude_vec.get_const_lite_iter(
                    idx_start, scp, __on_miss__, idx_start, idx_end);
                scp.haversine_it = haversine_distance_vec.get_lite_iter(
                    idx_start, scp, __on_miss__, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end; idx++, scp.next(__on_miss__)) {
                    *(scp.haversine_it) =
                        haversine(*(scp.pickup_latitude_it), *(scp.pickup_longitude_it),
                                  *(scp.dropoff_latitude_it), *(scp.dropoff_longitude_it));
                }
            } else if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
                scp.pickup_latitude_it =
                    pickup_latitude_vec.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                scp.pickup_longitude_it = pickup_longitude_vec.get_const_lite_iter(
                    idx_start, scp, idx_start, idx_end);
                scp.dropoff_latitude_it = dropoff_latitude_vec.get_const_lite_iter(
                    idx_start, scp, idx_start, idx_end);
                scp.dropoff_longitude_it = dropoff_longitude_vec.get_const_lite_iter(
                    idx_start, scp, idx_start, idx_end);
                scp.haversine_it =
                    haversine_distance_vec.get_lite_iter(idx_start, scp, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end;
                     idx += group_size, scp.next_group()) {
                    auto& haversine_group       = *scp.haversine_it.get_group_accessor();
                    auto& pickup_latitude_group = *scp.pickup_latitude_it.get_group_accessor();
                    auto& pickup_longitude_group =
                        *scp.pickup_longitude_it.get_group_accessor();
                    auto& dropoff_latitude_group =
                        *scp.dropoff_latitude_it.get_group_accessor();
                    auto& dropoff_longitude_group =
                        *scp.dropoff_longitude_it.get_group_accessor();
                    for (size_t ii = 0; ii < std::min(group_size, idx_end - idx); ii++) {
                        haversine_group[ii] =
                            haversine(pickup_latitude_group[ii], pickup_longitude_group[ii],
                                      dropoff_latitude_group[ii], dropoff_longitude_group[ii]);
                    }
                }
            } else {
                ERROR("algorithm dont exist");
            }
        });

    df.load_column<alg>("haversine_distance", std::move(haversine_distance_vec),
                        nan_policy::dont_pad_with_nans);

    // for (int i = 0; i < haversine_distance_vec.size(); i++) {
    //     std::cout << *haversine_distance_vec[i] << std::endl;
    // }

    auto sel_functor = [&](const uint64_t&, const double& dist) -> bool { return dist > 100; };
    auto sel_df      = df.get_data_by_sel<alg, trivial_opt, double, decltype(sel_functor), int,
                                          SimpleTime, double, char>("haversine_distance", sel_functor);
    std::cout << "Number of rows that have haversine_distance > 100 KM = "
              << sel_df.get_index().size() << std::endl;

    std::cout << std::endl;
}

template <Algorithm alg = DEFAULT_ALG>
void analyze_trip_timestamp(StdDataFrame<uint64_t>& df)
{
    std::cout << "analyze_trip_timestamp()" << std::endl;
    MaxVisitor<SimpleTime> max_visitor;
    MinVisitor<SimpleTime> min_visitor;
    df.multi_visit<alg>(std::make_pair("tpep_pickup_datetime", &max_visitor),
                        std::make_pair("tpep_pickup_datetime", &min_visitor));
    std::cout << max_visitor.get_result() << std::endl;
    std::cout << min_visitor.get_result() << std::endl;

    auto& pickup_time_vec = df.get_column<SimpleTime>("tpep_pickup_datetime");

    FarLib::FarVector<char> pickup_hour_vec(pickup_time_vec.size());
    FarLib::FarVector<char> pickup_day_vec(pickup_time_vec.size());
    FarLib::FarVector<char> pickup_month_vec(pickup_time_vec.size());

    std::map<char, int> pickup_hour_map;
    std::map<char, int> pickup_day_map;
    std::map<char, int> pickup_month_map;

    const size_t thread_cnt =
        alg == UTHREAD ? uthread::get_thread_count() * UTH_FACTOR : uthread::get_thread_count();
    // aligned to group
    const size_t block = (pickup_hour_vec.groups_count() + thread_cnt - 1) / thread_cnt *
                         pickup_hour_vec.GROUP_SIZE;
    std::vector<decltype(pickup_hour_map)> hmaps(thread_cnt);
    std::vector<decltype(pickup_day_map)> dmaps(thread_cnt);
    std::vector<decltype(pickup_month_map)> mmaps(thread_cnt);
    uthread::parallel_for_with_scope<1>(
        thread_cnt, thread_cnt, [&](size_t i, DereferenceScope& scope) {
            using t_it_t = decltype(pickup_time_vec.clbegin());
            using h_it_t = decltype(pickup_hour_vec.lbegin());
            using d_it_t = decltype(pickup_day_vec.lbegin());
            using m_it_t = decltype(pickup_month_vec.lbegin());
            struct Scope : public DereferenceScope {
                t_it_t time_it;
                h_it_t hour_it;
                d_it_t day_it;
                m_it_t month_it;
                void pin() const override
                {
                    time_it.pin();
                    hour_it.pin();
                    day_it.pin();
                    month_it.pin();
                }
                void unpin() const override
                {
                    time_it.unpin();
                    hour_it.unpin();
                    day_it.unpin();
                    month_it.unpin();
                }
                void next(__DMH__)
                {
                    time_it.next(*this, __on_miss__);
                    hour_it.next(*this, __on_miss__);
                    day_it.next(*this, __on_miss__);
                    month_it.next(*this, __on_miss__);
                }
                void next()
                {
                    time_it.next(*this);
                    hour_it.next(*this);
                    day_it.next(*this);
                    month_it.next(*this);
                }
                void next_group()
                {
                    hour_it.next_group(*this);
                    day_it.next_group(*this);
                    month_it.next_group(*this);
                }
                void next_time()
                {
                    time_it.next(*this);
                }
                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            const size_t idx_start = i * block;
            const size_t idx_end   = std::min(idx_start + block, pickup_time_vec.size());
            if (idx_start >= idx_end) {
                return;
            }
            auto& hmap = hmaps[i];
            auto& dmap = dmaps[i];
            auto& mmap = mmaps[i];
            if constexpr (alg == UTHREAD) {
                ON_MISS_BEGIN
                uthread::yield();
                ON_MISS_END
                scp.time_it  = pickup_time_vec.get_const_lite_iter(idx_start, scp, __on_miss__,
                                                                   idx_start, idx_end);
                scp.hour_it  = pickup_hour_vec.get_lite_iter(idx_start, scp, __on_miss__,
                                                             idx_start, idx_end);
                scp.day_it   = pickup_day_vec.get_lite_iter(idx_start, scp, __on_miss__,
                                                            idx_start, idx_end);
                scp.month_it = pickup_month_vec.get_lite_iter(idx_start, scp, __on_miss__,
                                                              idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end; idx++, scp.next(__on_miss__)) {
                    hmap[scp.time_it->hour_]++;
                    dmap[scp.time_it->day_]++;
                    mmap[scp.time_it->month_]++;
                    *(scp.hour_it)  = scp.time_it->hour_;
                    *(scp.day_it)   = scp.time_it->day_;
                    *(scp.month_it) = scp.time_it->month_;
                }
            } else if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
                // TODO pararoutine
                scp.time_it =
                    pickup_time_vec.get_const_lite_iter(idx_start, scp, idx_start, idx_end);
                scp.hour_it = pickup_hour_vec.get_lite_iter(idx_start, scp, idx_start, idx_end);
                scp.day_it  = pickup_day_vec.get_lite_iter(idx_start, scp, idx_start, idx_end);
                scp.month_it =
                    pickup_month_vec.get_lite_iter(idx_start, scp, idx_start, idx_end);
                for (size_t idx = idx_start; idx < idx_end;
                     idx += pickup_hour_vec.GROUP_SIZE, scp.next_group()) {
                    auto& hour_group  = *scp.hour_it.get_group_accessor();
                    auto& day_group   = *scp.day_it.get_group_accessor();
                    auto& month_group = *scp.month_it.get_group_accessor();
                    for (size_t ii = 0;
                         ii < std::min(pickup_hour_vec.GROUP_SIZE, idx_end - idx);
                         ii++, scp.next_time()) {
                        hmap[scp.time_it->hour_]++;
                        dmap[scp.time_it->day_]++;
                        mmap[scp.time_it->month_]++;
                        hour_group[ii]  = scp.time_it->hour_;
                        day_group[ii]   = scp.time_it->day_;
                        month_group[ii] = scp.time_it->month_;
                    }
                }
            } else {
                ERROR("algorithm dont exist");
            }
        });
    for (auto& m : hmaps) {
        for (auto& p : m) {
            pickup_hour_map[p.first] += p.second;
        }
    }
    for (auto& m : dmaps) {
        for (auto& p : m) {
            pickup_day_map[p.first] += p.second;
        }
    }
    for (auto& m : mmaps) {
        for (auto& p : m) {
            pickup_month_map[p.first] += p.second;
        }
    }

    df.load_column<alg>("pickup_hour", std::move(pickup_hour_vec), nan_policy::dont_pad_with_nans);
    df.load_column<alg>("pickup_day", std::move(pickup_day_vec), nan_policy::dont_pad_with_nans);
    df.load_column<alg>("pickup_month", std::move(pickup_month_vec),
                        nan_policy::dont_pad_with_nans);
    std::cout << "Print top 10 rows." << std::endl;
    {
        RootDereferenceScope scope;
        auto top_10_df = df.get_data_by_idx<int, SimpleTime, double, char>(
        Index2D<StdDataFrame<uint64_t>::IndexType>{0, 9}, scope);
        top_10_df.write_with_values_only<std::ostream, int, SimpleTime, double, char>(std::cout, false, io_format::json, scope);
    }

    for (auto& [hour, cnt] : pickup_hour_map) {
        std::cout << "pickup_hour = " << static_cast<int>(hour) << ", cnt = " << cnt << std::endl;
    }
    std::cout << std::endl;
    for (auto& [day, cnt] : pickup_day_map) {
        std::cout << "pickup_day = " << static_cast<int>(day) << ", cnt = " << cnt << std::endl;
    }
    std::cout << std::endl;
    for (auto& [month, cnt] : pickup_month_map) {
        std::cout << "pickup_month = " << static_cast<int>(month) << ", cnt = " << cnt << std::endl;
    }
    std::cout << std::endl;
}

template <typename T_Key, Algorithm alg = DEFAULT_ALG>
void analyze_trip_durations_of_timestamps(StdDataFrame<uint64_t>& df, const char* key_col_name)
{
    std::cout << "analyze_trip_durations_of_timestamps() on key = " << key_col_name << std::endl;

    StdDataFrame<uint64_t> df_key_duration;
    auto copy_index        = df.get_index();
    auto copy_key_col      = df.get_column<T_Key>(key_col_name);
    auto copy_key_duration = df.get_column<uint64_t>("duration");
    df_key_duration.load_data(std::move(copy_index),
                              std::make_pair(key_col_name, std::move(copy_key_col)),
                              std::make_pair("duration", std::move(copy_key_duration)));

    StdDataFrame<uint64_t> groupby_key =
        df_key_duration.groupby<alg, GroupbyMedian, T_Key, T_Key, uint64_t>(GroupbyMedian(),
                                                                            key_col_name);
    auto& key_vec      = groupby_key.get_column<T_Key>(key_col_name);
    auto& duration_vec = groupby_key.get_column<uint64_t>("duration");
    if constexpr (alg == UTHREAD) {
        const size_t thread_cnt = uthread::get_thread_count() * UTH_FACTOR;
        const size_t block      = (key_vec.size() + thread_cnt - 1) / thread_cnt;
        // output cannot asynchronous
        uthread::parallel_for_with_scope<1>(1, 1, [&](size_t i, DereferenceScope& scope) {
            ON_MISS_BEGIN
            uthread::yield();
            ON_MISS_END
            using k_it_t = decltype(key_vec.clbegin());
            using d_it_t = decltype(duration_vec.clbegin());
            struct Scope : public DereferenceScope {
                k_it_t key_it;
                d_it_t duration_it;

                void pin()
                {
                    key_it.pin();
                    duration_it.pin();
                }

                void unpin()
                {
                    key_it.unpin();
                    duration_it.unpin();
                }

                void next(__DMH__)
                {
                    key_it.next(*this, __on_miss__);
                    duration_it.next(*this, __on_miss__);
                }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            const size_t idx_start = i * block;
            const size_t idx_end   = std::min(idx_start + block, key_vec.size());
            if (idx_start >= idx_end) {
                return;
            }
            scp.key_it =
                key_vec.get_const_lite_iter(idx_start, scp, __on_miss__, idx_start, idx_end);
            scp.duration_it =
                duration_vec.get_const_lite_iter(idx_start, scp, __on_miss__, idx_start, idx_end);
            for (size_t idx = idx_start; idx < idx_end; idx++, scp.next(__on_miss__)) {
                std::cout << static_cast<int>(*(scp.key_it)) << " " << *(scp.duration_it)
                          << std::endl;
            }
        });
    } else if constexpr (alg == PREFETCH || alg == PARAROUTINE) {
        struct Scope : public RootDereferenceScope {
            decltype(key_vec.clbegin()) key_it;
            decltype(duration_vec.clbegin()) duration_it;

            void pin() const override
            {
                key_it.pin();
                duration_it.pin();
            }

            void unpin() const override
            {
                key_it.unpin();
                duration_it.unpin();
            }

            void next()
            {
                key_it.next(*this);
                duration_it.next(*this);
            }
        } scope;
        scope.key_it      = key_vec.clbegin(scope);
        scope.duration_it = duration_vec.clbegin(scope);
        for (uint64_t i = 0; i < key_vec.size(); i++, scope.next()) {
            std::cout << static_cast<int>(*scope.key_it) << " " << *scope.duration_it << std::endl;
        }
    } else {
        ERROR("alg not exists");
    }
    std::cout << std::endl;
}

int main(int argc, const char* argv[])
{
    /* config setting */
    Configure config;
#ifdef STANDALONE
    config.server_addr = "127.0.0.1";
    config.server_port = "1234";
#ifdef SIMPLE_BENCH
    // ~74M
    config.server_buffer_size = 1024L * 1024 * 256;
    config.client_buffer_size = 1024 * 1024 * 16;
    config.app_thread_count   = 8;
    config.worker_count       = 8;
#else
    // ~16G
    config.server_buffer_size = 1024L * 1024 * 1024 * 64;
    config.client_buffer_size = 1024L * 1024 * 1024 * 5;
    config.max_thread_cnt     = 8;
#endif
    config.evict_batch_size = 64 * 1024;
#else
    if (argc != 3 && argc != 4) {
        std::cout << "usage: " << argv[0] << " <configure file> <file_path> [client buffer size]"
                  << std::endl;
        return -1;
    }
    config.from_file(argv[1]);
    if (argc == 4) {
        config.client_buffer_size = std::stoul(argv[3]);
    }
#endif

    /* client-server connection */
#ifdef STANDALONE
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
#endif
    FarLib::runtime_init(config);
    FarLib::perf_init();
    srand(time(NULL));
    /* test */
    std::chrono::time_point<std::chrono::steady_clock> times[10];
    {
        auto df = load_data(argv[2]);
        FarLib::profile::beehive_profile([&] {
            FarLib::perf_profile([&] {
                times[0] = std::chrono::steady_clock::now();
                print_number_vendor_ids_and_unique(df);
                times[1] = std::chrono::steady_clock::now();
                print_passage_counts_by_vendor_id(df, 1);
                times[2] = std::chrono::steady_clock::now();
                print_passage_counts_by_vendor_id(df, 2);
                times[3] = std::chrono::steady_clock::now();
                calculate_trip_duration(df);
                times[4] = std::chrono::steady_clock::now();
                calculate_distribution_store_and_fwd_flag(df);
                times[5] = std::chrono::steady_clock::now();
                calculate_haversine_distance_column(df);
                times[6] = std::chrono::steady_clock::now();
                analyze_trip_timestamp(df);
                times[7] = std::chrono::steady_clock::now();
                analyze_trip_durations_of_timestamps<char>(df, "pickup_day");
                times[8] = std::chrono::steady_clock::now();
                analyze_trip_durations_of_timestamps<char>(df, "pickup_month");
                times[9] = std::chrono::steady_clock::now();

                for (uint32_t i = 1; i < std::size(times); i++) {
                    std::cout << "Step " << i << ": "
                              << std::chrono::duration_cast<std::chrono::microseconds>(times[i] -
                                                                                       times[i - 1])
                                     .count()
                              << " us" << std::endl;
                }
                std::cout << "Total: "
                          << std::chrono::duration_cast<std::chrono::microseconds>(times[9] -
                                                                                   times[0])
                                 .count()
                          << " us" << std::endl;
            }).print();
        });
    }
    clear_data_hetero_vector();
    FarLib::runtime_destroy();
#ifdef STANDALONE
    server_thread.join();
#endif
    return 0;
}
