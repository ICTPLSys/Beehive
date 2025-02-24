#include <DataFrame/DataFrame.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "async/scoped_inline_task.hpp"
#include "cache/accessor.hpp"
#include "option.hpp"
#include "rdma/client.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
// #define STANDALONE
// simple: ~74M, full: ~16G
// #define SIMPLE_BENCH

#ifdef STANDALONE
#include "rdma/server.hpp"
#endif

using namespace Beehive;
using namespace Beehive::rdma;
using namespace Beehive::cache;
using namespace std::chrono_literals;
using namespace hmdf;

static constexpr size_t TEST_COUNT         = 3;
static constexpr size_t DEFAULT_BATCH_SIZE = 32;
// Download dataset at https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
// The following code is implemented based on the format of 2016 datasets.

StdDataFrame<uint64_t> load_data()
{
#ifdef SIMPLE_BENCH
    // const char* file_path =
    // "/path/to/mem_parallel/motivation/Beehive/build/very_simple.csv";
    const char* file_path = "/mnt/simple.csv";

#else
    const char* file_path = "/mnt/all.csv";
#endif
    return read_csv<-1, int, SimpleTime, SimpleTime, int, double, double, double, int, char, double,
                    double, int, double, double, double, double, double, double, double>(
        file_path, "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "pickup_longitude", "pickup_latitude", "RatecodeID", "store_and_fwd_flag",
        "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount");
}

template <Algorithm alg = DEFAULT_ALG>
void print_hours_and_unique(StdDataFrame<uint64_t>& df, size_t uthread_cnt)
{
    std::cout << "print_hours_and_unique()" << std::endl;
    std::cout << "Number of hours in the train dataset: "
              << df.get_column<SimpleTime>("tpep_pickup_datetime").size() << std::endl;
    size_t siz = df.get_col_unique_values<alg, SimpleTime>(
                       "tpep_pickup_datetime",
                       [](const SimpleTime& st) {
                           constexpr uint64_t day_per_month[12] = {31, 28, 31, 30, 31, 30,
                                                                   31, 31, 30, 31, 30, 31};
                           size_t days                          = 0;
                           for (size_t i = 0; i < st.month_ - 1; i++) {
                               days += day_per_month[i];
                           }
                           days += st.day_;
                           return days * 24 * 60 + st.hour_ * 60 + st.min_;
                       },
                       uthread_cnt)
                     .size();
    std::cout << "Number of unique hours in the train dataset:" << siz << std::endl;
    std::cout << std::endl;
}

template <Algorithm alg = DEFAULT_ALG>
void print_trip_distance_hist(StdDataFrame<uint64_t>& df, size_t uthread_cnt)
{
    std::cout << "print_trip_distance_hist()" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    std::unordered_map<size_t, size_t> hist_map;
    perf_profile([&]() {
        hist_map = df.get_column_elem_count<alg, double, size_t>(
            "trip_distance", [](double distance) -> size_t { return distance * 100; }, uthread_cnt);
    }).print();

    auto end = std::chrono::high_resolution_clock::now();
    auto d   = end - start;
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(d) << std::endl;
    std::cout << "hist size: " << hist_map.size() << std::endl;
    size_t min_dist = UINT64_MAX;
    size_t max_dist = 0;

    for (auto& p : hist_map) {
        min_dist = std::min(min_dist, p.first);
        max_dist = std::max(max_dist, p.first);
    }
    std::cout << "min distance: " << min_dist << std::endl;
    std::cout << "max distance: " << max_dist << std::endl;
    size_t work_time = profile::collect_work_cycles();
    std::cout << "work time: " << static_cast<double>(work_time) / 3500'000 << " ms" << std::endl;
}

int main(int argc, const char* argv[])
{
    /* config setting */
    Configure config;
    size_t max_uthread_cnt;
    perf_init();
#ifdef STANDALONE
    config.server_addr = "127.0.0.1";
    config.server_port = "1234";
#ifdef SIMPLE_BENCH
    // ~74M
    config.server_buffer_size = 1024L * 1024 * 1024 * 2;
    config.client_buffer_size = 1024L * 1024 * 16;
#else
    // ~16G
    config.server_buffer_size = 1024L * 1024 * 1024 * 64;
    config.client_buffer_size = 1024L * 1024 * 1024 * 4;
#endif
    config.evict_batch_size = 64 * 1024;
    config.max_thread_cnt   = 4;
    max_uthread_cnt         = 4;
#else
    if (argc != 2 && argc != 4) {
        std::cout << "usage: " << argv[0] << " <configure file> [<core num> <uthread num>]"
                  << std::endl;
        return -1;
    }
    if (argc == 4) {
        config.max_thread_cnt = std::stoul(argv[2]);
        max_uthread_cnt       = std::stoul(argv[3]);
    }
    config.from_file(argv[1]);
#endif

    /* client-server connection */
#ifdef STANDALONE
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
#endif

    Beehive::runtime_init(config);
    /* test */
    std::chrono::time_point<std::chrono::steady_clock> times[10];

    {
        auto df = load_data();
        if constexpr (DEFAULT_ALG != PARAROUTINE) {
            for (size_t count = 0; count < TEST_COUNT; count++) {
                for (size_t uth_count = uthread::get_worker_count(); uth_count <= max_uthread_cnt;
                     uth_count += uth_count < 16 ? 1 : (uth_count < 64 ? 4 : uth_count)) {
                    std::string file_name = get_alg_str() + "/trip-duration-" + get_alg_str() +
                                            "-" + std::to_string(uthread::get_worker_count()) +
                                            "-" + std::to_string(uth_count) + "-" +
                                            std::to_string(count) + ".txt";
                    std::cout << file_name << std::endl;
                    std::ofstream of(file_name);
                    auto old = std::cout.rdbuf(of.rdbuf());
                    Cache::get_default()->flush();
                    profile::reset_all();
                    print_trip_distance_hist(df, uth_count);
                    std::cout.rdbuf(old);
                }
            }
        } else {
            for (size_t count = 0; count < TEST_COUNT; count++) {
                std::string file_name = get_alg_str() + "/trip-duration-" + get_alg_str() + "-" +
                                        std::to_string(uthread::get_worker_count()) + "-" +
                                        std::to_string(DEFAULT_BATCH_SIZE) + "-" +
                                        std::to_string(count) + ".txt";
                std::cout << file_name << std::endl;
                std::ofstream of(file_name);
                auto old = std::cout.rdbuf(of.rdbuf());
                Cache::get_default()->flush();
                profile::reset_all();
                print_trip_distance_hist(df, max_uthread_cnt);
                std::cout.rdbuf(old);
                std::cout << "core num: " << uthread::get_worker_count()
                          << ", uthread_cnt: " << max_uthread_cnt << std::endl;
            }
        }
    }
    /* destroy runtime */
    Beehive::runtime_destroy();
#ifdef STANDALONE
    server_thread.join();
#endif
    return 0;
}
