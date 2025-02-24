#include <array>
#include <boost/timer/progress_display.hpp>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <vector>

#define USE_FAR_MEMORY
// #define PTHREAD_YIELD
// #define PTHREAD_COUNT 16
// #define UTHREAD_YIELD
// #define UTHREAD_COUNT 16
// #define ENABLE_PREFETCH
#define ENABLE_RUN_AHEAD

#ifdef PTHREAD_YIELD
constexpr size_t ThreadCount = PTHREAD_COUNT;
#elif defined(UTHREAD_YIELD)
constexpr size_t ThreadCount = UTHREAD_COUNT;
#else
constexpr size_t ThreadCount = 1;
#endif

static thread_local size_t thread_idx = 0;

#include "utils/debug.hpp"

#ifdef USE_FAR_MEMORY
#include "cache/cache.hpp"
#include "data_structure/vector.hpp"
#include "rdma/client.hpp"
#include "utils/control.hpp"

#ifdef ENABLE_PREFETCH

#define FOR_EACH(VEC, FN) (VEC).for_each_mut(FN)
#define CONST_FOR_EACH(VEC, FN) (VEC).for_each_const(FN)

#elif defined(PTHREAD_YIELD)

template <typename Vec, typename Fn>
inline void vec_for_each(Vec &vec, Fn &&fn) {
    size_t size = vec.size();
    ON_MISS_BEGIN
        std::this_thread::yield();
    ON_MISS_END
    size_t per_thread_size = (size - 1) / PTHREAD_COUNT + 1;
    auto func = [&](size_t i) {
        thread_idx = i;
        vec.for_each_mut_no_prefetch(std::forward<Fn>(fn), i * per_thread_size,
                                     (i + 1) * per_thread_size, __on_miss__);
    };
    std::unique_ptr<std::thread> pthreads[PTHREAD_COUNT];
    for (size_t i = 0; i < PTHREAD_COUNT; i++) {
        auto thread = new std::thread(func, i);
        pthreads[i].reset(thread);
    }
    for (size_t i = 0; i < PTHREAD_COUNT; i++) {
        pthreads[i]->join();
    }
}

template <typename Vec, typename Fn>
inline void const_vec_for_each(const Vec &vec, Fn &&fn) {
    size_t size = vec.size();
    ON_MISS_BEGIN
        std::this_thread::yield();
    ON_MISS_END
    size_t per_thread_size = (size - 1) / PTHREAD_COUNT + 1;
    auto func = [&](size_t i) {
        thread_idx = i;
        vec.for_each_const_no_prefetch(std::forward<Fn>(fn),
                                       i * per_thread_size,
                                       (i + 1) * per_thread_size, __on_miss__);
    };
    std::unique_ptr<std::thread> pthreads[PTHREAD_COUNT];
    for (size_t i = 0; i < PTHREAD_COUNT; i++) {
        auto thread = new std::thread(func, i);
        pthreads[i].reset(thread);
    }
    for (size_t i = 0; i < PTHREAD_COUNT; i++) {
        pthreads[i]->join();
    }
}

#define FOR_EACH(VEC, FN) vec_for_each(VEC, FN)
#define CONST_FOR_EACH(VEC, FN) const_vec_for_each(VEC, FN)

#elif defined(UTHREAD_YIELD)

#include "libfibre/fibre.h"

struct UThreadTask {
    std::function<void(size_t)> *func;
    size_t arg;
};

void uthread_run(UThreadTask *task) { (*task->func)(task->arg); }

void uthread_run_func(std::function<void(size_t)> fn) {
    std::unique_ptr<Fibre> uthreads[UTHREAD_COUNT];
    UThreadTask tasks[UTHREAD_COUNT];
    for (size_t i = 0; i < UTHREAD_COUNT; i++) {
        tasks[i].func = &fn;
        tasks[i].arg = i;
        auto thread = new Fibre();
        thread->run(uthread_run, &tasks[i]);
        uthreads[i].reset(thread);
    }
    for (size_t i = 0; i < UTHREAD_COUNT; i++) {
        uthreads[i]->join();
    }
}

template <typename Vec, typename Fn>
inline void vec_for_each(Vec &vec, Fn &&fn) {
    size_t size = vec.size();
    ON_MISS_BEGIN
        Fibre::yieldGlobal();
    ON_MISS_END
    size_t per_thread_size = (size - 1) / UTHREAD_COUNT + 1;
    auto func = [&](size_t i) {
        thread_idx = i;
        vec.for_each_mut_no_prefetch(std::forward<Fn>(fn), i * per_thread_size,
                                     (i + 1) * per_thread_size, __on_miss__);
    };
    uthread_run_func(func);
}

template <typename Vec, typename Fn>
inline void const_vec_for_each(const Vec &vec, Fn &&fn) {
    size_t size = vec.size();
    ON_MISS_BEGIN
        Fibre::yieldGlobal();
    ON_MISS_END
    size_t per_thread_size = (size - 1) / UTHREAD_COUNT + 1;
    auto func = [&](size_t i) {
        thread_idx = i;
        vec.for_each_const_no_prefetch(std::forward<Fn>(fn),
                                       i * per_thread_size,
                                       (i + 1) * per_thread_size, __on_miss__);
    };
    uthread_run_func(func);
}

#define FOR_EACH(VEC, FN) vec_for_each(VEC, FN)
#define CONST_FOR_EACH(VEC, FN) const_vec_for_each(VEC, FN)

#elif defined(ENABLE_RUN_AHEAD)

#define FOR_EACH(VEC, FN) (VEC).for_each_mut_run_ahead(FN)
#define CONST_FOR_EACH(VEC, FN) (VEC).for_each_const_run_ahead(FN)

#else

#define FOR_EACH(VEC, FN) (VEC).for_each_mut_no_prefetch(FN)
#define CONST_FOR_EACH(VEC, FN) (VEC).for_each_const_no_prefetch(FN)

#endif

#else

template <typename T, typename Fn>
void for_each(std::vector<T> &vec, Fn &&fn) {
    for (T &item : vec) {
        fn(item);
    }
}

template <typename T, typename Fn>
void const_for_each(const std::vector<T> &vec, Fn &&fn) {
    for (const T &item : vec) {
        fn(item);
    }
}

#define FOR_EACH(VEC, FN) for_each(VEC, FN)
#define CONST_FOR_EACH(VEC, FN) const_for_each(VEC, FN)

#endif

template <size_t Dimension>
class KMeans {
public:
    typedef std::array<double, Dimension> Position;

    struct Point {
        Position position;
        size_t cluster_id;

        Point(Position position, size_t cluster_id)
            : position(position), cluster_id(cluster_id) {}
    };

    struct Cluster {
        Position position;
        size_t size;
    };

    struct ClusterSum {
        Position position_sum;
        size_t size;

        void reset() {
            for (size_t i = 0; i < Dimension; i++) {
                position_sum[i] = 0;
            }
            size = 0;
        }

        void add(const Position &pos) {
            for (size_t i = 0; i < Dimension; i++) {
                position_sum[i] += pos[i];
            }
            size++;
        }

        void add(const ClusterSum &c_sum) {
            for (size_t i = 0; i < Dimension; i++) {
                position_sum[i] += c_sum.position_sum[i];
            }
            size += c_sum.size;
        }

        void get_average_to(Cluster &result) {
            for (size_t j = 0; j < Dimension; j++) {
                result.position[j] = position_sum[j] / size;
                result.size = size;
            }
        }
    };

private:
    void init_clusters(size_t k, int seed = 0) {
        assert(k < points.size());
        std::default_random_engine re(seed);
        std::set<size_t> indexes;
        std::uniform_int_distribution<size_t> dist(0, points.size());
        for (size_t i = 0; i < k; i++) {
            size_t idx;
            do {
                idx = dist(re);
            } while (indexes.count(idx) != 0);
            indexes.insert(idx);
        }
        clusters.resize(k);
        for (size_t t = 0; t < ThreadCount; t++) {
            cluster_sum[t].resize(k);
        }
        auto it = indexes.begin();
        for (Cluster &c : clusters) {
#ifdef USE_FAR_MEMORY
            c.position = points.get(*it)->position;
#else
            c.position = points[*it].position;
#endif
            it++;
        }
    }

    static double get_distance_square(const Position &p0, const Position &p1) {
        double result = 0;
        for (size_t i = 0; i < Dimension; i++) {
            double di = p0[i] - p1[i];
            result += di * di;
        }
        return result;
    }

    size_t get_nearest_cluster(const Position &pos) {
        double min_distance_square = std::numeric_limits<double>::max();
        size_t nearest = 0;
        for (size_t i = 0; i < clusters.size(); i++) {
            double distance_square =
                get_distance_square(pos, clusters[i].position);
            if (distance_square < min_distance_square) {
                min_distance_square = distance_square;
                nearest = i;
            }
        }
        return nearest;
    }

    void iterate() {
        FOR_EACH(points, [this](Point &p) {
            p.cluster_id = get_nearest_cluster(p.position);
        });

        for (size_t t = 0; t < ThreadCount; t++) {
            for (ClusterSum &c_sum : cluster_sum[t]) {
                c_sum.reset();
            }
        }

        CONST_FOR_EACH(points, [this](const Point &p) {
            ClusterSum &c_sum = cluster_sum[thread_idx][p.cluster_id];
            c_sum.add(p.position);
        });
        for (size_t i = 0; i < clusters.size(); i++) {
            ClusterSum c_sum;
            c_sum.reset();
            for (size_t t = 0; t < ThreadCount; t++) {
                c_sum.add(cluster_sum[t][i]);
            }
            auto &c = clusters[i];
            c_sum.get_average_to(clusters[i]);
        }
    }

public:
    void add_data(const Position &p) { points.emplace_back(p, 0); }

    void run(size_t k, size_t iteration_count) {
        constexpr double CPU_FREQ = 2800.0;
        init_clusters(k);
        for (size_t i = 0; i < iteration_count; i++) {
            auto iterate_start = std::chrono::high_resolution_clock::now();
            iterate();
            auto iterate_end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> iterate_duration =
                iterate_end - iterate_start;
            std::cout << "iteration #" << i << ": " << iterate_duration.count()
                      << " ms" << std::endl;
        }
    }

    const auto &get_clusters() const { return clusters; }

    const auto &get_points() const { return points; }

private:
    std::vector<Cluster> clusters;
    std::vector<ClusterSum> cluster_sum[ThreadCount];
#ifdef USE_FAR_MEMORY
    static_assert(sizeof(Point) < 4096);
    Beehive::DenseVector<Point, 4096 / sizeof(Point)> points;
#else
    std::vector<Point> points;
#endif
};

void run(size_t k, size_t p_count, size_t it_count) {
    KMeans<3> kmeans;
    std::default_random_engine re(0);
    std::uniform_real_distribution<double> dist(0, 1);
    // std::cout << "Loading Data..." << std::endl;
    // boost::timer::progress_display show_progress(p_count);
    for (int i = 0; i < p_count; i++) {
        std::array<double, 3> data{dist(re), dist(re), dist(re)};
        kmeans.add_data(data);
        // ++show_progress;
    }
    kmeans.run(k, it_count);

    // std::cout << "-- clusters --" << std::endl;
    // for (auto &c : kmeans.get_clusters()) {
    //     std::cout << "(" << c.position[0] << ", " << c.position[1] << ", "
    //               << c.position[2] << ")"
    //               << " #" << c.size << std::endl;
    // }
}

void usage(const char *cmd_name) {
    std::cout << "usage: " << cmd_name << " <configure file>"
              << " <k>"
              << " <point count>"
              << " <iterations>"
              << " [local size]" << std::endl;
}

int main(int argc, const char *const argv[]) {
    size_t local_size = 0;
    if (argc == 6) {
        local_size = std::atol(argv[5]);
    } else if (argc != 5) {
        usage(argv[0]);
        return -1;
    }

#ifdef USE_FAR_MEMORY
    Beehive::rdma::Configure config;
    config.from_file(argv[1]);
    if (local_size != 0) {
        config.client_buffer_size = local_size;
    }
    Beehive::runtime_init(config);
#else
    Beehive::uthread::runtime_init();
#endif

    int k = std::atoi(argv[2]);
    int p_count = std::atoi(argv[3]);
    int it_count = std::atoi(argv[4]);
    ASSERT(k > 0);
    ASSERT(p_count > k);
    ASSERT(it_count > 0);

    run(k, p_count, it_count);

#ifdef USE_FAR_MEMORY
    Beehive::runtime_destroy();
#else
    Beehive::uthread::runtime_destroy();
#endif
    return 0;
}
