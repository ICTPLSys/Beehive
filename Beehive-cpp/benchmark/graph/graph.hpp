#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "data_structure/chunked_graph.hpp"
#include "utils/cpu_cycles.hpp"
#include "utils/debug.hpp"
#include "utils/fork_join.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
#include "utils/timer.hpp"

using namespace FarLib;

template <bool TimeStamp = false>
using Graph = ChunkedGraph<TimeStamp>;

using vertex_t = Graph<>::vertex_t;

struct all_exists {};
struct sparse {};
struct dense {};

// This is not data structure in far memory now
struct VertexSet {
    size_t n;  // graph vertex count
    size_t m;  // size of set
    std::vector<vertex_t> indices;
    std::unique_ptr<uint8_t[]> exists;
    bool is_dense;

    VertexSet(const VertexSet &) = delete;
    VertexSet(VertexSet &&) = default;
    VertexSet(size_t n) : n(n), m(0), is_dense(false) {}
    VertexSet(size_t n, vertex_t v_id) : n(n), m(1), is_dense(false) {
        indices.push_back(v_id);
    }
    VertexSet(size_t n, std::vector<vertex_t> &&indices, sparse)
        : n(n),
          m(indices.size()),
          indices(std::move(indices)),
          is_dense(false) {}
    VertexSet(size_t n, size_t m, std::unique_ptr<uint8_t[]> &&exists, dense)
        : n(n), m(m), exists(std::move(exists)), is_dense(true) {}
    VertexSet(size_t n, all_exists, size_t max_thread_count)
        : n(n), m(n), is_dense(true) {
        exists.reset(new uint8_t[n]);
        uthread::parallel_for<1024>(max_thread_count, n,
                                    [this](size_t i) { exists[i] = 1; });
    }
    VertexSet &operator=(const VertexSet &) = delete;
    VertexSet &operator=(VertexSet &&) = default;
    bool empty() const { return m == 0; }
    size_t size() const { return m; }

    template <size_t MaxThreadCount>
    void to_sparse() {
        if (indices.empty() && m != 0) {
            std::vector<vertex_t> v[MaxThreadCount];
            size_t v_offset[MaxThreadCount];
            uthread::fork_join(MaxThreadCount, [&](size_t tid) {
                size_t stride = (n + MaxThreadCount - 1) / MaxThreadCount;
                size_t start = stride * tid;
                size_t end = std::min(start + stride, n);
                for (size_t i = start; i < end; i++) {
                    if (exists[i]) {
                        v[tid].push_back(i);
                    }
                }
            });
            size_t off = 0;
            for (size_t i = 0; i < MaxThreadCount; i++) {
                v_offset[i] = off;
                off += v[i].size();
            }
            ASSERT(off == m);
            indices.resize(m);
            uthread::fork_join(MaxThreadCount, [&](size_t tid) {
                size_t offset = v_offset[tid];
                size_t nv = v[tid].size();
                for (size_t i = 0; i < nv; i++) {
                    indices[offset + i] = v[tid][i];
                }
            });
        }
        is_dense = false;
    }

    template <size_t MaxThreadCount>
    void to_dense() {
        if (exists.get() == nullptr) {
            exists.reset(new uint8_t[n]);
            uthread::parallel_for<1024>(
                MaxThreadCount, n, [this](size_t i) { exists[i] = false; });
            uthread::parallel_for<1024>(MaxThreadCount, m, [this](size_t i) {
                vertex_t v_id = indices[i];
                exists[v_id] = true;
            });
        }
        is_dense = true;
    }

    template <size_t MaxThreadCount, bool TimeStamp>
    size_t sum_out_degree(Graph<TimeStamp> &graph) {
        size_t sum_degree = 0;
        size_t partial_sum[MaxThreadCount];
        if (is_dense) {
            uthread::fork_join(MaxThreadCount, [&](size_t tid) {
                size_t s = 0;
                size_t stride = (n + MaxThreadCount - 1) / MaxThreadCount;
                size_t start = stride * tid;
                size_t end = std::min(start + stride, n);
                for (size_t i = start; i < end; i++) {
                    if (exists[i]) {
                        s += graph.out_degree(i);
                    }
                }
                partial_sum[tid] = s;
            });
            for (size_t i = 0; i < MaxThreadCount; i++)
                sum_degree += partial_sum[i];
        } else {
            size_t n_thread = std::min(MaxThreadCount, m / 1024 + 1);
            uthread::fork_join(n_thread, [&](size_t tid) {
                size_t s = 0;
                size_t stride = (m + n_thread - 1) / n_thread;
                size_t start = stride * tid;
                size_t end = std::min(start + stride, m);
                for (size_t i = start; i < end; i++) {
                    s += graph.out_degree(indices[i]);
                }
                partial_sum[tid] = s;
            });
            for (size_t i = 0; i < n_thread; i++) sum_degree += partial_sum[i];
        }
        return sum_degree;
    }
};

// TODO: parallelization
template <typename T, T Invalid>
std::vector<T> unique_vaild(std::vector<T> &&vec) {
    std::vector<T> unique_vec;
    if (vec.empty()) [[unlikely]]
        return {};
    std::sort(vec.begin(), vec.end());
    T last = vec[0];
    if (last != Invalid) [[likely]]
        unique_vec.push_back(last);
    for (T element : vec) {
        if (element != last) {
            last = element;
            if (last != Invalid) [[likely]]
                unique_vec.push_back(element);
        }
    }
    return unique_vec;
}

constexpr vertex_t InvalidID = std::numeric_limits<vertex_t>::max();

template <bool Optimize, size_t MaxThreadCount, typename F, bool TimeStamp>
VertexSet edge_map(Graph<TimeStamp> &graph, VertexSet &frontier, F &&f) {
    size_t vertex_count = graph.vertex_count();
    size_t sparse_threshold = graph.edge_count() / 20;
    uint64_t prepare_start_tsc = get_cycles();
    size_t sum_degree = frontier.template sum_out_degree<MaxThreadCount>(graph);
    bool sparse_mode = frontier.size() + sum_degree < sparse_threshold;
    uint64_t prepare_end_tsc = get_cycles();
    if (sparse_mode) {
        // sparse mode
        std::vector<vertex_t> next_frontier(sum_degree);
        frontier.to_sparse<MaxThreadCount>();
        std::unique_ptr<size_t[]> offsets(new size_t[frontier.size()]);
        uthread::parallel_for<1024>(MaxThreadCount, frontier.m, [&](size_t i) {
            vertex_t v_id = frontier.indices[i];
            offsets[i] = graph.out_degree(v_id);
        });
        size_t degree_sum[MaxThreadCount];
        size_t n_thread = std::min(MaxThreadCount, frontier.m / 1024 + 1);
        uthread::fork_join(n_thread, [&](size_t tid) {
            size_t s = 0;
            size_t stride = (frontier.m + n_thread - 1) / n_thread;
            size_t start = stride * tid;
            size_t end = std::min(start + stride, frontier.m);
            for (size_t i = start; i < end; i++) {
                s += offsets[i];
            }
            degree_sum[tid] = s;
        });
        for (size_t i = 1; i < n_thread; i++) {
            degree_sum[i] += degree_sum[i - 1];
        }
        ASSERT(degree_sum[n_thread - 1] == sum_degree);
        uthread::fork_join(n_thread, [&](size_t tid) {
            size_t stride = (frontier.m + n_thread - 1) / n_thread;
            size_t start = stride * tid;
            size_t end = std::min(start + stride, frontier.m);
            size_t base = (tid == 0) ? 0 : degree_sum[tid - 1];
            size_t d, off = base;
            for (size_t i = start; i < end; i++) {
                d = offsets[i];
                offsets[i] = off;
                off += d;
            }
        });
        // parallel
        graph.template for_each_out_list<Optimize, MaxThreadCount>(
            frontier.indices,
            [&](size_t i, const Graph<TimeStamp>::EdgeList *edge_list) {
                size_t offset = offsets[i];
                for (typename Graph<TimeStamp>::degree_t d = 0;
                     d < edge_list->degree; d++) {
                    auto &ngh = edge_list->neighbors[d];
                    if constexpr (TimeStamp) {
                        constexpr uint64_t fake_ts = 1;
                        if (ngh.create_ts > fake_ts ||
                            ngh.destroy_ts <= fake_ts)
                            break;
                    }
                    bool u = f.cond(ngh.ngh) &&
                             f.update_atomic(edge_list->vertex_id, ngh.ngh);
                    next_frontier[offset + d] = u ? ngh.ngh : InvalidID;
                }
            });
        return VertexSet(
            vertex_count,
            unique_vaild<vertex_t, InvalidID>(std::move(next_frontier)),
            sparse{});
    } else {
        // dense mode
        frontier.template to_dense<MaxThreadCount>();
        std::unique_ptr<uint8_t[]> next_frontier(new uint8_t[vertex_count]);
        uthread::parallel_for<1024>(
            MaxThreadCount, vertex_count,
            [&](size_t i) { next_frontier[i] = false; });
        // parallel
        graph.template for_each_in_list<Optimize, MaxThreadCount>(
            [&](const Graph<TimeStamp>::EdgeList *edge_list) {
                vertex_t v = edge_list->vertex_id;
                next_frontier[v] = false;
                if (f.cond(v)) {
                    for (size_t j = 0; j < edge_list->degree; j++) {
                        auto &ngh = edge_list->neighbors[j];
                        if constexpr (TimeStamp) {
                            constexpr uint64_t fake_ts = 1;
                            if (ngh.create_ts > fake_ts ||
                                ngh.destroy_ts <= fake_ts)
                                break;
                        }
                        if (frontier.exists[ngh.ngh] && f.update(ngh.ngh, v)) {
                            next_frontier[v] = true;
                            break;
                        }
                    }
                }
            });
        // count m
        size_t vcount[MaxThreadCount];
        uthread::fork_join(MaxThreadCount, [&](size_t tid) {
            size_t local_count = 0;
            size_t stride =
                (vertex_count + MaxThreadCount - 1) / MaxThreadCount;
            size_t start = stride * tid;
            size_t end = std::min(start + stride, vertex_count);
            for (size_t i = start; i < end; i++) {
                local_count += next_frontier[i];
            }
            vcount[tid] = local_count;
        });
        size_t m = 0;
        for (size_t i = 0; i < MaxThreadCount; i++) m += vcount[i];
        return VertexSet(vertex_count, m, std::move(next_frontier), dense{});
    }
}

template <size_t MaxThreadCount, std::invocable<vertex_t> F>
uint64_t vertex_map(const VertexSet &frontier, F &&f) {
    uint64_t start_tsc = get_cycles();
    if (frontier.is_dense) {
        uthread::parallel_for<1024>(MaxThreadCount, frontier.n, [&](size_t i) {
            if (frontier.exists[i]) f(i);
        });
    } else {
        uthread::parallel_for<1024>(MaxThreadCount, frontier.m,
                                    [&](size_t i) { f(frontier.indices[i]); });
    }
    uint64_t end_tsc = get_cycles();
    return end_tsc - start_tsc;
}

struct Options {
    std::string graph_file_name;
    std::string config_file_name;
    std::optional<size_t> local_memory;
    std::optional<vertex_t> src;
    size_t max_iteration;
    size_t evaluation_count;
    bool verbose;
};

void read_options(int argc, const char *const argv[], Options &options);

template <bool TimeStamp>
void evaluate(std::function<void(Graph<TimeStamp> &, const Options &)> compute,
              const Options &options) {
    srand(0);
    perf_init();
    Graph<TimeStamp> graph;
    graph.load(options.graph_file_name);
    std::cout << "loaded graph: " << graph.vertex_count() << " vertices, "
              << graph.edge_count() << " edges" << std::endl;
    // warm up
    std::cout << "warm up..." << std::endl;
    compute(graph, options);
    std::cout << std::endl;
    double opt_ms_total = 0;
    double par_ms_total = 0;
    std::cout << std::setw(16) << "#iteration" << std::setw(16) << "time (ms)"
              << std::endl;
    std::cout << std::string(16 * 3, '-') << std::endl;

    if (options.verbose) {
        std::cout << "==== break down for opt ====" << std::endl;
        graph.dense_edge_map_cycles = 0;
        graph.sparse_edge_map_cycles = 0;
        graph.vertex_map_cycles = 0;
        profile::reset_all();
        perf_profile([&] {
            profile::start_work();
            profile::thread_start_work();
            compute(graph, options);
            profile::thread_end_work();
            profile::end_work();
        }).print();
        profile::print_profile_data();
        std::cout << "dense map cycles: " << graph.dense_edge_map_cycles
                  << std::endl;
        std::cout << "sparse map cycles: " << graph.sparse_edge_map_cycles
                  << std::endl;
        std::cout << "vertex map cycles: " << graph.vertex_map_cycles
                  << std::endl;
    }

    for (size_t i = 0; i < options.evaluation_count; i++) {
        double opt_ms = time_for_ms([&] { compute(graph, options); });
        opt_ms_total += opt_ms;
        if (options.verbose) {
            std::cout << std::setw(16) << i << std::setw(16) << opt_ms
                      << std::endl;
        }
    }
    std::cout << std::setw(16) << "average" << std::setw(16)
              << opt_ms_total / options.evaluation_count << std::setw(16)
              << par_ms_total / options.evaluation_count << std::endl;

    exit(0);
    return;
}
