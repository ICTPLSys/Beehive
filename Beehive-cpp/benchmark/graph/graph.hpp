#include <cstddef>
#include <fstream>
#include <memory>
#include <optional>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/chunked_graph.hpp"

using namespace Beehive;
using Graph = ChunkedGraph<>;
using vertex_t = Graph::vertex_t;

// This is not data structure in far memory now
struct VertexSet {
    size_t n;  // graph vertex count
    size_t m;  // size of set
    std::vector<vertex_t> indices;
    std::vector<uint8_t> exists;  // std::vector<bool> is not concurrrent safe
    bool is_dense;

    VertexSet(const VertexSet &) = delete;
    VertexSet(VertexSet &&) = default;
    VertexSet(size_t n) : n(n), m(0), is_dense(false) {}
    VertexSet(size_t n, vertex_t v_id) : n(n), m(1), is_dense(false) {
        indices.push_back(v_id);
    }
    VertexSet(size_t n, std::vector<vertex_t> &&indices)
        : n(n),
          m(indices.size()),
          indices(std::move(indices)),
          is_dense(false) {}
    VertexSet(std::vector<uint8_t> &&exists)
        : n(exists.size()), exists(std::move(exists)), is_dense(true) {
        size_t count = 0;
        for (bool e : this->exists) {
            if (e) count++;
        }
        this->m = count;
    }
    VertexSet &operator=(const VertexSet &) = delete;
    VertexSet &operator=(VertexSet &&) = default;
    bool empty() const { return m == 0; }
    size_t size() const { return m; }
    void to_sparse() {
        if (indices.empty()) {
            indices.reserve(m);
            for (size_t i = 0; i < exists.size(); i++) {
                if (exists[i]) {
                    indices.push_back(i);
                }
            }
        }
        is_dense = false;
    }
    void to_dense() {
        if (exists.empty()) {
            exists.resize(n);
            for (vertex_t v_id : indices) {
                exists[v_id] = true;
            }
        }
        is_dense = true;
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

// TODO: parallelization
// out[0] = init
// out[i + 1] = fn(out[i], in[i])
// in & out can be the same array
template <typename InType, typename OutType, typename Fn>
    requires requires(Fn &&fn, InType in, OutType out) {
        { fn(out, in) } -> std::convertible_to<OutType>;
    }
OutType scan(InType *in, OutType *out, size_t len, Fn &&fn, OutType init) {
    OutType r = init;
    for (size_t i = 0; i < len; i++) {
        InType v = in[i];
        out[i] = r;
        r = fn(r, v);
    }
    return r;
}

constexpr vertex_t InvalidID = std::numeric_limits<vertex_t>::max();

template <bool Optimize, size_t MaxThreadCount, typename F>
VertexSet edge_map(Graph &graph, VertexSet &frontier, F &&f) {
    size_t vertex_count = graph.vertex_count();
    size_t sparse_threshold = graph.edge_count() / 20;
    frontier.to_sparse();
    // check whether sparse mode or dense mode
    std::vector<size_t> degree(frontier.size());
    for (size_t i = 0; i < frontier.size(); i++) {
        vertex_t v_id = frontier.indices[i];
        degree[i] = graph.out_degree(v_id);
    }
    size_t sum_degree = scan<size_t, size_t>(
        degree.data(), degree.data(), degree.size(),
        [](size_t x, size_t y) { return x + y; }, 0);
    auto &offsets = degree;
    bool sparse_mode = frontier.size() + sum_degree < sparse_threshold;
    if (sparse_mode) {
        // sparse mode
        std::vector<vertex_t> next_frontier(sum_degree);
        // parallel
        graph.template for_each_out_list<Optimize, MaxThreadCount>(
            frontier.indices, [&](size_t i, const Graph::EdgeList *edge_list) {
                size_t offset = offsets[i];
                for (Graph::degree_t d = 0; d < edge_list->degree; d++) {
                    vertex_t ngh = edge_list->neighbors[d];
                    bool u = f.cond(ngh) &&
                             f.update_atomic(edge_list->vertex_id, ngh);
                    next_frontier[offset + d] = u ? ngh : InvalidID;
                }
            });
        return VertexSet(vertex_count, unique_vaild<vertex_t, InvalidID>(
                                           std::move(next_frontier)));
    } else {
        // dense mode
        frontier.to_dense();
        std::vector<uint8_t> next_frontier(vertex_count);
        // parallel
        graph.template for_each_in_list<Optimize, MaxThreadCount>(
            [&](const Graph::EdgeList *edge_list) {
                vertex_t v = edge_list->vertex_id;
                next_frontier[v] = false;
                if (f.cond(v)) {
                    for (size_t j = 0; j < edge_list->degree; j++) {
                        vertex_t ngh = edge_list->neighbors[j];
                        if (frontier.exists[ngh] && f.update(ngh, v)) {
                            next_frontier[v] = true;
                            break;
                        }
                    }
                }
            });
        return VertexSet(std::move(next_frontier));
    }
}

template <size_t MaxThreadCount, std::invocable<vertex_t> F>
void vertex_map(const VertexSet &frontier, F &&f) {
    if (frontier.is_dense) {
        uthread::parallel_for<1024>(MaxThreadCount, frontier.n, [&](size_t i) {
            if (frontier.exists[i]) f(i);
        });
    } else {
        uthread::parallel_for<1024>(MaxThreadCount, frontier.m,
                                    [&](size_t i) { f(frontier.indices[i]); });
    }
}

struct Options {
    std::string graph_file_name;
    std::string config_file_name;
    std::optional<vertex_t> src;
    size_t max_iteration;
    size_t evaluation_count;
    bool verbose;
};

void read_options(int argc, const char *const argv[], Options &options);

using ComputeType = std::function<void(Graph &, const Options &)>;
void evaluate(ComputeType compute_opt, ComputeType compute_par,
              const Options &options);
