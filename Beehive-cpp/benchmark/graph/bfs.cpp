#include "graph.hpp"
#include "utils/control.hpp"

// returns a checksum
template <bool Optimize>
void bfs(Graph &graph, const Options &options) {
    constexpr size_t MaxThreadCount = 64;
    size_t vertex_count = graph.vertex_count();
    vertex_t src =
        options.src.has_value() ? options.src.value() : rand() % vertex_count;
    size_t max_hop = options.max_iteration;
    std::vector<std::atomic<int>> n_hop(vertex_count);
    for (auto &x : n_hop) {
        x.store(-1, std::memory_order::relaxed);
    }
    n_hop[src] = 0;

    struct BFS {
        int hop;
        std::vector<std::atomic<int>> &n_hop;
        BFS(std::vector<std::atomic<int>> &n_hop, int hop)
            : n_hop(n_hop), hop(hop) {}
        bool cond(vertex_t v) {
            return n_hop[v].load(std::memory_order::relaxed) == -1;
        }
        bool update(vertex_t src, vertex_t dst) {
            n_hop[dst] = hop;
            return true;
        }
        bool update_atomic(vertex_t src, vertex_t dst) {
            int expected = -1;
            return n_hop[dst].compare_exchange_strong(
                expected, hop, std::memory_order::relaxed);
        }
    };

    VertexSet frontier(vertex_count, src);
    for (size_t i = 0; i < max_hop && !frontier.empty(); i++) {
        frontier =
            edge_map<Optimize, MaxThreadCount>(graph, frontier, BFS(n_hop, i));
    }
}

int main(int argc, const char *const argv[]) {
    Options options;
    read_options(argc, argv, options);
    Beehive::rdma::Configure config;
    config.from_file(options.config_file_name.c_str());
    Beehive::runtime_init(config);
    evaluate(bfs<true>, bfs<false>, options);
    Beehive::runtime_destroy();
    return 0;
}
