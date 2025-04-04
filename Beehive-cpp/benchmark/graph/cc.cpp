
#include "graph.hpp"
#include "utils/control.hpp"

// returns a checksum
template <bool Optimize>
void connected_components(Graph<> &graph, const Options &options) {
    constexpr size_t MaxThreadCount = 128;
    size_t vertex_count = graph.vertex_count();
    std::vector<std::atomic<vertex_t>> id(vertex_count);
    std::vector<vertex_t> previous_id(vertex_count);
    for (size_t i = 0; i < vertex_count; i++) {
        id[i].store(i, std::memory_order::relaxed);
    }

    struct CC {
        std::vector<std::atomic<vertex_t>> &id;
        std::vector<vertex_t> &previous_id;
        CC(std::vector<std::atomic<vertex_t>> &id,
           std::vector<vertex_t> &previous_id)
            : id(id), previous_id(previous_id) {}
        bool cond(vertex_t v) { return true; }
        bool update(vertex_t src, vertex_t dst) {
            vertex_t origin_id = id[dst].load(std::memory_order::relaxed);
            vertex_t new_id = id[src].load(std::memory_order::relaxed);
            if (new_id < origin_id) {
                id[dst].store(new_id, std::memory_order::relaxed);
                return origin_id == previous_id[dst];
            }
            return false;
        }
        bool update_atomic(vertex_t src, vertex_t dst) {
            vertex_t origin_id = id[dst].load(std::memory_order::relaxed);
            vertex_t new_id = id[src].load(std::memory_order::relaxed);
        retry:
            if (id[src] < origin_id) {
                if (!id[dst].compare_exchange_weak(origin_id, new_id))
                    goto retry;
                return origin_id == previous_id[dst];
            }
            return false;
        }
    };

    VertexSet frontier(vertex_count, all_exists{}, MaxThreadCount);
    for (size_t i = 0; i < options.max_iteration && !frontier.empty(); i++) {
        graph.vertex_map_cycles += vertex_map<MaxThreadCount>(
            frontier, [&](vertex_t v) { previous_id[v] = id[v]; });
        frontier = edge_map<Optimize, MaxThreadCount>(graph, frontier,
                                                      CC(id, previous_id));
    }
}

int main(int argc, const char *const argv[]) {
    Options options;
    read_options(argc, argv, options);
    FarLib::rdma::Configure config;
    config.from_file(options.config_file_name.c_str());
    if (options.local_memory.has_value()) {
        config.client_buffer_size = options.local_memory.value();
    }
    FarLib::runtime_init(config);
    evaluate<false>(connected_components<true>, options);
    FarLib::runtime_destroy();
    return 0;
}
