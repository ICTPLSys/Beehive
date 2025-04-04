#include "graph.hpp"
#include "utils/control.hpp"

vertex_t default_src = 0;

// returns a checksum
template <bool Optimize, bool TimeStamp>
void betweenness_centrality(Graph<TimeStamp> &graph, const Options &options) {
    constexpr size_t MaxThreadCount = Optimize ? 64 : 256;
    size_t vertex_count = graph.vertex_count();
    std::vector<std::atomic<size_t>> num_paths(vertex_count);
    std::vector<uint8_t> visited(vertex_count);
    uthread::parallel_for<1024>(MaxThreadCount, vertex_count, [&](size_t i) {
        num_paths[i] = 0;
        visited[i] = false;
    });
    vertex_t src;
    if (options.src.has_value()) {
        src = options.src.value();
    } else {
        src = default_src % vertex_count;
        default_src++;
    }
    num_paths[src] = 1;
    visited[src] = true;

    struct PathsUpdate {
        std::vector<std::atomic<size_t>> &num_paths;
        std::vector<uint8_t> &visited;
        PathsUpdate(std::vector<std::atomic<size_t>> &num_paths,
                    std::vector<uint8_t> &visited)
            : num_paths(num_paths), visited(visited) {}
        bool cond(vertex_t v) { return !visited[v]; }
        bool update(vertex_t src, vertex_t dst) {
            size_t old_value = num_paths[dst].load(std::memory_order::relaxed);
            size_t src_value = num_paths[src].load(std::memory_order::relaxed);
            num_paths[dst].store(old_value + src_value,
                                 std::memory_order::relaxed);
            return old_value == 0;
        }
        bool update_atomic(vertex_t src, vertex_t dst) {
            size_t src_value = num_paths[src].load(std::memory_order::relaxed);
            size_t old_value =
                num_paths[dst].fetch_add(src_value, std::memory_order::relaxed);
            return old_value == 0;
        }
    };

    struct DependenciesUpdate {
        std::vector<std::atomic<size_t>> &num_paths;
        std::vector<std::atomic<double>> &dependencies;
        std::vector<uint8_t> &visited;
        DependenciesUpdate(std::vector<std::atomic<size_t>> &num_paths,
                           std::vector<std::atomic<double>> &dependencies,
                           std::vector<uint8_t> &visited)
            : num_paths(num_paths),
              dependencies(dependencies),
              visited(visited) {}
        bool cond(vertex_t v) { return !visited[v]; }
        bool update(vertex_t src, vertex_t dst) {
            double old_value =
                dependencies[dst].load(std::memory_order::relaxed);
            double dst_paths = num_paths[dst].load(std::memory_order::relaxed);
            double src_paths = num_paths[src].load(std::memory_order::relaxed);
            double src_dep = dependencies[src].load(std::memory_order::relaxed);
            double new_value =
                old_value + dst_paths / src_paths * (1 + src_dep);
            dependencies[dst].store(new_value, std::memory_order::relaxed);
            return old_value == 0.0;
        }
        bool update_atomic(vertex_t src, vertex_t dst) {
            double old_value =
                dependencies[dst].load(std::memory_order::relaxed);
            double dst_paths = num_paths[dst].load(std::memory_order::relaxed);
            double src_paths = num_paths[src].load(std::memory_order::relaxed);
            double src_dep = dependencies[src].load(std::memory_order::relaxed);
            double addition = dst_paths / src_paths * (1 + src_dep);
            dependencies[dst].fetch_add(addition, std::memory_order::relaxed);
            return old_value == 0.0;
        }
    };

    // phase 1
    std::vector<VertexSet> levels;
    levels.emplace_back(vertex_count, src);
    while (!levels.back().empty()) {
        VertexSet output = edge_map<Optimize, MaxThreadCount>(
            graph, levels.back(), PathsUpdate(num_paths, visited));
        graph.vertex_map_cycles += vertex_map<MaxThreadCount>(
            output, [&](vertex_t v) { visited[v] = true; });
        levels.push_back(std::move(output));
    }
    // phase 2
    graph.transpose();
    std::vector<std::atomic<double>> dependencies(vertex_count);
    uthread::parallel_for<1024>(MaxThreadCount, vertex_count, [&](size_t i) {
        dependencies[i] = 0.0;
        visited[i] = false;
    });
    for (auto it = levels.rbegin(); it != levels.rend(); it++) {
        auto &frointer = *it;
        graph.vertex_map_cycles += vertex_map<MaxThreadCount>(
            frointer, [&](vertex_t v) { visited[v] = true; });
        edge_map<Optimize, MaxThreadCount>(
            graph, frointer,
            DependenciesUpdate(num_paths, dependencies, visited));
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
    evaluate<true>(betweenness_centrality<true, true>, options);
    FarLib::runtime_destroy();
    return 0;
}
