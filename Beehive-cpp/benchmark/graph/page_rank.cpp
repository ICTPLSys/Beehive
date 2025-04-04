#include "graph.hpp"
#include "utils/control.hpp"

template <bool Optimize>
void page_rank(Graph<> &graph, const Options &options) {
    constexpr size_t MaxThreadCount = Optimize ? 16 : 256;
    constexpr double damping = 0.85;
    constexpr double epsilon = 0.0000001;
    const size_t vertex_count = graph.vertex_count();
    std::vector<double> pr_current(vertex_count);
    std::vector<double> pr_next(vertex_count);
    const double pr_init = 1 / static_cast<double>(vertex_count);
    uthread::parallel_for<64>(MaxThreadCount, vertex_count, [&](size_t i) {
        pr_current[i] = pr_init;
        pr_next[i] = 0;
    });

    for (size_t i = 0; i < options.max_iteration; i++) {
        graph.template for_each_in_list<Optimize, MaxThreadCount>(
            [&](const Graph<>::EdgeList *edge_list) {
                vertex_t v = edge_list->vertex_id;
                double pr = 0;
                for (size_t j = 0; j < edge_list->degree; j++) {
                    vertex_t ngh = edge_list->neighbors[j].ngh;
                    pr += pr_current[ngh] / graph.out_degree(ngh);
                }
                pr_next[v] = pr * damping + (1 - damping) * pr_init;
            });
        double L1_norm = 0;
        uthread::parallel_for<64>(MaxThreadCount, vertex_count, [&](size_t i) {
            L1_norm += std::abs(pr_next[i] - pr_current[i]);
        });
        if (L1_norm < epsilon) break;
        std::swap(pr_current, pr_next);
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
    evaluate<false>(page_rank<true>, options);
    FarLib::runtime_destroy();
    return 0;
}
