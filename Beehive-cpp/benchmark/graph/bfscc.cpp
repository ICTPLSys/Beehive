#include <cstdlib>
#include <set>
#include <vector>

#include "graph.hpp"
#include "utils/control.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
#include "utils/timer.hpp"
#include "utils/uthreads.hpp"

// returns a checksum
template <bool Optimize, size_t MaxThreadCount>
void component(Graph<> &graph, const Options &options) {
    constexpr bool Lockless = MaxThreadCount == 1;
    size_t vertex_count = graph.vertex_count();
    vertex_t src =
        options.src.has_value() ? options.src.value() : rand() % vertex_count;
    size_t max_hop = options.max_iteration;

    std::set<vertex_t> component;
    std::vector<vertex_t> frointer;
    component.insert(src);
    frointer.push_back(src);
    uthread::Mutex mutex;

    for (size_t i = 0; i < max_hop; i++) {
        std::vector<vertex_t> next_frointer;
        graph.template for_each_out_list<Optimize, MaxThreadCount>(
            frointer, [&](size_t i, const Graph<>::EdgeList *edge_list) {
                if constexpr (!Lockless) uthread::lock(&mutex);
                for (Graph<>::degree_t d = 0; d < edge_list->degree; d++) {
                    vertex_t ngh = edge_list->neighbors[d].ngh;
                    if (component.insert(ngh).second) {
                        next_frointer.push_back(ngh);
                    }
                }
                if constexpr (!Lockless) uthread::unlock(&mutex);
            });
        if (next_frointer.empty()) break;
        frointer.swap(next_frointer);
    }
}

void eval(Graph<> &graph, const Options &options) {
    // warm up
    std::cout << "warm up..." << std::endl;
    component<true, 1>(graph, options);
    component<false, 8>(graph, options);
    std::cout << std::endl;
    std::cout << std::setw(16) << "#iteration" << std::setw(16) << "optimized"
              << std::setw(16) << "mt-lockless" << std::setw(16) << "mt-lock-8"
              << std::setw(16) << "mt-lock-64" << std::endl;
    std::cout << std::string(16 * 5, '-') << std::endl;

    for (size_t i = 0; i < 8; i++) {
        std::atomic<double> opt_ms_total = 0;
        std::atomic<double> mt1_ms_total = 0;
        std::atomic<double> mt8_ms_total = 0;
        std::atomic<double> mt64_ms_total = 0;
        Cache::get_default()->invoke_eviction();
        Fibre::sleep(1);
        srand(i);
        auto mt1_perf_result = perf_profile([&] {
            uthread::parallel_for<1, true>(
                16 * 8, options.evaluation_count, [&](size_t) {
                    mt1_ms_total += time_for_ms(
                        [&] { component<false, 1>(graph, options); });
                });
        });
        Cache::get_default()->invoke_eviction();
        Fibre::sleep(1);
        srand(i);
        auto mt8_perf_result = perf_profile([&] {
            uthread::parallel_for<1, true>(
                16, options.evaluation_count, [&](size_t) {
                    mt8_ms_total += time_for_ms(
                        [&] { component<true, 8>(graph, options); });
                });
        });
        Cache::get_default()->invoke_eviction();
        Fibre::sleep(1);
        srand(i);
        auto mt64_perf_result = perf_profile([&] {
            uthread::parallel_for<1, true>(
                16, options.evaluation_count, [&](size_t) {
                    mt64_ms_total += time_for_ms(
                        [&] { component<true, 64>(graph, options); });
                });
        });
        Cache::get_default()->invoke_eviction();
        Fibre::sleep(1);
        srand(i);
        auto opt_perf_result = perf_profile([&] {
            uthread::parallel_for<1, true>(
                16, options.evaluation_count, [&](size_t) {
                    opt_ms_total += time_for_ms(
                        [&] { component<true, 1>(graph, options); });
                });
        });
        std::cout << std::setw(16) << "time" << std::setw(16)
                  << opt_ms_total / options.evaluation_count << std::setw(16)
                  << mt1_ms_total / options.evaluation_count << std::setw(16)
                  << mt8_ms_total / options.evaluation_count << std::setw(16)
                  << mt64_ms_total / options.evaluation_count << std::endl;
        std::cout << std::setw(16) << "l3_miss" << std::setw(16)
                  << opt_perf_result.l3_cache_miss / options.evaluation_count
                  << std::setw(16)
                  << mt1_perf_result.l3_cache_miss / options.evaluation_count
                  << std::setw(16)
                  << mt8_perf_result.l3_cache_miss / options.evaluation_count
                  << std::setw(16)
                  << mt64_perf_result.l3_cache_miss / options.evaluation_count
                  << std::endl;
        std::cout << std::endl;
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
    perf_init();
    Graph graph;
    graph.load(options.graph_file_name);
    std::cout << "loaded graph: " << graph.vertex_count() << " vertices, "
              << graph.edge_count() << " edges" << std::endl;
    eval(graph, options);
    FarLib::runtime_destroy();
    exit(0);
}
