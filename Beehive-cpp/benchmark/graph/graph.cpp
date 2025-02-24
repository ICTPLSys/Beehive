#include "graph.hpp"

#include <boost/program_options.hpp>

#include "utils/perf.hpp"
#include "utils/timer.hpp"

void read_options(int argc, const char *const argv[], Options &options) {
    namespace po = boost::program_options;
    po::options_description desc;
    desc.add_options()
        // help
        ("help,h", "print this message")
        // far memory config
        ("config,c", po::value<std::string>(), "far memory config file")
        // input
        ("input,i", po::value<std::string>(), "input graph file")
        // source
        ("source,s", po::value<vertex_t>(), "source vertex")
        // iteration count
        ("eval-count,n", po::value<size_t>()->default_value(8),
         "evaluation count")
        // verbose
        ("verbose,v", po::value<bool>()->default_value(false),
         "print each iteration time")
        // max iteration
        ("max-iteration,m", po::value<size_t>(), "max iteration");
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
    if (vm.contains("help") || !vm.contains("config") ||
        !vm.contains("input")) {
        std::cout << desc << std::endl;
        exit(-1);
    }
    options.config_file_name = vm["config"].as<std::string>();
    options.graph_file_name = vm["input"].as<std::string>();
    options.src = vm.contains("source")
                      ? std::make_optional(vm["source"].as<vertex_t>())
                      : std::nullopt;
    options.evaluation_count = vm["eval-count"].as<size_t>();
    options.verbose = vm["verbose"].as<bool>();
    options.max_iteration = vm.contains("max-iteration")
                                ? vm["max-iteration"].as<size_t>()
                                : std::numeric_limits<size_t>::max();
}

void evaluate(ComputeType compute_opt, ComputeType compute_par,
              const Options &options) {
    srand(0);
    perf_init();
    Graph graph;
    graph.load(options.graph_file_name);
    std::cout << "loaded graph: " << graph.vertex_count() << " vertices, "
              << graph.edge_count() << " edges" << std::endl;
    // warm up
    std::cout << "warm up..." << std::endl;
    compute_opt(graph, options);
    // compute_par(graph, options);
    std::cout << std::endl;
    double opt_ms_total = 0;
    double par_ms_total = 0;
    std::cout << std::setw(16) << "#iteration" << std::setw(16) << "optimized"
              << std::setw(16) << "multi-uthreads" << std::endl;
    std::cout << std::string(16 * 3, '-') << std::endl;

    if (options.verbose) {
        std::cout << "==== break down for opt ====" << std::endl;
        profile::reset_all();
        perf_profile([&] {
            profile::start_work();
            profile::thread_start_work();
            compute_opt(graph, options);
            profile::thread_end_work();
            profile::end_work();
        }).print();
        profile::print_profile_data();
        // std::cout << "==== break down for uthread ====" << std::endl;
        // profile::reset_all();
        // perf_profile([&] {
        //     profile::start_work();
        //     profile::thread_start_work();
        //     compute_par(graph, options);
        //     profile::thread_end_work();
        //     profile::end_work();
        // }).print();
        // profile::print_profile_data();
    }

    for (size_t i = 0; i < options.evaluation_count; i++) {
        double opt_ms = time_for_ms([&] { compute_opt(graph, options); });
        double par_ms = time_for_ms([&] { /* compute_par(graph, options); */ });
        opt_ms_total += opt_ms;
        par_ms_total += par_ms;
        if (options.verbose) {
            std::cout << std::setw(16) << i << std::setw(16) << opt_ms
                      << std::setw(16) << par_ms << std::endl;
        }
    }
    std::cout << std::setw(16) << "average" << std::setw(16)
              << opt_ms_total / options.evaluation_count << std::setw(16)
              << par_ms_total / options.evaluation_count << std::endl;
    return;
}
