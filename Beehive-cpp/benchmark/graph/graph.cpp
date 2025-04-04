#include "graph.hpp"

#include <boost/program_options.hpp>

void read_options(int argc, const char *const argv[], Options &options) {
    namespace po = boost::program_options;
    po::options_description desc;
    desc.add_options()
        // help
        ("help,h", "print this message")
        // far memory config
        ("config,c", po::value<std::string>(), "far memory config file")
        // local memory size
        ("memory,M", po::value<double>(), "local memory size in GiB")
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
    options.local_memory =
        vm.contains("memory")
            ? std::make_optional(vm["memory"].as<double>() * (1LL << 30))
            : std::nullopt;
    options.src = vm.contains("source")
                      ? std::make_optional(vm["source"].as<vertex_t>())
                      : std::nullopt;
    options.evaluation_count = vm["eval-count"].as<size_t>();
    options.verbose = vm["verbose"].as<bool>();
    options.max_iteration = vm.contains("max-iteration")
                                ? vm["max-iteration"].as<size_t>()
                                : std::numeric_limits<size_t>::max();
}
