#include "rdma/server.hpp"

#include <iostream>

using namespace Beehive::rdma;

int main(int argc, const char *const argv[]) {
    if (argc != 2) {
        std::cerr << argv[0] << " <configure file>" << std::endl;
        std::abort();
    }
    const char *configure_file_name = argv[1];
    Configure config;
    config.from_file(configure_file_name);
    Server server(config);
    server.start();
    std::cout << "Bye!" << std::endl;
    return 0;
}