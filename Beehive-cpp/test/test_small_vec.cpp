#include <chrono>
#include <thread>

#include "data_structure/small_vector.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

Configure config;

void test(size_t n) {
    std::cout << "testing n = " << n << std::endl;
    RootDereferenceScope scope;
    SmallVector<int> vec(scope);
    // allocation
    for (size_t i = 0; i < n; i++) {
        vec.push_back(i, scope);
    }
    size_t expected_i = 0;
    vec.for_each(
        [&](size_t i, int x) {
            ASSERT(i == expected_i);
            ASSERT(x == i);
            expected_i++;
        },
        scope);
    ASSERT(expected_i == n);
}

int main() {
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 1024L * 1024 * 1024 * 2;
    config.client_buffer_size = 64 * 1024 * 1024;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    Beehive::runtime_init(config);
    test(1);
    test(3);
    test(16);
    test(666);
    test(1018);
    test(1019);
    test(1024);
    test(1234);
    test(8765);
    test(64 * 1024);
    test(1024 * 1024);
    test(64 * 1024 * 1024);
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}
