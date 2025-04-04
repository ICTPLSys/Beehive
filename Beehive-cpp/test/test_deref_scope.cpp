#include <thread>

#include "cache/accessor.hpp"
#include "cache/cache.hpp"
#include "data_structure/vector.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"
using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

void run() {
    constexpr size_t GroupSize = 1024 / sizeof(int);
    constexpr size_t VecSize = 1024 * GroupSize - 3;
    DenseVector<int, GroupSize> vec;
    // allocation
    {
        RootDereferenceScope scope;
        for (size_t i = 0; i < VecSize; i++) {
            vec.emplace_back(scope, i);
        }
    }
    ASSERT(vec.size() == VecSize);
    constexpr int expected_sum =
        static_cast<int>(VecSize * ((VecSize - 1) / 2));
    // 1 thread
    {
        std::cout << "1 thread" << std::endl;
        struct Scope : public RootDereferenceScope {
            // this accessor will be held during the scope
            LiteAccessor<int> v0;
            void pin() const override { v0.pin(); }
            void unpin() const override { v0.unpin(); }
        };

        Scope scope;
        scope.v0 = vec.get_lite(0, scope);
        uint64_t v0_value = *scope.v0;
        int sum = 0;
        for (size_t i = 0; i < VecSize; i++) {
            sum += *vec.get_lite(i, scope);
        }
        ASSERT(*scope.v0 == v0_value);
        ASSERT(sum == expected_sum);
    }
    // 4 threads
    {
        std::cout << "4 thread" << std::endl;
        std::atomic_int sum = 0;
        uthread::parallel_for_with_scope<1>(
            4, VecSize, [&](size_t i, DereferenceScope &scope) {
                sum += *vec.get_lite(i, scope);
            });
        ASSERT(sum == expected_sum);
    }
}

int main() {
    Configure config;
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = 4L * 1024 * 1024;
    config.client_buffer_size = 1 * 1024 * 1024;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    FarLib::runtime_init(config);
    run();
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}
