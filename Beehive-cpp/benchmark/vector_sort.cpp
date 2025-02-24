#include <x86intrin.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/far_vector.hpp"
#include "rdma/client.hpp"
#include "utils/control.hpp"
using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

static size_t VECTOR_SIZE = 0;

template <size_t GroupSize>
void build_test_data(std::vector<int> &std_vec,
                     FarVector<int, GroupSize> &far_vec,
                     size_t test_size = VECTOR_SIZE) {
    for (int i = 0; i < VECTOR_SIZE; i++) {
        std_vec.push_back(rand());
    }
    for (int i = 0; i < VECTOR_SIZE; i++) {
        far_vec.push_back_slow(std_vec[i]);
    }
    ASSERT(far_vec.size() == VECTOR_SIZE);
    ASSERT(std_vec.size() == far_vec.size());
}

template <typename T>
void check(std::vector<T> &std_vec, FarVector<T> &far_vec) {
    int n = std_vec.size();
    ASSERT(n == far_vec.size());
    for (int i = 0; i < n; i++) {
        auto accessor = far_vec[i];
        ASSERT(std_vec[i] == *accessor);
    }
}

void run() {
    using sort_std_vec_t = std::vector<int>;
    using sort_far_vec_t = FarVector<int>;

    sort_std_vec_t std_vec;
    sort_far_vec_t far_vec;
    build_test_data(std_vec, far_vec);
    auto start = __rdtsc();
    std::sort(std_vec.begin(), std_vec.end());
    auto end = __rdtsc();
    std::cout << "std sort: " << end - start << std::endl;
    {
        start = __rdtsc();
        sort_far_vec_t::sort(far_vec.lbegin(), far_vec.lend());
        end = __rdtsc();
        std::cout << "remote sort: " << end - start << std::endl;
    }
    check(std_vec, far_vec);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cout << "usage: " << std::string(argv[0])
                  << " <config> <VECTOR_SIZE>" << std::endl;
        return 0;
    }
    srand(0);
    Configure config;
    config.from_file(argv[1]);
    VECTOR_SIZE = std::stoi(argv[2]);
    config.client_buffer_size = VECTOR_SIZE / 2;
    std::cout << "VECTOR SIZE(MB): " << VECTOR_SIZE / (1024 * 1024)
              << std::endl;
    Beehive::runtime_init(config);
    run();
    Beehive::runtime_destroy();
    return 0;
}