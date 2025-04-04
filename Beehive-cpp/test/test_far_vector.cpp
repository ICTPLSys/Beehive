#include <x86intrin.h>

#include <cstdlib>
#include <thread>
#include <vector>

#include "cache/cache.hpp"
#include "data_structure/far_vector.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

#define TEST_BASIC
#define TEST_ITER
#define TEST_CLR
#define TEST_LITE_SORT
#define TEST_VISIT

static constexpr double P = 998.244353;
static constexpr size_t DATA_SIZE = 32;
static constexpr size_t TEST_SIZE = 1024 * 1024 * 10;
static constexpr size_t POP_NUM = 10;
static constexpr size_t RESIZE_LARGE = TEST_SIZE + 1024;
static constexpr size_t RESIZE_SMALL = TEST_SIZE / 2;

struct MyData {
    char carr[4];
    int i;
    unsigned long ul;
    double d;
    char pad[DATA_SIZE - sizeof(char) * 4 - sizeof(int) -
             sizeof(unsigned long) - sizeof(double)];

public:
    MyData() = default;

    MyData(char c, int i, unsigned long ul, double d) : i(i), ul(ul), d(d) {
        carr[0] = c;
        for (int i = 1; i < 4; i++) {
            carr[i] = (carr[i - 1] + rand() % sizeof(char));
        }
    }

    void modify_by_data(MyData &data) {
        for (int j = 0; j < 4; j++) {
            carr[j] = (carr[j] * data.carr[i]) % sizeof(char);
        }
        i *= data.i;
        d = (data.d + P) * data.d;
        ul += data.ul;
    }

    MyData &operator=(const MyData &that) {
        memcpy(carr, that.carr, 4);
        i = that.i;
        ul = that.ul;
        d = that.d;
        return *this;
    }

    bool operator==(const MyData &that) const {
        return memcmp(carr, that.carr, 4) == 0 && i == that.i &&
               ul == that.ul && d == that.d;
    }

    static MyData rand_data() {
        return MyData(rand(), rand(), rand(), static_cast<double>(rand()) / P);
    }

    bool operator<(const MyData &that) {
        if (this->i == that.i) {
            return this->ul < that.ul;
        }
        return this->i < that.i;
    }

    bool operator<=(const MyData &that) {
        if (this->i == that.i) {
            return this->ul <= that.ul;
        }
        return this->i <= that.i;
    }

    bool operator>(const MyData &that) {
        if (this->i == that.i) {
            return this->ul > that.ul;
        }
        return this->i > that.i;
    }
};

static_assert(sizeof(MyData) == DATA_SIZE);
template <size_t GroupSize>
void build_test_data(std::vector<MyData> &std_vec,
                     FarVector<MyData, GroupSize> &far_vec,
                     size_t test_size = TEST_SIZE) {
    for (int i = 0; i < TEST_SIZE; i++) {
        std_vec.push_back(MyData::rand_data());
    }
    {
        RootDereferenceScope scope;
        for (int i = 0; i < TEST_SIZE; i++) {
            far_vec.push_back(std_vec[i], scope);
        }
    }
    std_vec.emplace_back('1', 1, 2147483648, 1.23);
    far_vec.emplace_back('1', 1, 2147483648, 1.23, scope);
    ASSERT(far_vec.size() == TEST_SIZE + 1);
    ASSERT(std_vec.size() == far_vec.size());
}

template <size_t GroupSize>
void build_test_data(std::vector<int> &std_vec,
                     FarVector<int, GroupSize> &far_vec,
                     size_t test_size = TEST_SIZE) {
    for (int i = 0; i < TEST_SIZE; i++) {
        std_vec.push_back(rand());
    }
    {
        RootDereferenceScope scope;
        for (int i = 0; i < TEST_SIZE; i++) {
            far_vec.push_back(std_vec[i], scope);
        }
    }
    ASSERT(far_vec.size() == TEST_SIZE);
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

void test() {
    using sort_std_vec_t = std::vector<int>;
    using sort_far_vec_t = FarVector<int>;
#ifdef TEST_BASIC
    {
        std::vector<MyData> std_vec;
        FarVector<MyData> far_vec;
        build_test_data(std_vec, far_vec);
        // TEST READ
        check(std_vec, far_vec);
        std::cout << "test read complete" << std::endl;
        // TEST WRITE
        for (int i = 1; i < TEST_SIZE; i++) {
            MyData &d0 = std_vec[i - 1];
            MyData &d1 = std_vec[i];
            d1.modify_by_data(d0);
        }

        for (int i = 1; i < TEST_SIZE; i++) {
            auto acc0 = far_vec[i - 1];
            auto acc1 = far_vec[i];
            acc1->modify_by_data(*acc0);
        }

        check(std_vec, far_vec);
        std::cout << "test write complete" << std::endl;
        // TEST pop back
        for (int i = 0; i < POP_NUM; i++) {
            std_vec.pop_back();
            far_vec.pop_back();
        }

        check(std_vec, far_vec);
        std::cout << "test pop back complete" << std::endl;
        // TEST resize
        std_vec.resize(RESIZE_LARGE);
        far_vec.resize(RESIZE_LARGE);
        ASSERT(std_vec.size() == far_vec.size());
        std_vec.resize(RESIZE_SMALL);
        far_vec.resize(RESIZE_SMALL);
        check(std_vec, far_vec);
        std::cout << "test resize complete" << std::endl;
        // TEST front, back, at
        // front
        {
            auto front_acc = far_vec.front_mut();
            ASSERT(std_vec.front() == *front_acc);
            auto const_front_acc = far_vec.front();
            ASSERT(std_vec.front() == *const_front_acc);
        }
        // back
        {
            auto back_acc = far_vec.back_mut();
            ASSERT(std_vec.back() == *back_acc);
            auto const_back_acc = far_vec.back();
            ASSERT(std_vec.back() == *const_back_acc);
        }
        // at
        int n = std_vec.size();
        for (int i = 0; i < n; i++) {
            auto accessor = far_vec.at_mut(i);
            auto const_accessor = far_vec.at(i);
            ASSERT(std_vec[i] == *accessor);
            ASSERT(std_vec[i] == *const_accessor);
        }
        std::cout << "test basic complete" << std::endl;
    }
#endif

#ifdef TEST_ITER
    {
        std::vector<MyData> std_vec;
        FarVector<MyData> far_vec;
        build_test_data(std_vec, far_vec);
        {
            auto std_it = std_vec.begin();
            auto far_it = far_vec.begin();
            int i = 0;
            while (std_it < std_vec.end() && far_it < far_vec.end()) {
                ASSERT(*std_it == *far_it);
                std_it++;
                far_it++;
            }
            ASSERT(std_it == std_vec.end());
            ASSERT(far_it == far_vec.end());
            std::cout << "test iterator complete" << std::endl;
        }
        {
            auto std_it = std_vec.cbegin();
            auto far_it = far_vec.cbegin();
            while (std_it < std_vec.cend() && far_it < far_vec.cend()) {
                ASSERT(*std_it == *far_it);
                std_it++;
                far_it++;
            }
            ASSERT(std_it == std_vec.cend());
            ASSERT(far_it == far_vec.cend());
            std::cout << "test const iterator complete" << std::endl;
        }
        {
            auto std_it = std_vec.rbegin();
            auto far_it = far_vec.rbegin();
            while (std_it < std_vec.rend() && far_it < far_vec.rend()) {
                ASSERT(*std_it == *far_it);
                std_it++;
                far_it++;
            }
            ASSERT(std_it == std_vec.rend());
            ASSERT(far_it == far_vec.rend());
            std::cout << "test reverse iterator complete" << std::endl;
        }
        {
            auto std_it = std_vec.crbegin();
            auto far_it = far_vec.crbegin();
            while (std_it < std_vec.crend() && far_it < far_vec.crend()) {
                ASSERT(*std_it == *far_it);
                std_it++;
                far_it++;
            }
            ASSERT(std_it == std_vec.crend());
            ASSERT(far_it == far_vec.crend());
            std::cout << "test const reverse iterator complete" << std::endl;
        }
        {
            RootDereferenceScope scope;
            auto std_it = std_vec.begin();
            auto far_it = far_vec.lbegin(scope);
            while (std_it < std_vec.end() && far_it < far_vec.lend(scope)) {
                ASSERT(*std_it == *far_it);
                ++std_it;
                far_it.next(scope);
            }
            ASSERT(std_it == std_vec.end());
            ASSERT(far_it == far_vec.lend(scope));
            std::cout << "test lite iterator complete" << std::endl;
        }
#ifdef TEST_CLR
        {
            std_vec.clear();
            far_vec.clear();
            ASSERT(far_vec.empty() && std_vec.size() == far_vec.size());
            std::cout << "test clear complete" << std::endl;
        }
#endif
    }
#endif
#ifdef TEST_LITE_SORT
    {
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
        std::cout << "test sort complete" << std::endl;
    }
#endif
#ifdef TEST_VISIT
    {
        std::vector<MyData> std_vec;
        FarVector<MyData> far_vec;
        build_test_data(std_vec, far_vec);
        for (auto &d : std_vec) {
            d.ul += d.i;
            d.d *= d.i;
        }
        far_vec.visit([](MyData &d) {
            d.ul += d.i;
            d.d *= d.i;
        });
        check(std_vec, far_vec);
    }
#endif
}

int main() {
    srand(time(NULL));
    Configure config;
    config.server_addr = "127.0.0.1";
    config.server_port = "1234";
    config.server_buffer_size = 1024L * 1024 * 1024 * 4;
    config.client_buffer_size = TEST_SIZE / 2;
    config.evict_batch_size = 64 * 1024;
    config.qp_count = 16;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    FarLib::runtime_init(config);
    test();
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}