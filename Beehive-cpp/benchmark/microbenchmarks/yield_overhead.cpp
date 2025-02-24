// In this microbenchmark,
// we will access objects in an array randomly,
// to compare the overhead of
// 1. uthread yield
// 2. user level thread
// 3. stackless out-of-order yield

#include <iomanip>
#include <iostream>
#include <memory>
#include <random>

#include "async/scoped_inline_task.hpp"
#include "cache/cache.hpp"
#include "utils/control.hpp"
#include "utils/parallel.hpp"
#include "utils/timer.hpp"

using namespace Beehive;

constexpr size_t ObjectSize = 256;

static_assert(ObjectSize >= sizeof(size_t));

struct Object {
    size_t id;
    char padding[ObjectSize - sizeof(size_t)];
};

static_assert(ObjectSize == sizeof(Object));

std::unique_ptr<UniqueFarPtr<Object>[]> init_objects(size_t object_count) {
    auto objects = std::make_unique<UniqueFarPtr<Object>[]>(object_count);
    for (size_t i = 0; i < object_count; i++) {
        auto accessor = objects[i].allocate();
        accessor->id = i;
    }
    return objects;
}

uint64_t delay_cycles = 0;
void delay() {
    if (delay_cycles > 0) {
        uint64_t cycles_start = get_cycles();
        while (get_cycles() < cycles_start + delay_cycles);
    }
}

size_t get_expected_sum(auto &random_engine, auto &distribution,
                        size_t access_count) {
    size_t sum = 0;
    for (size_t i = 0; i < access_count; i++) {
        sum += distribution(random_engine);
    }
    return sum;
}

size_t run_sequential(auto &random_engine, auto &distribution,
                      UniqueFarPtr<Object> *objects, size_t access_count) {
    RootDereferenceScope scope;
    size_t sum = 0;
    for (size_t i = 0; i < access_count; i++) {
        size_t idx = distribution(random_engine);
        delay();
        LiteAccessor<Object> accessor(objects[idx], scope);
        sum += accessor->id;
    }
    return sum;
}

// should run on only 1 core
size_t run_uthreads(auto &random_engine, auto &distribution,
                    UniqueFarPtr<Object> *objects, size_t access_count) {
    constexpr size_t UthreadCount = 16;
    std::size_t sum = 0;
    uthread::parallel_for_with_scope<1>(
        UthreadCount, access_count, [&](size_t i, DereferenceScope &scope) {
            ON_MISS_BEGIN
                uthread::yield();
            ON_MISS_END
            size_t idx = distribution(random_engine);
            delay();
            LiteAccessor<Object> accessor(objects[idx], __on_miss__, scope);
            sum += accessor->id;
        });
    return sum;
}

size_t run_stackless(auto &random_engine, auto &distribution,
                     UniqueFarPtr<Object> *objects, size_t access_count) {
    struct Context {
        LiteAccessor<Object> accessor;
        size_t *sum;
        UniqueFarPtr<Object> *object;

        Context(size_t &sum, UniqueFarPtr<Object> &object)
            : sum(&sum), object(&object) {}

        bool run(DereferenceScope &scope) {
            if (accessor.is_null()) {
                bool at_local = accessor.async_fetch(*object, scope);
                if (!at_local) return false;
            }
            *sum += accessor->id;
            return true;
        }
        bool fetched() const { return cache::at_local(accessor); }
        void pin() const { accessor.pin(); }
        void unpin() const { accessor.unpin(); }
    };
    RootDereferenceScope scope;
    size_t sum = 0;
    SCOPED_INLINE_ASYNC_FOR(Context, size_t, i, 0, i < access_count, i++, scope)
        size_t idx = distribution(random_engine);
        delay();
        return Context(sum, objects[idx]);
    SCOPED_INLINE_ASYNC_FOR_END
    return sum;
}

void fetch_only(auto &random_engine, auto &distribution,
                UniqueFarPtr<Object> *objects, size_t access_count) {
    for (size_t i = 0; i < access_count; i++) {
        size_t idx = distribution(random_engine);
        delay();
        Cache::get_default()->prefetch(objects[idx].obj());
    }
}

void dry_run(auto &random_engine, auto &distribution,
             UniqueFarPtr<Object> *objects, size_t access_count) {
    for (size_t i = 0; i < access_count; i++) {
        size_t idx = distribution(random_engine);
        delay();
    }
}

void run(size_t object_count, size_t access_count) {
    auto objects = init_objects(object_count);
    std::uniform_int_distribution<size_t> dist(0, object_count - 1);
    std::cout << std::setw(16) << "";
    std::cout << std::setw(16) << "dry run";
    std::cout << std::setw(16) << "sequential";
    std::cout << std::setw(16) << "uthreads";
    std::cout << std::setw(16) << "stackless ooo";
    std::cout << std::setw(16) << "fetch only";
    std::cout << std::endl;
    std::cout << std::string(16 * 6, '-') << std::endl;
    std::default_random_engine re(0);
    size_t expected_sum = get_expected_sum(re, dist, access_count);
    for (size_t i = 0; i < 3; i++) {
        std::cout << std::setw(16) << i << std::flush;

        re.seed(0);
        double dry_run_time = time_for_ms(
            [&] { dry_run(re, dist, objects.get(), access_count); });
        std::cout << std::setw(16) << dry_run_time << std::flush;

        re.seed(0);
        double sequential_ms = time_for_ms([&] {
            ASSERT(expected_sum ==
                   run_sequential(re, dist, objects.get(), access_count));
        });
        std::cout << std::setw(16) << sequential_ms << std::flush;

        re.seed(0);
        double uthreads_ms = time_for_ms([&] {
            ASSERT(expected_sum ==
                   run_uthreads(re, dist, objects.get(), access_count));
        });
        std::cout << std::setw(16) << uthreads_ms << std::flush;

        re.seed(0);
        double stackless_ms = time_for_ms([&] {
            ASSERT(expected_sum ==
                   run_stackless(re, dist, objects.get(), access_count));
        });
        std::cout << std::setw(16) << stackless_ms << std::flush;

        re.seed(0);
        double fetch_ms = time_for_ms(
            [&] { fetch_only(re, dist, objects.get(), access_count); });
        std::cout << std::setw(16) << fetch_ms << std::flush;

        std::cout << std::endl;
    }
}

int main(int argc, const char *const argv[]) {
    if (argc != 4 && argc != 5) {
        std::cout << "usage: " << argv[0]
                  << " <configure file> <memory size> <access count> "
                     "[additional delay (cycle) =0]"
                  << std::endl;
        return -1;
    }
    Beehive::rdma::Configure config;
    config.from_file(argv[1]);
    Beehive::runtime_init(config);
    size_t mem_size = std::atoll(argv[2]);
    size_t access_count = std::atoll(argv[3]);
    if (argc == 5) {
        delay_cycles = std::atoll(argv[4]);
    }
    ASSERT(mem_size > 0);
    ASSERT(mem_size <= config.server_buffer_size);
    ASSERT(access_count > 0);
    run(mem_size / sizeof(Object), access_count);
    Beehive::runtime_destroy();
    return 0;
}
