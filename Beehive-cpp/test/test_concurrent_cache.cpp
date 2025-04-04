#include <thread>
#include <vector>

#include "cache/accessor.hpp"
#include "cache/cache.hpp"
#include "cache/scope.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/parallel.hpp"

using namespace FarLib;
using namespace FarLib::cache;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

Configure config;
std::unique_ptr<Server> server;

constexpr size_t ThreadCount = 4;
constexpr size_t ObjCount = 1024 * 16;
constexpr size_t ObjSize = 1024 * 4;

void test() {
    auto local_cache = Cache::get_default();
    std::vector<far_obj_t> objs;
    objs.resize(ObjCount);

    // allocation
    uthread::parallel_for_with_scope<1>(
        ThreadCount, ObjCount, [&](size_t i, DereferenceScope &scope) {
            auto [obj, ptr] = local_cache->allocate<true>(ObjSize, true, scope);
            *static_cast<size_t *>(ptr) = i;
            objs[i] = obj;
        });
    printf("allocated & initialized\n");

    // sync_fetch & write back
    uthread::parallel_for_with_scope<1>(
        ThreadCount, ObjCount, [&](size_t i, DereferenceScope &scope) {
            auto obj = objs[i];
            void *ptr = local_cache->sync_fetch(obj, scope);
            size_t v = *static_cast<size_t *>(ptr);
            ASSERT(v == i);
            *static_cast<size_t *>(ptr) = i * i;
            local_cache->release_cache(obj, true);
        });
    printf("modified\n");

    uthread::parallel_for_with_scope<1>(
        ThreadCount, ObjCount, [&](size_t i, DereferenceScope &scope) {
            auto obj = objs[i];
            void *ptr = local_cache->sync_fetch(obj, scope);
            size_t v = *static_cast<size_t *>(ptr);
            ASSERT(v == i * i);
            local_cache->release_cache(obj, false);
        });
    printf("verified\n");

    size_t expected_sum = 0;
    for (size_t i = 0; i < ObjCount; i++) {
        expected_sum += i * i;
    }
    {
        std::atomic_size_t sum = 0;
        uthread::parallel_for_with_scope<1>(
            ThreadCount, ObjCount, [&](size_t i, DereferenceScope &scope) {
                auto obj = objs[i];
                cache::LiteAccessor<size_t> accessor(obj, scope);
                sum.fetch_add(*accessor);
            });
        ASSERT(sum.load() == expected_sum);
    }
    printf("tested accessor\n");

    // cache miss handler
    {
        std::atomic_size_t sum = 0;
        std::atomic_size_t on_miss = 0;
        ON_MISS_BEGIN
            on_miss.fetch_add(1);
        ON_MISS_END
        uthread::parallel_for_with_scope<1>(
            ThreadCount, ObjCount, [&](size_t i, DereferenceScope &scope) {
                auto obj = objs[i];
                auto accessor =
                    cache::LiteAccessor<size_t>(obj, __on_miss__, scope);
                sum += *accessor;
            });

        ASSERT(sum == expected_sum);
        ASSERT(on_miss >= 0 && on_miss <= objs.size());
        printf("on_miss: %ld\n", on_miss.load());
    }
    printf("tested miss handler\n");

    // lite accessor & write back
    {
        uthread::parallel_for_with_scope<1>(
            ThreadCount, ObjCount, [&](size_t i, DereferenceScope &scope) {
                ON_MISS_BEGIN
                    __define_oms__(scope);
                    for (size_t j = i + 1; j < std::min(ObjCount, i + 16);
                         j++) {
                        Cache::get_default()->prefetch(objs[j], oms);
                    }
                ON_MISS_END
                auto obj = objs[i];
                auto accessor =
                    LiteAccessor<std::array<int, ObjSize / sizeof(int)>, true>(
                        obj, __on_miss__, scope);
                for (int &v : *accessor) {
                    v = i;
                }
            });
        uthread::parallel_for_with_scope<1>(
            ThreadCount, ObjCount, [&](size_t i, DereferenceScope &scope) {
                ON_MISS_BEGIN
                    __define_oms__(scope);
                    for (size_t j = i + 1; j < std::min(ObjCount, i + 16);
                         j++) {
                        Cache::get_default()->prefetch(objs[j], oms);
                    }
                ON_MISS_END
                auto obj = objs[i];
                auto accessor =
                    LiteAccessor<std::array<int, ObjSize / sizeof(int)>>(
                        obj, __on_miss__, scope);
                for (int v : *accessor) {
                    ASSERT(v == i);
                }
            });
        printf("tested lite accessor\n");
    }
}

int main() {
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.server_buffer_size = ObjCount * ObjSize * 2;
    config.client_buffer_size = 2 * ThreadCount * allocator::RegionSize;
    config.evict_batch_size = 4 * 1024;
    config.max_thread_cnt = ThreadCount;
    server.reset(new Server(config));
    std::thread server_thread([] { server->start(); });
    std::this_thread::sleep_for(1s);
    FarLib::runtime_init(config);
    test();
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}
