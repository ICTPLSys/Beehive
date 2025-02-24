#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <thread>
#include <utility>
#include <vector>

#include "async/generic_stream.hpp"
#include "async/scoped_inline_task.hpp"
#include "cache/cache.hpp"
#include "data_structure/concurrent_hashmap.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/cpu_cycles.hpp"
#include "utils/debug.hpp"

static constexpr size_t cmax(size_t n1, size_t n2) { return n1 > n2 ? n1 : n2; }
using namespace Beehive;
using namespace Beehive::cache;
using namespace Beehive::rdma;
using namespace std::chrono_literals;
using Map = ConcurrentHashMap<int32_t, int32_t>;
using StdMap = std::unordered_map<int32_t, int32_t>;

template <bool Mut>
struct GetCont {
    int32_t* result;
    GetCont(int32_t* r) : result(r) {}
    void operator()(LiteAccessor<int32_t, Mut> p) {
        if (p.is_null())
            *result = 0;
        else
            *result = *p;
    }
};

struct BoolCont {
    bool* result;
    BoolCont(bool* r) : result(r) {}
    void operator()(bool v) { *result = v; }
};

template <bool Mut>
using GetContext = Map::GetContext<Mut, GetCont<Mut>>;
using PutContext = Map::PutContext<BoolCont>;
using RemoveContext = Map::RemoveContext<BoolCont>;
static constexpr size_t ContextSize =
    cmax(sizeof(GetContext<true>),
         cmax(sizeof(GetContext<false>),
              cmax(sizeof(PutContext), sizeof(RemoveContext))));
static constexpr size_t EntryShift = 27;
static constexpr size_t OpCountPerRound = 1024 * 1024;
static constexpr size_t RoundCount = 16;
struct GetOp {
    int32_t k;
    int32_t v;
};

enum Op { GET, PUT, REMOVE };
struct PutOp {
    int32_t k;
    int32_t v;
    bool res;
};

struct RemoveOp {
    int32_t k;
    bool res;
};

union Operation {
    PutOp put_op;
    RemoveOp remove_op;
};
using OpsType = std::vector<std::pair<Op, Operation>>;
using GetOpsType = std::vector<GetOp>;
struct KeyGenerator {
    int32_t key;

    KeyGenerator() : key(0) {}

    template <bool Put>
    int32_t gen() {
        return rand() % 2 ? gen_exist() : Put ? gen_next() : gen_non_exist();
    }

    int32_t gen_exist() { return rand() % (key + 1); }

    int32_t gen_non_exist() { return key + 11111; }

    int32_t gen_next() { return key++; }
};

OpsType gen_ops_put_and_remove(KeyGenerator& key_generator) {
    OpsType ops;
    for (int i = 0; i < OpCountPerRound; i++) {
        Op op = rand() % 2 ? PUT : REMOVE;
        Operation operation;
        if (op == PUT) {
            PutOp put_op{
                .k = key_generator.gen<true>(),
                .v = rand(),
            };
            operation.put_op = put_op;
        } else {
            RemoveOp remove_op{
                .k = key_generator.gen<true>(),
            };
            operation.remove_op = remove_op;
        }
        ops.push_back(std::make_pair(op, operation));
    }
    return ops;
}

void run_runner_put_and_remove(OpsType& ops, Map& map,
                               DereferenceScope& scope) {
    // here only ordered is allowed
    async::GenericStreamRunner<ContextSize, true> runner(scope);
    for (auto& p : ops) {
        auto& op = p.first;
        if (op == PUT) {
            auto& put_op = p.second.put_op;
            runner.async_run([&](async::GenericOrderedContext* ctx) {
                new (ctx) PutContext(&map, put_op.k, put_op.v, &put_op.res);
            });
        } else {
            auto& remove_op = p.second.remove_op;
            runner.async_run([&](async::GenericOrderedContext* ctx) {
                new (ctx) RemoveContext(&map, &remove_op.k, &remove_op.res);
            });
        }
    }
    runner.await();
}

void check_std(OpsType& ops, StdMap& std_map) {
    for (auto& p : ops) {
        auto& op = p.first;
        if (p.first == PUT) {
            auto& put_op = p.second.put_op;
            std_map[put_op.k] = put_op.v;
            ASSERT(put_op.res);
        } else {
            auto& remove_op = p.second.remove_op;
            ASSERT(remove_op.res == std_map.count(remove_op.k));
            std_map.erase(remove_op.k);
        }
    }
}

GetOpsType gen_get_ops(KeyGenerator& generator) {
    GetOpsType ops;
    for (int i = 0; i < OpCountPerRound; i++) {
        GetOp get_op{.k = generator.gen<false>()};
        ops.push_back(get_op);
    }
    return ops;
}

template <bool Ordered>
void run_runner_get(GetOpsType& ops, Map& map, const DereferenceScope& scope) {
    async::GenericStreamRunner<ContextSize, Ordered> runner(scope);
    for (auto& op : ops) {
        runner.async_run([&](void* ctx) {
            new (ctx) GetContext<false>(&map, &op.k, &op.v);
        });
    }
    runner.await();
}

void check_std(GetOpsType& ops, StdMap& map) {
    for (auto& op : ops) {
        ASSERT(map.count(op.k) && op.v == map[op.k] ||
               !map.count(op.k) && op.v == 0);
    }
}

template <bool Ordered>
void test() {
    std::cout << "test " << (Ordered ? "ordered" : "non-ordered") << std::endl;
    StdMap std_map;
    Map map(EntryShift);
    KeyGenerator key_generator;
    std::cout << "test start" << std::endl;
    for (int r = 0; r < RoundCount; r++) {
        // put & remove
        {
            RootDereferenceScope scope;
            auto ops = gen_ops_put_and_remove(key_generator);
            uint64_t start_tsc = get_cycles();
            run_runner_put_and_remove(ops, map, scope);
            uint64_t end_tsc = get_cycles();
            std::cout << "r = " << r << ", put and remove, "
                      << end_tsc - start_tsc << std::endl;
            if constexpr (Ordered) {
                check_std(ops, std_map);
            }
        }
        {
            RootDereferenceScope root_scope;
            auto get_ops = gen_get_ops(key_generator);
            uint64_t start_tsc = get_cycles();
            run_runner_get<Ordered>(get_ops, map, root_scope);
            uint64_t end_tsc = get_cycles();
            std::cout << "r = " << r << ", get, " << end_tsc - start_tsc
                      << std::endl;
            // get
            if constexpr (Ordered) {
                check_std(get_ops, std_map);
            }
        }
        {
            // for_range
            RootDereferenceScope scope;
            auto ops = gen_get_ops(key_generator);
            uint64_t start_tsc = get_cycles();
            async::for_range<Ordered>(scope, 0, ops.size(), [&](size_t i) {
                auto& op = ops[i];
                return GetContext<false>(&map, &op.k, &op.v);
            });
            uint64_t end_tsc = get_cycles();
            std::cout << "r = " << r << ", get for range, "
                      << end_tsc - start_tsc << std::endl;
        }
    }
    std::cout << "test end" << std::endl;
}

int main() {
    Configure config{
        .server_addr = "127.0.0.1",
        .server_port = "50000",
        .server_buffer_size = 1024 * 1024 * 1024,
        .client_buffer_size = 16 * 1024 * 1024,
        .evict_batch_size = 64 * 1024,
    };
    srand(0);
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);
    Beehive::runtime_init(config);
    test<true>();   // ordered
    test<false>();  // non ordered
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}