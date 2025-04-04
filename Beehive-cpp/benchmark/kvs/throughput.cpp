#include <hdr/hdr_histogram.h>

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/timer/progress_display.hpp>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "async/stream_runner.hpp"
#include "cache/cache.hpp"
#include "data_structure/concurrent_hashmap.hpp"
#include "test/fixed_size_string.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/perf.hpp"
#include "utils/stats.hpp"
#include "utils/threads.hpp"
#include "utils/zipfian.hpp"

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

#define DISPLAY_PROGRESS

constexpr bool EnableLocalBench = false;

using str_key_t = FixedSizeString<32>;
using str_value_t = FixedSizeString<256>;

using LocalHashTable = std::unordered_map<str_key_t, str_value_t>;
using RemoteHashTable = ConcurrentHashMap<str_key_t, str_value_t>;

class Server;

class Workload {
public:
    enum OpType { GET, PUT, REMOVE };

    struct Request {
        OpType op_type;
        const str_key_t& key;
        const str_value_t& value;
        uint64_t request_start_ns;

        Request(OpType op_type, const str_key_t& key, const str_value_t& value)
            : op_type(op_type), key(key), value(value) {}
    };

    struct Response {
        uint64_t in_queue_lat_ns;
        uint64_t service_latency_ns;
    };

    struct Config {
        size_t n_server_thread;
        size_t n_client_thread;
        double put_ratio;
        double remove_ratio;
        double zipfian_constant;
        std::chrono::nanoseconds max_runtime;
        uint64_t max_serve_count;
    };

    // 10GB
    // static constexpr size_t InitialDataCount = 32 * 1024 * 1024;
    static constexpr size_t InitialDataCount = 1024 * 1024;
    static constexpr size_t DataSizeShift = 27;
    static constexpr size_t DataSize = 1 << DataSizeShift;
    static_assert(InitialDataCount < DataSize);

public:
    Workload() : remote_hash_table(DataSizeShift) { init_data(); }

    LocalHashTable* get_local_hash_table() { return &local_hash_table; }

    RemoteHashTable* get_remote_hash_table() { return &remote_hash_table; }

    uint64_t gen_requests(Server& server, const Config& config, size_t qi);

    uint64_t gen_requests_max_speed(Server& server, const Config& config,
                                    size_t qi_start, size_t qi_end);

private:
    void init_data() {
        std::cout << "Initialize: loading " << InitialDataCount << " K-V pairs"
                  << std::endl;
        data.reserve(InitialDataCount);
        local_hash_table.reserve(InitialDataCount);
#ifdef DISPLAY_PROGRESS
        std::cout << "Loading Data..." << std::endl;
        boost::timer::progress_display progress(InitialDataCount);
#endif
        RootDereferenceScope scope;
        for (size_t i = 0; i < InitialDataCount; i++) {
            str_key_t key = str_key_t::random();
            str_value_t value = str_value_t::random();
            ASSERT(remote_hash_table.put(key, value, scope));
            data.push_back({key, value});
            if constexpr (EnableLocalBench) {
                local_hash_table[key] = value;
            }
#ifdef DISPLAY_PROGRESS
            ++progress;
#endif
        }
    }

private:
    std::vector<std::pair<str_key_t, str_value_t>> data;
    LocalHashTable local_hash_table;
    RemoteHashTable remote_hash_table;
};

hdr_histogram* hist = nullptr;

void init_hist() {
    int err = hdr_init(1, 1'000'000, 3, &hist);
    if (err != 0) abort();
}

void print_hist() {
    double mean = hdr_mean(hist);
    uint64_t p50 = hdr_value_at_percentile(hist, 50);
    uint64_t p90 = hdr_value_at_percentile(hist, 90);
    uint64_t p95 = hdr_value_at_percentile(hist, 95);
    uint64_t p99 = hdr_value_at_percentile(hist, 99);
    printf("%f %lu %lu %lu %lu\n", mean, p50, p90, p95, p99);
    FILE* f = fopen("kvs.hist", "w");
    hdr_percentiles_print(hist, f, 100, 1, format_type::CLASSIC);
    fclose(f);
    hdr_close(hist);
}

class Server {
public:
    static constexpr size_t QueueCap = 64;
    using RequestQueue =
        boost::lockfree::spsc_queue<Workload::Request*,
                                    boost::lockfree::capacity<QueueCap>>;

protected:
    static constexpr size_t MaxNQueue = 8;

    size_t n_queue;
    std::atomic_bool serving;
    RequestQueue request_queue[MaxNQueue];
    std::atomic_size_t n_served = 0;

public:
    Server(size_t n_queue) : n_queue(n_queue) {
        ASSERT(n_queue <= MaxNQueue);
        serving = true;
    }

    ~Server() {}

    virtual void serve(size_t qi) = 0;

    bool send_request(size_t qi, Workload::Request* req) {
        req->request_start_ns = get_time_ns();
        bool sent = request_queue[qi].push(req);
        door_bell(qi);
        return sent;
    }

    virtual void door_bell(size_t qi) {}

    virtual void finish_serving() { serving.store(false); }

    size_t get_served_count() const { return n_served.load(); }

protected:
    bool get_request(size_t qi, Workload::Request*& request,
                     Workload::Response& response) {
        if (request_queue[qi].pop(request)) {
            response.in_queue_lat_ns =
                get_time_ns() - request->request_start_ns;
            return true;
        }
        return false;
    }

    void send_response(Workload::Request* request,
                       Workload::Response& response) {
        response.service_latency_ns = get_time_ns() - request->request_start_ns;
        n_served.fetch_add(1, std::memory_order::relaxed);
        delete request;
    }

    bool is_serving() { return serving.load(std::memory_order::relaxed); }
};

class DryServer : public Server {
public:
    DryServer(size_t n_queue, Workload& workload) : Server(n_queue) {}

    void serve(size_t qi) override {
        while (is_serving()) {
            Workload::Request* request;
            Workload::Response response;
            if (get_request(qi, request, response)) {
                send_response(request, response);
            }
        }
    }
};

class LocalServer : public Server {
public:
    LocalServer(size_t n_queue, Workload& workload) : Server(n_queue) {
        hash_table = workload.get_local_hash_table();
    }

    void serve(size_t qi) override {
        while (is_serving()) {
            Workload::Request* request;
            Workload::Response response;
            if (get_request(qi, request, response)) {
                switch (request->op_type) {
                case Workload::GET: {
                    auto it = hash_table->find(request->key);
                    ASSERT(it == hash_table->end() ||
                           it->second == request->value);
                    break;
                }
                case Workload::PUT: {
                    hash_table->insert_or_assign(request->key, request->value);
                    break;
                }
                case Workload::REMOVE: {
                    hash_table->erase(request->key);
                    break;
                }
                }
                send_response(request, response);
            }
        }
    }

protected:
    LocalHashTable* hash_table;
};

class RemotableServer : public Server {
public:
    RemotableServer(size_t n_queue, Workload& workload) : Server(n_queue) {
        hash_table = workload.get_remote_hash_table();
    }

protected:
    RemoteHashTable* hash_table;
};

class SyncServer : public RemotableServer {
public:
    SyncServer(size_t n_queue, Workload& workload)
        : RemotableServer(n_queue, workload) {}

    void serve(size_t qi) override {
        RootDereferenceScope scope;
        while (is_serving()) {
            Workload::Request* request;
            Workload::Response response;
            if (get_request(qi, request, response)) {
                auto start = get_time_ns();
                switch (request->op_type) {
                case Workload::GET: {
                    str_value_t value;
                    bool found = hash_table->get(request->key, &value, scope);
                    ASSERT(!found || value == request->value);
                    break;
                }
                case Workload::PUT: {
                    hash_table->put(request->key, request->value, scope);
                    break;
                }
                case Workload::REMOVE: {
                    hash_table->remove(request->key, scope);
                }
                }
                auto end = get_time_ns();
                hdr_record_value_atomic(hist, end - start);
                send_response(request, response);
            }
        }
    }
};

class UThreadServer : public RemotableServer {
public:
    UThreadServer(size_t n_queue, Workload& workload)
        : RemotableServer(n_queue, workload), request_cond(n_queue) {}

    void door_bell(size_t qi) override {
        uthread::notify(&request_cond[qi], &request_cond_mutex);
    }

    void finish_serving() override {
        serving.store(false);
        for (auto& c : request_cond) {
            uthread::notify_all(&c, &request_cond_mutex);
        }
    }

    void serve(size_t qi) override { serve_thread(qi); }

private:
    void serve_thread(size_t qi) {
        ON_MISS_BEGIN
            uthread::yield();
        ON_MISS_END
        RootDereferenceScope scope;
        while (is_serving()) {
            Workload::Request* request;
            Workload::Response response;
            while (get_request(qi, request, response)) {
                switch (request->op_type) {
                case Workload::GET: {
                    str_value_t value;
                    bool found = hash_table->get(request->key, &value,
                                                 __on_miss__, scope);
                    ASSERT(!found || value == request->value);
                    break;
                }
                case Workload::PUT: {
                    hash_table->put(request->key, request->value, __on_miss__,
                                    scope);
                    break;
                }
                case Workload::REMOVE: {
                    hash_table->remove(request->key, __on_miss__, scope);
                    break;
                }
                }
                send_response(request, response);
            }
            if (serving.load())
                uthread::wait(&request_cond[qi], &request_cond_mutex);
        }
    }

private:
    std::vector<uthread::Condition> request_cond;
    uthread::Mutex request_cond_mutex;
};

class PararoutineServer : public RemotableServer {
    struct Context {
        PararoutineServer* server;
        Workload::OpType op_type;
        Workload::Request* request;
        Workload::Response response;

        RemoteHashTable* get_hash_map() { return server->hash_table; }
        const str_key_t* get_key() const { return &(request->key); }
        const str_value_t& get_value() const { return request->value; }
        // get
        void make_result(const str_value_t* v) {
            ASSERT(v == nullptr || *v == request->value);
            server->send_response(request, response);
        }
        // put & remove
        void make_result(bool r) { server->send_response(request, response); }
    };
    using GetFrame = RemoteHashTable::GetFrame<Context>;
    using PutFrame = RemoteHashTable::PutFrame<Context>;
    using RemoveFrame = RemoteHashTable::RemoveFrame<Context>;
    union Frame {
        Context base_frame;
        GetFrame get_frame;
        PutFrame put_frame;
        RemoveFrame remove_frame;

        void init() {
            base_frame.op_type = base_frame.request->op_type;
            switch (base_frame.op_type) {
            case Workload::GET:
                get_frame.init();
                break;
            case Workload::PUT:
                put_frame.init();
                break;
            case Workload::REMOVE:
                remove_frame.init();
                break;
            }
        }
        size_t conflict_id() const {
            switch (base_frame.op_type) {
            case Workload::GET:
                return get_frame.conflict_id();
            case Workload::PUT:
                return put_frame.conflict_id();
            case Workload::REMOVE:
                return remove_frame.conflict_id();
            }
            __builtin_unreachable();
            return 0;
        }
        ~Frame() {
            switch (base_frame.op_type) {
            case Workload::GET:
                std::destroy_at(&get_frame);
                break;
            case Workload::PUT:
                std::destroy_at(&put_frame);
                break;
            case Workload::REMOVE:
                std::destroy_at(&remove_frame);
                break;
            }
        }

        bool fetched() {
            switch (base_frame.op_type) {
            case Workload::GET:
                return get_frame.fetched();
            case Workload::PUT:
                return put_frame.fetched();
            case Workload::REMOVE:
                return remove_frame.fetched();
            }
            __builtin_unreachable();
            return true;
        }
        void pin() {
            switch (base_frame.op_type) {
            case Workload::GET:
                get_frame.pin();
                return;
            case Workload::PUT:
                put_frame.pin();
                return;
            case Workload::REMOVE:
                remove_frame.pin();
                return;
            }
            __builtin_unreachable();
        }
        void unpin() {
            switch (base_frame.op_type) {
            case Workload::GET:
                get_frame.unpin();
                return;
            case Workload::PUT:
                put_frame.unpin();
                return;
            case Workload::REMOVE:
                remove_frame.unpin();
                return;
            }
            __builtin_unreachable();
        }
        bool run(DereferenceScope& scope) {
            switch (base_frame.op_type) {
            case Workload::GET:
                return get_frame.run(scope);
            case Workload::PUT:
                return put_frame.run(scope);
            case Workload::REMOVE:
                return remove_frame.run(scope);
            }
            __builtin_unreachable();
        }
    };
    struct ReqStream {
        using Context = Frame;
        PararoutineServer* server;
        size_t qi;

        ReqStream(PararoutineServer* server, size_t qi)
            : server(server), qi(qi) {}

        async::StreamState get(Context* ctx) {
            if (!server->is_serving()) return async::StreamState::FINISHED;
            if (!server->get_request(qi, ctx->base_frame.request,
                                     ctx->base_frame.response)) {
                return async::StreamState::WAITING;
            }
            ctx->base_frame.server = server;
            ctx->init();
            return async::StreamState::READY;
        }
    };

public:
    PararoutineServer(size_t n_queue, Workload& workload)
        : RemotableServer(n_queue, workload) {}

    void serve(size_t qi) override {
        process_pararoutine_stream<ReqStream, true>(ReqStream(this, qi));
    }
};

static Workload::OpType get_op_type(float rand_num,
                                    const Workload::Config& config) {
    if (rand_num < config.put_ratio) {
        return Workload::PUT;
    } else if (rand_num < config.put_ratio + config.remove_ratio) {
        return Workload::REMOVE;
    } else {
        return Workload::GET;
    }
}

// main funtion for client
uint64_t Workload::gen_requests(Server& server, const Config& config,
                                size_t qi) {
    uint64_t max_runtime_ns = config.max_runtime.count();
    uint64_t max_serve_count = config.max_serve_count;
    std::default_random_engine random_engine(std::random_device{}());
    std::uniform_real_distribution<float> op_type_dist(0.0, 1.0);
    ZipfianGenerator<false> idx_generator(InitialDataCount,
                                          config.zipfian_constant);

    uint64_t period_start_time = get_time_ns();
    uint64_t final_deadline = period_start_time + max_runtime_ns;
    uint64_t op_count = 0;

    while (true) {
        int idx = idx_generator(random_engine);
        ASSERT(idx >= 0 && idx < data.size());
        float op_type_v = op_type_dist(random_engine);
        Request* request = new Request(get_op_type(op_type_v, config),
                                       data[idx].first, data[idx].second);
        // send request until success
        bool sent;
        do {
            sent = server.send_request(qi, request);
        } while (!sent);
        op_count++;
        uint64_t current_time = get_time_ns();
        // break if timeout
        if (current_time >= final_deadline) break;
        if (server.get_served_count() >= max_serve_count) break;
    }
    return op_count;
}

template <std::integral T>
inline T div_ceil(T a, T b) {
    return (a + b - 1) / b;
}

uint64_t Workload::gen_requests_max_speed(Server& server, const Config& config,
                                          size_t qi_start, size_t qi_end) {
    uint64_t max_runtime_ns = config.max_runtime.count();
    std::default_random_engine random_engine(std::random_device{}());
    std::uniform_real_distribution<float> op_type_dist(0.0, 1.0);
    ZipfianGenerator<false> idx_generator(InitialDataCount,
                                          config.zipfian_constant);

    uint64_t final_deadline = get_time_ns() + max_runtime_ns;
    uint64_t op_count = 0;
    size_t qi = qi_start;
    while (get_time_ns() < final_deadline) {
        int idx = idx_generator(random_engine);
        ASSERT(idx >= 0 && idx < data.size());
        float op_type_v = op_type_dist(random_engine);
        Request* request = new Request(get_op_type(op_type_v, config),
                                       data[idx].first, data[idx].second);
        // send request until success
        while (true) {
            assert(qi >= qi_start && qi < qi_end);
            bool sent = server.send_request(qi, request);
            qi = qi + 1 >= qi_end ? qi_start : qi + 1;
            if (sent) break;
            if (get_time_ns() >= final_deadline) [[unlikely]] {
                delete request;
                break;
            }
        }
        op_count++;
        if (server.get_served_count() >= config.max_serve_count) break;
    }
    return op_count;
}

const char* const ColumnNames[] = {"name",        "op-duration", "run-time",
                                   "req-cnt",     "serv-cnt",    "instructions",
                                   "l2-miss",     "l3-miss",     "async-total",
                                   "async-sched", "gc-mark",     "gc-evict"};
constexpr size_t ColumnWidth = 16;
constexpr size_t ColumnCount = sizeof(ColumnNames) / sizeof(*ColumnNames);

void run(const char* name, Server* server, Workload* workload,
         const Workload::Config& config) {
    init_hist();
    async::StreamRunnerProfiler::reset();
    profile::reset_all();
    void (*serve_fn)(std::pair<Server*, size_t>*) =
        [](std::pair<Server*, size_t>* args) {
            args->first->serve(args->second);
        };
    std::vector<std::pair<Server*, size_t>> server_args(config.n_server_thread);
    std::vector<std::unique_ptr<UThread>> server_threads(
        config.n_server_thread);
    for (size_t i = 0; i < config.n_server_thread; i++) {
        server_args[i] = {server, i};
        server_threads[i] = uthread::create(serve_fn, &(server_args[i]));
    }
    std::atomic_uint64_t req_count;
    size_t n_server_per_client =
        div_ceil(config.n_server_thread, config.n_client_thread);
    std::function<void(size_t)> gen_fn = [&](size_t i) {
        size_t qi_start = n_server_per_client * i;
        size_t qi_end =
            std::min(n_server_per_client * (i + 1), config.n_server_thread);
        req_count +=
            workload->gen_requests_max_speed(*server, config, qi_start, qi_end);
    };
    auto perf_result = perf_profile(
        [&] { uthread::fork_join(config.n_client_thread, gen_fn); });
    server->finish_serving();
    for (auto& t : server_threads) {
        uthread::join(std::move(t));
    }
    print_hist();
    if (name) {
        std::cout << std::setw(ColumnWidth) << name;
        std::cout << std::setw(ColumnWidth) << "max";
        std::cout << std::setw(ColumnWidth) << perf_result.runtime_ms / 1e3;
        std::cout << std::setw(ColumnWidth) << req_count;
        std::cout << std::setw(ColumnWidth) << server->get_served_count();
        std::cout << std::setw(ColumnWidth) << perf_result.instructions;
        std::cout << std::setw(ColumnWidth) << perf_result.l2_cache_miss;
        std::cout << std::setw(ColumnWidth) << perf_result.l3_cache_miss;
        std::cout << std::setw(ColumnWidth)
                  << async::StreamRunnerProfiler::get_total_cycles();
        std::cout << std::setw(ColumnWidth)
                  << async::StreamRunnerProfiler::get_sched_cycles();
        std::cout << std::setw(ColumnWidth) << profile::collect_mark_cycles();
        std::cout << std::setw(ColumnWidth) << profile::collect_evict_cycles();
        std::cout << std::endl;
    }
}

void run(size_t n_server_core) {
    constexpr size_t NEvalRepeat = 1;
    Workload workload;
    Workload::Config config{
        .n_server_thread = n_server_core,
        .n_client_thread = n_server_core,
        .put_ratio = 0.5,
        .remove_ratio = 0.0,
        .zipfian_constant = 0.99,
        .max_runtime = 100s,
        .max_serve_count = 50'000'000,
    };

    std::cout << "setup: " << config.n_server_thread << " server cores; "
              << config.n_client_thread << " clients" << std::endl;

    for (auto name : ColumnNames) {
        std::cout << std::setw(ColumnWidth) << name;
    }
    std::cout << std::endl;

    // warm up
    SyncServer server(n_server_core, workload);
    run("warm-up", &server, &workload, config);
    for (size_t i = 0; i < NEvalRepeat; i++) {
        PararoutineServer server(n_server_core, workload);
        run("async", &server, &workload, config);
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cout << "usage: " << argv[0]
                  << " <configure file> <local memory (GB)>" << std::endl;
        return -1;
    }

    perf_init();
    Configure config;
    config.from_file(argv[1]);
    double local_mem = std::atof(argv[2]);
    ASSERT(config.max_thread_cnt >= 2);
    ASSERT(config.max_thread_cnt % 2 == 0);  // client : server = 1 : 1
    ASSERT(local_mem > 0);
    size_t n_server_core = config.max_thread_cnt / 2;
    config.client_buffer_size = local_mem * (1LL << 30);
    std::cout << "config: client buffer size = " << config.client_buffer_size
              << std::endl;
    runtime_init(config);
    run(n_server_core);
    runtime_destroy();
    return 0;
}
