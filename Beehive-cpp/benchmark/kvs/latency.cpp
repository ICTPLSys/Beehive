#include <hdr/hdr_histogram.h>

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/timer/progress_display.hpp>
#include <chrono>
#include <cstdlib>
#include <ctime>
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
        str_key_t key;
        str_value_t value;
        uint64_t request_start_ns;
    };

    struct Response {
        uint64_t in_queue_lat_ns;
        uint64_t service_latency_ns;
    };

    struct Config {
        size_t n_server_core;
        double put_ratio;
        double remove_ratio;
        double zipfian_constant;
        size_t max_request_count;
        std::chrono::nanoseconds max_runtime;
        std::chrono::nanoseconds op_duration;
        std::chrono::nanoseconds op_speed_reset;
    };

    // 10GB
    static constexpr size_t InitialDataCount = 32 * 1024 * 1024;
    static constexpr size_t DataSizeShift = 27;
    static constexpr size_t DataSize = 1 << DataSizeShift;
    static_assert(InitialDataCount < DataSize);

public:
    Workload() : remote_hash_table(DataSizeShift + 2) { init_data(); }

    LocalHashTable* get_local_hash_table() { return &local_hash_table; }

    RemoteHashTable* get_remote_hash_table() { return &remote_hash_table; }

    uint64_t gen_requests(Server& server, const Config& config, size_t qi);

private:
    void init_data() {
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

constexpr size_t NQueue = 16;

class Server {
public:
    template <typename Request>
    using RequestQueue =
        boost::lockfree::queue<Request, boost::lockfree::capacity<64 * 16>>;

protected:
    std::atomic_bool serving;
    RequestQueue<Workload::Request*> request_queue[NQueue];
    hdr_histogram* hist_in_queue = nullptr;
    hdr_histogram* hist_serve = nullptr;

public:
    struct Stats {
        int64_t served_count;
        double q_mean;
        int64_t q_p50;
        int64_t q_p90;
        int64_t q_p95;
        int64_t q_p99;
        double s_mean;
        int64_t s_p50;
        int64_t s_p90;
        int64_t s_p95;
        int64_t s_p99;
    };

    Stats get_stats() {
        return Stats{
            .served_count = hist_in_queue->total_count,
            .q_mean = hdr_mean(hist_in_queue),
            .q_p50 = hdr_value_at_percentile(hist_in_queue, 50),
            .q_p90 = hdr_value_at_percentile(hist_in_queue, 90),
            .q_p95 = hdr_value_at_percentile(hist_in_queue, 95),
            .q_p99 = hdr_value_at_percentile(hist_in_queue, 99),
            .s_mean = hdr_mean(hist_serve),
            .s_p50 = hdr_value_at_percentile(hist_serve, 50),
            .s_p90 = hdr_value_at_percentile(hist_serve, 90),
            .s_p95 = hdr_value_at_percentile(hist_serve, 95),
            .s_p99 = hdr_value_at_percentile(hist_serve, 99),
        };
    }

    void print_cdf() {
        hdr_percentiles_print(hist_serve, stdout, 100, 1, format_type::CLASSIC);
    }

public:
    Server() : serving(true) {
        int err;
        err = hdr_init(1, 100'000'000, 3, &hist_in_queue);
        ASSERT(err == 0);
        err = hdr_init(1, 100'000'000, 3, &hist_serve);
        ASSERT(err == 0);
    }

    ~Server() {
        hdr_close(hist_in_queue);
        hdr_close(hist_serve);
    }

    virtual void serve(size_t qi) = 0;

    bool send_request(size_t qi, Workload::Request* req) {
        req->request_start_ns = get_time_ns();
        bool sent = request_queue[qi].push(req);
        door_bell(qi);
        return sent;
    }

    virtual void door_bell(size_t qi) {}

    virtual void finish_serving() { serving.store(false); }

protected:
    bool get_request(size_t qi, Workload::Request*& request,
                     Workload::Response& response) {
        while (request_queue[qi].pop(request)) {
            response.in_queue_lat_ns =
                get_time_ns() - request->request_start_ns;
            return true;
        }
        return false;
    }

    void send_response(Workload::Request* request,
                       Workload::Response& response) {
        response.service_latency_ns = get_time_ns() - request->request_start_ns;
        hdr_record_value_atomic(hist_in_queue, response.in_queue_lat_ns);
        hdr_record_value_atomic(hist_serve, response.service_latency_ns);
        delete request;
    }

    bool is_serving() { return serving.load(std::memory_order::relaxed); }
};

class DryServer : public Server {
public:
    DryServer(Workload& workload) {}

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

class RemotableServer : public Server {
public:
    RemotableServer(Workload& workload) {
        hash_table = workload.get_remote_hash_table();
    }

protected:
    RemoteHashTable* hash_table;
};

class SyncServer : public RemotableServer {
public:
    SyncServer(Workload& workload) : RemotableServer(workload) {}

    void serve(size_t qi) override {
        RootDereferenceScope scope;
        while (is_serving()) {
            Workload::Request* request;
            Workload::Response response;
            if (get_request(qi, request, response)) {
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
                send_response(request, response);
            }
        }
    }
};

class UThreadServer : public RemotableServer {
public:
    UThreadServer(Workload& workload) : RemotableServer(workload) {}

    void door_bell(size_t qi) override {
        uthread::notify(&request_cond[qi], &request_cond_mutex);
    }

    void finish_serving() override {
        serving.store(false);
        for (auto& c : request_cond) {
            uthread::notify_all(&c, &request_cond_mutex);
        }
    }

    void serve(size_t qi) override {
        std::function<void(size_t)> fn = [this, qi](size_t) {
            serve_thread(qi);
        };
        uthread::fork_join(16, fn);
    }

private:
    void serve_thread(size_t qi) {
        if (serving.load()) {
            uthread::wait(&request_cond[qi], &request_cond_mutex);
        }
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
    uthread::Condition request_cond[NQueue];
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
        str_value_t get_value() const { return request->value; }
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
    PararoutineServer(Workload& workload) : RemotableServer(workload) {}

    void serve(size_t qi) override {
        process_pararoutine_stream<ReqStream, true>(ReqStream(this, qi));
    }
};

uint64_t Workload::gen_requests(Server& server, const Config& config,
                                size_t qi) {
    uint64_t op_duration_ns = config.op_duration.count();
    uint64_t op_speed_reset_ns = config.op_speed_reset.count();
    uint64_t max_runtime_ns = config.max_runtime.count();
    assert(op_duration_ns < op_speed_reset_ns);
    assert(op_duration_ns < max_runtime_ns);
    std::default_random_engine random_engine(std::random_device{}());
    std::exponential_distribution req_interval_dist(1.0 / op_duration_ns);
    std::uniform_real_distribution<float> op_type_dist(0.0, 1.0);
    ZipfianGenerator<false> idx_generator(InitialDataCount,
                                          config.zipfian_constant);
    uint64_t period_start_time = get_time_ns();
    uint64_t final_deadline = period_start_time + max_runtime_ns;
    uint64_t period_deadline = period_start_time + op_speed_reset_ns;
    uint64_t next_op_start_time = period_start_time;
    uint64_t op_count = 0;
    while (op_count < config.max_request_count) {
        int idx = idx_generator(random_engine);
        ASSERT(idx >= 0 && idx < data.size());
        float op_type_v = op_type_dist(random_engine);
        Workload::OpType op_type;
        if (op_type_v < config.put_ratio) {
            op_type = Workload::PUT;
        } else if (op_type_v < config.put_ratio + config.remove_ratio) {
            op_type = Workload::REMOVE;
        } else {
            op_type = Workload::GET;
        }
        Request* request = new Request;
        request->op_type = op_type;
        request->value = data[idx].second;
        request->key = data[idx].first;
        // send request until success
        bool sent;
        do {
            sent = server.send_request(qi, request);
        } while (!sent);
        op_count++;
        next_op_start_time += req_interval_dist(random_engine);
        // wait until time to send next request
        uint64_t current_time;
        while ((current_time = get_time_ns()) < next_op_start_time);
        // break if timeout
        if (current_time >= final_deadline) break;
        // reset deadlines if last period finished
        if (current_time >= period_deadline) [[unlikely]] {
            period_start_time = current_time;
            period_deadline = current_time + op_speed_reset_ns;
            next_op_start_time = period_start_time;
        }
    }
    return op_count;
}

const char* const ColumnNames[] = {
    "goal-lat", "op-duration", "run-time", "req-cnt", "serv-cnt",
    "q-mean",   "q-p50",       "q-p90",    "q-p95",   "q-p99",
    "s-mean",   "s-p50",       "s-p90",    "s-p95",   "s-p99"};
constexpr size_t ColumnWidth = 12;
constexpr size_t ColumnCount = sizeof(ColumnNames) / sizeof(*ColumnNames);

void run(const char* name, Server* server, Workload* workload,
         const Workload::Config& config) {
    void (*serve_fn)(std::pair<Server*, size_t>*) =
        [](std::pair<Server*, size_t>* args) {
            args->first->serve(args->second);
        };
    std::vector<std::pair<Server*, size_t>> server_args(config.n_server_core);
    std::vector<std::unique_ptr<UThread>> server_threads(config.n_server_core);
    for (size_t i = 0; i < config.n_server_core; i++) {
        server_args[i] = {server, i};
        server_threads[i] = uthread::create(serve_fn, &(server_args[i]));
    }
    std::atomic_uint64_t req_count;
    std::function<void(size_t)> gen_fn = [&](size_t i) {
        req_count += workload->gen_requests(*server, config, i);
    };
    uint64_t start_ns = get_time_ns();
    uthread::fork_join(config.n_server_core, gen_fn);
    uint64_t end_ns = get_time_ns();
    server->finish_serving();
    for (auto& t : server_threads) {
        uthread::join(std::move(t));
    }
    double run_time = (double)(end_ns - start_ns) / 1e9;
    auto stats = server->get_stats();
    std::cout << std::setw(ColumnWidth) << config.op_duration.count();
    std::cout << std::setw(ColumnWidth) << run_time;
    std::cout << std::setw(ColumnWidth) << req_count;
    std::cout << std::setw(ColumnWidth) << stats.served_count;
    std::cout << std::setw(ColumnWidth) << stats.q_mean;
    std::cout << std::setw(ColumnWidth) << stats.q_p50;
    std::cout << std::setw(ColumnWidth) << stats.q_p90;
    std::cout << std::setw(ColumnWidth) << stats.q_p95;
    std::cout << std::setw(ColumnWidth) << stats.q_p99;
    std::cout << std::setw(ColumnWidth) << stats.s_mean;
    std::cout << std::setw(ColumnWidth) << stats.s_p50;
    std::cout << std::setw(ColumnWidth) << stats.s_p90;
    std::cout << std::setw(ColumnWidth) << stats.s_p95;
    std::cout << std::setw(ColumnWidth) << stats.s_p99;
    std::cout << std::endl;
    server->print_cdf();
}

void run(size_t n_server_core) {
    Workload workload;

    for (auto name : ColumnNames) {
        std::cout << std::setw(12) << name;
    }
    std::cout << std::endl;

    Workload::Config config{
        .n_server_core = n_server_core,
        .put_ratio = 0.5,
        .remove_ratio = 0.0,
        .zipfian_constant = 0.99,
        .max_request_count = 20'000'000,
        .max_runtime = 10s,
        .op_duration = 10ns,
        .op_speed_reset = 100ms,
    };
    const std::chrono::duration<double, std::nano> OpDurations[] = {
        3.2us
        // 16.0us, 8.0us, 5.3us, 4.0us, 3.2us, 2.7us, 2.3us, 2.0us
        // 2.0us,
        // 1.8us, 1.7us, 1.5us, 1.3us
    };

#define RUN(NAME, SERVER)                      \
    {                                          \
        SERVER server(workload);               \
        run(NAME, &server, &workload, config); \
    }

    // warm up
    RUN("proutine", PararoutineServer);

    // {
    //     PararoutineServer server(workload);
    //     profile::reset_all();
    //     profile::start_work();
    //     run("proutine", &server, &workload, config);
    //     profile::end_work();
    //     profile::print_profile_data();
    // }
    // return;

    for (auto op_duration : OpDurations) {
        config.op_duration =
            std::chrono::duration_cast<std::chrono::nanoseconds>(op_duration);

        profile::reset_all();
        perf_profile([&] {
            profile::start_work();
            profile::thread_start_work();
            RUN("proutine", PararoutineServer);
            profile::thread_end_work();
            profile::end_work();
        }).print();
        profile::print_profile_data();

        // std::cout << std::endl;
    }

#undef RUN
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file> " << std::endl;
        return -1;
    }

    perf_init();
    Configure config;
    config.from_file(argv[1]);
    ASSERT(config.max_thread_cnt >= 2);
    ASSERT(config.max_thread_cnt / 2 <= NQueue);
    ASSERT(config.max_thread_cnt % 2 == 0);
    std::cout << "config: client buffer size = " << config.client_buffer_size
              << std::endl;
    runtime_init(config);
    std::cout << "running server on " << config.max_thread_cnt / 2 << " cores"
              << std::endl;
    run(config.max_thread_cnt / 2);
    runtime_destroy();
    return 0;
}
