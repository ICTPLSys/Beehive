#include <iostream>

#include "utils/signal.hpp"  // IWYU pragma: keep

#ifdef SIGNAL_ENABLED

#include <hdr/hdr_histogram.h>

#include <array>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/timer/progress_display.hpp>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <unordered_map>
#include <vector>

#include "async/stream_runner.hpp"
#include "cache/cache.hpp"
#include "data_structure/hopscotch.hpp"
#include "test/fixed_size_string.hpp"
#include "utils/control.hpp"
#include "utils/debug.hpp"
#include "utils/threads.hpp"
#include "utils/zipfian.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace std::chrono_literals;

#define DISPLAY_PROGRESS

using str_key_t = FixedSizeString<16>;
using str_value_t = FixedSizeString<64>;

struct HopscotchObject {
    str_key_t key;
    str_value_t value;
    bool equals(const str_key_t& cmp) const { return cmp == key; }
};

uint64_t get_time_ns() {
    std::chrono::nanoseconds ns =
        std::chrono::high_resolution_clock::now().time_since_epoch();
    return ns.count();
}

using LocalHashTable = std::unordered_map<str_key_t, str_value_t>;
using RemoteHashTable = Hopscotch<str_key_t, HopscotchObject>;

class Server;

// Workload A: hashtable get
class WorkloadA {
public:
    struct Request {
        str_value_t* expected_value;
        str_key_t key;
        uint64_t request_start_ns;
    };

    struct Response {
        str_value_t* expected_value;
        str_value_t value;
        uint64_t in_queue_lat_ns;
        uint64_t service_latency_ns;  // reset the speed controller every * ns
    };

    struct Config {
        double zipfian_constant;
        size_t max_request_count;
        std::chrono::nanoseconds max_runtime;
        std::chrono::nanoseconds op_duration;
        std::chrono::nanoseconds op_speed_reset;
    };

    static constexpr size_t RequestCount = 1024 * 1024;
    static constexpr size_t DataSizeShift = 20;
    static constexpr size_t DataSize = 1 << DataSizeShift;

public:
    WorkloadA() { init_data(); }

    LocalHashTable* get_local_hash_table() { return &local_hash_table; }

    RemoteHashTable* get_remote_hash_table() { return remote_hash_table.get(); }

    uint64_t gen_requests(Server& server, const Config& config);

private:
    void init_data() {
        remote_hash_table =
            std::make_unique<RemoteHashTable>(DataSizeShift + 2);
        data.reserve(DataSize);
#ifdef DISPLAY_PROGRESS
        std::cout << "Loading Data..." << std::endl;
        boost::timer::progress_display progress(DataSize);
#endif
        for (size_t i = 0; i < DataSize; i++) {
            str_key_t key = str_key_t::random();
            str_value_t value = str_value_t::random();
            ON_MISS_BEGIN
            ON_MISS_END
            auto [accessor, inserted] = remote_hash_table->put(
                key, sizeof(HopscotchObject), __on_miss__);
            data.push_back({.key = key, .value = value});
            local_hash_table[key] = value;
            accessor->key = key;
            accessor->value = value;
#ifdef DISPLAY_PROGRESS
            ++progress;
#endif
        }
    }

private:
    std::vector<HopscotchObject> data;
    LocalHashTable local_hash_table;
    std::unique_ptr<RemoteHashTable> remote_hash_table;
};

// Workload B: dummy
class WorkloadB {
private:
    std::atomic_size_t value;

public:
    WorkloadB() : value(0) {}

    void run() {
        size_t n = rand() % 1024;
        value.store(n);
        if (n == 0) return;
        while (value.fetch_sub(1) != 1);
    }
};

class Server {
public:
    template <typename Request>
    using RequestQueue =
        boost::lockfree::spsc_queue<Request, boost::lockfree::capacity<64>>;

protected:
    std::atomic_bool serving;
    RequestQueue<WorkloadA::Request> request_queue;
    hdr_histogram* hist_in_queue = nullptr;
    hdr_histogram* hist_serve = nullptr;
    size_t work_b_cnt;
    WorkloadB& workload_b;

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
        size_t work_b_cnt;
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
            .work_b_cnt = work_b_cnt,
        };
    }

public:
    Server(WorkloadB& workload_b) : serving(true), workload_b(workload_b) {
        int err;
        err = hdr_init(1, 1'000'000, 3, &hist_in_queue);
        ASSERT(err == 0);
        err = hdr_init(1, 1'000'000, 3, &hist_serve);
        ASSERT(err == 0);
        work_b_cnt = 0;
    }

    ~Server() {
        hdr_close(hist_in_queue);
        hdr_close(hist_serve);
    }

    virtual void serve() = 0;

    bool send_request_a(WorkloadA::Request& req) {
        req.request_start_ns = get_time_ns();
        bool sent = request_queue.push(req);
        door_bell();
        return sent;
    }

    bool send_request_b() {
        workload_b.run();
        work_b_cnt++;
        return true;
    }

    virtual void door_bell() {}

    virtual void poll() {}

    virtual void finish_serving() { serving.store(false); }

protected:
    bool get_request_a(WorkloadA::Request& request,
                       WorkloadA::Response& response) {
        if (request_queue.pop(request)) {
            response.in_queue_lat_ns = get_time_ns() - request.request_start_ns;
            response.expected_value = request.expected_value;
            return true;
        }
        return false;
    }

    template <bool Check = true>
    void send_response_a(const WorkloadA::Request& request,
                         WorkloadA::Response& response) {
        response.service_latency_ns = get_time_ns() - request.request_start_ns;
        if constexpr (Check) {
            ASSERT(response.value == *response.expected_value);
        }
        hdr_record_value(hist_in_queue, response.in_queue_lat_ns);
        hdr_record_value(hist_serve, response.service_latency_ns);
    }

    bool is_serving() { return serving.load(std::memory_order::relaxed); }
};

class DryServer : public Server {
public:
    DryServer(WorkloadA& workload_a, WorkloadB& workload_b)
        : Server(workload_b) {}

    void serve() override {
        while (is_serving()) {
            WorkloadA::Request request;
            WorkloadA::Response response;
            if (get_request_a(request, response)) {
                send_response_a<false>(request, response);
            }
        }
    }
};

template <bool WorkB>
class LocalServer : public Server {
public:
    LocalServer(WorkloadA& workload_a, WorkloadB& workload_b)
        : Server(workload_b) {
        hash_table = workload_a.get_local_hash_table();
    }

    void serve() override {
        while (is_serving()) {
            WorkloadA::Request request;
            WorkloadA::Response response;
            if (get_request_a(request, response)) {
                auto it = hash_table->find(request.key);
                ASSERT(it != hash_table->end());
                response.value = it->second;
                send_response_a(request, response);
            } else if constexpr (WorkB) {
                send_request_b();
            }
        }
    }

protected:
    LocalHashTable* hash_table;
};

class RemotableServer : public Server {
public:
    RemotableServer(WorkloadA& workload_a, WorkloadB& workload_b)
        : Server(workload_b) {
        hash_table = workload_a.get_remote_hash_table();
    }

protected:
    RemoteHashTable* hash_table;
};

template <bool WorkB>
class SyncServer : public RemotableServer {
public:
    SyncServer(WorkloadA& workload_a, WorkloadB& workload_b)
        : RemotableServer(workload_a, workload_b) {}

    void serve() override {
        RootDereferenceScope scope;
        ON_MISS_BEGIN
        ON_MISS_END
        while (is_serving()) {
            WorkloadA::Request request;
            WorkloadA::Response response;
            if (get_request_a(request, response)) {
                auto accessor =
                    hash_table->get(request.key, __on_miss__, scope);
                ASSERT(!accessor.is_null());
                response.value = accessor->value;
                send_response_a(request, response);
            } else if constexpr (WorkB) {
                send_request_b();
            }
        }
    }
};

template <bool WorkB>
class UThreadServer : public RemotableServer {
public:
    UThreadServer(WorkloadA& workload_a, WorkloadB& workload_b)
        : RemotableServer(workload_a, workload_b) {}

    void door_bell() override {
        uthread::notify(&request_cond, &request_cond_mutex);
        if constexpr (WorkB) {
            // uthread preempt, need to impl. in uthread lib
            if (workb_working.load(std::memory_order::relaxed) &&
                !signaled.load(std::memory_order::relaxed)) {
                signaled.store(true, std::memory_order::relaxed);
                signal::signal(pthread_id);
            }
        }
    }

    void poll() override {
        if constexpr (WorkB) {
            if (workb_working.load(std::memory_order::relaxed)) {
                if (cache::check_cq() != 0) door_bell();
            }
        }
    }

    void finish_serving() override {
        serving.store(false);
        uthread::notify_all(&request_cond, &request_cond_mutex);
    }

    void serve() {
        pthread_id = pthread_self();
        std::unique_ptr<uthread::UThread> threads[16];
        std::unique_ptr<uthread::UThread> b_thread;
        for (auto& t : threads) {
            t = uthread::create(serve_a_thread_fn, this);
        }
        if constexpr (WorkB) {
            // low priority
            b_thread = uthread::create<true>(serve_b_thread_fn, this);
        }
        for (auto& t : threads) {
            uthread::join(std::move(t));
        }
        if constexpr (WorkB) {
            uthread::join(std::move(b_thread));
        }
    }

private:
    static void serve_a_thread_fn(UThreadServer* server) {
        server->serve_a_thread();
    }

    static void serve_b_thread_fn(UThreadServer* server) {
        server->serve_b_thread();
    }

    void serve_a_thread() {
        ON_MISS_BEGIN
            uthread::yield();
        ON_MISS_END
        while (is_serving()) {
            WorkloadA::Request request;
            WorkloadA::Response response;
            {
                RootDereferenceScope scope;
                while (get_request_a(request, response)) {
                    auto accessor =
                        hash_table->get(request.key, __on_miss__, scope);
                    ASSERT(!accessor.is_null());
                    response.value = accessor->value;
                    send_response_a(request, response);
                }
            }
            if (serving.load())
                uthread::wait(&request_cond, &request_cond_mutex);
        }
    }

    void serve_b_thread() {
        signal::on_sigusr1 = [this] { thread_b_on_signal(); };
        workb_working.store(true, std::memory_order::relaxed);
        signaled.store(false, std::memory_order::relaxed);
        while (is_serving()) {
            send_request_b();
            if (signaled.load(std::memory_order::relaxed)) {
                thread_b_on_signal();
            }
        }
        workb_working.store(false, std::memory_order::relaxed);
        signal::on_sigusr1 = [] {};
    }

    void thread_b_on_signal() {
        signal::disable_signal();
        workb_working.store(false, std::memory_order::relaxed);
        signal::on_sigusr1 = [] {};
        signal::enable_signal();
        uthread::yield();
        signal::disable_signal();
        signal::on_sigusr1 = [this] { thread_b_on_signal(); };
        workb_working.store(true, std::memory_order::relaxed);
        signaled.store(false, std::memory_order::relaxed);
        signal::enable_signal();
    }

private:
    uthread::Condition request_cond;
    uthread::Mutex request_cond_mutex;
    std::atomic_bool workb_working = false;
    std::atomic_bool signaled = false;
    pthread_t pthread_id;
};

template <bool WorkB, bool Preempt = false>
class PararoutineServer : public RemotableServer {
    friend class WorkloadABaseContext;
    friend class WorkloadAReqStream;
    struct WorkloadABaseContext {
        PararoutineServer* server;
        WorkloadA::Request request;
        WorkloadA::Response response;

        RemoteHashTable* get_this() { return server->hash_table; }
        const str_key_t& get_key() const { return request.key; }
        void make_result(LiteAccessor<HopscotchObject>&& result) {
            ASSERT(!result.is_null());
            response.value = result->value;
            server->send_response_a(request, response);
        }
    };

    struct WorkloadAReqStream {
        using Context = RemoteHashTable::GetContext<WorkloadABaseContext>;
        PararoutineServer* server;

        WorkloadAReqStream(PararoutineServer* server) : server(server) {}

        async::StreamState get(Context* ctx) {
            new (ctx) Context;
            if (!server->is_serving()) return async::StreamState::FINISHED;
            if (!server->get_request_a(ctx->request, ctx->response)) {
                ctx->~Context();
                return async::StreamState::WAITING;
            }
            ctx->server = server;
            return async::StreamState::READY;
        }
    };

public:
    PararoutineServer(WorkloadA& workload_a, WorkloadB& workload_b)
        : RemotableServer(workload_a, workload_b) {}

    void serve() override {
        if constexpr (WorkB) {
            if constexpr (Preempt) {
                serve_ab_preempt();
            } else {
                serve_ab_no_preempt();
            }
        } else {
            serve_a();
        }
    }

    void door_bell() override {
        if constexpr (WorkB && Preempt) {
            if (workb_working.load(std::memory_order::relaxed) &&
                !signaled.load(std::memory_order::relaxed)) {
                signaled.store(true, std::memory_order::relaxed);
                signal::signal(pthread_id);
            }
        }
    }

    void poll() override {
        if constexpr (WorkB) {
            if (workb_working.load(std::memory_order::relaxed)) {
                if (cache::check_cq() != 0) door_bell();
            }
            if constexpr (Preempt) {
                if (check_timer(get_time_ns())) {
                    door_bell();
                }
            }
        }
    }

private:
    void serve_a() { process_pararoutine_stream(WorkloadAReqStream(this)); }

    void serve_ab_no_preempt() {
        RootDereferenceScope scope;
        async::StreamRunner<WorkloadAReqStream, false, true> runner(
            WorkloadAReqStream(this), scope);
        bool finished = runner.resume();
        if (!finished) {
            while (is_serving()) {
                send_request_b();
                if (!runner.task_list_empty() || !request_queue.empty()) {
                    runner.resume();
                }
            }
        }
    }

    void serve_ab_preempt() {
        RootDereferenceScope scope;
        async::StreamRunner<WorkloadAReqStream, false, true> runner(
            WorkloadAReqStream(this), scope);
        bool finished = runner.resume();
        if (!finished) {
            if (uint64_t ddl = runner.min_access_ddl()) {
                set_timer(ddl - 1000);
            }
            signal::on_sigusr1 = [this, &runner] {
                if (workb_working.load(std::memory_order::relaxed)) {
                    workb_working.store(false, std::memory_order::relaxed);
                    set_timer(0);  // timer off
                resume:
                    runner.resume();
                    if (uint64_t ddl = runner.min_access_ddl()) {
                        if (ddl - 2000 < get_time_ns()) goto resume;
                        set_timer(ddl - 2000);
                    }
                    workb_working.store(true, std::memory_order::relaxed);
                    signaled.store(false, std::memory_order::relaxed);
                }
            };
            pthread_id = pthread_self();
            workb_working.store(true, std::memory_order::relaxed);
            while (is_serving()) {
                send_request_b();
                if (signaled.load(std::memory_order::relaxed)) {
                    signal::on_sigusr1();
                }
            }
            workb_working.store(false, std::memory_order::relaxed);
            signal::on_sigusr1 = [] {};
        }
    }

    bool check_timer(uint64_t current) {
        uint64_t timer_value = timer.load(std::memory_order::relaxed);
        if (timer_value != 0 && current >= timer_value) {
            timer.store(0, std::memory_order::relaxed);
            return true;
        }
        return false;
    }

    void set_timer(uint64_t ddl) {
        timer.store(ddl, std::memory_order::relaxed);
    }

private:
    std::atomic_bool workb_working = false;
    std::atomic_bool signaled = false;
    pthread_t pthread_id;
    std::atomic_uint64_t timer = 0;
};

uint64_t WorkloadA::gen_requests(Server& server, const Config& config) {
    uint64_t op_duration_ns = config.op_duration.count();
    uint64_t op_speed_reset_ns = config.op_speed_reset.count();
    uint64_t max_runtime_ns = config.max_runtime.count();
    assert(op_duration_ns < op_speed_reset_ns);
    assert(op_duration_ns < max_runtime_ns);
    std::default_random_engine random_engine(std::random_device{}());
    std::exponential_distribution req_interval_dist(1.0 / op_duration_ns);
    ZipfianGenerator<false> idx_generator(DataSize, config.zipfian_constant);
    uint64_t period_start_time = get_time_ns();
    uint64_t final_deadline = period_start_time + max_runtime_ns;
    uint64_t period_deadline = period_start_time + op_speed_reset_ns;
    uint64_t next_op_start_time = period_start_time;
    uint64_t op_count = 0;
    while (op_count < config.max_request_count) {
        int idx = idx_generator(random_engine);
        Request request{
            .expected_value = &(data[idx].value),
            .key = data[idx].key,
        };
        // send request until success
        bool sent;
        do {
            sent = server.send_request_a(request);
        } while (!sent);
        op_count++;
        next_op_start_time += req_interval_dist(random_engine);
        // wait until time to send next request
        uint64_t current_time;
        while ((current_time = get_time_ns()) < next_op_start_time) {
            server.poll();
        }
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
    "run time", "req cnt", "serv cnt", "q-mean",    "q-p50",
    "q-p90",    "q-p95",   "q-p99",    "s-mean",    "s-p50",
    "s-p90",    "s-p95",   "s-p99",    "work-b-cnt"};
constexpr size_t ColumnWidth = 12;
constexpr size_t ColumnCount = sizeof(ColumnNames) / sizeof(*ColumnNames);

void run(const char* name, Server* server, WorkloadA* workload_a,
         const WorkloadA::Config& config) {
    void (*serve_fn)(Server*) = [](Server* s) { s->serve(); };
    auto server_thread = uthread::create(serve_fn, server);
    uint64_t start_ns = get_time_ns();
    uint64_t req_count = workload_a->gen_requests(*server, config);
    uint64_t end_ns = get_time_ns();
    server->finish_serving();
    uthread::join(std::move(server_thread));
    double run_time = (double)(end_ns - start_ns) / 1e9;
    auto stats = server->get_stats();
    std::cout << std::setw(ColumnWidth) << name;
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
    std::cout << std::setw(ColumnWidth) << stats.work_b_cnt;
    std::cout << std::endl;
}

template <bool P>
using PrAB = PararoutineServer<true, P>;

void run() {
    WorkloadA workload_a;
    WorkloadB workload_b;

    std::cout << std::setw(ColumnWidth) << "";
    for (auto name : ColumnNames) {
        std::cout << std::setw(12) << name;
    }
    std::cout << std::endl;

    WorkloadA::Config config{
        .zipfian_constant = 0,
        .max_request_count = 50'000'000,
        .max_runtime = 10s,
        .op_duration = 2us,
        .op_speed_reset = 10ms,
    };
    // constexpr double ZipfianConstants[] = {0, 0.5, 0.8, 0.99};
    constexpr double ZipfianConstants[] = {0.5};
    constexpr std::chrono::nanoseconds OpDurations[] = {
        10us,   8us,    6us,    5us,    4000ns, 3500ns, 3000ns,
        2500ns, 2000ns, 1500ns, 1000ns, 900ns,  800ns,  700ns};

#define RUN(NAME, SERVER)                        \
    {                                            \
        SERVER server(workload_a, workload_b);   \
        run(NAME, &server, &workload_a, config); \
    }

    for (double z : ZipfianConstants) {
        for (auto d : OpDurations) {
            std::cout << "zipfian constant: " << z << std::endl;
            config.zipfian_constant = z;
            std::cout << "op duration: " << d.count() << " ns" << std::endl;
            config.op_duration = d;

            // RUN("dry", DryServer);
            // RUN("local-a", LocalServer<false>);
            // RUN("local-b", LocalServer<true>);
            // RUN("sync-a", SyncServer<false>);
            // RUN("sync-b", SyncServer<true>);
            RUN("uthread-a", UThreadServer<false>);
            RUN("uthread-ab", UThreadServer<true>);
            RUN("pr-a", PararoutineServer<false>);
            RUN("pr-ab-np", PrAB<false>);
            RUN("pr-ab-p", PrAB<true>);

            std::cout << std::endl;
        }
    }

#undef RUN
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file> " << std::endl;
        return -1;
    }

    Configure config;
    config.from_file(argv[1]);
    config.max_thread_cnt = 2;
    runtime_init(config);
    run();
    runtime_destroy();
    return 0;
}

#else

int main() {
    std::cerr << "Error: signal not enabled" << std::endl;
    return -1;
}

#endif