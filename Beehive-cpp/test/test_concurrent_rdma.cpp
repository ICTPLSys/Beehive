#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <thread>

#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/uthreads.hpp"

using namespace Beehive;
using namespace Beehive::rdma;
using namespace Beehive::uthread;
using namespace std::chrono_literals;

constexpr size_t BUFFER_SIZE = 16 * 1024 * 1024;
constexpr size_t PER_THREAD_WRITE_COUNT = 16 * 1024;
constexpr size_t PER_THREAD_READ_COUNT = 16 * 1024;
constexpr size_t THREAD_COUNT = 4;
constexpr size_t WRITE_COUNT = PER_THREAD_READ_COUNT * THREAD_COUNT;
constexpr size_t READ_COUNT = PER_THREAD_WRITE_COUNT * THREAD_COUNT;
static_assert(READ_COUNT == WRITE_COUNT);

std::vector<uint64_t> workloads;

void init_workloads() {
    workloads.resize(WRITE_COUNT);
    constexpr size_t step = BUFFER_SIZE / READ_COUNT;
    static_assert(step >= sizeof(uint64_t));
    for (size_t i = 0; i < READ_COUNT; i++) {
        workloads[i] = i * step;
    }
}

void shuffle_workloads() {
    std::random_shuffle(workloads.begin(), workloads.end());
}

void post_writes(size_t idx_begin, size_t idx_end) {
    Client *client = Client::get_default();
    void *buffer = client->get_buffer();
    for (size_t idx = idx_begin; idx < idx_end; idx++) {
        uint64_t offset = workloads[idx];
        void *p = (char *)buffer + offset;
        *(uint64_t *)p = (uint64_t)p;
        while (
            !client->post_write(offset, p, sizeof(uint64_t), (uint64_t)p, idx));
    }
}

void post_reads(size_t idx_begin, size_t idx_end) {
    Client *client = Client::get_default();
    void *buffer = client->get_buffer();
    for (size_t idx = idx_begin; idx < idx_end; idx++) {
        uint64_t offset = workloads[idx];
        void *p = (char *)buffer + offset;
        *(uint64_t *)p = 0;
        while (
            !client->post_read(offset, p, sizeof(uint64_t), (uint64_t)p, idx));
    }
}

void poll_cq_write() {
    Client *client = Client::get_default();
    void *buffer = client->get_buffer();
    ibv_wc wc[16];
    size_t write_complete_cnt = 0;
    while (write_complete_cnt < WRITE_COUNT) {
        size_t cnt = client->check_cq(wc, 16);
        ASSERT(cnt <= 16);
        for (size_t i = 0; i < cnt; i++) {
            ASSERT(wc[i].status == IBV_WC_SUCCESS);
            ASSERT(wc[i].opcode == IBV_WC_RDMA_WRITE);
            ASSERT(wc[i].wr_id >= reinterpret_cast<uint64_t>(buffer));
            ASSERT(wc[i].wr_id <
                   reinterpret_cast<uint64_t>(buffer) + BUFFER_SIZE);
            write_complete_cnt++;
        }
    }
}

void poll_cq_read() {
    Client *client = Client::get_default();
    void *buffer = client->get_buffer();
    ibv_wc wc[16];
    size_t read_complete_cnt = 0;
    while (read_complete_cnt < READ_COUNT) {
        size_t cnt = client->check_cq(wc, 16);
        ASSERT(cnt <= 16);
        for (size_t i = 0; i < cnt; i++) {
            ASSERT(wc[i].status == IBV_WC_SUCCESS);
            ASSERT(wc[i].opcode == IBV_WC_RDMA_READ);
            read_complete_cnt++;
            ASSERT(wc[i].wr_id >= reinterpret_cast<uint64_t>(buffer));
            ASSERT(wc[i].wr_id <
                   reinterpret_cast<uint64_t>(buffer) + BUFFER_SIZE);
            ASSERT(*reinterpret_cast<uint64_t *>(wc[i].wr_id) == wc[i].wr_id);
        }
    }
}

void test() {
    std::srand(0);
    init_workloads();
    shuffle_workloads();
    std::vector<std::unique_ptr<UThread>> uthreads;
    std::unique_ptr<UThread> cq_thread(uthread::create(poll_cq_write));
    {
        struct Args {
            decltype(post_writes) &f;
            size_t i;
            Args() : f(post_writes) {}
        };

        void (*run_func)(Args *) = [](Args *args) {
            args->f(args->i * PER_THREAD_WRITE_COUNT,
                    (args->i + 1) * PER_THREAD_WRITE_COUNT);
        };
        std::unique_ptr<Args[]> args(new Args[THREAD_COUNT]());
        for (size_t i = 0; i < THREAD_COUNT; i++) {
            args[i].i = i;
        }
        for (size_t i = 0; i < THREAD_COUNT; i++) {
            uthreads.push_back(uthread::create(run_func, &args[i]));
        }
        for (size_t i = 0; i < THREAD_COUNT; i++) {
            uthreads[i]->join();
        }
        cq_thread->join();
        printf("post writes finished\n");
    }
    for (size_t n = 0; n < 4; n++) {
        shuffle_workloads();
        cq_thread.reset();
        cq_thread = uthread::create(poll_cq_read);
        struct Args {
            decltype(post_reads) &f;
            size_t i;
            Args() : f(post_reads) {}
        };
        void (*run_func)(Args *) = [](Args *args) {
            args->f(args->i * PER_THREAD_READ_COUNT,
                    (args->i + 1) * PER_THREAD_READ_COUNT);
        };
        std::unique_ptr<Args[]> args(new Args[THREAD_COUNT]());
        for (size_t i = 0; i < THREAD_COUNT; i++) {
            args[i].i = i;
        }
        for (size_t i = 0; i < THREAD_COUNT; i++) {
            uthreads[i].reset();
            uthreads[i] = uthread::create(run_func, &args[i]);
        }
        for (size_t i = 0; i < THREAD_COUNT; i++) {
            uthreads[i]->join();
        }
        cq_thread->join();
        printf("post reads (%ld / 4) finished\n", n + 1);
    }
}

int main() {
    Configure config;
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.client_buffer_size = BUFFER_SIZE;
    config.server_buffer_size = BUFFER_SIZE;
    config.qp_count = 3;
    config.max_thread_cnt = THREAD_COUNT + 1;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);  // wait for server start, FIXME
    Beehive::runtime_init(config);
    test();
    Beehive::runtime_destroy();
    server_thread.join();
    return 0;
}
