#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>

#include "rdma/client.hpp"
#include "utils/control.hpp"
#include "utils/fork_join.hpp"

using namespace FarLib;
using namespace FarLib::rdma;

Configure config;

constexpr size_t RequestSize = 4096;
constexpr uint32_t CPUFreqMHz = 2800;

static inline void my_delay_cycles(int32_t cycles) {
    uint64_t start = __rdtsc();
    while (__rdtsc() < start + cycles);
}

void do_work(size_t nthreads, size_t delay_ns) {
    std::function<void(size_t)> fn_bench = [&](size_t tid) {
        auto client = Client::get_default();
        void *local_addr =
            static_cast<std::byte *>(client->get_buffer()) + tid * RequestSize;
        ibv_wc wc[16];
        for (uint64_t raddr = RequestSize * tid;
             raddr + RequestSize < config.server_buffer_size;
             raddr += RequestSize * nthreads) {
            if (raddr / (RequestSize * nthreads) % 64 == 0) {
                if (!client->post_read<true>(raddr, local_addr, RequestSize,
                                             raddr, 0)) {
                    ERROR("post read error");
                }
                while (!client->check_cq(wc, 16));
            } else {
                if (!client->post_read(raddr, local_addr, RequestSize, raddr,
                                       0)) {
                    ERROR("post read error");
                }
            }
            my_delay_cycles(delay_ns / 1000.0 * CPUFreqMHz);
        }
    };
    for (size_t i = 0; i < 2; i++) {
        auto start_ts = std::chrono::steady_clock::now();
        uthread::fork_join(nthreads, fn_bench);
        auto end_ts = std::chrono::steady_clock::now();
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                           end_ts - start_ts)
                           .count();
        printf("nthreads: %lu\tdelay: %lu\ttime: %lu us\tMpps: %lf\n", nthreads,
               delay_ns, time_us,
               (double)config.server_buffer_size / RequestSize / time_us);
    }
}

int main(int argc, const char *argv[]) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <cfg_file>" << std::endl;
        return -EINVAL;
    }
    config.from_file(argv[1]);
    runtime_init(config);

    const size_t DelayNS[] = {0, 400};
    for (size_t delay_ns : DelayNS) {
        do_work(1, delay_ns);
        do_work(8, delay_ns);
    }

    runtime_destroy();

    return 0;
}
