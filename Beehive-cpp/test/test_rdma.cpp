#include <cassert>
#include <chrono>
#include <thread>

#include "rdma/client.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"

using namespace FarLib;
using namespace FarLib::rdma;
using namespace std::chrono_literals;

int main() {
    Configure config;
    config.server_addr = "127.0.0.1";
    config.server_port = "50000";
    config.qp_count = 3;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
    std::this_thread::sleep_for(1s);  // wait for server start, FIXME
    FarLib::runtime_init(config, false);
    Client *client = Client::get_default();
    int *buffer = static_cast<int *>(client->get_buffer());
    buffer[0] = 0x666;
    ibv_wc wc[rdma::CHECK_CQ_BATCH_SIZE];
    for (size_t i = 0; i < 8; i++) {
        bool posted = client->post_write(0, buffer, sizeof(int), 0x233, 0);
        ASSERT(posted);
        while (true) {
            size_t count = client->check_cq(wc, rdma::CHECK_CQ_BATCH_SIZE);
            if (count != 0) {
                ASSERT(count == 1);
                ASSERT(wc[0].wr_id == 0x233);
                break;
            }
        }
        posted = client->post_read(0, buffer + 1, sizeof(int), 0x888, 0);
        ASSERT(posted);
        while (true) {
            size_t count = client->check_cq(wc, rdma::CHECK_CQ_BATCH_SIZE);
            if (count != 0) {
                ASSERT(count == 1);
                ASSERT(wc[0].wr_id == 0x888);
                break;
            }
        }
        ASSERT(buffer[1] == 0x666);
    }
    FarLib::runtime_destroy();
    server_thread.join();
    return 0;
}