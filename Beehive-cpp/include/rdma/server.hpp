#pragma once

#include <infiniband/verbs.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>

#include "rdma/exchange_msg.hpp"
#include "rdma/rdma.hpp"
#include "utils/debug.hpp"
#include "utils/defer.hpp"

namespace FarLib {
namespace rdma {

class Server {
private:
    Configure config;
    void *buffer;

    Context ctx;
    ProtectionDomain pd;
    MemoryRegion mr;
    CompleteChannel channel;
    CompleteQueue cq;

    // qps[0]: control qp
    // qps[1...]: data qp
    std::vector<QueuePair> qps;

public:
    Server(const Configure &config)
        : config(config),
          buffer(
              std::aligned_alloc(FarLib::PAGE_SIZE, config.server_buffer_size)),
          ctx(),
          pd(ctx),
          mr(pd, buffer, config.server_buffer_size),
          channel(ctx),
          cq(ctx, config, channel) {}

    Server(const Server &) = delete;

    ~Server() { std::free(buffer); }

    void *get(size_t offset) { return static_cast<char *>(buffer) + offset; }

    void start() {
        cq.req_notify(true);
        connect();
        std::cout << "server started" << std::endl;

        // wait for control message
        ibv_recv_wr recv_stop_wr = {
            .wr_id = RQ_STOP,
            .next = nullptr,
            .sg_list = nullptr,
            .num_sge = 0,
        };

        ibv_recv_wr *bad_wr;
        QueuePair &qp = qps[0];

        ibv_post_recv(qp.queue_pair, &recv_stop_wr, &bad_wr);
        channel.wait_cq_event();
        // construct order must be cq->qp
        while (true) {
            ibv_wc work_completion;
            int poll_ret = ibv_poll_cq(cq.complete_queue, 1, &work_completion);
            ASSERT(poll_ret <= 1);
            if (poll_ret == 1) {
                ASSERT(work_completion.wr_id == RQ_STOP);
                ASSERT(work_completion.status == IBV_WC_SUCCESS);
                std::cout << "server stopped" << std::endl;
                break;
            }
        }
        qps.clear();
    }

private:
    void connect() {
        srand48(time(nullptr));
        uint32_t psn = lrand48() & 0xffffff;
        ibv_port_attr port_attr;
        ibv_query_port(ctx.context, config.ib_port, &port_attr);

        int sock_fd;
        int conn_fd = tcp::listen_for_client(
            config.server_addr.c_str(), config.server_port.c_str(), sock_fd);
        DEFER({ CHECK_ERR(close(conn_fd)); });
        DEFER({ CHECK_ERR(close(sock_fd)); });

        ClientConnectionInfo client_info_head;
        ssize_t recv_size =
            recv(conn_fd, &client_info_head, sizeof(ClientConnectionInfo), 0);
        ASSERT(recv_size == sizeof(ClientConnectionInfo));

        size_t qp_count = client_info_head.qp_count;
        ASSERT(qp_count > 0);
        std::unique_ptr<uint32_t[]> client_qpn(new uint32_t[qp_count]);
        size_t qpn_size_in_byte = sizeof(uint32_t) * qp_count;
        recv_size = recv(conn_fd, client_qpn.get(), qpn_size_in_byte, 0);
        ASSERT(recv_size == qpn_size_in_byte);

        for (size_t i = 0; i < qp_count; i++) {
            qps.emplace_back(ctx, cq, pd, config);
        }

        config.qp_max_rd_atomic = client_info_head.max_rd_atomic;
        config.qp_mtu = client_info_head.mtu;
        ASSERT(config.server_buffer_size >=
               client_info_head.server_buffer_size);

        size_t server_info_size =
            sizeof(ServerConnectionInfo) + sizeof(uint32_t) * qp_count;
        auto server_info =
            static_cast<ServerConnectionInfo *>(std::malloc(server_info_size));
        DEFER({ std::free(server_info); });

        server_info->addr = reinterpret_cast<uint64_t>(buffer);
        server_info->lid = port_attr.lid;
        server_info->psn = psn;
        server_info->qp_count = qp_count;
        server_info->rkey = mr.memory_region->rkey;

        for (size_t i = 0; i < qp_count; i++) {
            server_info->qpn[i] = qps[i].queue_pair->qp_num;
        }
        ssize_t sent_size = send(conn_fd, server_info, server_info_size, 0);
        ASSERT(sent_size == server_info_size);

        // modify state to RTR
        for (size_t i = 0; i < qp_count; i++) {
            qps[i].ready_to_recv(client_info_head.lid, client_info_head.psn,
                                 client_qpn[i], config);
        }
    }
};

}  // namespace rdma
}  // namespace FarLib
