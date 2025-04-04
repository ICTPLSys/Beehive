#include "rdma/client.hpp"

#include <infiniband/verbs.h>

#include "rdma/exchange_msg.hpp"
#include "rdma/rdma.hpp"
#include "utils/debug.hpp"
#include "utils/defer.hpp"

namespace FarLib {
namespace rdma {

ClientControl::ClientControl(const Configure &config)
    : config(config),
      buffer(allocate_buffer(config.client_buffer_size)),
      ctx(),
      pd(ctx),
      mr(pd, buffer, config.client_buffer_size),
      control_cq(ctx, config),
      data_cq(ctx, config),
      control_qp(ctx, control_cq, pd, config) {
    INFO("initializing rdma client");

    size_t data_qp_count = config.max_thread_cnt * config.qp_count;
    size_t used_qp_count = data_qp_count + 1;

    // allocate queue pairs
    data_qps.reset(new QueuePair[data_qp_count]);
    for (size_t i = 0; i < data_qp_count; i++) {
        data_qps[i].init(ctx, data_cq, pd, config);
    }

    // prepare port attr & psn
    ibv_port_attr port_attr;
    ibv_query_port(ctx.context, config.ib_port, &port_attr);
    srand48(time(nullptr));
    uint32_t psn = lrand48() & 0xffffff;

    // allocate tcp message buffer
    size_t client_info_size =
        sizeof(ClientConnectionInfo) + sizeof(uint32_t) * used_qp_count;
    size_t server_info_size =
        sizeof(ServerConnectionInfo) + sizeof(uint32_t) * used_qp_count;
    auto client_info =
        static_cast<ClientConnectionInfo *>(std::malloc(client_info_size));
    DEFER({ std::free(client_info); });
    auto server_info =
        static_cast<ServerConnectionInfo *>(std::malloc(server_info_size));
    DEFER({ std::free(server_info); });

    // prepare client info
    client_info->psn = psn;
    client_info->lid = port_attr.lid;
    client_info->max_rd_atomic = config.qp_max_rd_atomic;
    client_info->mtu = config.qp_mtu;
    client_info->server_buffer_size = config.server_buffer_size;
    client_info->qp_count = used_qp_count;
    client_info->qpn[0] = control_qp.queue_pair->qp_num;
    for (size_t i = 0; i < data_qp_count; i++) {
        client_info->qpn[i + 1] = data_qps[i].queue_pair->qp_num;
    }

    // set up tcp connection & exchange message
    int conn_fd = tcp::connect_to_server(config.server_addr.c_str(),
                                         config.server_port.c_str());
    ssize_t sent_size = send(conn_fd, client_info, client_info_size, 0);
    ASSERT(sent_size == client_info_size);
    ssize_t recv_size = recv(conn_fd, server_info, server_info_size, 0);
    ASSERT(recv_size == server_info_size);
    close(conn_fd);
    ASSERT(server_info->qp_count == used_qp_count);

    // copy remote info
    this->remote_base_addr = server_info->addr;
    this->remote_key = server_info->rkey;

    // set up queue pairs
    control_qp.ready_to_recv(server_info->lid, server_info->psn,
                             server_info->qpn[0], config);
    control_qp.ready_to_send(psn, config);
    for (size_t i = 0; i < data_qp_count; i++) {
        data_qps[i].ready_to_recv(server_info->lid, server_info->psn,
                                  server_info->qpn[i + 1], config);
        data_qps[i].ready_to_send(psn, config);
    }

    // set up connection pool
    conn_pool.init(data_qps.get(), data_qp_count, config.qp_count);
}

void ClientControl::post_stop() {
    ibv_send_wr stop_wr = {
        .wr_id = RQ_STOP,
        .next = nullptr,
        .sg_list = nullptr,
        .num_sge = 0,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED | IBV_SEND_SOLICITED,
    };
    ibv_send_wr *bad_wr;
retry:
    int send_ret = ibv_post_send(control_qp.queue_pair, &stop_wr, &bad_wr);
    bool posted = check_ibv_post_send_ret(send_ret);
    ibv_wc wc;
    if (!posted) {
        ibv_poll_cq(control_cq.complete_queue, 1, &wc);
        goto retry;
    }
    while (true) {
        std::size_t n = ibv_poll_cq(control_cq.complete_queue, 1, &wc);
        ASSERT(n <= 1);
        if (n == 1) {
            if (wc.wr_id == stop_wr.wr_id && wc.opcode == IBV_WC_SEND) {
                assert(wc.status == IBV_WC_SUCCESS);
                break;
            }
        }
    }
}

void ConnectionPool::add_client(Client *client) {
    if (qp_count == 0 || qp_per_client == 0) {
        // connection pool not initialized yet
        client->init(nullptr, 0);
        return;
    }
    size_t qp_idx = alloc_idx.load();
retry:
    if (qp_idx + qp_per_client > qp_count) {
        ERROR("Can not allocate connection");
    }
    if (!alloc_idx.compare_exchange_weak(qp_idx, qp_idx + qp_per_client))
        goto retry;
    client->init(qps + qp_idx, qp_per_client);
}

ConnectionPool conn_pool;
std::unique_ptr<ClientControl> ClientControl::default_instance;
thread_local Client client;

}  // namespace rdma
}  // namespace FarLib