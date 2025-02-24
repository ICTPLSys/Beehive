#pragma once

#include <netdb.h>
#include <sys/mman.h>
#include <sys/socket.h>

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <unordered_set>
#include <vector>

#include "cache/region_based_allocator.hpp"
#include "cache/selection.hpp"
#include "config.hpp"
#include "exchange_msg.hpp"
#include "rdma.hpp"
#include "utils/debug.hpp"

namespace Beehive {

namespace rdma {

constexpr size_t CHECK_CQ_BATCH_SIZE = 16;

struct RDMAConnectionNode {
    RDMAConnectionNode *next;
    CompleteQueue &cq;
    QueuePair *qps_ptr;
    const size_t qp_cnt;
    size_t wsize;
    size_t rsize;

    RDMAConnectionNode(Context &ctx, CompleteQueue &cq, QueuePair *qps_ptr,
                       ProtectionDomain &pd, const Configure &config)
        : next(nullptr),
          cq(cq),
          qps_ptr(qps_ptr),
          qp_cnt(config.qp_count),
          wsize(0),
          rsize(0) {}

    inline QueuePair &get_queue_pair(size_t idx) const {
        assert(idx < qp_cnt);
        return qps_ptr[idx];
    }

    inline void record_write(size_t size) { wsize += size; }

    inline void record_read(size_t size) { rsize += size; }

    inline size_t get_rsize() const { return rsize; }

    inline size_t get_wsize() const { return wsize; }

    void clear_record() {
        rsize = 0;
        wsize = 0;
    }

    size_t get_qp_count() const { return qp_cnt; }
};
class ClientControl {
private:
    void *buffer;
    Context ctx;
    ProtectionDomain pd;
    CompleteQueue control_cq;
    CompleteQueue data_cq;
    QueuePair control_qp;
    std::unique_ptr<QueuePair[]> data_qps;
    MemoryRegion mr;
    ServerConnectionInfo *server_info;
    const Configure &config;
    std::unordered_set<RDMAConnectionNode *> nodes;

    std::atomic<RDMAConnectionNode *> conn_head;

    static std::unique_ptr<ClientControl> default_instance;

private:
    static bool check_ibv_post_send_ret(int ret) {
        switch (ret) {
        case 0:
            return true;
        case EINVAL:
            std::cerr << "ibv_post_send: Invalid value provided in wr"
                      << std::endl;
            break;
        case ENOMEM:
            return false;
        case EFAULT:
            std::cerr << "ibv_post_send: Invalid value provided in qp"
                      << std::endl;
            break;
        default:
            std::cerr << "ibv_post_send: failed, errno = " << errno
                      << std::endl;
            break;
        }
        abort();
    }

    void post_stop() {
        ibv_send_wr stop_wr = {
            .wr_id = RQ_STOP,
            .next = nullptr,
            .sg_list = nullptr,
            .num_sge = 0,
            .opcode = IBV_WR_SEND,
            .send_flags = IBV_SEND_SIGNALED,
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

    RDMAConnectionNode *alloc_connection() {
        RDMAConnectionNode *conn = conn_head.load();
        while (!conn_head.compare_exchange_weak(conn, conn->next)) {
        }
        conn->next = nullptr;
        return conn;
    }

    void dealloc_connection(RDMAConnectionNode *&conn) {
        RDMAConnectionNode *old_conn_head = conn_head.load();
        do {
            conn->next = old_conn_head;
        } while (!conn_head.compare_exchange_weak(old_conn_head, conn));
        conn = nullptr;
    }

    static size_t mmap_length(size_t size) {
        const size_t page_size = 1 << 21;
        const size_t alignment = std::max(allocator::RegionSize, page_size);
        return (size % alignment == 0) ? size
                                       : (size / alignment + 1) * alignment;
    }

    static void *allocate_buffer(size_t size) {
        void *ptr =
            mmap(nullptr, mmap_length(size), PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANON | MAP_HUGETLB | (21 << MAP_HUGE_SHIFT),
                 -1, 0);
        if (ptr == MAP_FAILED) {
            ERROR("mmap failed");
        }
        return ptr;
    }

    static void deallocate_buffer(void *ptr, size_t size) {
        CHECK_ERR(munmap(ptr, mmap_length(size)));
    }

public:
    ClientControl(const Configure &config)
        : config(config),
          buffer(allocate_buffer(config.client_buffer_size)),
          ctx(),
          pd(ctx),
          mr(pd, buffer, config.client_buffer_size),
          control_cq(ctx, config),
          data_cq(ctx, config),
          control_qp(ctx, control_cq, pd, config),
          data_qps(new QueuePair[config.max_thread_cnt * config.qp_count]()) {
        std::cout << "initializing rdma client" << std::endl;

        for (size_t i = 0; i < config.max_thread_cnt * config.qp_count; i++) {
            data_qps[i].init(ctx, data_cq, pd, config);
        }
        ibv_port_attr port_attr;
        ibv_query_port(ctx.context, config.ib_port, &port_attr);
        ASSERT(port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND);
        srand48(time(nullptr));
        uint32_t psn = lrand48() & 0xffffff;
        size_t used_qp_count = config.max_thread_cnt * config.qp_count + 1;
        size_t client_info_size =
            sizeof(ClientConnectionInfo) + sizeof(uint32_t) * used_qp_count;
        size_t server_info_size =
            sizeof(ServerConnectionInfo) + sizeof(uint32_t) * used_qp_count;
        auto client_info =
            static_cast<ClientConnectionInfo *>(std::malloc(client_info_size));
        server_info =
            static_cast<ServerConnectionInfo *>(std::malloc(server_info_size));
        client_info->psn = psn;
        client_info->lid = port_attr.lid;
        client_info->qp_count = used_qp_count;

        std::vector<RDMAConnectionNode *> conn_nodes;
        for (size_t i = 0; i < config.max_thread_cnt; i++) {
            conn_nodes.push_back(new RDMAConnectionNode(
                ctx, data_cq, data_qps.get() + i * config.qp_count, pd,
                config));
        }
        client_info->qpn[0] = control_qp.queue_pair->qp_num;
        {
            size_t idx = 1;
            for (RDMAConnectionNode *node : conn_nodes) {
                for (size_t i = 0; i < config.qp_count; i++) {
                    client_info->qpn[idx] =
                        node->get_queue_pair(i).queue_pair->qp_num;
                    idx++;
                }
            }
        }

        int conn_fd = tcp::connect_to_server(config.server_addr.c_str(),
                                             config.server_port.c_str());
        ssize_t sent_size = send(conn_fd, client_info, client_info_size, 0);
        ASSERT(sent_size == client_info_size);
        ssize_t recv_size =
            tcp::recieve_all(conn_fd, server_info, server_info_size, 0);
        ASSERT(recv_size == server_info_size);
        close(conn_fd);
        std::free(client_info);

        control_qp.ready_to_recv(server_info->lid, server_info->psn,
                                 server_info->qpn[0], config);
        control_qp.ready_to_send(psn, config);

        {
            size_t idx = 1;
            for (RDMAConnectionNode *node : conn_nodes) {
                for (size_t i = 0; i < config.qp_count; i++) {
                    node->get_queue_pair(i).ready_to_recv(
                        server_info->lid, server_info->psn,
                        server_info->qpn[idx], config);
                    node->get_queue_pair(i).ready_to_send(psn, config);
                    idx++;
                }
            }
        }
        // link all nodes to a list
        for (size_t i = 0; i < conn_nodes.size() - 1; i++) {
            conn_nodes[i]->next = conn_nodes[i + 1];
        }

        conn_head.store(conn_nodes[0]);

        // store all nodes in drill down set
        for (auto *node : conn_nodes) {
            nodes.insert(node);
        }
    }

    ~ClientControl() {
        std::cout << "destroying rdma client" << std::endl;
        post_stop();
        RDMAConnectionNode *head = conn_head.load();
        while (head != nullptr) {
            RDMAConnectionNode *node = head;
            head = head->next;
            delete node;
        }
        std::free(server_info);
        deallocate_buffer(buffer, config.client_buffer_size);
    }

    static ClientControl *init_default(const Configure &config) {
#ifndef NO_REMOTE
        default_instance.reset(new ClientControl(config));
#endif
        return default_instance.get();
    }

    static ClientControl *get_default() { return default_instance.get(); }

    static void destroy_default() { default_instance.reset(); }

    void *get_buffer() const { return buffer; }

    void check_event() {
        // only for debug
        std::cout << "check event" << std::endl;
        ibv_async_event event;
        int ret = ibv_get_async_event(ctx.context, &event);
        ibv_ack_async_event(&event);
        std::cout << event.event_type << std::endl;
    }

    void clear_record() {
        for (auto *node : nodes) {
            node->clear_record();
        }
    }

    size_t get_all_write_bytes() const {
        size_t wbytes = 0;
        for (auto *node : nodes) {
            wbytes += node->get_wsize();
        }
        return wbytes;
    }

    size_t get_all_read_bytes() const {
        size_t rbytes = 0;
        for (auto *node : nodes) {
            rbytes += node->get_rsize();
        }
        return rbytes;
    }
    friend class Client;
};

class Client;
extern thread_local Client client;

class Client {
private:
    size_t qp_idx;
    RDMAConnectionNode *conn;

public:
    Client() : qp_idx(0), conn(nullptr) { connect(); }

    ~Client() { disconnect(); }

    void connect() {
        if (conn == nullptr && ClientControl::get_default()) {
            conn = ClientControl::get_default()->alloc_connection();
        }
    }

    void disconnect() {
        if (conn) {
            if (ClientControl::get_default()) {
                ClientControl::get_default()->dealloc_connection(conn);
            } else {
                delete conn;
            }
#ifndef NDEBUG
            std::cout << "client disconnected" << std::endl;
#endif
        }
    }

    void *get_buffer() { return ClientControl::get_default()->get_buffer(); }

    static bool check_ibv_post_send_ret(int ret) {
        return ClientControl::check_ibv_post_send_ret(ret);
    }

    void build_send_wr(ibv_send_wr &wr, ibv_sge &sge, std::size_t remote_offset,
                       void *local_addr, uint32_t length, uint64_t wr_id,
                       bool signal = true,
                       ibv_wr_opcode opcode = IBV_WR_RDMA_READ) {
        MemoryRegion &mr = ClientControl::get_default()->mr;
        sge.addr = reinterpret_cast<uint64_t>(local_addr);
        sge.length = length;
        sge.lkey = mr.memory_region->lkey;
        wr.wr_id = wr_id;
        wr.next = nullptr;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = opcode;
        wr.send_flags = signal ? IBV_SEND_SIGNALED : 0;
        wr.wr.rdma = {
            .remote_addr =
                ClientControl::get_default()->server_info->addr + remote_offset,
            .rkey = ClientControl::get_default()->server_info->rkey};
    }

    // return true if success
    // return false if send queue if full
    // abort if other error occurs
    bool post_read(std::size_t remote_offset, void *local_addr, uint32_t length,
                   uint64_t wr_id, uint64_t obj_id) {
        ibv_sge read_sge;
        ibv_send_wr read_wr;
        ibv_send_wr *bad_wr;
        build_send_wr(read_wr, read_sge, remote_offset, local_addr, length,
                      wr_id, true, IBV_WR_RDMA_READ);
        conn->record_read(length);
        size_t qpid = (remote_offset >> 20) % conn->get_qp_count();
        int send_ret = ibv_post_send(conn->get_queue_pair(qpid).queue_pair,
                                     &read_wr, &bad_wr);
        qp_idx = (qp_idx + 1 >= conn->qp_cnt) ? 0 : qp_idx + 1;
        bool ret = check_ibv_post_send_ret(send_ret);
        return ret;
    }

    // return true if success
    // return false if send queue if full
    // abort if other error occurs
    bool post_write(std::size_t remote_offset, void *local_addr,
                    uint32_t length, uint64_t wr_id, uint64_t obj_id) {
        ibv_sge write_sge;
        ibv_send_wr write_wr;
        ibv_send_wr *bad_wr;
        build_send_wr(write_wr, write_sge, remote_offset, local_addr, length,
                      wr_id, true, IBV_WR_RDMA_WRITE);
        conn->record_write(length);
        size_t qpid = (remote_offset >> 20) % conn->get_qp_count();
        int send_ret = ibv_post_send(conn->get_queue_pair(qpid).queue_pair,
                                     &write_wr, &bad_wr);
        qp_idx = (qp_idx + 1 >= conn->qp_cnt) ? 0 : qp_idx + 1;
        bool ret = check_ibv_post_send_ret(send_ret);
        return ret;
    }

    // abort if other error occurs
    bool post_writes(ibv_send_wr *write_wr, ibv_send_wr **bad_wr) {
        int send_ret = ibv_post_send(conn->get_queue_pair(qp_idx).queue_pair,
                                     write_wr, bad_wr);
        qp_idx = (qp_idx + 1 >= conn->qp_cnt) ? 0 : qp_idx + 1;
        return check_ibv_post_send_ret(send_ret);
    }

    size_t check_cq(ibv_wc *wc, size_t wc_length) {
        return ibv_poll_cq(conn->cq.complete_queue, wc_length, wc);
    }

    static Client *get_default() { return &client; }
};
}  // namespace rdma

}  // namespace Beehive