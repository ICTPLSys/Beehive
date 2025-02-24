/**
 * RAII style structures to initialize RDMA connection
 */

#pragma once
#include <infiniband/verbs.h>

#include <iostream>

#include "config.hpp"
#include "utils/debug.hpp"

namespace Beehive {

namespace rdma {
enum RequestType {
    RQ_STOP = 0,
};

struct ClientConnectionInfo {
    uint32_t psn;       // Packet Sequence Number
    uint16_t lid;       // Local IDentifier
    uint16_t qp_count;  // QP Count
    uint32_t qpn[0];    // QP Numbers
};

// The following info should be matched
// Some of them are constant or assumed to be the same at now
struct ServerConnectionInfo {
    // p_key
    // path mtu
    // max_rd_atomic & max_dest_rd_atomic
    uint32_t psn;       // Packet Sequence Number
    uint16_t lid;       // Local IDentifier
    uint16_t qp_count;  // QP Count
    uint32_t rkey;
    uint64_t addr;
    uint32_t qpn[0];  // QP Numbers
};

// use RAII to manage resources
struct Context {
    ibv_context *context;

    Context() {
        int num_devices;
        ibv_device **dev_list = ibv_get_device_list(&num_devices);
        ASSERT(dev_list && num_devices > 0);
        ibv_device *device = dev_list[0];
        ibv_free_device_list(dev_list);

        // open device
        context = ibv_open_device(device);
        ASSERT(context);
    }

    Context(const Context &) = delete;
    Context(Context &&) = delete;

    ~Context() { CHECK_ERR(ibv_close_device(context)); }
};

struct ProtectionDomain {
    ibv_pd *protection_domain;

    explicit ProtectionDomain(Context &ctx) {
        protection_domain = ibv_alloc_pd(ctx.context);
        ASSERT(protection_domain);
    }

    ProtectionDomain(const ProtectionDomain &) = delete;
    ProtectionDomain(ProtectionDomain &&) = delete;

    ~ProtectionDomain() { CHECK_ERR(ibv_dealloc_pd(protection_domain)); }
};

struct MemoryRegion {
    ibv_mr *memory_region;

    MemoryRegion(ProtectionDomain &pd, void *buffer, size_t buffer_size) {
        int access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE;
        ASSERT(buffer != nullptr);
        memory_region =
            ibv_reg_mr(pd.protection_domain, buffer, buffer_size, access_flags);
        ASSERT(memory_region);
    }

    MemoryRegion(const MemoryRegion &) = delete;
    MemoryRegion(MemoryRegion &&other) {
        memory_region = other.memory_region;
        other.memory_region = nullptr;
    }

    ~MemoryRegion() {
        if (memory_region != nullptr) {
            CHECK_ERR(ibv_dereg_mr(memory_region));
        }
    }
};

struct CompleteQueue {
    ibv_cq *complete_queue;

    CompleteQueue(Context &ctx, const Configure &config) {
        complete_queue =
            ibv_create_cq(ctx.context, config.cq_entries, nullptr, nullptr, 0);
        ASSERT(complete_queue);
    }

    ~CompleteQueue() { CHECK_ERR(ibv_destroy_cq(complete_queue)); }
};

struct QueuePair {
    ibv_qp *queue_pair;

    QueuePair() = default;

    QueuePair(Context &ctx, CompleteQueue &cq, ProtectionDomain &pd,
              const Configure &config) {
        init(ctx, cq, pd, config);
    }

    QueuePair(const QueuePair &) = delete;
    QueuePair(QueuePair &&other) {
        queue_pair = other.queue_pair;
        other.queue_pair = nullptr;
    }

    ~QueuePair() {
        if (queue_pair != nullptr) {
            CHECK_ERR(ibv_destroy_qp(queue_pair));
        }
    }

    void init(Context &ctx, CompleteQueue &cq, ProtectionDomain &pd,
              const Configure &config) {
        ibv_qp_cap qp_cap = {
            .max_send_wr = config.qp_send_cap,
            .max_recv_wr = config.qp_recv_cap,
            .max_send_sge = config.qp_max_send_sge,
            .max_recv_sge = config.qp_max_recv_sge,
            .max_inline_data = 0,
        };
        ibv_qp_init_attr qp_init_attr = {
            .qp_context = ctx.context,
            .send_cq = cq.complete_queue,
            .recv_cq = cq.complete_queue,
            .srq = nullptr,
            .cap = qp_cap,
            .qp_type = IBV_QPT_RC,
            .sq_sig_all =
                0,  // will NOT submit work completion for all requests
        };
        queue_pair = ibv_create_qp(pd.protection_domain, &qp_init_attr);
        int attr_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                        IBV_QP_ACCESS_FLAGS;
        ibv_qp_attr qp_attr = {
            .qp_state = IBV_QPS_INIT,
            .qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                               IBV_ACCESS_REMOTE_READ |
                               IBV_ACCESS_REMOTE_ATOMIC,
            .pkey_index = 0,
            .port_num = config.ib_port,
        };
        CHECK_ERR(ibv_modify_qp(queue_pair, &qp_attr, attr_mask));
    }

    void ready_to_recv(uint16_t lid, uint32_t psn, uint32_t qpn,
                       const Configure &config) {
        int attr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                        IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                        IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
        ibv_ah_attr ah_attr = {
            .dlid = lid,
            .sl = 0,  // service level
            .src_path_bits = 0,
            .is_global = 0,
            .port_num = config.ib_port,
        };
        ibv_qp_attr qp_attr = {
            .qp_state = IBV_QPS_RTR,
            .path_mtu = static_cast<ibv_mtu>(config.qp_mtu),
            .rq_psn = psn,
            .dest_qp_num = qpn,
            .ah_attr = ah_attr,
            .max_dest_rd_atomic = config.qp_max_dest_rd_atomic,
            .min_rnr_timer = config.qp_min_rnr_timer,
        };
        CHECK_ERR(ibv_modify_qp(queue_pair, &qp_attr, attr_mask));
    }

    void ready_to_send(uint32_t psn, const Configure &config) {
        int attr_mask = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                        IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                        IBV_QP_MAX_QP_RD_ATOMIC;
        ibv_qp_attr qp_attr = {
            .qp_state = IBV_QPS_RTS,
            .sq_psn = psn,
            .max_rd_atomic = config.qp_max_rd_atomic,
            .timeout = config.qp_timeout,
            .retry_cnt = config.qp_retry_cnt,
            .rnr_retry = config.qp_rnr_retry,
        };
        CHECK_ERR(ibv_modify_qp(queue_pair, &qp_attr, attr_mask));
    }
};

}  // namespace rdma

}  // namespace Beehive
