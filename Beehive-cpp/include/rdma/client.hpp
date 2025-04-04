#pragma once

#include <netdb.h>
#include <sys/mman.h>
#include <sys/socket.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>

#include "cache/region_based_allocator.hpp"
#include "config.hpp"
#include "rdma.hpp"
#include "utils/debug.hpp"

namespace FarLib {

namespace rdma {

constexpr size_t CHECK_CQ_BATCH_SIZE = 16;

class Client;

// For now it is an arena allocator
class ConnectionPool {
private:
    struct Node {
        Node *next;
        size_t qp_idx;
    };

private:
    QueuePair *qps;
    std::atomic_size_t alloc_idx;
    size_t qp_count;
    size_t qp_per_client;

public:
    ConnectionPool()
        : qps(nullptr), alloc_idx(0), qp_count(0), qp_per_client(0) {}
    void init(QueuePair *qps, size_t qp_count, size_t qp_per_client) {
        this->qps = qps;
        this->alloc_idx = 0;
        this->qp_count = qp_count;
        this->qp_per_client = qp_per_client;
    }
    void add_client(Client *client);
};

extern ConnectionPool conn_pool;

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
    uint64_t remote_base_addr;
    uint32_t remote_key;
    const Configure &config;

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

    void post_stop();

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
    ClientControl(const Configure &config);

    ~ClientControl() {
        INFO("destroying rdma client");
        post_stop();
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

    // DEBUG ONLY
    void check_event() {
        // only for debug
        std::cout << "check event" << std::endl;
        ibv_async_event event;
        int ret = ibv_get_async_event(ctx.context, &event);
        ibv_ack_async_event(&event);
        std::cout << event.event_type << std::endl;
    }

    friend class Client;
};

extern thread_local Client client;

class Client {
private:
    QueuePair *qps;
    size_t qp_count;
    size_t qp_idx;

public:
    // TODO: can we remove this TLS variable constructor?
    Client() : qps(nullptr) { conn_pool.add_client(this); }
    ~Client() {}
    void init() {
        if (qps == nullptr) conn_pool.add_client(this);
    }
    void init(QueuePair *qps, size_t qp_count) {
        if (this->qps != nullptr) {
            ERROR("client double init");
        }
        this->qps = qps;
        this->qp_count = qp_count;
        this->qp_idx = 0;
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
                ClientControl::get_default()->remote_base_addr + remote_offset,
            .rkey = ClientControl::get_default()->remote_key};
    }

    void update_qp_idx() { qp_idx = (qp_idx + 1 >= qp_count) ? 0 : qp_idx + 1; }

    // return true if success
    // return false if send queue if full
    // abort if other error occurs
    template <bool Signal = true>
    bool post_read(std::size_t remote_offset, void *local_addr, uint32_t length,
                   uint64_t wr_id, uint64_t obj_id) {
        ibv_sge read_sge;
        ibv_send_wr read_wr;
        ibv_send_wr *bad_wr;
        build_send_wr(read_wr, read_sge, remote_offset, local_addr, length,
                      wr_id, Signal, IBV_WR_RDMA_READ);
        int send_ret = ibv_post_send(qps[qp_idx].queue_pair, &read_wr, &bad_wr);
        update_qp_idx();
        bool ret = check_ibv_post_send_ret(send_ret);
        return ret;
    }

    // return true if success
    // return false if send queue if full
    // abort if other error occurs
    template <bool Signal = true>
    bool post_write(std::size_t remote_offset, void *local_addr,
                    uint32_t length, uint64_t wr_id, uint64_t obj_id) {
        ibv_sge write_sge;
        ibv_send_wr write_wr;
        ibv_send_wr *bad_wr;
        build_send_wr(write_wr, write_sge, remote_offset, local_addr, length,
                      wr_id, Signal, IBV_WR_RDMA_WRITE);
        int send_ret =
            ibv_post_send(qps[qp_idx].queue_pair, &write_wr, &bad_wr);
        update_qp_idx();
        bool ret = check_ibv_post_send_ret(send_ret);
        return ret;
    }

    // return true if success
    // return false if send queue if full
    // abort if other error occurs
    bool post_writes(ibv_send_wr *write_wr, ibv_send_wr **bad_wr) {
        int send_ret = ibv_post_send(qps[qp_idx].queue_pair, write_wr, bad_wr);
        update_qp_idx();
        return check_ibv_post_send_ret(send_ret);
    }

    static size_t check_cq(ibv_wc *wc, size_t wc_length) {
        ibv_cq *cq = ClientControl::get_default()->data_cq.complete_queue;
        return ibv_poll_cq(cq, wc_length, wc);
    }

    static Client *get_default() { return &client; }
};
}  // namespace rdma

}  // namespace FarLib